# Bridge refactor — spec & A/B evaluation

Companion to kanban `card_52bd51bb`. Captures the "first step" deliverables:
locate the bridge code, evaluate Hermes server-mode capability, and compare
two refactor candidates.

## 1. Current state (as of 2026-04-28)

**File**: `plugin/eclaw_bridge.py` (308 lines, single-file aiohttp app on port 8644).

**Hot path**: `ask_hermes()` at L169.

```python
HERMES_TIMEOUT = int(os.environ.get("HERMES_TIMEOUT_SECS", "900"))
_hermes_lock = asyncio.Lock()  # serialise Hermes calls

async def ask_hermes(prompt: str) -> str:
    args = [
        "/home/node/hermes-agent/.venv/bin/hermes",
        "chat", "-q", clean, "--continue",
    ]
    async with _hermes_lock:
        proc = await asyncio.create_subprocess_exec(
            *args, cwd="/home/node/hermes-agent",
            env={**os.environ, "PATH": "/home/node/.local/bin:" + os.environ.get("PATH", "")},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=HERMES_TIMEOUT)
```

**Key facts the refactor must preserve**:

- `--continue` reuses the most recent session so memory carries across spawns.
- Calls are serialised with `_hermes_lock` because concurrent `hermes chat`
  spawns corrupt `~/.hermes/sessions/*.jsonl`.
- Stdout passes through `_extract_hermes_reply()` to strip the verbose CLI's
  banner box, query echo, and session footer.
- `[SILENT]` token short-circuits the reply path.

**Short-term patches already on main**:

| Patch | Status | Source |
|---|---|---|
| `HERMES_TIMEOUT_SECS` env var, default 900 | ✅ live | L124 |
| Drop `-Q`, keep `-q` (so tool previews leak through) | ✅ live | L189 |

(Self-check / auto-wake / hermes-trace patches live on the *channel* bridge
side — `claude-code-eclaw-channel` and EClaw repos respectively — not in
this repo.)

## 2. Hermes server-mode survey

Searched `openclaw-docker/project-b/hermes-agent/` for any built-in HTTP /
daemon mode the bridge could call into.

**What exists**:

- `hermes_cli/gateway.py` — systemd / launchd service installer for the
  Hermes process itself. Does *not* expose an HTTP API; it manages the
  lifecycle of long-running Hermes processes (cron jobs, webhook
  subscriptions, persistent skill workers). 1100+ lines but every function
  is process supervision (kill/restart/install/probe), not RPC.
- `hermes webhook subscribe` — Hermes can *receive* webhooks (GitHub events,
  custom endpoints) and run an agent prompt on the payload. This is the
  reverse direction: external thing → Hermes. The bridge needs the opposite
  (bridge → Hermes → reply).
- `hermes chat` — the only "talk to the agent" entry point, and it's
  optimised for interactive TTY. There's a non-interactive `-q / -Q` quiet
  mode (which the bridge uses), but no `hermes serve` / `hermes daemon` /
  `hermes api` subcommand.

**Conclusion**: Hermes does not ship an HTTP API. Any `bridge → HTTP →
Hermes` path requires us to *build* the Hermes-side server, not just wire
into one.

## 3. Candidate comparison

### Option A — Persistent Python worker (in-process)

Long-lived Python process imports `hermes_cli.main` and calls into the
agent loop directly. The bridge gets a function call instead of a
subprocess spawn.

**Pros**:

- Zero cold-start (no `uv run hermes` overhead, no Python interpreter
  reinit, no model client re-handshake).
- Direct access to streaming events — bridge can forward intermediate tool
  calls / token deltas to EClaw chat without screen-scraping the verbose
  CLI envelope.
- No timeout cliff: the worker's event loop is owned by us, so we can
  cooperatively yield progress for arbitrarily long tasks.
- `--continue` becomes a no-op: session state lives in worker memory.

**Cons**:

- Tightly couples the bridge to `hermes_cli` internals. Any Hermes upgrade
  that refactors the agent entry point breaks us.
- Crash isolation is gone — a bad agent run can OOM/segfault and take the
  whole bridge with it. Need a supervisor that restarts the worker.
- We have to teach the worker about Hermes' session-file invariants
  (lock, append, fsync) ourselves; today the CLI does it for us.
- Memory footprint goes up (model client kept hot, embedding cache, MCP
  connections all live continuously).

### Option B — HTTP API daemon (out-of-process)

Stand up a thin Python server inside the Hermes container that wraps
`hermes_cli` behind a single `POST /chat {prompt} → SSE stream / JSON`
endpoint. Bridge calls it via `aiohttp.ClientSession.post()`.

**Pros**:

- Crash isolation: bridge stays up even if the daemon dies. Standard HTTP
  retry / circuit-breaker patterns apply.
- The daemon is the right place for connection pooling: model client,
  embedding cache, MCP sockets all reused across prompts.
- SSE / chunked-transfer makes streaming mid-task progress trivial.
- Clean upgrade path — Hermes one day ships its own server, we swap the URL.

**Cons**:

- Two deployment artefacts instead of one (bridge container + daemon
  container, or a multi-process container). More moving parts.
- We're still calling `hermes_cli` internals — the daemon is mostly a
  thin RPC wrapper. So we don't escape the upstream-coupling risk;
  we just move it across a process boundary.
- HTTP adds ~1ms per call. Negligible vs the fixes it enables.

### Option C — Stdin-pumped persistent CLI

Spawn `hermes chat` once with `--stdin`-style multi-turn input, keep the
process alive, pipe each prompt in / read each response out. Essentially A
without importing hermes_cli internals.

**Pros**:

- Lowest delta from current code (still `subprocess`-shaped).
- Zero coupling to Hermes internals beyond the existing CLI contract.
- Survives Hermes upgrades unless they break interactive REPL stdin.

**Cons**:

- Hermes' interactive REPL was *not* designed to be driven by a robot.
  Turn-boundary detection means parsing the verbose envelope on every
  reply (we already do this for one-shot). With multi-turn output,
  turn-boundary regex becomes brittle.
- Can't do streaming progress without inventing our own framing protocol.
- Single process = single failure mode = same crash-takes-everything risk
  as A.

## 4. Recommendation

**Ship B (HTTP API daemon). Park A as a "later if B's coupling pain
materialises" fallback. Skip C — it has all of A's risks and none of its
upside.**

Reasoning:

- The thing the card actually needs to fix — long tasks dying at the
  timeout cliff and no mid-task progress signal — is a streaming problem,
  not a cold-start problem. SSE solves it cleanly; pipe-pumping doesn't.
- B's main cost (extra deployment artefact) is paid once; A's main cost
  (coupling-to-internals) is paid on every Hermes upgrade.
- B is the only option that scales to multi-tenant (multiple bridge
  instances, one Hermes pool) without rewriting later.

## 5. Out-of-scope for this spec

- **Concrete API surface** (request/response schema, auth, error model)
  — separate PR after Hank signs off the direction.
- **Session persistence under daemon mode** — needs to be designed once
  we know if multiple concurrent prompts share a session or each gets its
  own.
- **Backward-compat shim** — for the rollout window we'd want the bridge
  to fall back to subprocess if the daemon is unreachable. Spec for that
  comes with the prototype branch.

## 6. Decisions needed from Hank

1. Approve direction B, or pick a different one.
2. Daemon containerisation: separate container next to the bridge, or
   colocate into the existing `hermes-eclaw-bridge` container?
3. Streaming format: SSE (browser-friendly, simpler) or NDJSON over
   chunked-transfer (smaller payload, easier to parse from Python)?

Card: `card_52bd51bb`
