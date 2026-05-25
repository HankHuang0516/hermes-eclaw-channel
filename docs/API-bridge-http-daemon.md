# Bridge → Hermes HTTP Daemon API

> Prototype for option B (out-of-process daemon) chosen on card_52bd51bb (2026-04-28).
> Spec context: `docs/SPEC-bridge-refactor.md` (PR #2).

## 0. Why this exists

`plugin/eclaw_bridge.py::ask_hermes()` currently spawns a fresh `hermes chat -q
… --continue` subprocess on every inbound message. That gives us cold-start cost
(roughly 5–8 s per call), forces `_hermes_lock` to serialise the entire bridge,
and blows the per-request timeout budget when Hermes itself runs slow.

This daemon owns a long-lived Hermes worker, exposes it over HTTP+SSE, and lets
the bridge degrade back to subprocess-spawn when the daemon is unreachable.

## 1. Topology

```
┌──────────────┐   webhook   ┌───────────────┐   POST /chat   ┌──────────────┐
│  EClaw API   │ ─────────▶  │ eclaw_bridge  │ ─────────────▶ │ hermes_daemon│
│              │             │ (aiohttp:8644)│  SSE stream    │ (aiohttp:8645│
└──────────────┘ ◀── reply ──└───────────────┘ ◀───────────── │  in Hermes   │
                                                              │  container)  │
                                                              └──────────────┘
                                                                     │
                                                       spawn / pipe  ▼
                                                          ┌────────────────┐
                                                          │ hermes CLI     │
                                                          │  (--continue)  │
                                                          └────────────────┘
```

The daemon co-locates with the Hermes Python venv (same container, same `~/.hermes`
directory). Bridge talks to it over loopback or a private network; no public
exposure.

## 2. Endpoints

### `POST /chat`

Request a chat completion. Default response: `text/event-stream` (SSE). Clients
that prefer aggregation can pass `Accept: application/json` to get a single
`{ ok, reply, duration_ms }` body once the worker is done.

**Request body** (JSON, `Content-Type: application/json`):

| field          | type    | required | notes                                                  |
|----------------|---------|----------|--------------------------------------------------------|
| `prompt`       | string  | yes      | The bridge sends the result of `_strip_eclaw_context`. |
| `silent_mode`  | bool    | no       | If true, daemon honours `[SILENT]` token (default true).|
| `session_hint` | string  | no       | Reserved for future multi-session routing. Ignored in v0; daemon uses `--continue` (single session). |
| `request_id`   | string  | yes      | Client-supplied UUID; echoed in every event for log correlation. |

**SSE event types** (one per `data:` line, `event:` selects the type):

```
event: started
data: {"request_id":"<uuid>","ts":1730000000.123}

event: progress
data: {"request_id":"<uuid>","chunk":"…partial token / tool-call preview…"}

event: silent
data: {"request_id":"<uuid>"}            # short-circuit: hermes returned [SILENT]

event: done
data: {"request_id":"<uuid>","reply":"<final extracted body>","duration_ms":4321}

event: error
data: {"request_id":"<uuid>","kind":"timeout|spawn_failed|hermes_exit|bad_request","detail":"<short>"}
```

The bridge MUST treat `silent` and `done` as terminal-success and `error` as
terminal-failure. The stream closes after any terminal event.

**JSON-mode response** (`Accept: application/json`):

```json
{
  "ok": true,
  "reply": "<final body, empty if silent>",
  "silent": false,
  "duration_ms": 4321
}
```

…or:

```json
{ "ok": false, "error": { "kind": "timeout", "detail": "idle for 60s" } }
// or, if the wall-clock backstop fires instead of idle:
{ "ok": false, "error": { "kind": "timeout", "detail": "wall-clock 900s" } }
```

### `GET /health`

```
200 OK
{ "status": "ok", "service": "hermes-daemon", "uptime_s": 1234,
  "hermes_version": "0.x.y", "in_flight": 1, "queue_depth": 0,
  "queue_max": 8, "calls_total": 42,
  "resume_auto_disabled": false, "no_resume_env": false,
  "idle_timeout_s": 60, "wall_timeout_s": 900 }
```

`in_flight` + `queue_depth` + `queue_max` let the bridge surface back-pressure
(e.g. log a warning when queue grows past N). Phase H1 added the worker-state
fields so the autoheal sidecar / bridge can detect degraded mode without
exec'ing into the container:

- `calls_total` — monotonic counter; jumps + queue_depth=0 + in_flight=0
  means the daemon is healthy and processing.
- `resume_auto_disabled` — `true` once consecutive `--continue` calls cross
  `MAX_CONSECUTIVE_TIMEOUTS` timeouts (NousResearch issue #7536 mitigation).
  Stays on for the daemon's lifetime; restart re-evaluates.
- `no_resume_env` — reflects `HERMES_NO_RESUME` env (manual kill switch).
- `idle_timeout_s` / `wall_timeout_s` — the two deadlines (idle is primary,
  wall is backstop).

### `GET /version`

```
{ "service":"hermes-daemon", "version":"0.1.0", "git_sha":"<short>",
  "started_at":"2026-04-28T08:00:00Z" }
```

## 3. Auth

Shared bearer token: `Authorization: Bearer <HERMES_DAEMON_TOKEN>`.

- Token lives in the daemon's env (`HERMES_DAEMON_TOKEN`) and the bridge's env
  (same name).
- Daemon binds to `127.0.0.1` by default; override with `HERMES_DAEMON_BIND` if
  bridge runs in a separate container on the same private network.
- No per-user auth; the daemon trusts every authenticated caller. Production
  multi-tenant will require a session-routing layer — out of scope here.

## 4. Error model

| `kind`         | HTTP (JSON-mode) | When                                                             | Bridge action                      |
|----------------|------------------|------------------------------------------------------------------|------------------------------------|
| `bad_request`  | 400              | Missing `prompt` / `request_id`, malformed JSON.                | Log + drop (don't retry).          |
| `unauthorized` | 401              | Bearer mismatch.                                                | Log + escalate (config bug).       |
| `busy`         | 503              | Queue depth exceeds `HERMES_DAEMON_QUEUE_MAX` (default 8). Phase H1: was 429 — switched to 503 per vLLM RFC #18826 backpressure (429 implies "retry now" which sustains overload). | Exponential backoff retry; if still busy, return a busy message without bypassing the queue. |
| `timeout`      | 504              | Phase H1: idle-activity (no stdout chunk for `HERMES_IDLE_TIMEOUT_SECS`, default 60) **or** wall-clock (`HERMES_DAEMON_CHAT_TIMEOUT_SECS`, default 900). `detail` is `idle for Ns` or `wall-clock Ns`. | Return `[Hermes 回應超時]` to user. |
| `spawn_failed` | 503              | Worker process couldn't start.                                   | Fall back to subprocess.           |
| `hermes_exit`  | 502              | Worker exited non-zero.                                          | Return `[Hermes 回覆失敗 — 請查 log]`. |

In SSE mode the same `kind` values appear inside the `error` event payload; the
HTTP status is always 200 once the stream is open.

## 5. Backward-compatibility shim

`plugin/eclaw_bridge.py::ask_hermes()` becomes:

```python
async def ask_hermes(prompt: str) -> str:
    if HERMES_DAEMON_URL:
        try:
            return await _ask_hermes_via_daemon(prompt)
        except (ClientConnectorError, asyncio.TimeoutError, _DaemonUnavailable):
            log.warning("[hermes] daemon unreachable, falling back to subprocess")
            # fall through to legacy path
    return await _ask_hermes_subprocess(prompt)
```

Rules for the shim:

1. **Daemon disabled by default** in v0 — bridge keeps spawning subprocesses
   until `HERMES_DAEMON_URL` is set in env. This means the existing prod
   deployment is untouched after merge; you opt in per-container.
2. **Fallback only triggers on connection-level failures** (`ECONNREFUSED`,
   socket timeout opening the stream, daemon `503/spawn_failed`). A clean
   `error/timeout` from the daemon is NOT a fallback trigger — Hermes itself
   was slow; subprocess will be slow too.
3. **No prompt mutation** between paths. Both call `_strip_eclaw_context()`
   first; daemon receives the cleaned prompt; subprocess receives the same.
4. **Output equivalence**: daemon returns the same body the bridge currently
   gets out of `_extract_hermes_reply()`. The daemon owns the envelope-stripping
   regexes — the bridge does not parse Hermes output in daemon-mode.
5. **Shared fallback guardrails.** The direct subprocess fallback calls
   `hermes_worker.run_subprocess_chat()`, not its own `proc.communicate()`
   implementation, so daemon-down mode keeps the H1 idle-timeout,
   no-resume fuse, bash-sandbox preamble, and timeout diagnostics.

## 6. Invariants the daemon MUST preserve

These come from `SPEC-bridge-refactor.md §1` (current bridge contract). The
daemon implementation cannot break any of them:

1. **Session continuity is best-effort, not guaranteed.** v0 used `--continue`
   unconditionally. **Phase H1**: `--continue` drops on `HERMES_NO_RESUME=1`
   *or* after consecutive `--continue` calls cross the timeout threshold
   (issue #7536 mitigation).
   Bridge MUST NOT depend on Hermes remembering prior context across daemon
   restarts. `_hermes_lock` still serialises all CLI spawns from the daemon.
2. **`[SILENT]` short-circuit.** If the extracted reply contains the literal
   `[SILENT]`, daemon emits an `event: silent` and the bridge skips delivery.
3. **`_extract_hermes_reply` envelope-stripping.** The daemon imports the same
   helper (or a copy) from `plugin/eclaw_bridge.py` so head/tail markers stay
   in lock-step. Tests for both must share fixtures.
4. **Two timeouts, idle is primary.** `HERMES_IDLE_TIMEOUT_SECS` (default 60)
   kills the subprocess when no stdout chunk has arrived for that long;
   `HERMES_DAEMON_CHAT_TIMEOUT_SECS` (default 900) is the wall-clock backstop.
   v0 only had wall-clock; Phase H1 added idle-activity per issue #4815.
5. **No prompt prefixing.** Daemon does NOT inject sender labels — that's the
   bridge's job (see `process_message` for `entity_message`/`broadcast`).

## 7. Out of scope for this branch

- Multi-session routing (one Hermes session per EClaw conversation).
- Memory snapshots / rotation when sessions get too large.
- Auth scoping per-bot.
- Container packaging (docker-compose). Hank's call: bare systemd/pm2 first,
  containerise only after stability is proven.
- Streaming the SSE all the way back to EClaw chat (today's bridge buffers the
  whole reply before sending; we keep that).

## 8. Files this branch will add

```
daemon/
  hermes_daemon.py          # aiohttp app, /chat /health /version
  hermes_worker.py          # subprocess wrapper, lock, [SILENT] detection
  __init__.py
plugin/
  eclaw_bridge.py           # shim: HERMES_DAEMON_URL → daemon, else subprocess
docs/
  API-bridge-http-daemon.md # this doc
tests/
  test_daemon_api.py        # contract tests against a running daemon
  test_bridge_shim.py       # mocks daemon-up / daemon-down paths
scripts/
  run-daemon.sh             # local dev launcher
```

## 8b. Phase H1 environment variables

Added 2026-04-28 in response to PR #2201 same-day recurrence. All optional —
sensible defaults match the v0 contract.

| Var | Default | Purpose |
|-----|---------|---------|
| `HERMES_IDLE_TIMEOUT_SECS` | `60` | Kill subprocess if no stdout chunk arrives for this many seconds. Primary deadline. |
| `HERMES_DAEMON_CHAT_TIMEOUT_SECS` | `900` | Wall-clock backstop. Was the only deadline in v0. |
| `HERMES_NO_RESUME` | `""` (off) | When `1`/`true`/`yes`, daemon never passes `--continue` to hermes. Manual kill switch for stuck sessions. Auto-engaged for the daemon's lifetime once consecutive `--continue` timeouts cross `MAX_CONSECUTIVE_TIMEOUTS`. |
| `HERMES_DAEMON_QUEUE_MAX` | `8` | When queue depth ≥ this, return `503 busy` (was `429` in v0). |
| `HERMES_STALE_WEBHOOK_THRESHOLD_S` | `300` | Bridge drops EClaw webhook deliveries whose `timestamp` is older than this many seconds; mitigates startup-flood-of-stale-messages re-triggering the same wedge. |
| `HERMES_DAEMON_BUSY_RETRIES` | `3` | Bridge retries daemon `busy` responses this many times before returning `[Hermes 忙碌中 — 請稍後再試]`. |
| `HERMES_DAEMON_BUSY_BACKOFF_BASE_S` | `1` | First daemon busy retry delay; later retries double up to the max. |
| `HERMES_DAEMON_BUSY_BACKOFF_MAX_S` | `10` | Cap for daemon busy exponential backoff. |
| `HERMES_ECLAW_API_RATE_LIMIT_PER_MIN` | `30` | Local bridge limiter for EClaw delivery endpoints (`/api/transform` or `/api/channel/message`). Set `0` to disable. |
| `HERMES_ECLAW_API_BACKOFF_RETRIES` | `3` | Retries EClaw delivery responses with HTTP `429` or `503`, honoring `Retry-After` when present. |
| `HERMES_ECLAW_API_BACKOFF_BASE_S` | `1` | First EClaw delivery retry delay; later retries double up to the max. |
| `HERMES_ECLAW_API_BACKOFF_MAX_S` | `30` | Cap for EClaw delivery exponential backoff and `Retry-After` waits. |

## 8c. Phase H2 health-check cron

`scripts/hermes-healthcheck-cron.py` is the first Operational Maturity probe:
run it every 6h from cron or the `hermes-healthcheck` compose service. It
checks daemon `/health`, runs a git push probe, records JSONL probe events for
SLA rollups, and alerts the commander after repeated failures.

| Var | Default | Purpose |
|-----|---------|---------|
| `HERMES_HEALTH_INTERVAL_SECS` | `21600` | Compose loop sleep; 6h target. |
| `HERMES_HEALTH_DAEMON_URL` | `http://127.0.0.1:8645/health` | Daemon endpoint to probe. |
| `HERMES_HEALTH_DAEMON_TIMEOUT_S` | `10` | Daemon probe HTTP timeout. |
| `HERMES_HEALTH_REPO_URL` | `HERMES_PR_REPO_URL` or EClaw repo URL | Remote used for git push probe. |
| `HERMES_HEALTH_BRANCH` | `hermes-healthcheck` | Dedicated probe branch. |
| `HERMES_HEALTH_PUSH_MODE` | `dry-run` | `dry-run` verifies auth/push path without writes; `write` pushes the probe commit. |
| `HERMES_HEALTH_ALERT_AFTER_FAILURES` | `3` | Consecutive failures before commander alert. |
| `HERMES_HEALTH_COMMANDER_TARGET` | `2` | EClaw `speakTo` target for alerts. |
| `HERMES_HEALTH_STATE_FILE` | `~/.hermes/eclaw-healthcheck-state.json` | Persistent failure streak state. |
| `HERMES_HEALTH_EVENTS_FILE` | `~/.hermes/eclaw-healthcheck-events.jsonl` | Append-only SLA probe event log. |
| `HERMES_HEALTH_SLA_WINDOW_SECS` | `604800` | Rolling SLA window for state-file uptime summary. |
| `HERMES_HEALTH_SLA_TARGET_PCT` | `99.0` | Target uptime percentage for `state.sla.within_sla`. |

## 9. Open questions for review

- **Worker concurrency.** v0 = 1 worker, lock-serialised. If we bump that, we
  need per-worker session dirs (`~/.hermes/sessions-<n>`) — which breaks
  `--continue` continuity. Defer.
- **SSE keepalive.** EClaw inbound timeout is generous; bridge → daemon path
  is loopback, so no proxy idle-cut. We probably don't need pings for v0.
- **Metrics.** `/health` exposes counters; do we want Prometheus? Probably
  yes eventually — out of scope here.
