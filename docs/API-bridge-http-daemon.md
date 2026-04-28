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
{ "ok": false, "error": { "kind": "timeout", "detail": "after 900s" } }
```

### `GET /health`

```
200 OK
{ "status": "ok", "service": "hermes-daemon", "uptime_s": 1234,
  "hermes_version": "0.x.y", "in_flight": 1, "queue_depth": 0 }
```

`in_flight` + `queue_depth` let the bridge surface back-pressure (e.g. log a
warning when queue grows past N).

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
| `busy`         | 429              | Queue depth exceeds `HERMES_DAEMON_QUEUE_MAX` (default 8).      | Fall back to subprocess.           |
| `timeout`      | 504              | Hermes worker exceeded `HERMES_DAEMON_CHAT_TIMEOUT_SECS` (900). | Return `[Hermes 回應超時]` to user. |
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

## 6. Invariants the daemon MUST preserve

These come from `SPEC-bridge-refactor.md §1` (current bridge contract). The
daemon implementation cannot break any of them:

1. **Single-session `--continue` semantics.** v0 uses one Hermes session for
   the whole daemon process. `_hermes_lock` is internal to the daemon and gates
   the worker; from the bridge's perspective concurrent `POST /chat` calls just
   queue.
2. **`[SILENT]` short-circuit.** If the extracted reply contains the literal
   `[SILENT]`, daemon emits an `event: silent` and the bridge skips delivery.
3. **`_extract_hermes_reply` envelope-stripping.** The daemon imports the same
   helper (or a copy) from `plugin/eclaw_bridge.py` so head/tail markers stay
   in lock-step. Tests for both must share fixtures.
4. **Timeout budget = 900 s** by default, env-overridable. Same default as the
   current bridge.
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

## 9. Open questions for review

- **Worker concurrency.** v0 = 1 worker, lock-serialised. If we bump that, we
  need per-worker session dirs (`~/.hermes/sessions-<n>`) — which breaks
  `--continue` continuity. Defer.
- **SSE keepalive.** EClaw inbound timeout is generous; bridge → daemon path
  is loopback, so no proxy idle-cut. We probably don't need pings for v0.
- **Metrics.** `/health` exposes counters; do we want Prometheus? Probably
  yes eventually — out of scope here.
