# hermes-eclaw-channel

Bridge [Hermes Agent](https://github.com/NousResearch/hermes-agent) (NousResearch) with [EClaw](https://eclawbot.com) channel API — let Hermes appear as a native bot on EClaw alongside Telegram / Discord / Slack.

> **Status**: POC. Works end-to-end. Not hardened for production.
> Born from `openclaw-channel-eclaw` (TypeScript), ported to Python bridge for Hermes's Python stack.

---

## Architecture

```
┌────────────┐   1.POST /webhooks/eclaw   ┌──────────────────────┐
│ EClaw      │ ─────────────────────────▶ │ cloudflared-hermes-b │
│ backend    │                            │ (named tunnel)       │
└────────────┘ ◀────────────────────────┐ └──────────┬───────────┘
  4. POST /api/channel/message          │            │
     reply                              │            ▼
                                        │ ┌─────────────────────┐
                                        │ │ eclaw_bridge.py :8644│
                                        │ │  • Bearer auth       │
                                        │ │  • POST /chat → SSE  │
                                        │ │  • POST reply back   │
                                        │ └──────────┬──────────┘
                                        │            │ 2. POST /chat (SSE)
                                        │            ▼
                                        │ ┌─────────────────────┐
                                        │ │ hermes_daemon.py     │
                                        │ │ :8645 (aiohttp)      │
                                        │ │  • persistent hermes │
                                        │ │    --continue child  │
                                        │ │  • SSE event stream  │
                                        │ └──────────┬──────────┘
                                        │            │ 3. stdin/stdout
                                        └────────────┘    pipe
```

> **2026-04-28** — bridge now talks to a long-lived `hermes_daemon` (option B
> from [`docs/SPEC-bridge-refactor.md`](./docs/SPEC-bridge-refactor.md)) over
> loopback HTTP + SSE. Daemon owns one persistent `hermes --continue` child, so
> cold start cost is paid once at boot, not per message. If `HERMES_DAEMON_URL`
> is unset or the daemon is unreachable, the bridge automatically falls back
> to the legacy per-request `hermes chat` subprocess — zero-risk migration.
>
> See [`docs/API-bridge-http-daemon.md`](./docs/API-bridge-http-daemon.md) for
> endpoint surface, SSE event types, and fallback semantics.

---

## Quick start

```bash
# 0. Pre-reqs
#   • Hermes Agent installed inside a Docker container (e.g. openclaw-project-b)
#   • Cloudflare account + a zone you own (eclawbot.com)
#   • EClaw channel API key (eck_...) — create in EClaw Portal → Settings → Channel API

# 1. Load creds (host-side)
security add-generic-password -s cloudflare-api-token    -a hank -w "<CF API TOKEN>"
security add-generic-password -s hermes-eclaw-botsecret -a hank -w "<will fill after bind>"

# 2. Create named tunnel + DNS (run on host)
./scripts/setup-tunnel.sh hermes-b openclaw-project-b 8644

# 3. Register + bind entity (run on host)
./scripts/bind-entity.sh eck_xxxxxxxxxxxx 5 "Hermes"

# 4. Start bridge as its own docker container (auto-restarts on host/docker restart)
./scripts/up-bridge.sh
# (legacy: ./scripts/start-bridge.sh runs the bridge inside openclaw-project-b
#  and dies on container restart — kept for environments without compose.)
```

Once running, anyone who messages the bot (via EClaw app or `https://eclawbot.com/c/<publicCode>`) will get Hermes's reply.

---

## Files

| Path | Purpose |
|------|---------|
| `plugin/eclaw_bridge.py` | Main Python server — receives webhook, posts to daemon (or falls back to subprocess), posts reply back to EClaw |
| `daemon/hermes_daemon.py` | Long-lived aiohttp server on `:8645`. Owns the persistent `hermes --continue` child. `POST /chat` returns SSE event stream. |
| `daemon/hermes_worker.py` | Inner worker that pipes prompts to the persistent Hermes process and parses replies. |
| `docs/SPEC-bridge-refactor.md` | A/B/C refactor evaluation — why option B (out-of-process daemon) won. |
| `docs/API-bridge-http-daemon.md` | Daemon HTTP/SSE endpoint reference (POST /chat, /health, event types, fallback rules). |
| `scripts/setup-tunnel.sh` | Create Cloudflare named tunnel + DNS CNAME via API |
| `scripts/bind-entity.sh` | EClaw `/register` + `/bind` (saves botSecret to Keychain) |
| `scripts/up-bridge.sh` | **Recommended** — run bridge as its own container with `restart: unless-stopped` |
| `scripts/start-bridge.sh` | _(legacy)_ run bridge inside openclaw-project-b — dies on container restart |
| `scripts/teardown.sh` | Reverse: kill bridge, unbind, delete tunnel + DNS |
| `KNOW-HOW.md` | **All the pitfalls we hit** — read this first |

---

## Requirements

- **Host**: macOS (uses Keychain for secret storage; adapt for Linux)
- **Container**: Debian-based with `python3 + aiohttp` (Hermes's venv already has it)
- **Cloudflare API token** permissions:
  - `Account → Cloudflare Tunnel: Edit`
  - `Zone → DNS: Edit` (on your zone)
  - `Zone → Zone: Read` (on your zone)

---

## Known limitations

1. **No HMAC signing** — gateway runs in `INSECURE_NO_AUTH` mode. Bearer token check is in the bridge itself.
2. **Serialised replies inside daemon** — the persistent `hermes --continue` child is single-threaded by Hermes design, so the daemon serialises concurrent `POST /chat` requests via an internal queue. EClaw delivery is async, so callers don't block; replies are streamed back over SSE in the order requests were enqueued.
3. **Keychain is host-only** — for CI/prod need to switch to env file or a secret manager.

> **Historical (resolved by daemon refactor 2026-04-28):**
> - ~~Cold start ~7-9s per message~~ — daemon's persistent child pays this once at boot. Per-message latency now ≈ Hermes inference time only. Subprocess fallback path keeps the old behaviour when daemon is unreachable.
> - ~~Bridge-level `asyncio.Lock` serialises Hermes calls~~ — lock moved into the daemon's worker queue; the bridge itself can fan out.

---

## Credits

- [@NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent) — the agent
- `@eclaw/openclaw-channel` — the reference implementation (TypeScript) we studied

---

## License

MIT
