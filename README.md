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
  3. POST /api/channel/message          │            │
     reply                              │            ▼
                                        │ ┌─────────────────────┐
                                        │ │ eclaw_bridge.py :8644│
                                        │ │  • Bearer auth       │
                                        │ │  • spawn hermes chat │
                                        │ │  • POST reply back   │
                                        │ └─────────────────────┘
                                        │            │
                                        └────────────┘ 2. hermes chat -q
```

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
| `plugin/eclaw_bridge.py` | Main Python server — receives webhook, invokes Hermes, posts reply |
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

1. **Cold start ~7-9s per message** — each inbound spawns `hermes chat`. See [KNOW-HOW §14](./KNOW-HOW.md) for ideas to make warm.
2. **No HMAC signing** — gateway runs in `INSECURE_NO_AUTH` mode. Bearer token check is in the bridge itself.
3. **Serialized replies** — `asyncio.Lock` serializes Hermes calls. Two concurrent EClaw messages are handled sequentially.
4. **Keychain is host-only** — for CI/prod need to switch to env file or a secret manager.

---

## Credits

- [@NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent) — the agent
- `@eclaw/openclaw-channel` — the reference implementation (TypeScript) we studied

---

## License

MIT
