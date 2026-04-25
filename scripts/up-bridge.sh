#!/usr/bin/env bash
# Bring up (or restart) the hermes-bridge docker container.
# Pulls HERMES_ECLAW_BOT_SECRET from macOS Keychain at run time and exports
# it so docker compose's variable interpolation can pick it up; the secret
# is NOT written to .env or any file on disk.
#
# Other identifiers (deviceId, entityId, callback_token, api_key_id) live
# in ../../.env which is gitignored — those aren't strictly secret on
# their own (no reply-as-bot capability without botSecret).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$HERE/../.."

set -a; source "$COMPOSE_DIR/.env"; set +a

HERMES_ECLAW_BOT_SECRET="$(security find-generic-password -s hermes-eclaw-botsecret -a hank -w)"
export HERMES_ECLAW_BOT_SECRET

cd "$COMPOSE_DIR"

# Drop any leftover in-container bridge from the legacy start-bridge.sh path
# (running this on a fresh box is a no-op).
docker exec openclaw-project-b bash -lc "pgrep -f eclaw_bridge | xargs -r kill 2>/dev/null || true" 2>/dev/null || true

docker compose up -d hermes-bridge "$@"

sleep 3
if docker exec hermes-bridge curl -sf -m 3 http://localhost:8644/health >/dev/null 2>&1; then
    echo "✓ hermes-bridge healthy on :8644 (network shared with openclaw-project-b)"
    echo "  public URL: ${HERMES_ECLAW_CALLBACK_URL%/webhooks/eclaw}/health"
    echo "  tail log:   docker logs -f hermes-bridge"
else
    echo "✗ hermes-bridge did not respond — recent logs:"
    docker logs hermes-bridge --tail 30 2>&1 | tail -30
    exit 1
fi
