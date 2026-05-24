#!/usr/bin/env bash
# DEPRECATED — prefer ./up-bridge.sh which runs the bridge as its own
# docker container with restart=unless-stopped. This script is kept only
# for environments where you can't (or don't want to) add the
# hermes-bridge service to docker-compose.yml.
#
# Start Hermes × EClaw bridge inside openclaw-project-b.
# Pre-requisites:
#   - cloudflared-hermes-b running (tunnel live)
#   - EClaw register+bind already done (callback_token valid)
#   - botSecret in macOS keychain: cloudflared-tunnel-hermes-b -> n/a; hermes-eclaw-botsecret -> yes
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."

# Load non-secret config from .env
set -a
source "$HERE/../../.env"
set +a

# botSecret from keychain (macOS host)
BOT_SECRET=$(security find-generic-password -s "hermes-eclaw-botsecret" -a "hank" -w)

# Stop Hermes webhook gateway if it's holding 8644 (bridge will take over)
docker exec openclaw-project-b bash -lc "pgrep -f 'hermes gateway run' | xargs -r kill 2>/dev/null || true; sleep 1" || true

# Copy bridge script + shared worker package into container. The bridge's
# daemon-down subprocess fallback imports daemon.hermes_worker for H1
# safeguards even on this deprecated launcher path.
docker exec openclaw-project-b bash -lc "rm -rf /tmp/daemon"
docker cp plugin/eclaw_bridge.py openclaw-project-b:/tmp/eclaw_bridge.py
docker cp daemon openclaw-project-b:/tmp/daemon

# Kill any previous bridge
docker exec openclaw-project-b bash -lc "pgrep -f eclaw_bridge | xargs -r kill 2>/dev/null || true; sleep 1" || true

# Start bridge (detached). Uses Hermes's venv so aiohttp is available.
docker exec -d openclaw-project-b bash -c "
export PATH=/home/node/.local/bin:\$PATH
export HERMES_ECLAW_API_KEY='$HERMES_ECLAW_API_KEY_ID'
export HERMES_ECLAW_DEVICE_ID='$HERMES_ECLAW_DEVICE_ID'
export HERMES_ECLAW_ENTITY_ID='$HERMES_ECLAW_ENTITY_ID'
export HERMES_ECLAW_BOT_SECRET='$BOT_SECRET'
export HERMES_ECLAW_API_BASE='$HERMES_ECLAW_API_BASE'
export HERMES_ECLAW_CALLBACK_TOKEN='$HERMES_ECLAW_CALLBACK_TOKEN'
export HERMES_PORT='8644'
export PYTHONPATH=/tmp:\${PYTHONPATH:-}
cd /home/node/hermes-agent
nohup .venv/bin/python3 /tmp/eclaw_bridge.py > /tmp/eclaw-bridge.log 2>&1 &
"

sleep 3
# Verify
if docker exec openclaw-project-b bash -lc "curl -sf -m 3 http://localhost:8644/health" >/dev/null 2>&1; then
    echo "✓ bridge up on :8644"
    echo "  public URL: $HERMES_ECLAW_CALLBACK_URL"
    echo "  tail log:  docker exec openclaw-project-b tail -f /tmp/eclaw-bridge.log"
else
    echo "✗ bridge did not start — see log:"
    docker exec openclaw-project-b tail -30 /tmp/eclaw-bridge.log 2>/dev/null || true
    exit 1
fi
