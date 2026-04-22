#!/usr/bin/env bash
# Register callback URL and bind an entity via EClaw Channel API.
#
# Usage: ./bind-entity.sh <channel_api_key> <entity_id> <bot_name>
#   e.g.  ./bind-entity.sh eck_xxx 5 Hermes
#
# On success, saves botSecret → macOS Keychain (hermes-eclaw-botsecret)
# and appends non-secret identifiers to ../../.env
set -euo pipefail

API_KEY="${1:?channel_api_key required}"
ENTITY_ID="${2:?entity_id required}"
BOT_NAME="${3:?bot name required}"

HERE="$(cd "$(dirname "$0")" && pwd)"
set -a; source "$HERE/../../.env"; set +a

API_BASE="${HERMES_ECLAW_API_BASE:-https://eclawbot.com}"
CALLBACK_URL="${HERMES_ECLAW_CALLBACK_URL}"
CALLBACK_TOKEN="${HERMES_ECLAW_CALLBACK_TOKEN:-$(openssl rand -hex 16)}"

echo "== POST /api/channel/register =="
REG=$(curl -sf -X POST "$API_BASE/api/channel/register" \
    -H 'Content-Type: application/json' \
    -d "{\"channel_api_key\":\"$API_KEY\",\"callback_url\":\"$CALLBACK_URL\",\"callback_token\":\"$CALLBACK_TOKEN\"}")
DEVICE_ID=$(echo "$REG" | python3 -c "import sys,json;print(json.load(sys.stdin)['deviceId'])")
echo "  deviceId=$DEVICE_ID"

echo "== POST /api/channel/bind (entityId=$ENTITY_ID, name=$BOT_NAME) =="
BIND=$(curl -sf -X POST "$API_BASE/api/channel/bind" \
    -H 'Content-Type: application/json' \
    -d "{\"channel_api_key\":\"$API_KEY\",\"entityId\":$ENTITY_ID,\"name\":\"$BOT_NAME\"}")
OK=$(echo "$BIND" | python3 -c "import sys,json;print(json.load(sys.stdin).get('success', False))")
if [ "$OK" != "True" ]; then
    echo "  bind failed:"
    echo "$BIND" | python3 -m json.tool
    exit 1
fi
BOT_SECRET=$(echo "$BIND" | python3 -c "import sys,json;print(json.load(sys.stdin)['botSecret'])")
PUBLIC_CODE=$(echo "$BIND" | python3 -c "import sys,json;print(json.load(sys.stdin)['publicCode'])")
echo "  entityId=$ENTITY_ID  publicCode=$PUBLIC_CODE"

security delete-generic-password -s hermes-eclaw-botsecret -a hank 2>/dev/null || true
security add-generic-password -s hermes-eclaw-botsecret -a hank -w "$BOT_SECRET"
echo "  botSecret → keychain (hermes-eclaw-botsecret)"

cat >> "$HERE/../../.env" <<EOF

# Hermes EClaw channel binding — bound $(date -u +%Y-%m-%d)
HERMES_ECLAW_API_KEY_ID=$API_KEY
HERMES_ECLAW_DEVICE_ID=$DEVICE_ID
HERMES_ECLAW_ENTITY_ID=$ENTITY_ID
HERMES_ECLAW_PUBLIC_CODE=$PUBLIC_CODE
HERMES_ECLAW_CALLBACK_TOKEN=$CALLBACK_TOKEN
EOF
echo "  ids → .env"
echo
echo "Bound. Public URL: $API_BASE/c/$PUBLIC_CODE"
