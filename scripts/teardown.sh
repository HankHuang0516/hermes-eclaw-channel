#!/usr/bin/env bash
# Reverse setup — stop bridge, unbind entity (safe), remove tunnel + DNS + cloudflared.
# Does NOT touch the Hermes/Minimax config.
#
# Usage: ./teardown.sh <subdomain>
#   e.g.  ./teardown.sh hermes-b
set -euo pipefail

SUBDOMAIN="${1:?subdomain required}"
DOMAIN="eclawbot.com"
HOSTNAME="$SUBDOMAIN.$DOMAIN"

HERE="$(cd "$(dirname "$0")" && pwd)"
set -a; source "$HERE/../../.env"; set +a

TOKEN=$(security find-generic-password -s cloudflare-api-token -a hank -w)
ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID}"
ZONE_ID="${CLOUDFLARE_ZONE_ID_ECLAWBOT}"

echo "== stopping bridge + cloudflared =="
docker exec openclaw-project-b bash -lc "pgrep -f eclaw_bridge | xargs -r kill 2>/dev/null || true"
docker rm -f "cloudflared-$SUBDOMAIN" >/dev/null 2>&1 || true

echo "== unbind entity (safe, keeps entity slot) =="
if [ -n "${HERMES_ECLAW_ENTITY_ID:-}" ]; then
    BOT_SECRET=$(security find-generic-password -s hermes-eclaw-botsecret -a hank -w 2>/dev/null || true)
    if [ -n "$BOT_SECRET" ]; then
        curl -sf -X DELETE "https://eclawbot.com/api/entity" \
            -H 'Content-Type: application/json' \
            -d "{\"deviceId\":\"$HERMES_ECLAW_DEVICE_ID\",\"botSecret\":\"$BOT_SECRET\",\"entityId\":$HERMES_ECLAW_ENTITY_ID}" \
            && echo "  entity $HERMES_ECLAW_ENTITY_ID unbound"
    fi
fi

echo "== DNS + tunnel cleanup =="
# Look up DNS record by name
DNS_ID=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/dns_records?name=$HOSTNAME" \
    | python3 -c "import sys,json;r=json.load(sys.stdin)['result'];print(r[0]['id'] if r else '')")
if [ -n "$DNS_ID" ]; then
    curl -sf -X DELETE -H "Authorization: Bearer $TOKEN" \
        "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/dns_records/$DNS_ID" >/dev/null
    echo "  DNS record removed"
fi

# Look up tunnel by name
TUNNEL_ID=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/cfd_tunnel?name=$SUBDOMAIN" \
    | python3 -c "import sys,json;r=json.load(sys.stdin)['result'];print(r[0]['id'] if r else '')")
if [ -n "$TUNNEL_ID" ]; then
    curl -sf -X DELETE -H "Authorization: Bearer $TOKEN" \
        "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/cfd_tunnel/$TUNNEL_ID" >/dev/null
    echo "  tunnel deleted"
fi

echo "== keychain cleanup =="
security delete-generic-password -s "cloudflared-tunnel-$SUBDOMAIN" -a hank 2>/dev/null || true
security delete-generic-password -s hermes-eclaw-botsecret -a hank 2>/dev/null || true
echo "  done"
