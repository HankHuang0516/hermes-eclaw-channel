#!/usr/bin/env bash
# Create a Cloudflare named tunnel + DNS CNAME, then start a cloudflared
# container that routes {subdomain}.{zone} → {target_container}:{target_port}.
#
# Usage: ./setup-tunnel.sh <subdomain> <target-container> <target-port>
#   e.g.  ./setup-tunnel.sh hermes-b openclaw-project-b 8644
#
# Reads:
#   macOS Keychain: cloudflare-api-token / hank
#   .env (sibling): CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_ZONE_ID_ECLAWBOT
#
# Writes:
#   macOS Keychain: cloudflared-tunnel-<subdomain> / hank  (connector token)
set -euo pipefail

SUBDOMAIN="${1:?subdomain required}"
TARGET_CONTAINER="${2:?target container name required}"
TARGET_PORT="${3:?target port required}"

HERE="$(cd "$(dirname "$0")" && pwd)"
set -a; source "$HERE/../../.env"; set +a

TOKEN=$(security find-generic-password -s cloudflare-api-token -a hank -w)
ZONE_ID="${CLOUDFLARE_ZONE_ID_ECLAWBOT}"
ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID}"
DOMAIN="eclawbot.com"    # parameterize if you have more than one zone
HOSTNAME="$SUBDOMAIN.$DOMAIN"

cf_api() {
    curl -sf -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"
}

echo "== creating tunnel $SUBDOMAIN =="
TUNNEL_SECRET=$(openssl rand -base64 32)
RES=$(cf_api "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/cfd_tunnel" \
    -d "{\"name\":\"$SUBDOMAIN\",\"tunnel_secret\":\"$TUNNEL_SECRET\",\"config_src\":\"cloudflare\"}")
TUNNEL_ID=$(echo "$RES" | python3 -c "import sys,json;print(json.load(sys.stdin)['result']['id'])")
TUNNEL_TOKEN=$(echo "$RES" | python3 -c "import sys,json;print(json.load(sys.stdin)['result']['token'])")
echo "  tunnel_id=$TUNNEL_ID"

security delete-generic-password -s "cloudflared-tunnel-$SUBDOMAIN" -a hank 2>/dev/null || true
security add-generic-password -s "cloudflared-tunnel-$SUBDOMAIN" -a hank -w "$TUNNEL_TOKEN"
echo "  token → keychain (cloudflared-tunnel-$SUBDOMAIN)"

echo "== configuring ingress $HOSTNAME → $TARGET_CONTAINER:$TARGET_PORT =="
cf_api -X PUT "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/cfd_tunnel/$TUNNEL_ID/configurations" \
    -d "{\"config\":{\"ingress\":[{\"hostname\":\"$HOSTNAME\",\"service\":\"http://$TARGET_CONTAINER:$TARGET_PORT\"},{\"service\":\"http_status:404\"}]}}" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('  ingress OK' if d.get('success') else d)"

echo "== adding DNS CNAME =="
cf_api -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/dns_records" \
    -d "{\"type\":\"CNAME\",\"name\":\"$SUBDOMAIN\",\"content\":\"$TUNNEL_ID.cfargotunnel.com\",\"proxied\":true,\"ttl\":1}" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('  DNS OK' if d.get('success') else d)"

echo "== starting cloudflared container =="
docker rm -f "cloudflared-$SUBDOMAIN" >/dev/null 2>&1 || true
docker run -d --name "cloudflared-$SUBDOMAIN" \
    --network openclaw-docker_default \
    --restart unless-stopped \
    cloudflare/cloudflared:latest \
    tunnel --no-autoupdate run --token "$TUNNEL_TOKEN" >/dev/null
sleep 5

echo "== verifying =="
if curl -sf -m 10 "https://$HOSTNAME/health" >/dev/null 2>&1; then
    echo "  ✓ $HOSTNAME is live"
else
    echo "  ⚠ $HOSTNAME is not yet responding — DNS may still be propagating"
    echo "    tail logs:  docker logs -f cloudflared-$SUBDOMAIN"
fi

echo
echo "Done. Public URL: https://$HOSTNAME"
echo "Tunnel ID:        $TUNNEL_ID"
