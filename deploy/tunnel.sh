#!/usr/bin/env bash
# Cloudflare quick tunnel for the FlowScalp dashboard.
# Publishes 127.0.0.1:8080 at a random https://*.trycloudflare.com URL and
# writes that URL where the Telegram bot can report it (/dashboard).
set -u

URL_FILE=/opt/flowscalp/data/tunnel_url.txt
PORT="${DASH_PORT:-8080}"
rm -f "$URL_FILE"

cloudflared tunnel --no-autoupdate --url "http://127.0.0.1:${PORT}" 2>&1 | \
  while IFS= read -r line; do
    echo "$line"
    u=$(printf '%s' "$line" | grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' | head -1)
    if [ -n "$u" ] && [ ! -s "$URL_FILE" ]; then
      echo "$u" > "$URL_FILE"
      chown flowscalp:flowscalp "$URL_FILE" 2>/dev/null || true
      echo "dashboard tunnel ready: $u"
    fi
  done
