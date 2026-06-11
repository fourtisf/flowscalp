#!/usr/bin/env bash
# FlowScalp installer — run as root on Ubuntu 22.04/24.04.
# Installs to /opt/flowscalp under the dedicated non-root user `flowscalp`.
set -euo pipefail

APP_DIR=/opt/flowscalp
APP_USER=flowscalp
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "[1/7] apt dependencies"
apt-get update -qq
apt-get install -y -qq sqlite3 rsync

# find a Python >= 3.11 (Ubuntu 24.04 ships 3.12, Debian 12 ships 3.11, …)
PY=""
for cand in python3 python3.12 python3.11; do
  command -v "$cand" >/dev/null 2>&1 || continue
  if "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
    PY=$cand; break
  fi
done
if [ -z "$PY" ]; then
  # older distros (Ubuntu 22.04): python3.11 lives in universe
  apt-get install -y -qq python3.11 python3.11-venv && PY=python3.11 || true
fi
if [ -z "$PY" ]; then
  echo "ERROR: no Python >= 3.11 available. Install one and re-run." >&2
  exit 1
fi
PYVER=$("$PY" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
apt-get install -y -qq "python${PYVER}-venv" || apt-get install -y -qq python3-venv
echo "    using $PY (Python $PYVER)"

echo "[2/7] user + directories"
id -u $APP_USER &>/dev/null || useradd --system --home $APP_DIR --shell /usr/sbin/nologin $APP_USER
mkdir -p $APP_DIR/{data,logs,backups}

echo "[3/7] sync code"
rsync -a --delete \
  --exclude '.git' --exclude 'venv' --exclude 'data' --exclude 'logs' \
  --exclude 'backups' --exclude '.env' --exclude '__pycache__' \
  "$REPO_DIR/" $APP_DIR/

echo "[4/7] venv + python deps"
[ -d $APP_DIR/venv ] || "$PY" -m venv $APP_DIR/venv
$APP_DIR/venv/bin/pip install -q --upgrade pip
$APP_DIR/venv/bin/pip install -q -r $APP_DIR/requirements.txt

echo "[5/7] .env"
if [ ! -f $APP_DIR/.env ]; then
  cp $APP_DIR/.env.example $APP_DIR/.env
  echo "    → created $APP_DIR/.env from template. EDIT IT before starting."
fi
chown -R $APP_USER:$APP_USER $APP_DIR
chmod 600 $APP_DIR/.env

echo "[6/7] systemd unit"
cp $APP_DIR/deploy/flowscalp.service /etc/systemd/system/flowscalp.service
systemctl daemon-reload
systemctl enable flowscalp

echo "[7/7] nightly DB backup (00:15 UTC, keep 14)"
cat > /etc/cron.d/flowscalp-backup <<'CRON'
15 0 * * * flowscalp sqlite3 /opt/flowscalp/data/flowscalp.db ".backup '/opt/flowscalp/backups/flowscalp-$(date +\%F).db'" && find /opt/flowscalp/backups -name 'flowscalp-*.db' -mtime +14 -delete
CRON
chmod 644 /etc/cron.d/flowscalp-backup

echo
echo "done. next steps:"
echo "  1. edit $APP_DIR/.env  (agent wallet key, telegram ids, dashboard token)"
echo "  2. systemctl start flowscalp"
echo "  3. journalctl -u flowscalp -f"
