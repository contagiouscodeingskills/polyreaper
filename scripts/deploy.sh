#!/usr/bin/env bash
# One-command deploy to the recorder VPS.
#
# Usage:    ./scripts/deploy.sh
#           POLYBOT_VPS=root@1.2.3.4 ./scripts/deploy.sh
#
# Steps (on the VPS):
#   1. git pull --ff-only on /home/polybot/polybot
#   2. cargo build --release
#   3. restart polybot-recorder.service if it's installed
#   4. tail journalctl for 30s so you see the boot sequence

set -euo pipefail

VPS_HOST="${POLYBOT_VPS:-159.69.0.78}"

echo "==> Deploying main to ${VPS_HOST}"

ssh "root@${VPS_HOST}" bash -s <<'REMOTE'
set -euo pipefail
sudo -u polybot -H bash <<'INNER'
set -euo pipefail
source ~/.cargo/env
cd ~/polybot
echo "==> git pull"
git pull --ff-only
echo "==> cargo build --release"
cargo build --release 2>&1 | tail -3
INNER

if systemctl list-unit-files polybot-recorder.service --no-legend 2>/dev/null | grep -q polybot-recorder; then
    echo "==> systemctl restart polybot-recorder"
    systemctl restart polybot-recorder
    sleep 1
    systemctl is-active polybot-recorder && echo "service is active"
else
    echo "==> systemd unit not installed yet (see docs/SETUP_VPS.md §8)"
fi
REMOTE

echo "==> tailing journalctl (Ctrl-C to stop)"
ssh "root@${VPS_HOST}" 'journalctl -fu polybot-recorder --since "30 sec ago" 2>/dev/null || true'
