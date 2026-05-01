#!/bin/bash
set -euo pipefail

APP_DIR="/opt/metro-t3-discord-bot"
DATA_DIR="/var/lib/metro-bot/t3_learner"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run with sudo/root"
  exit 1
fi

useradd --system --home "$APP_DIR" --shell /usr/sbin/nologin metrobot 2>/dev/null || true

mkdir -p "$APP_DIR"
mkdir -p "$DATA_DIR"/{exports,uploads,backups,logs,missing_topology}
chown -R metrobot:metrobot /var/lib/metro-bot || true

cp deploy/metro-t3-learner-start.sh /usr/local/bin/metro-t3-learner-start.sh
chmod +x /usr/local/bin/metro-t3-learner-start.sh
cp deploy/metro-t3-learner.service /etc/systemd/system/metro-t3-learner.service

if [ ! -f /etc/metro-bot.env ]; then
  cp .env.example /etc/metro-bot.env
  chmod 600 /etc/metro-bot.env
  echo "Created /etc/metro-bot.env - edit this and add DISCORD_TOKEN + NR credentials."
fi

systemctl daemon-reload
systemctl enable metro-t3-learner

echo "Installed service."
echo "Next:"
echo "  sudo nano /etc/metro-bot.env"
echo "  sudo systemctl start metro-t3-learner"
