#!/bin/bash
set -euo pipefail

APP_DIR="/opt/metro-t3-discord-bot"

cd "$APP_DIR"

# Auto-update code from GitHub on every service start.
git fetch --all --prune
git reset --hard origin/HEAD

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate

python -m pip install -U pip
python -m pip install -r requirements.txt

exec python bot.py
