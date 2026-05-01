# READ ME MUST ALWAYS BE UPDATED

# Metro T3 Learner Discord Bot

This is a simple Discord slash-command controlled bot wrapped around `t3_learner_clean.py`.

It is meant as a base model for learning, signal bit inspection, stats, downloads/uploads and live Network Rail TD feed status.

## Main idea

- GitHub stores the code.
- `/opt/metro-t3-discord-bot` is the server checkout of the GitHub repo.
- `/var/lib/metro-bot/t3_learner` stores live learner data.
- systemd starts the bot automatically.
- the startup script pulls latest GitHub code on every restart.
- runtime data is not stored in the repo, so `git reset --hard` cannot wipe it.
- SQLite is set to `journal_mode=DELETE`, so it should stay as one `.sqlite` file instead of `.sqlite`, `.sqlite-wal`, and `.sqlite-shm`.

## Discord slash commands

- `/status` - shows Discord status, NR feed connected/running state, message count, DB path, known CSV path and latest error.
- `/nr_start` - starts the live Network Rail feed learner.
- `/nr_stop` - stops the live Network Rail feed learner.
- `/nr_restart` - restarts the live feed learner.
- `/report signal:6232` - runs the learner report for a signal. Known CSV path and DB path are automatic.
- `/progress` - compact learning progress summary.
- `/signal signal:6232` - shows berth/headcode occupancy and known signal bit state if known.
- `/bit bit:25:3` - shows the current/latest state of a byte:bit.
- `/known signal:6232` - shows known_bits.csv rows for a signal.
- `/moves signal:6244` - shows learned movements involving a berth/signal.
- `/bytes` - shows S-Class byte addresses seen.
- `/missing` - shows missing topology observations.
- `/download` - sends a zip containing the SQLite DB, known_bits.csv and missing topology CSVs.
- `/upload attachment:file` - accepts known_bits.csv, a SQLite DB, or a zip containing them.
- `/check` - checks topology, known CSV and DB load cleanly.

## Files in this package

```text
bot.py
requirements.txt
t3_learner_clean.py
known_bits.csv
.env.example
README.md
deploy/metro-t3-learner.service
deploy/metro-t3-learner-start.sh
scripts/install_service.sh
```

## Persistent data layout

```text
/var/lib/metro-bot/t3_learner/
├── td_signal_bit_learner.sqlite
├── known_bits.csv
├── exports/
├── uploads/
├── backups/
├── logs/
└── missing_topology/
```

## Environment file

Create/edit:

```bash
sudo nano /etc/metro-bot.env
```

Minimum:

```env
DISCORD_TOKEN=your_discord_bot_token
METRO_BOT_DATA_DIR=/var/lib/metro-bot
NROD_USER=your_network_rail_username
NROD_PASS=your_network_rail_password
NR_ENABLED=true
NR_AREA=T3
```

For fast slash command sync to one Discord server, set:

```env
DISCORD_GUILD_ID=your_discord_server_id
```

## Install on Debian/LXC

Clone to `/opt`:

```bash
cd /opt
sudo git clone YOUR_GITHUB_REPO_URL metro-t3-discord-bot
sudo chown -R metrobot:metrobot /opt/metro-t3-discord-bot || true
cd /opt/metro-t3-discord-bot
```

Run installer:

```bash
sudo bash scripts/install_service.sh
```

Edit env:

```bash
sudo nano /etc/metro-bot.env
```

Start service:

```bash
sudo systemctl start metro-t3-learner
```

Check service:

```bash
systemctl status metro-t3-learner
journalctl -u metro-t3-learner -n 100 --no-pager
journalctl -u metro-t3-learner -f
```

Restart and pull latest GitHub code:

```bash
sudo systemctl restart metro-t3-learner
```

## Notes

`/signal` is intentionally basic in this base version. It shows:

- whether the signal/berth currently has a stored headcode from latest CA movement state
- the known S-Class bit for that signal from `known_bits.csv`
- whether that bit is currently 1 or 0
- display meaning used here: `1 = ON/red`, `0 = OFF/proceed`

The full advanced aspect/route decoder can be built later on top of this.
