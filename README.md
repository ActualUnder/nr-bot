# READ ME MUST ALWAYS BE UPDATED

# Metro T3 Learner Discord Bot

This is a Discord slash-command controlled bot wrapped around `t3_learner_clean.py` for the T3 Network Rail TD/S-Class learner.

It is designed for live learning, signal/bit inspection, route-bit discovery, stats, upload/download, and live Network Rail TD feed status while keeping runtime data outside the Git repository.

## Main idea

- GitHub stores the code.
- `/opt/metro-t3-discord-bot` is the server checkout of the GitHub repo.
- `/var/lib/metro-bot/t3_learner` stores live learner data.
- systemd starts the bot automatically.
- the startup script pulls latest GitHub code on every restart.
- runtime data is not stored in the repo, so `git reset --hard` cannot wipe it.
- SQLite is set to `journal_mode=DELETE` inside the learner, so it should normally stay as one `.sqlite` file instead of `.sqlite`, `.sqlite-wal`, and `.sqlite-shm`.

## Optimisation pass

The bot wrapper has been optimised so routine Discord commands do less work and use less memory:

- `known_bits.csv` is cached by file mtime/size instead of being parsed for every command.
- topology is cached instead of rebuilt for every feed reconnect.
- SQLite inspection commands use read-only connections where possible.
- SQLite read cache, busy timeout and optional mmap are configurable from env vars.
- expensive learner CLI subprocesses are limited by a semaphore so multiple Discord commands cannot spawn lots of Python processes at once.
- live learner recent-event memory is configurable with `NR_LEARNER_RECENT_KEEP_SECONDS`.
- feed tick interval is configurable with `NR_FEED_TICK_SECONDS`.
- reconnect backoff resets after a successful connection.
- `/download` uses SQLite's online backup API to create a safer DB snapshot instead of zipping the live DB file directly.
- `/db_optimise` can run SQLite `ANALYZE`, `PRAGMA optimize`, optional purge and optional `VACUUM`.
- `/recent_bits` reads the raw S-Class bit-change table directly.
- `/route_bits` reads pass-window evidence directly to find likely route bits.

## Discord slash commands

- `/status` - shows Discord status, NR feed connected/running state, message count, DB path, known CSV rows, memory and latest error.
- `/nr_start` - starts the live Network Rail feed learner.
- `/nr_stop` - stops the live Network Rail feed learner.
- `/nr_restart` - restarts the live feed learner and reloads known bits.
- `/report signal:6232` - runs the learner report for a signal. Known CSV path and DB path are automatic.
- `/progress` - compact learning progress summary.
- `/signal signal:6232` - shows berth/headcode occupancy, configured routes/next berths, CSV mapped raw bit state, and the strongest learned bit candidates from pass evidence.
- `/bit bit:25:3` - shows the current/latest state of a byte:bit.
- `/recent_bits` - shows the most recent raw S-Class bit changes, optionally filtered by signal, byte:bit, known-only, and recent time window.
- `/route_bits signal:6248` - scores likely route bits from stored pass evidence.
- `/known signal:6232` - shows known_bits.csv rows for a signal.
- `/moves signal:6244` - shows learned movements involving a berth/signal.
- `/bytes` - shows S-Class byte addresses seen.
- `/missing` - shows missing topology observations.
- `/db_stats` - shows SQLite file size, page/free-page counts and table row counts.
- `/db_optimise` - runs SQLite optimisation and optional purge/vacuum safely by stopping/restarting the live feed.
- `/download` - sends a zip containing a SQLite DB snapshot, known_bits.csv and missing topology CSVs.
- `/upload attachment:file` - accepts known_bits.csv, a SQLite DB, or a zip containing them.
- `/check` - checks topology, known CSV and DB load cleanly.

## Useful command examples

```text
/recent_bits limit:30
/recent_bits signal:6248 known_only:true
/recent_bits bit:25:3 since_minutes:120
/route_bits signal:6248
/route_bits signal:6248 to:6244 phase:before min_hits:2
/db_stats
/db_optimise vacuum:true
/db_optimise purge_days:30 vacuum:true
```

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

Optional optimisation settings:

```env
# Limit expensive learner subprocesses. Keep 1 on small LXCs.
NR_CLI_CONCURRENCY=1
NR_CLI_TIMEOUT_SECONDS=60

# SQLite command/read tuning. Negative cache_size means KiB.
NR_DB_BUSY_TIMEOUT_MS=5000
NR_DB_READ_CACHE_KIB=4096
NR_DB_MMAP_MIB=0

# Live learner memory/CPU tuning.
# Lower recent keep uses less memory but reduces pass-window context.
NR_LEARNER_RECENT_KEEP_SECONDS=180
NR_FEED_TICK_SECONDS=1.0

# Reconnect backoff.
NR_RECONNECT_INITIAL_SECONDS=10
NR_RECONNECT_MAX_SECONDS=300

# Discord command output limits.
NR_COMMAND_DEFAULT_LIMIT=25
NR_COMMAND_MAX_LIMIT=100
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

## Operational notes

`/signal` no longer assumes every S-Class bit uses `1 = red` and `0 = proceed`. The command now shows the raw bit value unless `known_bits.csv` explicitly declares the polarity in the `Active State` column. This avoids false output where an unverified CSV row made a red signal look like proceed.

`/signal` also shows learned candidates from `pass_bit_events`. These are evidence-based correlations around CA pass windows. The useful patterns are:

- `before 1->0` usually means a red/danger bit cleared before a train passed, so `1 = red/danger` and `0 = not red/cleared`.
- `after 0->1` usually means a red/danger bit restored after a train passed, so `1 = red/danger` and `0 = not red/cleared`.
- `before 0->1` usually means a proceed/route bit set before a train passed, so `1 = proceed/route set`.
- `after 1->0` usually means a proceed/route bit released after a train passed.

Treat old compact CSV mappings as suspect if the CSV bit is not one of the top learned candidates for that signal. In that case, confirm against the panel before using it as the live aspect.

Berth/headcode state is now updated from CA step, CB cancel and CC interpose messages. Older runs only used CA, which meant directly interposed or cancelled headcodes could be missing/stale in `/signal`. Restart the bot and let the live feed run to refresh the berth table.

Route-bit discovery is evidence-based. `/route_bits` does not permanently edit `known_bits.csv`; it helps you decide which bits to add to the CSV after checking the score, pass count and route context.
