# READ ME MUST ALWAYS BE UPDATED

### 2026-05-05 v6 bit-correlation scan update

This version adds `/bit_correlate`, which scans one raw S-Class byte:bit against all captured C-Class/pass_log movements instead of checking only one chosen signal. This is useful for low-use siding bits such as `25:3`, where the bit may line up with a 6244 move but also toggle at other times because it actually belongs to another signal/berth, a shared route condition, or a noisy/wrong CSV row.

Use it like:

```text
/bit_correlate bit:25:3 since_minutes:240 match_window_seconds:180 limit:20
```

Read the output as a ranking, not proof. A good signal-specific candidate should have lots of matched bit changes, low average delta, and not many extra raw changes away from that signal's movements. If the CSV mapped signal is not near the top, or many neighbouring berths score better, downgrade the CSV row until checked against the panel.


### 2026-05-05 v4 diagnostic update

This version improves the live signal diagnostics further:

- `/signal` now shows the latest C-Class pass/move history involving the berth, so a train that has already stepped out can still be seen in context instead of only showing the current clear/occupied state.
- `/bit` now shows the learned interpretation for mapped signal bits, including low-evidence rows clearly marked as low evidence. For example, a mapped proceed bit with raw `0` may be shown as "low-evidence suggests red/danger or no route set" rather than just "raw 0".
- `/moves` exposes `show_events` and `event_limit` in Discord, matching the CLI `--show-events` investigation mode.
- Report hints now mention both Discord option names and CLI flags, e.g. `show_cross_known:true` as well as `--show-cross-known`.

Important: a berth state is a latest-state cache. If a train was in berth 6244 at 09:20 and then stepped to 6241/6242, `/signal 6244` should show the berth as clear later, while the new movement history should still show the 09:20 move.


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
- `/bit_trace` compares one bit against one signal's movements.
- `/bit_correlate` ranks all signals/berths that move near one raw bit, which helps find wrong/shared CSV mappings.
- `/route_bits` reads pass-window evidence directly to find likely route bits.


## Current v3 signal-state fixes

This version includes extra guards for the 6244/6263 style fault where `/bit` could briefly disagree with `/recent_bits`:

- stale/out-of-order S-Class byte snapshots are ignored in the live learner instead of overwriting newer byte state;
- stale/out-of-order C-Class berth messages are ignored so berth/headcode state cannot be rolled backwards by an older CA/CB/CC;
- SQLite `s_bytes` and `berth_state` updates now only replace existing rows when the incoming TD timestamp is newer or equal;
- `/bit` and `/signal` recover from older databases by preferring a newer raw `s_bit_events` entry over a stale `s_bytes` snapshot for the exact bit;
- `/report` now exposes `show_cross_known`, `min_pass_count`, `min_pct`, and `max_avg_delta` as Discord slash-command options.

For low-evidence signals such as `6244` with only one or two finalised passes, treat `25:3` as an observed proceed/route-set candidate until more pass windows confirm it. A pattern like `0->1` just before the train leaves and `1->0` after it passes means `1 = proceed/route set`, not `1 = red`.

## Discord slash commands

- `/status` - shows Discord status, NR feed connected/running state, message count, DB path, known CSV rows, memory and latest error.
- `/nr_start` - starts the live Network Rail feed learner.
- `/nr_stop` - stops the live Network Rail feed learner.
- `/nr_restart` - restarts the live feed learner and reloads known bits.
- `/report signal:6232` - runs the learner report for a signal. Known CSV path and DB path are automatic. Options include `show_known`, `show_cross_known`, `min_pass_count`, `min_pct`, and `max_avg_delta`.
- `/progress` - compact learning progress summary.
- `/signal signal:6232` - shows berth/headcode occupancy, configured routes/next berths, CSV mapped raw bit state, and the strongest learned bit candidates from pass evidence.
- `/bit bit:25:3` - shows the current/latest state of a byte:bit.
- `/recent_bits` - shows the most recent raw S-Class bit changes, optionally filtered by signal, byte:bit, known-only, and recent time window.
- `/bit_trace bit:25:3 signal:6244` - checks whether one bit's changes line up with one signal/berth's movements.
- `/bit_correlate bit:25:3` - scans that bit against all captured signal/berth movements and ranks likely correlations.
- `/route_bits signal:6248` - scores likely route bits from stored pass evidence.
- `/known signal:6232` - shows known_bits.csv rows for a signal.
- `/moves signal:6244` - shows learned movements involving a berth/signal.
- `/berths` - shows currently stored berth/headcode states; useful when `/signal` does not show the headcode you expect.
- `/bytes` - shows S-Class byte addresses seen.
- `/missing` - shows missing topology observations.
- `/db_stats` - shows SQLite file size, page/free-page counts and table row counts.
- `/db_optimise` - runs SQLite optimisation and optional purge/vacuum safely by stopping/restarting the live feed.
- `/download` - sends a zip containing a SQLite DB snapshot, known_bits.csv and missing topology CSVs.
- `/upload attachment:file` - accepts known_bits.csv, a SQLite DB, or a zip containing them.
- `/check` - checks topology, known CSV and DB load cleanly.

## Useful command examples

```text
/report signal:6244 show_known:true min_pass_count:1 max_avg_delta:0
/report signal:6244 show_known:true show_cross_known:true min_pass_count:1 max_avg_delta:0
/recent_bits limit:30
/recent_bits signal:6248 known_only:true
/recent_bits bit:25:3 since_minutes:120
/bit_trace bit:25:3 signal:6244 since_minutes:240 match_window_seconds:180
/bit_correlate bit:25:3 since_minutes:240 match_window_seconds:180 limit:20
/route_bits signal:6248
/berths occupied_only:true limit:50
/berths berth:62 occupied_only:false
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

## 2026-05-05 signal-state interpretation fix

The Discord `/signal` command now uses stricter evidence before it derives a live signal state from learned pass-window candidates.

Default live-state thresholds:

```env
NR_DERIVE_MIN_SUPPORT=3
NR_DERIVE_MIN_PCT=0.80
NR_DERIVE_MAX_AVG_DELTA=3.0
NR_DERIVE_FLICKER_WINDOW_SECONDS=7200
NR_DERIVE_FLICKER_WARN_CHANGES=8
```

This stops one-pass rows such as `1/1 (100%)` from being shown as if they prove a live signal state. A single train movement can capture unrelated S-Class changes nearby, so those rows are now displayed as low-evidence only.

For high-confidence proceed-active bits, `/signal` now interprets the useful operational state like this:

- raw `1` on a learned proceed bit = likely proceed/route set
- raw `0` on a learned proceed bit = likely red/danger or no route set, because no proceed is proved

This is why a real red signal may correctly show its learned proceed bit as `0`. The raw bit is not necessarily a red lamp bit; it may be a proceed proving, route set, or control indication bit.

If a CSV mapped bit changes repeatedly while the panel signal looks steady, `/signal` warns that the bit is probably not a steady red-lamp state bit. Use `/recent_bits bit:XX:Y since_minutes:120` and `/report signal:#### show_known:true` to check whether the CSV row is contaminated or only a route/proceed bit.

The offline `report` command now defaults to `--min-pct 0.80 --min-pass-count 3`. Use `--min-pass-count 1` only for raw investigation when you intentionally want to see weak one-pass rows.


## v5 signal/bit correlation diagnostics

Low-use sidings can make bad CSV mappings obvious. A bit may line up with the one or two captured passes, but still toggle many other times when that siding has no movement. The bot now compares raw S-Class bit flips against C-Class pass/move rows for the mapped signal.

Useful commands:

```text
/signal 6244
/bit_trace bit:25:3 signal:6244 since_minutes:180 match_window_seconds:180
/recent_bits bit:25:3 since_minutes:180
/moves signal:6244 limit:20 show_events:true
```

If `/bit_trace` shows many `NO MATCH to this signal` rows, do not trust that bit as the live signal state even if `/report show_known:true` says the bit lined up with the small number of passes. Then use `/bit_correlate` to find whether the same raw bit lines up better with a neighbouring signal, from-berth, to-berth, or shared route movement.

Times are shown using the host machine timezone. If the LXC is on UTC, the bot output will be UTC rather than BST. Use `timedatectl` on the host/container if you want Discord output to show Europe/London local time.
