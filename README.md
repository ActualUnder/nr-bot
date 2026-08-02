# Metro T3 Network Rail Protocol Bot

## Private T3 snapshot and event bridge

This release provides two optional, read-only endpoints for the separate Metro bot:

```text
GET /v1/t3/snapshot
GET /v1/t3/events?after=<cursor>&limit=<1..500>
```

It binds only to a configured private address, uses HTTPS with mandatory client
certificates, authorises the `metro-bot` certificate identity, and exposes T3
berth snapshots only. C-Class berth rows are stamped with a connection
generation so occupations left behind by a feed outage are withheld after
reconnect. Accepted `CA`, `CC` and `CB` messages are also written once to a
durable monotonic event log. Metro can therefore consume every retained step,
interpose and cancel without relying on a 30-second snapshot interval. See
[T3_BRIDGE_SETUP.md](T3_BRIDGE_SETUP.md).

## July 2026 command and timezone hotfix

- Fixed the live `NameError: UK_TIMEZONE is not defined` raised when a C-Class berth step was formatted by the learner.
- Fixed `/status` so the full NR Feed section is shown even before the first completed connection.
- Replaced the large flat slash-command list with grouped commands and added `/help`.
- Existing SQLite data and known_bits.csv files remain compatible.

Discord bot and offline learner for the Network Rail T3 Train Describer feed.

This version replaces the old “a bit changed near a train pass, therefore it is the signal aspect” logic with a protocol-level model of C-Class and S-Class messages.

## What changed

### C-Class is modelled as berth data

A `CA` message is stored as a **berth step**:

```text
headcode moved from berth A to berth B
```

It is no longer described internally as proof that the train physically passed signal A at that exact instant. The canonical table is `berth_steps`.

`CB` and `CC` continue to maintain the latest berth/headcode cache.

All three accepted C-Class movement types also populate `t3_bridge_events`.
The stream keeps its identity in `t3_bridge_event_meta`, preserves the headcode
on cancellation, and uses the existing protocol fingerprint to avoid duplicate
events after durable redelivery.

### S-Class messages are treated differently by type

- `SF` contains one precisely timed changed byte. Only `SF` creates timed entries in `s_bit_events`.
- `SG` contains four refresh bytes and starts/continues a staged refresh.
- `SH` contains the final four refresh bytes and completes the refresh.

`SG` and `SH` are applied transactionally. Their differences are stored in `s_snapshot_differences`, not `s_bit_events`, because a refresh tells the bot the current state but not the precise time at which an old stored value changed.

The previous bug that discarded the four data bytes contained in `SH` has been removed.

### A persisted database is not automatically “live”

After every startup or connection gap, the S-Class snapshot is invalidated. `/signal show`, `/raw bit`, and `/signal observe` withhold current raw state until a complete `SG ... SH` refresh has arrived.

The first `SF` received for an address before a refresh establishes a baseline. It does not create a fake edge from an old persisted byte.

### Strict validation

The bot now rejects instead of repairing:

- missing or malformed Network Rail timestamps;
- odd-length or non-hex S-Class data;
- `SF` messages that are not exactly one byte;
- `SG` or `SH` messages that are not exactly four bytes;
- an orphan `SH` received without the start of its refresh.

Bad timestamps are never replaced with the local current time.

### Reliable delivery and deduplication

The feed uses `client-individual` acknowledgement. A frame is acknowledged only after its messages have been processed and committed.

A stable durable subscription is enabled by default. Every inner TD message is stored with a SHA-256 fingerprint in `raw_td_messages`, so a broker redelivery or reconnect cannot create duplicate evidence. SQLite runs in WAL mode so Discord read commands do not unnecessarily block the live feed writer; exports use SQLite's backup API to produce a single consistent database file.

Set `NR_DURABLE_SUBSCRIPTION=false` only if the broker/account rejects durable subscriptions.

### Existing mappings are unverified references

`known_bits.csv` now includes:

- `Provenance`
- `Verified`
- `Element Group`

The original compact mappings were imported as:

```text
Provenance=reference-unverified
Verified=false
```

They can be displayed, but they cannot drive RED/OFF output. A mapping is only allowed to produce a physical aspect interpretation when it has an explicit polarity and is marked verified with suitable provenance such as SOP/ECS, as-built information, or reviewed repeated physical observations.

## Why 24:7 is rejected for signal 6239

The protocol analyser looks for complete patterns around canonical CA berth steps and uses unrelated steps as a control population.

A bit is classified as a `movement_pulse` when it normally:

1. changes at or immediately after the berth step;
2. reverses shortly afterwards; and
3. repeats this pattern on a large proportion of movements.

That matches the observed `24:7` behaviour at 6239: it did not change when the physical signal cleared, then went `0 -> 1` at the 6239-to-6243 berth step and returned after roughly 18 seconds. `/signal show signal:6239` therefore reports it as a rejected track/step/release-shaped mapping, not as the aspect.

A bit which changes before the berth step and restores later can be classified as a `pre_step_control`, but it is still described as **signal OR route**. S-Class timing alone cannot safely distinguish a signal indication from a route indication.

## Physical observation workflow

Use one session per approach:

```text
/signal observe signal:6239 state:red headcode:2T10
/signal observe signal:6239 state:off
/signal observe signal:6239 state:post_pass
```

- `red` starts a new session and captures the complete live snapshot.
- `off` captures the snapshot as soon as the signal is physically observed to clear.
- `post_pass` captures the restored state and closes the session.
- `cancel` cancels the current open session.

Use:

```text
/signal observations signal:6239
```

A candidate is not shown until it has at least two consistent paired RED/OFF sessions. It is still not automatically promoted to a verified live mapping.

## Main Discord commands

The old flat list has been replaced with a small command hierarchy. Use `/help` in Discord for the same guide.

### Everyday

- `/status` — connection, refresh and database health.
- `/signal show signal:6239` — berth state plus verified/cautious signal evidence.
- `/td berths` — current stored berth/headcode states.
- `/td moves signal:6239` — recent canonical C-Class berth movements.

### Signal mapping

- `/signal observe` — capture RED, OFF and post-pass snapshots.
- `/signal observations` — review paired physical evidence.
- `/signal analyse` — detailed protocol candidate report.
- `/signal mappings` — known_bits.csv mappings, provenance and verification.
- `/signal routes` — route-specific pre-step controls without calling them aspects.

### Feed and diagnostics

- `/feed start`, `/feed stop`, `/feed restart` — control the live NR connection.
- `/diagnostics progress`, `/diagnostics check`, `/diagnostics missing` — ingestion, configuration and topology checks.
- `/raw bit`, `/raw recent`, `/raw trace`, `/raw correlate`, `/raw bytes` — low-level S-Class tools.

### Database

- `/database stats` — file size and row counts.
- `/database optimise` — ANALYZE/optimize, optional purge and VACUUM.
- `/database export` — export the SQLite database, mappings and topology files.
- `/database import` — import a supported database, CSV or ZIP backup.

The raw correlation commands are exploratory diagnostics only. Their timing correlations are not proof of a physical signal aspect.

## Database migration

The existing SQLite database can be retained. Startup creates the new tables and backfills `berth_steps` from the old `pass_log` table.

Legacy tables are retained so old evidence and diagnostic commands continue to work:

```text
pass_log
pass_bit_events
```

They are no longer authoritative for physical signal-state interpretation.

New canonical tables include:

```text
raw_td_messages
feed_state
refresh_history
s_snapshot_differences
berth_steps
t3_bridge_event_meta
t3_bridge_events
signal_observation_sessions
signal_observations
```

Historical `SG` and `SH` rows previously written into `s_bit_events` are ignored by the new protocol analyser, which only uses `SF` rows.

## Environment

Minimum:

```env
DISCORD_TOKEN=your_discord_bot_token
NRBOT_DATA_DIR=/var/lib/nr-bot
NROD_USER=your_network_rail_username
NROD_PASS=your_network_rail_password
NR_ENABLED=true
NR_AREA=T3
```

`METRO_BOT_DATA_DIR` remains accepted as a migration fallback for an existing
installation; do not move the live database merely to rename the variable.

Reliable subscription settings:

```env
NR_DURABLE_SUBSCRIPTION=true
NR_SUBSCRIPTION_ID=metro-nr-bot-t3
# Optional override. By default the client-id is your NROD username/email.
NR_CLIENT_ID=
NR_DURABLE_NAME=metro-nr-bot-t3
```

Snapshot/refresh settings:

```env
NR_SNAPSHOT_STALE_SECONDS=600
NR_REFRESH_MAX_GAP_SECONDS=60
NR_REFRESH_EXPECTED_START_ADDRESS=00
# Lower bound only; the bot also requires enough bytes to cover known_bits.csv.
NR_REFRESH_MIN_BYTES=8
```

Private bridge event retention defaults to seven days:

```env
T3_API_EVENT_RETENTION_DAYS=7
```

Normal feed settings:

```env
NR_TOPIC=/topic/TD_ALL_SIG_AREA
NR_HOST=publicdatafeeds.networkrail.co.uk
NR_PORT=61618
NR_RECONNECT_INITIAL_SECONDS=10
NR_RECONNECT_MAX_SECONDS=300
```

## Running

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run the Discord bot:

```bash
python bot.py
```

The standalone learner remains available:

```bash
python t3_learner_clean.py live --all
```

It also uses durable/client-individual delivery by default. Use `--no-durable` only when required.

## Tests

Run:

```bash
python -m unittest discover -s tests -v
```

The tests cover:

- ordered, contiguous, transactional SG/SH processing including SH payload bytes;
- rejection of partial, gapped, and orphan refreshes;
- snapshot invalidation and first-SF baseline handling;
- STOMP 1.2 ACK handling and malformed poison-frame consumption;
- message deduplication;
- strict timestamp and payload validation;
- one-to-one edge/berth-step matching;
- rejection of the 6239-style short post-step pulse;
- separation of signal mappings from route mappings;
- paired physical observation analysis.

## Protocol references

- S-Class messages: https://wiki.openraildata.com/index.php/S_Class_Messages
- C-Class messages: https://wiki.openraildata.com/index.php/C_Class_Messages
- Durable subscriptions: https://wiki.openraildata.com/index.php/Durable_Subscription
