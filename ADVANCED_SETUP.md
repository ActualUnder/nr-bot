# Advanced Tyneside monitor: setup

This release adds code to your existing NR bot. It has not been deployed to
your LXC or connected to your NR/Discord accounts here. Keep your current data
directory, credentials and private T3 bridge settings.

## 1. Upgrade the application

Back up the application and runtime data first, then stop the existing NR bot
service. Extract this project over its application directory, including the
new `nr_advanced/`, `config/`, `docs/` and `network_monitor.py` files. Do not
replace your live environment file with `env.example`.

Using the bot's existing Python environment:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Python 3.11 or newer is required. Preserve the existing `NRBOT_DATA_DIR` (or
historical `METRO_BOT_DATA_DIR`) so the old learner continues using its database.
The advanced database is created in a separate `network/` subdirectory.

## 2. Enable feeds on the Network Rail account

In your account's feed settings, enable the corresponding products:

| Product | Subscription / access |
|---|---|
| Train Describer | Existing `/topic/TD_ALL_SIG_AREA` |
| Train Movements (TRUST) | `/topic/TRAIN_MVT_ALL_TOC` |
| Temporary Speed Restrictions | `/topic/TSR_ALL_ROUTE` |
| Very Short Term Planning | `/topic/VSTP_ALL` |
| All Reference Data | Allows CORPUS and SMART HTTPS downloads |

These use your existing `NROD_USER` and `NROD_PASS`. Do not add another TD client
with the same durable client ID. This release multiplexes the extra live
subscriptions over the existing STOMP connection. Their subscription IDs and
durable names append `-trust`, `-tsr` and `-vstp` to the existing names.

If a product is not enabled, switch its `NR_*_ENABLED` setting off until access
is available. A rejected subscription may cause the broker to drop the whole
connection. Account access and actual feed availability need live verification.

TSRs are published weekly, Friday at about 06:00 according to the feed
documentation. Subscribing after publication does **not** download the missed
snapshot. Leave the stable durable subscription enabled before publication,
or import a genuine saved TSR message. Next week's WON is retained alongside
this week's; it only applies within its own validity interval. Do not treat
missing TSR data as proof of unrestricted running.

## 3. Add these settings to the existing environment file

```dotenv
NR_ADVANCED_ENABLED=true
NR_AREA=T3
NR_AREAS=T0,T1,T2,T3
NR_TRUST_ENABLED=true
NR_TSR_ENABLED=true
NR_VSTP_ENABLED=true
NR_REFERENCE_AUTO_REFRESH=true
NR_DURABLE_SUBSCRIPTION=true
NR_MAP_ENABLED=true
NR_MAP_BIND=127.0.0.1
NR_MAP_PORT=8790
```

`NR_AREA=T3` still selects the legacy signal learner. `NR_AREAS` selects the
independent wider map. They intentionally have different jobs.

| Area | Initial scope |
|---|---|
| T0 | TYROC Ashington workstation / Northumberland line |
| T1 | TYROC Newcastle / Gateshead |
| T2 | TYROC Darlington and adjoining approaches |
| T3 | TYROC Sunderland, including the supplied Pelaw/South Hylton mappings |

TD feed availability and observed routes determine actual coverage. This is
not all of the Nexus Metro network, and no external adjacent area or handover
is guessed. Add other verified TD IDs to `NR_AREAS` when you want to extend it.

Restart your existing NR bot service, then run `/network status` in Discord.
`/status` continues to provide the detailed legacy T3 learner/bridge status.
New slash commands may take time to appear after a global Discord sync; your
existing `DISCORD_GUILD_ID` can be used for a faster guild sync.

## 4. View the map on your phone

The default URL is `http://127.0.0.1:8790` on the bot computer. For phone access,
bind to the LXC's reachable private LAN address or use Tailscale. Generate a
separate map key:

```bash
python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

Set the following with your actual address and generated key:

```dotenv
NR_MAP_BIND=YOUR_LXC_PRIVATE_IP
NR_MAP_TOKEN=YOUR_GENERATED_MAP_KEY
NR_MAP_PUBLIC_URL=http://YOUR_LXC_PRIVATE_IP:8790
```

Restart the bot. Open `/network map`, follow the URL and enter the map key.
The server refuses a non-loopback bind without a key of at least 24 characters.
The browser never receives Network Rail or Discord credentials. API access
uses an HttpOnly session cookie, or a Bearer key for your own read-only client.

Use Tailscale for encrypted remote access rather than forwarding this HTTP
port onto the internet. An HTTPS reverse proxy is also possible; preserve the
request host and configure the correct public URL. The existing T3 mTLS bridge
remains on its own endpoint and does not share the browser access key.

### Reading the diagram

- Choose T0/T1/T2/T3 or All. All displays separate area panels; it does not imply
  cross-area topology.
- Search for a headcode, `T3:6239`, or a place such as East Boldon. Click a result
  to zoom to it. Drag to pan; use the wheel, +/−, Fit or keyboard arrows.
- Solid links have observed CA steps. Dashed links are supplied/SMART references.
  SMART is a reporting-offset dataset, not a complete set of physical tracks.
- Green occupied berths are fresh descriptions in a currently live area. Faded
  amber occupations are unconfirmed after gaps, old updates or a view outage.
- Empty/unseen descriptions do not prove that track circuits are clear. S-Class
  bits are not automatically interpreted as signal aspects on this map.
- An animation represents a received berth step. It is not GPS interpolation
  or proof that the train physically passed a signal at that exact instant.
- Conflicting labels are flagged. The supplied catalogue contains ambiguities
  such as `6298` and `6217`; this upgrade preserves and exposes them.
- T1/T0/T2 can initially be empty. SMART imports and actual C-Class traffic add
  berths. Unseen links cannot be reconstructed from silence. Use
  `config/tyneside.json` to refine schematic positions after verifying IDs.

## 5. Delay and restriction analysis

There are two independently labelled sources of evidence:

1. **TD timing:** consecutive CA steps for the same description and a continuous
   connection establish berth-to-berth elapsed time. At least eight samples on
   the same directed edge and train class are needed by default. A new traversal
   must be at least 1.8× the median and 45 seconds slower before being flagged.
   Interposes, reconnects, out-of-order data and gaps cannot create a sample.
2. **TRUST running time:** a departure and next arrival at different, mapped
   locations are compared against their planned times. A gain of two minutes
   is flagged by default. Station dwell and off-route reports are excluded.
   Corrections replace the logical report and can remove an invalid finding.

The seeded corridor between Sunderland and Newcastle includes East Boldon, so
a loss of time on that corridor can surface an applicable East Boldon TSR.
Restriction endpoints, aliases, corridor membership, validity and known
direction are checked. Exact route-code/mileage/line overlap is **not** yet a
verified infrastructure join; all matches remain possible contributors. Other
causes, emergency restrictions and operational allowances may apply.

TD elapsed time is not mph. Baselines include operational stopping patterns,
and require comparable real observations. Samples with a matching restriction
are excluded from the normal baseline. No arbitrary baseline is assumed on
first startup. Baselines are not retrospectively reclassified if a restriction
snapshot arrives after those samples were recorded.

The direct TRUST feed has documented BST timestamp quirks. The default
`NR_TRUST_CLOCK=network-rail` corrects affected wall-time fields while leaving
activation/queue, TD and TSR UTC timestamps unchanged. Set `utc` only if your
upstream source already normalises TRUST timestamps. The repeated hour at the
autumn clock change is inherently ambiguous in those affected fields.

Headcodes can repeat between areas and runs. The detail view lists dated
**candidate** TRUST activations; it does not silently attach one unrelated
train's timetable or delay to a berth. Metro headcodes may have no TRUST run.
VSTP provides short-notice plans; it is not the complete working timetable.

## 6. Optional automatic Discord alerts

```dotenv
NR_ALERT_CHANNEL_ID=YOUR_CHANNEL_ID
NR_ALERT_MIN_TRAINS=3
```

Blank channel ID means no automatic posts. With a channel configured, three
distinct descriptions showing the same section issue within 15 minutes are
required by default. There is a 15-minute section cooldown, persisted sent
state and feed-freshness gating. The bot needs permission to view and send to
that channel. Restriction text always describes a possible contribution.
No automatic messages have been sent while developing this release.

## 7. Offline imports, backups and demonstration

```bash
python network_monitor.py status
python network_monitor.py import corpus /path/to/CORPUS.json
python network_monitor.py import smart /path/to/SMART.json
python network_monitor.py import tsr /path/to/genuine-tsr-message.json
python network_monitor.py import schedule /path/to/schedule-json-lines.gz
python network_monitor.py backup /path/to/backups/network-2026-10-02.sqlite
python network_monitor.py demo
```

Imports and backup accept `--data-dir /path/to/network` for an alternate
network database. Imports retain good data on invalid snapshots, report
rejections and do not disconnect the running monitor. `schedule` expects the
Network Rail JSON-lines extract, not CIF. It stores schedule candidates and
delete tombstones; it does not claim to resolve the full STP timetable or
generate public arrival predictions. Periodic SCHEDULE download is not enabled.

The existing `/database export` and `/database import` remain **T3 learner**
tools. Back up the new database with the command above; do not upload it into
the old T3 database importer. Stop the bot before manually restoring a network
backup to `NRBOT_DATA_DIR/network/network.sqlite` and handle its WAL/SHM files
as part of that stopped-database restore. Never copy a live SQLite main file
alone as a backup.

The demo runs at `http://127.0.0.1:8791`, uses fresh temporary data and contacts
neither Network Rail nor Discord. Example T0/T1/T2 berth IDs, trains and the
East Boldon TSR in it are synthetic and explicitly marked. Do not treat them
as a supplied real infrastructure map or a real restriction.

Detailed logs remain on your bot service. `/network status` and the map source
cards show reference-download failures, stale areas and rejected input counts.
All new history is pruned after 14 days by default; learned topology remains.
After CORPUS is available, nationwide TRUST movement reports are filtered to
configured locations, SMART coverage and already-followed trains. Nationwide
activations are retained to avoid losing inbound train identity. Monitor disk
usage and adjust `NR_NETWORK_RETENTION_DAYS` to your LXC's capacity.
