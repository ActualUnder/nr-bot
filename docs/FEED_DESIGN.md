# Feed model and implementation notes

## Modules and ownership

`bot.py` retains the original Discord service and T3 learner. Its single STOMP
connection now dispatches subscription IDs to `nr_advanced`. The T3 learner
receives TD only, and still filters its own `NR_AREA`. Its database schema and
the private HTTPS/mTLS bridge were not changed.

| Module | Responsibility |
|---|---|
| `config.py` | Independent area list, thresholds, feed switches and map settings |
| `store.py` | Separate SQLite WAL database, atomic ingestion, deduplication, TD state, history and baselines |
| `feeds.py` | TRUST, TSR, VSTP/SCHEDULE and reference snapshot adapters |
| `analysis.py` | Candidate restriction matching, TRUST report corrections, snapshot and train views |
| `runtime.py` | Background reference refresh, retention and map lifecycle |
| `web.py` / `web/` | Read-only authenticated aiohttp API and dependency-free SVG interface |
| `discord_commands.py` | `/network` commands and optional coalesced alerts |
| `demo.py` | Isolated, labelled synthetic examples |

## Consistency and freshness

The advanced writer uses a reentrant lock and one SQLite connection. A frame's
dedup hashes and accepted changes commit together with `synchronous=FULL`.
Malformed individual items are counted and skipped inside savepoints; a
database failure rolls back the frame and leaves it unacknowledged. The listener
reconnects after processing failures to permit durable redelivery. A malformed
outer JSON frame is recorded and consumed rather than blocking the subscription.

Every berth key includes the area. Leading zeroes in berth IDs are preserved.
There is no global one-headcode-one-train assumption. A connection generation
invalidates previous positive occupations, and source event timestamps stop
delayed broker traffic from becoming fresh merely because it was received now.
Out-of-order steps/clears cannot overwrite newer endpoint states. A mismatching
CB does not remove another description's occupation.

TD is a delta feed. There is no invented complete occupancy snapshot after
reconnect. A stationary train may remain unconfirmed until a fresh event.
The browser also marks the whole view unconfirmed if polling stops; serving an
old page is not evidence that the feed is still live.

The advanced module does not expand S-Class aspect interpretation to new
areas. Existing verified/unverified T3 mapping rules remain authoritative.

## Timing and identity

TD samples require consecutive CA events with continuity through the same
berth, headcode and connection generation, event times at/after connection,
and elapsed times between 3 and 1,200 seconds. The comparison population is
the previous 200 samples within retention, keyed by area, directed edge and
headcode's first character. Samples matched to a restriction at ingestion are
excluded from the baseline. Observed timing differences may include dwell,
signalling delay or feed behaviour. They are never converted to mph.

TRUST runs are keyed by activation ID plus the origin departure date in London
time. The origin timestamp and queue timestamp are UTC. Direct TRUST movement
and specified cancellation/origin-change fields have the documented BST
wall-time correction, configurable for already-normalised input. Timetable
variation is signed using EARLY/LATE, and OFF ROUTE is never taken as zero delay.

A logical movement key includes train ID, event type, location and planned
timestamp. It does not depend on whether the activation has arrived yet.
Later queue timestamps can correct that report. A late activation attaches
orphan reports within a bounded run window. Comparison uses a departure and
the next arrival at a different mapped location; it excludes station dwell,
off-route records, negative intervals and unknown/ambiguous seeded corridors.

Candidate TRUST runs shown for a TD headcode are explicitly not identity proof.
No automatic per-berth delay colour uses an ambiguous TRUST candidate. VSTP
schedules and optional SCHEDULE records retain UID/start-date/STP/source keys
and deletion tombstones. The UI displays applicable candidates; full STP
overlay resolution and passenger predictions are outside this release.

## TSR matching and limits

TSRBatchMsgV1 is parsed using the published schema. Full route-group batches
are replaced atomically, separately for each WON interval, only by a newer
publication. Empty full batches can retire prior entries. Missing lists,
duplicate IDs, invalid dates and partial coverage are rejected; a malformed
batch cannot erase the last valid snapshot. Upcoming WONs do not replace the
currently applicable one.

A candidate restriction must satisfy its individual date range and its WON
date range, from a recent applicable publication. Matching uses full location
names, curated aliases and unambiguous corridor membership. When a movement's
direction is known, an explicitly opposite-direction TSR is excluded.

This is not an infrastructure-mileage join. SMART line codes, textual WON line
names and route codes are not assumed interchangeable. Exact line, mileage,
diversion, allowance and infrastructure effects need additional verified
data. The output says **possible contributor**, with publication provenance
and the uncertainty. It never reports a confirmed cause from mere coincidence.

## Map coverage and provenance

Supplied T3 labels and links load unchanged, including conflicting labels.
Manually seeded drawing positions in `config/tyneside.json` are schematic.
Unknown IDs are shown as extensions instead of being assigned invented
geographic coordinates. Other areas are laid out deterministically from
reference/observed graph connections. No cross-area edges are guessed.

SMART rows are reporting relationships with timing offsets. Only direct
between-berth references are drawn as dashed links; real observed CA links are
solid. They do not form a guaranteed complete physical track plan. A SMART
label is explicitly a reporting location, and all nodes disclose its source.

The weekly TSR list may omit subsequent emergency changes. Missing batches,
reference access failures, absent timing baselines and feed outages remain
visible as missing evidence, not assertions that the railway is normal.

## Source documentation consulted

- [Network Rail: open data feeds](https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/)
- [About the Network Rail feeds](https://wiki.openraildata.com/index.php/About_the_Network_Rail_feeds)
- [List of Train Describers](https://wiki.openraildata.com/index.php/List_of_Train_Describers)
- [TSR schema, weekly publication and topic](https://wiki.openraildata.com/index.php/TSR)
- [Train Movements, topics and timestamp caveat](https://wiki.openraildata.com/index.php/Train_Movements)
- [Train Activation](https://wiki.openraildata.com/index.php/Train_Activation)
- [Train Movement](https://wiki.openraildata.com/index.php/Train_Movement)
- [Train Cancellation](https://wiki.openraildata.com/index.php/Train_Cancellation)
- [Change of Identity](https://wiki.openraildata.com/index.php/Change_of_Identity)
- [VSTP](https://wiki.openraildata.com/index.php/VSTP)
- [CORPUS and SMART reference data](https://wiki.openraildata.com/index.php/Reference_data)

Documentation was checked on 2 October 2026. Some wiki pages returned HTTP 403
on direct opening; indexed page content and Network Rail's own feed catalogue
were used to verify the documented interfaces. No private feeds or real user
credentials were available during development.
