# Discord command migration

The command functions are unchanged; only their Discord layout has been simplified.

| Old command | New command |
|---|---|
| `/nr_start` | `/feed start` |
| `/nr_stop` | `/feed stop` |
| `/nr_restart` | `/feed restart` |
| `/signal` | `/signal show` |
| `/report` | `/signal analyse` |
| `/known` | `/signal mappings` |
| `/observe_signal` | `/signal observe` |
| `/observations` | `/signal observations` |
| `/route_bits` | `/signal routes` |
| `/moves` | `/td moves` |
| `/berths` | `/td berths` |
| `/bit` | `/raw bit` |
| `/recent_bits` | `/raw recent` |
| `/bit_trace` | `/raw trace` |
| `/bit_correlate` | `/raw correlate` |
| `/bytes` | `/raw bytes` |
| `/progress` | `/diagnostics progress` |
| `/check` | `/diagnostics check` |
| `/missing` | `/diagnostics missing` |
| `/db_stats` | `/database stats` |
| `/db_optimise` | `/database optimise` |
| `/download` | `/database export` |
| `/upload` | `/database import` |

`/status` remains unchanged. `/help` is new.

Discord may briefly show cached old global commands after the first restart. Guild-scoped commands normally update immediately; global command caches can take longer to disappear.
