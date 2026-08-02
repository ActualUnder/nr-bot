# Private T3 bridge setup

This release links the two bots with two read-only endpoints:

```text
GET https://<NR private IP>:8765/v1/t3/snapshot
GET https://<NR private IP>:8765/v1/t3/events?after=<cursor>&limit=<n>
```

The snapshot exposes current-connection T3 berth occupations only. The event
endpoint exposes the retained, cursor-addressed `CA`/`CC`/`CB` movement log.
There is no SQL,
write, restart, upload, signal-control or credential endpoint.

## Safety model

- The endpoint is HTTPS only.
- The NR server certificate is checked by Metro.
- The Metro client certificate is checked by NR.
- The authorised client certificate common name is `metro-bot`.
- The listener refuses wildcard and public literal IP addresses.
- The endpoint is restricted to signal area `T3`.
- C-Class data is treated as a conservative delta snapshot.
- Persisted occupations from an earlier feed connection are withheld.
- An NR position is positive evidence only. Missing NR data never proves that
  a Metro train is absent or out of service.
- Metro keeps passenger-service state (POP) separate from physical train state
  (T3). T3 presence cannot label a train as carrying passengers.
- Metro's new T3 operational alerts are built but disabled by default. They
  require both a global Metro environment opt-in and a per-Discord-server
  category opt-in.

The supplied catalogue contains the normal Pelaw–South Hylton berths, the
Hebburn–Jarrow TWPS chain, and the supplied main-line boundary berths.

## 1. Private Proxmox network

Create an internal Proxmox bridge with no physical port or default gateway and
attach only the NR and Metro guests. One example addressing plan is:

| Guest | Private address |
|---|---|
| NR bot | `10.77.0.1/30` |
| Metro bot | `10.77.0.2/30` |

Keep the guests' existing normal network interfaces for Discord and external
API access. The new interface is only for the bridge.

Apply firewall rules at Proxmox and/or inside the NR guest:

- Allow TCP `8765` to `10.77.0.1` from `10.77.0.2`.
- Deny TCP `8765` from every other source.
- Do not forward or route the `10.77.0.0/30` bridge externally.

The application also refuses `0.0.0.0`, `::` and publicly routable literal
addresses, but the firewall remains a separate protection layer.

## 2. Create the private certificates

Run this on an administrative machine, not inside either bot's Git checkout:

```bash
chmod +x scripts/create_bridge_certs.sh
./scripts/create_bridge_certs.sh ./bridge-certs nr-bot 10.77.0.1 metro-bot
```

The script creates:

```text
bridge-certs/
  authority/ca.crt
  authority/ca.key       KEEP OFFLINE
  nr-bot/ca.crt
  nr-bot/server.crt
  nr-bot/server.key
  metro-bot/ca.crt
  metro-bot/client.crt
  metro-bot/client.key
```

Do not copy `authority/ca.key` to either guest and do not commit any generated
certificate directory to Git.

Copy only the following files:

### NR guest

```text
/etc/nr-bot/tls/ca.crt
/etc/nr-bot/tls/server.crt
/etc/nr-bot/tls/server.key
```

### Metro guest

```text
/etc/metro-bot/tls/ca.crt
/etc/metro-bot/tls/client.crt
/etc/metro-bot/tls/client.key
```

Make each private key readable only by its bot service account. For example,
substitute the actual users used by your services:

```bash
sudo chown -R nr-bot:nr-bot /etc/nr-bot/tls
sudo chmod 700 /etc/nr-bot/tls
sudo chmod 600 /etc/nr-bot/tls/server.key
sudo chmod 644 /etc/nr-bot/tls/server.crt /etc/nr-bot/tls/ca.crt
```

```bash
sudo chown -R metro-bot:metro-bot /etc/metro-bot/tls
sudo chmod 700 /etc/metro-bot/tls
sudo chmod 600 /etc/metro-bot/tls/client.key
sudo chmod 644 /etc/metro-bot/tls/client.crt /etc/metro-bot/tls/ca.crt
```

## 3. Configure the NR bot

Use the NR bot's environment file:

```env
NRBOT_DATA_DIR=/var/lib/nr-bot

T3_API_ENABLED=true
T3_API_BIND=10.77.0.1
T3_API_PORT=8765
T3_API_SERVER_CERT=/etc/nr-bot/tls/server.crt
T3_API_SERVER_KEY=/etc/nr-bot/tls/server.key
T3_API_CLIENT_CA=/etc/nr-bot/tls/ca.crt
T3_API_ALLOWED_CLIENT_CN=metro-bot
T3_API_STALE_SECONDS=180
T3_API_EVENT_RETENTION_DAYS=7
```

Existing installs can temporarily retain `METRO_BOT_DATA_DIR`; it remains a
migration fallback. New installs should use `NRBOT_DATA_DIR`.

When enabled, missing or invalid TLS files leave the API offline and `/status`
shows the exact startup error. It never falls back to plain HTTP.

## 4. Configure the Metro bot

Use the Metro bot's environment file:

```env
NR_BRIDGE_ENABLED=true
NR_BRIDGE_URL=https://10.77.0.1:8765/v1/t3/snapshot
NR_BRIDGE_CA_CERT=/etc/metro-bot/tls/ca.crt
NR_BRIDGE_CLIENT_CERT=/etc/metro-bot/tls/client.crt
NR_BRIDGE_CLIENT_KEY=/etc/metro-bot/tls/client.key
NR_BRIDGE_TIMEOUT_SECONDS=3
NR_BRIDGE_CACHE_SECONDS=20
NR_BRIDGE_MAX_SNAPSHOT_AGE_SECONDS=180
```

The generated NR certificate includes `10.77.0.1` as an IP subject alternative
name, so normal hostname verification works with this URL. Do not disable
hostname or CA verification.

## 5. Restart and verify

Restart the NR bot first, then the Metro bot. On the NR Discord bot, `/status`
should show:

```text
Private T3 bridge API
Enabled: true
Listening: true
```

From the Metro guest, an authenticated test is:

```bash
curl --fail --silent --show-error \
  --cacert /etc/metro-bot/tls/ca.crt \
  --cert /etc/metro-bot/tls/client.crt \
  --key /etc/metro-bot/tls/client.key \
  https://10.77.0.1:8765/v1/t3/snapshot \
  | python -m json.tool
```

A request without the client certificate must fail during the TLS handshake.

Verify the cursor stream separately:

```bash
curl --fail --silent --show-error \
  --cacert /etc/metro-bot/tls/ca.crt \
  --cert /etc/metro-bot/tls/client.crt \
  --key /etc/metro-bot/tls/client.key \
  'https://10.77.0.1:8765/v1/t3/events?after=0&limit=20' \
  | python -m json.tool
```

Metro refreshes the snapshot and consumes outstanding events during its normal
train poll, before deciding whether a POP train has disappeared. `/active`,
`/train`, `/map` and `/status` show source-aware results. `/t3` contains the
`live`, `train`, `traffic`, `compare`, `history` and `health` subcommands.

If POP removes a booked train while T3 still detects it, Metro describes it as
physically tracked but absent from passenger tracking—likely withdrawn from
passenger service, with a possible fault that is explicitly not confirmed.
That inference does not automatically create a confirmed unit-failure record;
normal failure review/evidence is still required. T3 absence still proves
nothing.

On a test Metro instance, operational alerts can be enabled with:

```env
NR_T3_ALERTS_ENABLED=true
```

Then use Metro's `/alerts` command to enable **T3 Operational Analysis (Test)**
only on the intended Discord test server. Both switches are off after an
upgrade, so production servers do not start receiving these alerts by accident.

Existing generic POP disappearance alerts continue as before. The extra T3
withdrawal/possible-fault interpretation is selected per guild and is only
rendered where that T3 category is enabled.

The event table is retained for `T3_API_EVENT_RETENTION_DAYS` (seven days by
default). A fresh Metro install can backfill retained movements. If a cursor is
older than the retained range, the response marks it expired and Metro uses the
snapshot as its recovery baseline before continuing.

## Snapshot freshness after restart

Network Rail C-Class messages update individual berths rather than providing a
guaranteed complete berth image after every reconnect. The database therefore
stores a connection generation:

1. A new NR connection increments the generation.
2. Every CA, CB or CC berth update is stamped with that generation.
3. The API returns occupied rows only from the current generation.
4. Earlier occupations are counted as withheld, not returned.

The snapshot will fill naturally as trains berth-step or are interposed in the
new connection. This is deliberately safer than returning a complete-looking
but potentially stale map.

## Supplied description ambiguities

The supplied CSVs contain alternative labels for berths `6298` and `6217`, and
two descriptions for movement `6209 → 6217`. The API preserves these under
`description_alternatives`; it does not silently choose and delete one. Raw
berth IDs, headcodes and timestamps remain authoritative.

## Headcode conversion

The Metro convention is applied in one place:

| NR headcode | Metro TDN |
|---|---|
| `2I01` | `T101` |
| `2I10` | `T110` |
| `2I51` | `T151` |
| `2I92` | `T192` |

Non-`2Ixx` trains remain in the snapshot with `tdn: null`. This allows future
live train-ahead analysis without pretending every non-Metro headcode is
freight.
