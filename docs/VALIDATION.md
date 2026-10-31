# Release validation — 16 October 2026

Validated locally with Python 3.12. The application requires Python 3.11 or
newer. No Network Rail or Discord account was connected for these checks.

## Automated checks

`python -m unittest discover -s tests -v` completed successfully: **58 tests**.
This includes the existing protocol and private T3 bridge tests, plus:

- TD area isolation, leading zeroes, replay, ordering, reconnect freshness,
  malformed items, transaction rollback and supported timing baselines.
- Current/future TSR windows, snapshot replacement, malformed batch retention,
  direction/location matching and the Sunderland–Newcastle/East Boldon example.
- TRUST corrections, late activations, candidate identities, cancellations,
  off-route/dwell exclusions and the documented BST timestamp handling.
- CORPUS/SMART imports, VSTP tombstones, disabled feed reporting, web access
  controls, private/Tailscale binds and configured HTTPS reverse proxy support.
- Bot message routing and acknowledgement after successful processing.

JavaScript syntax checks passed. The new Discord command group registered in
an offline import smoke check without connecting a Discord client.

## Browser checks

The isolated synthetic demo was exercised in Chromium at desktop and phone
sizes. Area filters, place/headcode search, berth detail, map resize behaviour,
and mobile page width passed. No browser JavaScript errors were reported.
The demo contains synthetic trains, extra-area berth IDs and an example TSR;
these are not evidence of live network coverage or a real restriction.

## Deployment verification still required

After following `ADVANCED_SETUP.md`, check `/network status` for successful TD,
TRUST, TSR and VSTP subscriptions and CORPUS/SMART downloads. A new TSR
subscription may need to wait for the next weekly publication. Check an actual
berth step in each configured area, map access from the intended device and
the existing T3 bridge consumer before relying on the service.

The archive contains no credentials, live database, browser dependency or QA
screenshot. The original T3 learner, bridge implementation, reference CSVs and
their existing tests were preserved unchanged.
