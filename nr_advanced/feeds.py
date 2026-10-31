from __future__ import annotations

import datetime as dt
import json
import math
import re

from .protocol import (InvalidFeedMessage, UK, berth, clean, fingerprint, is_true,
                       location_key, timestamp, trust_timestamp)


def ingest_tsr(store, item, now):
    wrapper = item.get("TSRBatchMsgV1")
    if not isinstance(wrapper, dict) or not isinstance(wrapper.get("TSRBatchMsg"), dict):
        raise InvalidFeedMessage("expected TSRBatchMsgV1.TSRBatchMsg")
    batch = wrapper["TSRBatchMsg"]
    group = clean(batch.get("routeGroupCode"))
    if not group or batch.get("routeGroupCoverage", "").lower() != "full":
        raise InvalidFeedMessage("TSR batch needs a route group and full coverage")
    start, end = timestamp(batch.get("WONStartDate")), timestamp(batch.get("WONEndDate"))
    published = timestamp(batch.get("publishDate"))
    if start >= end or published > now + 300:
        raise InvalidFeedMessage("invalid TSR publication/validity interval")
    rows = batch.get("tsr")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise InvalidFeedMessage("TSR batch must contain a tsr list; absent is not an empty snapshot")
    parsed, ids = [], set()
    bid = fingerprint([group, start, end])
    for row in rows:
        rid = clean(row.get("TSRID"))
        if not rid or rid in ids:
            raise InvalidFeedMessage("missing or duplicate TSRID")
        ids.add(rid)
        valid_from, valid_to = timestamp(row.get("ValidFromDate")), timestamp(row.get("ValidToDate"))
        if valid_to <= valid_from:
            raise InvalidFeedMessage("invalid individual TSR validity")
        detail = {k: row.get(k) for k in (
            "TSRID", "TSRReference", "RouteCode", "FromLocation", "ToLocation", "LineName", "Direction",
            "MileageFrom", "MileageTo", "SubunitFrom", "SubunitTo", "SubunitType", "MovingMileage",
            "PassengerSpeed", "FreightSpeed", "Reason", "Comments")}
        if not clean(detail["FromLocation"]) or not clean(detail["ToLocation"]):
            raise InvalidFeedMessage("TSR locations missing")
        parsed.append((bid, rid, valid_from, valid_to, json.dumps(detail)))
    previous = store.db.execute("SELECT published FROM tsr_batches WHERE id=?", (bid,)).fetchone()
    if previous and previous[0] >= published:
        return True
    # Replace only this route group's WON interval. Next week's publication
    # must not retire restrictions in the currently applicable WON.
    store.db.execute("INSERT OR REPLACE INTO tsr_batches VALUES(?,?,?,?,?,?,?,?)",
                     (bid, group, published, start, end, clean(batch.get("publishSource")), "full", now))
    store.db.execute("DELETE FROM restrictions WHERE batch_id=?", (bid,))
    store.db.executemany("INSERT INTO restrictions VALUES(?,?,?,?,?)", parsed)
    store._touch("tsr", now, published)
    return True


def _headcode(train_id):
    return train_id[2:6].upper() if len(train_id) == 10 else ""


def _run_for(store, train_id, at):
    # The same TRUST ID can recur next month. Origin date is part of our key.
    return store.db.execute(
        "SELECT * FROM runs WHERE (train_id=? OR current_id=?) AND origin_at BETWEEN ? AND ? "
        "ORDER BY origin_at DESC LIMIT 1", (train_id, train_id, at - 2 * 86400, at + 12 * 3600)).fetchone()


def _stanox(body):
    reporting = clean(body.get("reporting_stanox"))
    if reporting == "00000":
        return clean(body.get("original_loc_stanox") or body.get("loc_stanox"))
    return clean(body.get("loc_stanox") or reporting)


def ingest_trust(store, item, now):
    header, body = item.get("header"), item.get("body")
    if not isinstance(header, dict) or not isinstance(body, dict):
        raise InvalidFeedMessage("TRUST header/body missing")
    kind = clean(header.get("msg_type"))
    if kind not in {"0001", "0002", "0003", "0005", "0006", "0007", "0008"}:
        return False
    queued = timestamp(header.get("msg_queue_timestamp"))
    train_id = clean(body.get("train_id"), 10)
    if len(train_id) != 10:
        raise InvalidFeedMessage("invalid TRUST train_id")
    db = store.db
    mode = store.config.trust_clock
    if kind == "0001":
        origin = timestamp(body.get("origin_dep_timestamp"))  # genuine UTC
        activation = timestamp(body.get("creation_timestamp"))
        rid = train_id + ":" + dt.datetime.fromtimestamp(origin, UK).date().isoformat()
        old = db.execute("SELECT activated_at FROM runs WHERE id=?", (rid,)).fetchone()
        if old and old[0] >= activation:
            return True
        db.execute("INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                   "uid=excluded.uid,activated_at=excluded.activated_at,details=excluded.details",
                   (rid, train_id, train_id, _headcode(train_id), clean(body.get("train_uid")),
                    origin, activation, activation, "activated", json.dumps(body)))
        # Reports can precede a delayed activation. Attach only this run window.
        db.execute("UPDATE trust_events SET run_id=? WHERE train_id=? AND run_id IS NULL AND event_at BETWEEN ? AND ?",
                   (rid, train_id, origin - 12 * 3600, origin + 2 * 86400))
        latest = db.execute("SELECT * FROM trust_events WHERE run_id=? ORDER BY event_at DESC,queued_at DESC LIMIT 1", (rid,)).fetchone()
        if latest:
            _apply_run_event(db, rid, latest["kind"], latest["event_at"], json.loads(latest["details"]))
        from .analysis import rebuild_trust_findings
        rebuild_trust_findings(store, train_id, rid, now)
        store._touch("trust", now, queued)
        return True
    fields = {"0002": "canx_timestamp", "0003": "actual_timestamp", "0005": "reinstatement_timestamp",
              "0006": "dep_timestamp", "0007": "event_timestamp", "0008": "event_timestamp"}
    raw_at = body.get(fields[kind])
    # Queue time remains the fallback for administrative reports without an
    # event timestamp; never substitute wall-clock receipt for movement times.
    if kind == "0003" or raw_at:
        at = (trust_timestamp(raw_at, mode) if kind in {"0002", "0003", "0006"} else timestamp(raw_at))
    else:
        at = queued
    planned = trust_timestamp(body.get("planned_timestamp"), mode, optional=True) if kind == "0003" else None
    run = _run_for(store, train_id, at)
    rid = run["id"] if run else None
    stanox = _stanox(body)
    if kind == "0003" and db.execute("SELECT 1 FROM corpus LIMIT 1").fetchone():
        from .analysis import canonical_locations
        names = [r[0] for r in db.execute("SELECT name FROM corpus WHERE stanox=?", (stanox,))]
        regional = bool(canonical_locations(store, names)) or bool(db.execute("SELECT 1 FROM smart WHERE stanox=? LIMIT 1", (stanox,)).fetchone())
        already_tracked = rid and db.execute("SELECT 1 FROM trust_events WHERE run_id=? LIMIT 1", (rid,)).fetchone()
        in_td = db.execute("SELECT 1 FROM berths WHERE headcode=? AND event_at>? LIMIT 1", (run["headcode"] if run else _headcode(train_id), now-3600)).fetchone()
        if not (regional or already_tracked or in_td):
            store._touch("trust", now, queued)
            return False
    detail = dict(body)
    detail["source"] = header.get("original_data_source", "")
    detail["headcode"] = run["headcode"] if run else _headcode(train_id)
    detail["delay_minutes"] = None
    if kind == "0003":
        status = clean(body.get("variation_status")).upper()
        if not is_true(body.get("offroute_ind")) and status != "OFF ROUTE":
            try:
                variation = float(body.get("timetable_variation", ""))
                if not math.isfinite(variation):
                    raise ValueError("non-finite timetable variation")
                if status == "EARLY":
                    detail["delay_minutes"] = -abs(variation)
                elif status == "LATE":
                    detail["delay_minutes"] = abs(variation)
                elif status == "ON TIME":
                    detail["delay_minutes"] = 0
            except (ValueError, TypeError):
                pass
        if clean(body.get("event_type")) not in {"ARRIVAL", "DEPARTURE"}:
            raise InvalidFeedMessage("unsupported TRUST movement event_type")
    # Corrections replace their logical planned event, not append false delays.
    eid = fingerprint([train_id, kind, stanox, planned if planned else at,
                       body.get("event_type", "")])
    old = db.execute("SELECT queued_at FROM trust_events WHERE id=?", (eid,)).fetchone()
    if old and old[0] > queued:
        return True
    db.execute("INSERT OR REPLACE INTO trust_events VALUES(?,?,?,?,?,?,?,?,?)",
               (eid, train_id, rid, kind, at, queued, stanox, planned, json.dumps(detail)))
    if run:
        _apply_run_event(db, rid, kind, at, body)
    if kind == "0003":
        from .analysis import rebuild_trust_findings
        rebuild_trust_findings(store, train_id, rid, now)
    store._touch("trust", now, queued)
    return True


def _apply_run_event(db, rid, kind, at, body):
    run = db.execute("SELECT * FROM runs WHERE id=?", (rid,)).fetchone()
    if not run or run["last_at"] > at:
        return
    status = {"0002": "cancelled", "0005": "reinstated", "0006": "origin changed", "0008": "location changed"}.get(kind)
    if kind == "0003":
        status = "terminated" if is_true(body.get("train_terminated")) else "running"
    if kind == "0007":
        new_id = clean(body.get("revised_train_id"), 10)
        if len(new_id) != 10:
            raise InvalidFeedMessage("invalid revised_train_id")
        db.execute("UPDATE runs SET current_id=?,headcode=?,last_at=? WHERE id=?", (new_id, _headcode(new_id), at, rid))
    elif status:
        db.execute("UPDATE runs SET status=?,last_at=? WHERE id=?", (status, at, rid))


def ingest_schedule(store, item, now, source):
    wrapper = item.get("VSTPCIFMsgV1") if source == "vstp" else item
    if source == "schedule" and "JsonScheduleV1" not in item:
        return False  # JSON-lines extracts also contain TIPLOC, association and EOF records.
    schedule = wrapper.get("schedule") if source == "vstp" and isinstance(wrapper, dict) else item.get("JsonScheduleV1")
    if not isinstance(schedule, dict):
        raise InvalidFeedMessage("schedule envelope missing")
    uid = clean(schedule.get("CIF_train_uid"))
    start, end = clean(schedule.get("schedule_start_date")), clean(schedule.get("schedule_end_date"))
    stp = clean(schedule.get("CIF_stp_indicator"))
    try:
        dt.date.fromisoformat(start)
        if end:
            dt.date.fromisoformat(end)
    except ValueError as exc:
        raise InvalidFeedMessage("invalid schedule date") from exc
    if not uid or stp not in {"P", "N", "O", "C"}:
        raise InvalidFeedMessage("schedule UID or STP indicator missing")
    updated = timestamp(wrapper.get("timestamp")) if source == "vstp" else now
    sid = fingerprint([source, uid, start, stp])
    old = store.db.execute("SELECT updated_at FROM schedules WHERE id=?", (sid,)).fetchone()
    if old and old[0] > updated:
        return True
    action = clean(schedule.get("transaction_type")).lower()
    if action not in {"create", "delete", "update"}:
        raise InvalidFeedMessage("unknown schedule transaction_type")
    # Keep delete tombstones so durable old Creates cannot resurrect a schedule.
    detail = {"deleted": action == "delete", "schedule": schedule}
    store.db.execute("INSERT OR REPLACE INTO schedules VALUES(?,?,?,?,?,?,?,?)",
                     (sid, uid, start, end or start, stp, updated, source, json.dumps(detail)))
    store._touch(source, now, updated)
    return True


def reference_rows(data, dataset):
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        key = "TIPLOCDATA" if dataset == "corpus" else "BERTHDATA"
        rows = next((v for k, v in data.items() if k.upper() == key), None)
    else:
        rows = None
    if not isinstance(rows, list) or not rows or not all(isinstance(r, dict) for r in rows):
        raise InvalidFeedMessage(f"invalid or empty {dataset.upper()} reference snapshot")
    return [{k.upper(): v for k, v in r.items()} for r in rows]


def import_reference(store, dataset, data, now):
    rows = reference_rows(data, dataset)
    db = store.db
    parsed = []
    if dataset == "corpus":
        for r in rows:
            stanox, tiploc = clean(r.get("STANOX")), clean(r.get("TIPLOC"))
            name = clean(r.get("NLCDESC") or r.get("NLCDESC16"))
            if name and (stanox or tiploc):
                parsed.append((stanox, tiploc, clean(r.get("3ALPHA")), name, clean(r.get("NLCDESC16"))))
        if not parsed:
            raise InvalidFeedMessage("CORPUS contains no usable locations")
        with store.lock, db:
            db.execute("DELETE FROM corpus")
            db.executemany("INSERT OR REPLACE INTO corpus VALUES(?,?,?,?,?)", parsed)
            store._touch(dataset, now, now)
    elif dataset == "smart":
        if not all("TD" in r and "FROMBERTH" in r and "TOBERTH" in r for r in rows):
            raise InvalidFeedMessage("SMART fields missing")
        for r in rows:
            area = clean(r.get("TD")).upper()
            if area not in store.config.areas:
                continue
            origin, dest = clean(r.get("FROMBERTH")).upper(), clean(r.get("TOBERTH")).upper()
            # Blank endpoints and special/wildcard rows are not physical links.
            if not all(not x or re.fullmatch(r"[A-Z0-9]{1,4}", x) for x in (origin, dest)):
                continue
            event = clean(r.get("EVENT"))
            direction = "up" if event in {"A", "B"} else "down" if event in {"C", "D"} else ""
            parsed.append((area, origin, dest, clean(r.get("STANOX")), clean(r.get("STANME")),
                           direction, clean(r.get("TOLINE") or r.get("FROMLINE")), event,
                           clean(r.get("STEPTYPE")), clean(r.get("PLATFORM"))))
        with store.lock, db:
            db.execute("DELETE FROM smart")
            db.execute("DELETE FROM labels WHERE source='SMART reporting location'")
            db.execute("DELETE FROM edges WHERE source='SMART reference'")
            db.executemany("INSERT OR IGNORE INTO smart VALUES(?,?,?,?,?,?,?,?,?,?)", parsed)
            for area, origin, dest, stanox, name, direction, line, event, step, platform in parsed:
                # Location applies to a reporting event with offsets, not necessarily
                # the exact physical berth. Preserve that qualifier in the UI.
                target = dest if step in {"T", "I", "B"} else origin if step in {"F", "C"} else ""
                corpus = db.execute("SELECT name FROM corpus WHERE stanox=? AND stanox!='' LIMIT 1", (stanox,)).fetchone()
                label = corpus[0] if corpus else name
                if target and label:
                    db.execute("INSERT OR IGNORE INTO labels VALUES(?,?,?,?)", (area, target, label, "SMART reporting location"))
                if origin and dest and step == "B":
                    db.execute("INSERT OR IGNORE INTO edges VALUES(?,?,?,'SMART reference',0,NULL)", (area, origin, dest))
            store._touch(dataset, now, now)
    else:
        raise InvalidFeedMessage("dataset must be corpus or smart")
    return len(parsed)
