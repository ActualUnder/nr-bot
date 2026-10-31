from __future__ import annotations

import datetime as dt
import json
import re
import time
from collections import defaultdict

from .config import AREA_NAMES
from .protocol import UK, clean, is_true, location_key


def geography(store):
    # Reload only when changed; an operator can refine names/layout without
    # replacing the observed movement graph or the legacy mapping files.
    path = store.config.app_dir / "config" / "tyneside.json"
    modified = path.stat().st_mtime if path.exists() else 0
    cached = getattr(store, "_geo_cache", None)
    if cached is None or cached[0] != modified:
        data = json.loads(path.read_text()) if modified else {"aliases": {}, "corridors": [], "layout": {}}
        store._geo_cache = (modified, data)
    return store._geo_cache[1]


def canonical_locations(store, labels):
    aliases = geography(store).get("aliases", {})
    found = set()
    for label in labels:
        normalized = " " + location_key(label) + " "
        for name, variants in aliases.items():
            if any(" " + location_key(x) + " " in normalized for x in [name, *variants]):
                found.add(name)
    return sorted(found)


def segment_locations(store, area, origin, dest):
    labels = store.db.execute("SELECT description FROM labels WHERE area=? AND berth IN (?,?)", (area, origin, dest)).fetchall()
    rows = store.db.execute("SELECT s.*,c.name AS full_name FROM smart s LEFT JOIN corpus c "
                            "ON c.stanox=s.stanox AND c.stanox!='' WHERE area=? AND origin=? AND destination=?",
                            (area, origin, dest)).fetchall()
    names = canonical_locations(store, [r[0] for r in labels] + [r["full_name"] or r["name"] for r in rows])
    # Also allow exact full SMART/CORPUS names outside the seeded corridors.
    names = sorted(set(names + [r["full_name"] or r["name"] for r in rows if r["full_name"] or r["name"]]))
    dirs = {r["direction"] for r in rows if r["direction"]}
    lines = {r["line"] for r in rows if r["line"]}
    return names, next(iter(dirs)) if len(dirs) == 1 else "", next(iter(lines)) if len(lines) == 1 else ""


def active_restrictions(store, at):
    # A new snapshot with zero restrictions still supersedes older snapshots.
    batches = store.db.execute("SELECT * FROM tsr_batches WHERE valid_from<=? AND valid_to>=? AND published>=? "
                               "ORDER BY published DESC", (at, at, at - 9 * 86400)).fetchall()
    seen, output = set(), []
    for batch in batches:
        if batch["route_group"] in seen:
            continue
        seen.add(batch["route_group"])
        for row in store.db.execute("SELECT * FROM restrictions WHERE batch_id=? AND valid_from<=? AND valid_to>=?",
                                    (batch["id"], at, at)):
            value = json.loads(row["details"])
            value.update(id=row["id"], published_at=batch["published"], publication=batch["source"],
                         won_start=batch["valid_from"], won_end=batch["valid_to"],
                         valid_from=row["valid_from"], valid_to=row["valid_to"])
            output.append(value)
    return output


def _location_matches(store, name, restriction):
    variants = [name, *geography(store).get("aliases", {}).get(name, [])]
    targets = [" " + location_key(restriction.get(k, "")) + " " for k in ("FromLocation", "ToLocation")]
    direct = any(len(location_key(v)) > 3 and " " + location_key(v) + " " in t for v in variants for t in targets)
    if direct:
        return True
    start = canonical_locations(store, [restriction.get("FromLocation", "")])
    end = canonical_locations(store, [restriction.get("ToLocation", "")])
    if len(start) == len(end) == 1:
        return name in corridor_section(store, start[0], end[0])
    return False


def match_restrictions(store, names, at, direction="", line=""):
    output = []
    for row in active_restrictions(store, at):
        matched = [name for name in names if _location_matches(store, name, row)]
        if not matched:
            continue
        rd = clean(row.get("Direction")).lower()
        if direction and rd in {"up", "down"} and rd != direction.lower():
            continue
        # Short SMART line codes cannot safely be compared with WON line names.
        # Even direction-matched rows remain hypotheses without mileage overlap.
        value = dict(row)
        value["matched_locations"] = matched
        value["confidence"] = "possible contributor"
        value["match_basis"] = "location and dates" + (", direction" if direction and rd in {"up", "down"} else "")
        value["caveat"] = "Exact line and mileage overlap are unverified; this does not prove the cause."
        output.append(value)
    return output


def _location_for_stanox(store, stanox):
    rows = store.db.execute("SELECT name,short_name FROM corpus WHERE stanox=?", (stanox,)).fetchall()
    known = canonical_locations(store, [x for r in rows for x in r])
    return known[0] if len(known) == 1 else None


def corridor_section(store, start, end):
    if not start or not end or start == end:
        return []
    candidates = []
    for corridor in geography(store).get("corridors", []):
        stops = corridor["locations"]
        if start in stops and end in stops:
            a, b = sorted((stops.index(start), stops.index(end)))
            candidates.append(stops[a:b + 1])
    # Do not infer a route where two different corridors could apply.
    unique = {tuple(x) for x in candidates}
    return list(next(iter(unique))) if len(unique) == 1 else []


def rebuild_trust_findings(store, train_id, rid, now):
    db = store.db
    if not rid:
        return  # Wait for an activation to establish the dated train run.
    records = db.execute("SELECT * FROM trust_events WHERE run_id=? AND kind='0003' ORDER BY event_at,queued_at", (rid,)).fetchall()
    valid_ids = set()
    previous = None
    for row in records:
        body = json.loads(row["details"])
        offroute = is_true(body.get("offroute_ind")) or body.get("variation_status") == "OFF ROUTE"
        fid = "trust:" + row["id"]
        if not offroute and row["planned_at"] and previous:
            old, old_body = previous
            old_offroute = is_true(old_body.get("offroute_ind")) or old_body.get("variation_status") == "OFF ROUTE"
            if (not old_offroute and old_body.get("event_type") == "DEPARTURE" and body.get("event_type") == "ARRIVAL"
                    and old["stanox"] != row["stanox"] and old["planned_at"]):
                actual = row["event_at"] - old["event_at"]
                planned = row["planned_at"] - old["planned_at"]
                extra = actual - planned
                start, end = _location_for_stanox(store, old["stanox"]), _location_for_stanox(store, row["stanox"])
                locations = corridor_section(store, start, end)
                if locations and 0 < planned <= 3 * 3600 and actual > 0 and extra >= store.config.trust_delay_minutes * 60:
                    restrictions = match_restrictions(store, locations, row["event_at"], direction=clean(body.get("direction_ind")).lower())
                    store.finding(fid, "trust_delay", "TRUST", body["headcode"], row["event_at"], f"{start} → {end}", {
                        "train_id": train_id, "run_id": rid, "locations": locations,
                        "seconds": actual, "planned_seconds": planned, "extra_seconds": extra,
                        "restriction_matches": restrictions,
                        "summary": f"Gained {extra / 60:.1f} minutes between {start} and {end} against the reported plan.",
                        "confidence": "TRUST running-time loss; cause remains unconfirmed.",
                    })
                    valid_ids.add(fid)
        previous = (row, body)
    # A correction can invalidate an earlier finding. Keep alert_at on valid
    # upserts so repeated reports never re-notify for the same evidence.
    for row in db.execute("SELECT id,details FROM findings WHERE kind='trust_delay'").fetchall():
        if json.loads(row["details"]).get("run_id") == rid and row["id"] not in valid_ids:
            db.execute("DELETE FROM findings WHERE id=?", (row["id"],))


def feed_states(store, now):
    result = store.rows("SELECT * FROM feed_health ORDER BY feed")
    for h in result:
        feed = h["feed"]
        if (feed in {"trust", "tsr", "vstp"} and feed not in store.config.topics) or (feed.startswith("td:") and feed[3:] not in store.config.areas):
            h["state"] = "disabled"
        elif feed in {"corpus", "smart"}:
            h["state"] = "loaded" if h["received_at"] and now - h["received_at"] < 35 * 86400 else "missing or old"
        elif feed == "tsr":
            has_batch = store.db.execute("SELECT 1 FROM tsr_batches WHERE valid_from<=? AND valid_to>=? AND published>=? LIMIT 1",
                                        (now, now, now - 9 * 86400)).fetchone()
            h["state"] = "weekly snapshot available" if has_batch else "waiting for applicable weekly snapshot"
        elif not h["connected"]:
            h["state"] = "disconnected"
        elif not h["event_at"] or not h["received_at"] or h["received_at"] < (h["connected_at"] or 0):
            h["state"] = "connected; waiting for data"
        elif feed in {"vstp", "schedule"}:
            h["state"] = "connected (event driven)"
        elif now - h["event_at"] > store.config.stale_seconds:
            h["state"] = "stale"
        else:
            h["state"] = "live"
    return result


def recent_findings(store, now, seconds=3600):
    rows = store.rows("SELECT * FROM findings WHERE event_at BETWEEN ? AND ? ORDER BY event_at DESC LIMIT 100", (now - seconds, now + 60))
    for row in rows:
        row["details"] = json.loads(row["details"])
    return rows


def snapshot(store, now=None):
    now = time.time() if now is None else now
    with store.lock:
        health = feed_states(store, now)
        by_feed = {h["feed"]: h for h in health}
        nodes = {}
        for r in store.rows("SELECT * FROM labels"):
            if r["area"] not in store.config.areas:
                continue
            key = r["area"] + ":" + r["berth"]
            node = nodes.setdefault(key, {"id": key, "area": r["area"], "berth": r["berth"], "labels": []})
            node["labels"].append({"description": r["description"], "source": r["source"]})
        edges = [e for e in store.rows("SELECT * FROM edges") if e["area"] in store.config.areas]
        for e in edges:
            for b in (e["origin"], e["destination"]):
                key = e["area"] + ":" + b
                nodes.setdefault(key, {"id": key, "area": e["area"], "berth": b, "labels": []})
        for b in store.rows("SELECT * FROM berths"):
            if b["area"] not in store.config.areas:
                continue
            key = b["area"] + ":" + b["berth"]
            node = nodes.setdefault(key, {"id": key, "area": b["area"], "berth": b["berth"], "labels": []})
            h = by_feed.get("td:" + b["area"], {})
            live = (h.get("state") == "live" and b["generation"] == h["generation"]
                    and b["event_at"] >= (h["connected_at"] or now) - store.config.stale_seconds
                    and now - b["event_at"] <= store.config.berth_max_age)
            node.update(b, live=bool(live), age_seconds=round(max(0, now - b["event_at"])))
        layout = geography(store).get("layout", {})
        for key, node in nodes.items():
            node["position"] = layout.get(key)
            supplied = {r["description"] for r in node["labels"] if r["source"] == "supplied T3 catalogue"}
            node["ambiguous"] = len(supplied) > 1
        names = list(geography(store).get("aliases", {}))
        restrictions = match_restrictions(store, names, now)
        baseline_count = store.db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        return {
            "version": 2, "generated_at": now, "demo": store.meta("demo", False),
            "areas": [{"id": a, "name": AREA_NAMES.get(a, a)} for a in store.config.areas],
            "feeds": health, "nodes": list(nodes.values()), "edges": edges,
            "events": store.rows("SELECT * FROM td_events ORDER BY id DESC LIMIT 120"),
            "findings": recent_findings(store, now), "restrictions": restrictions,
            "baseline_samples": baseline_count,
            "coverage_note": "Schematic of supplied, SMART and observed berth links. Unseen links and non-NR Metro areas may be absent.",
        }


def train_detail(store, headcode, now=None):
    now = time.time() if now is None else now
    headcode = clean(headcode, 16).upper()
    with store.lock:
        result = {"headcode": headcode, "events": store.rows(
            "SELECT * FROM td_events WHERE headcode=? ORDER BY event_at DESC LIMIT 80", (headcode,)), "runs": []}
        candidates = store.rows("SELECT * FROM runs WHERE headcode=? AND origin_at BETWEEN ? AND ? ORDER BY origin_at DESC LIMIT 8",
                                (headcode, now - 36 * 3600, now + 4 * 3600))
        for run in candidates:
            run["details"] = json.loads(run["details"])
            run["reports"] = store.rows("SELECT * FROM trust_events WHERE run_id=? ORDER BY event_at DESC LIMIT 30", (run["id"],))
            for event in run["reports"]:
                event["details"] = json.loads(event["details"])
                names = store.rows("SELECT DISTINCT name FROM corpus WHERE stanox=?", (event["stanox"],))
                event["location_names"] = [r["name"] for r in names]
            run["schedules"] = []
            origin_day = dt.datetime.fromtimestamp(run["origin_at"], UK).date().isoformat()
            weekday = dt.date.fromisoformat(origin_day).weekday()
            for schedule in store.rows("SELECT * FROM schedules WHERE uid=? AND start_date<=? AND end_date>=? ORDER BY updated_at DESC",
                                       (run["uid"], origin_day, origin_day)):
                detail = json.loads(schedule["details"])
                days = detail["schedule"].get("schedule_days_runs", "1111111")
                if not detail["deleted"] and len(days) == 7 and days[weekday] == "1":
                    schedule["details"] = detail["schedule"]
                    run["schedules"].append(schedule)
            result["runs"].append(run)
        result["identity_note"] = "Candidate TRUST runs sharing this headcode and time window. A headcode alone is not a unique train identity."
        return result
