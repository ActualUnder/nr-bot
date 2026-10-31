from __future__ import annotations

import csv
import json
import sqlite3
import statistics
import threading
import time
from pathlib import Path

from .config import AdvancedConfig
from .protocol import InvalidFeedMessage, berth, clean, fingerprint, objects, timestamp


class NetworkStore:
    """One serialised writer; each frame and its dedup IDs commit together.

    This database intentionally does not share the legacy learner's berth-only
    primary keys. Never copy T1/T2/T0 events into the T3 learner database.
    """

    def __init__(self, config: AdvancedConfig, *, reset_connections=True):
        self.config = config
        self.lock = threading.RLock()
        config.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(config.db_path, check_same_thread=False, timeout=15)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA busy_timeout=15000;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS feed_health (
                feed TEXT PRIMARY KEY, connected INTEGER DEFAULT 0, generation INTEGER DEFAULT 0,
                connected_at REAL, received_at REAL, event_at REAL, messages INTEGER DEFAULT 0,
                rejected INTEGER DEFAULT 0, error TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS dedup (id TEXT PRIMARY KEY, received_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS berths (
                area TEXT, berth TEXT, headcode TEXT, event_at REAL, received_at REAL,
                generation INTEGER, source TEXT, PRIMARY KEY(area,berth));
            CREATE TABLE IF NOT EXISTS labels (
                area TEXT, berth TEXT, description TEXT, source TEXT,
                PRIMARY KEY(area,berth,description,source));
            CREATE TABLE IF NOT EXISTS edges (
                area TEXT, origin TEXT, destination TEXT, source TEXT, observations INTEGER DEFAULT 0,
                last_at REAL, PRIMARY KEY(area,origin,destination,source));
            CREATE TABLE IF NOT EXISTS td_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT UNIQUE, area TEXT,
                kind TEXT, headcode TEXT, origin TEXT, destination TEXT, event_at REAL,
                received_at REAL, generation INTEGER);
            CREATE INDEX IF NOT EXISTS td_event_lookup ON td_events(area,headcode,event_at);
            CREATE INDEX IF NOT EXISTS td_event_time ON td_events(event_at);
            CREATE TABLE IF NOT EXISTS tracks (
                area TEXT, headcode TEXT, berth TEXT, event_at REAL, generation INTEGER,
                PRIMARY KEY(area,headcode));
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY, area TEXT, origin TEXT, destination TEXT,
                train_class TEXT, headcode TEXT, seconds REAL, event_at REAL, restricted INTEGER);
            CREATE INDEX IF NOT EXISTS sample_lookup ON samples(area,origin,destination,train_class,event_at);
            CREATE TABLE IF NOT EXISTS findings (
                id TEXT PRIMARY KEY, kind TEXT, area TEXT, headcode TEXT,
                event_at REAL, section TEXT, details TEXT, alert_at REAL);
            CREATE INDEX IF NOT EXISTS findings_time ON findings(event_at);
            CREATE TABLE IF NOT EXISTS corpus (
                stanox TEXT, tiploc TEXT, crs TEXT, name TEXT, short_name TEXT,
                PRIMARY KEY(stanox,tiploc,name));
            CREATE INDEX IF NOT EXISTS corpus_tiploc ON corpus(tiploc);
            CREATE TABLE IF NOT EXISTS smart (
                area TEXT, origin TEXT, destination TEXT, stanox TEXT, name TEXT,
                direction TEXT, line TEXT, event TEXT, step_type TEXT, platform TEXT,
                PRIMARY KEY(area,origin,destination,stanox,event,step_type,line));
            CREATE TABLE IF NOT EXISTS tsr_batches (
                id TEXT PRIMARY KEY, route_group TEXT, published REAL, valid_from REAL,
                valid_to REAL, source TEXT, coverage TEXT, received_at REAL);
            CREATE INDEX IF NOT EXISTS tsr_batch_time ON tsr_batches(valid_from,valid_to);
            CREATE TABLE IF NOT EXISTS restrictions (
                batch_id TEXT, id TEXT, valid_from REAL, valid_to REAL, details TEXT,
                PRIMARY KEY(batch_id,id));
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY, train_id TEXT, current_id TEXT, headcode TEXT, uid TEXT,
                origin_at REAL, activated_at REAL, last_at REAL, status TEXT, details TEXT);
            CREATE INDEX IF NOT EXISTS runs_headcode ON runs(headcode,origin_at);
            CREATE INDEX IF NOT EXISTS runs_train_id ON runs(train_id,origin_at);
            CREATE TABLE IF NOT EXISTS trust_events (
                id TEXT PRIMARY KEY, train_id TEXT, run_id TEXT, kind TEXT, event_at REAL,
                queued_at REAL, stanox TEXT, planned_at REAL, details TEXT);
            CREATE INDEX IF NOT EXISTS trust_event_lookup ON trust_events(train_id,event_at);
            CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY, uid TEXT, start_date TEXT, end_date TEXT, stp TEXT,
                updated_at REAL, source TEXT, details TEXT);
            CREATE INDEX IF NOT EXISTS schedule_uid ON schedules(uid,start_date);
        """)
        with self.lock, self.db:
            if reset_connections:
                self.db.execute("UPDATE feed_health SET connected=0")
            for name in [*("td:" + a for a in config.areas), *config.topics, "corpus", "smart"]:
                self.db.execute("INSERT OR IGNORE INTO feed_health(feed) VALUES(?)", (name,))
        self.seed_catalogue()

    def close(self):
        with self.lock:
            self.db.close()

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.db.execute(sql, args).fetchall()]

    def meta(self, key, default=None):
        rows = self.rows("SELECT value FROM metadata WHERE key=?", (key,))
        return json.loads(rows[0]["value"]) if rows else default

    def set_meta(self, key, value):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)", (key, json.dumps(value)))

    def seed_catalogue(self):
        with self.lock, self.db:
            self.db.execute("DELETE FROM labels WHERE source='supplied T3 catalogue'")
            self.db.execute("DELETE FROM edges WHERE source='supplied T3 catalogue'")
            if "T3" not in self.config.areas:
                return
            for filename, width in (("berth-to-description.csv", 2), ("berth-step-to-description.csv", 3)):
                path = self.config.app_dir / filename
                if not path.exists():
                    continue
                with path.open(encoding="utf-8-sig", newline="") as f:
                    for row in csv.reader(f):
                        if len(row) != width:
                            continue
                        if width == 2 and row[0]:
                            self.db.execute("INSERT OR IGNORE INTO labels VALUES('T3',?,?,?)",
                                            (berth(row[0]), row[1], "supplied T3 catalogue"))
                        elif width == 3 and row[0] and row[1]:
                            self.db.execute("INSERT OR IGNORE INTO edges VALUES('T3',?,?,?,0,NULL)",
                                            (berth(row[0]), berth(row[1]), "supplied T3 catalogue"))

    def connected(self, now=None):
        now = time.time() if now is None else now
        with self.lock, self.db:
            for feed in [*("td:" + a for a in self.config.areas), *self.config.topics]:
                self.db.execute("UPDATE feed_health SET connected=1, generation=generation+1, connected_at=?, error='' WHERE feed=?", (now, feed))
            self.db.execute("DELETE FROM tracks")

    def disconnected(self, reason="Disconnected"):
        with self.lock, self.db:
            self.db.execute("UPDATE feed_health SET connected=0,error=? WHERE feed LIKE 'td:%' "
                            "OR feed IN ('trust','tsr','vstp')", (clean(reason, 240),))
            self.db.execute("DELETE FROM tracks")

    def error(self, feed, message):
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO feed_health(feed) VALUES(?)", (feed,))
            self.db.execute("UPDATE feed_health SET error=?, rejected=rejected+1 WHERE feed=?",
                            (clean(message, 240), feed))

    def _touch(self, feed, received, event):
        self.db.execute("INSERT OR IGNORE INTO feed_health(feed) VALUES(?)", (feed,))
        self.db.execute("UPDATE feed_health SET received_at=?, event_at=MAX(COALESCE(event_at,0),?),"
                        " messages=messages+1,error='' WHERE feed=?", (received, event, feed))

    def ingest(self, feed: str, body, received=None):
        """Poison items are quarantined individually; DB failures roll back all.

        ACK is the caller's responsibility and must follow this commit. Parser
        errors never swallow sqlite or programming exceptions.
        """
        from .feeds import ingest_trust, ingest_tsr, ingest_schedule
        now = time.time() if received is None else received
        items = objects(body)
        accepted = 0
        with self.lock, self.db:
            for item in items:
                fid = fingerprint([feed, item])
                if self.db.execute("SELECT 1 FROM dedup WHERE id=?", (fid,)).fetchone():
                    continue
                self.db.execute("SAVEPOINT feed_item")
                try:
                    if feed == "td":
                        used = self._td(item, now, fid)
                    elif feed == "trust":
                        used = ingest_trust(self, item, now)
                    elif feed == "tsr":
                        used = ingest_tsr(self, item, now)
                    elif feed in {"vstp", "schedule"}:
                        used = ingest_schedule(self, item, now, feed)
                    else:
                        raise InvalidFeedMessage("unknown feed")
                    # Do not index the nationwide TD traffic we deliberately ignore.
                    if used:
                        self.db.execute("INSERT INTO dedup VALUES(?,?)", (fid, now))
                        accepted += 1
                    self.db.execute("RELEASE feed_item")
                except InvalidFeedMessage as exc:
                    self.db.execute("ROLLBACK TO feed_item")
                    self.db.execute("RELEASE feed_item")
                    self.db.execute("INSERT OR IGNORE INTO feed_health(feed) VALUES(?)", (feed,))
                    self.db.execute("UPDATE feed_health SET rejected=rejected+1,error=? WHERE feed=?",
                                    (clean(str(exc), 240), feed))
                    self.db.execute("INSERT INTO dedup VALUES(?,?)", (fid, now))
        return accepted

    def _td(self, item, now, fid):
        if len(item) == 1:
            key, msg = next(iter(item.items()))
        else:
            key, msg = item.get("msg_type", ""), item
        if not isinstance(msg, dict):
            raise InvalidFeedMessage("TD payload must be an object")
        area = clean(msg.get("area_id")).upper()
        if area not in self.config.areas:
            return False
        kind = clean(msg.get("msg_type") or str(key).replace("_MSG", "")).upper()
        if kind not in {"CA", "CB", "CC", "CT"}:
            return False  # The original T3 learner owns S-Class processing.
        at = timestamp(msg.get("time"))
        if at > now + 60:
            raise InvalidFeedMessage("TD timestamp is in the future")
        origin = berth(msg.get("from")) if kind in {"CA", "CB"} else ""
        dest = berth(msg.get("to")) if kind in {"CA", "CC"} else ""
        headcode = clean(msg.get("descr"), 16).upper()
        if kind in {"CA", "CC"} and not headcode:
            raise InvalidFeedMessage("TD description is missing")
        health = self.db.execute("SELECT * FROM feed_health WHERE feed=?", ("td:" + area,)).fetchone()
        generation = health["generation"]
        self._touch("td:" + area, now, at)
        if kind == "CT":
            return True
        endpoints = [x for x in (origin, dest) if x]
        previous = {r["berth"]: r for r in self.db.execute(
            "SELECT * FROM berths WHERE area=? AND berth IN (%s)" % ",".join("?" * len(endpoints)),
            (area, *endpoints))}
        # A late clear/step must never erase a newer occupation at either end.
        if any(r["event_at"] > at for r in previous.values()):
            return True
        if kind == "CB" and origin in previous and headcode and previous[origin]["headcode"] not in {"", headcode}:
            return True
        if not headcode and origin in previous:
            headcode = previous[origin]["headcode"]
        for bid, code in ((origin, ""), (dest, headcode)):
            if bid:
                self.db.execute("INSERT OR REPLACE INTO berths VALUES(?,?,?,?,?,?,?)",
                                (area, bid, code, at, now, generation, kind))
        self.db.execute("INSERT INTO td_events(fingerprint,area,kind,headcode,origin,destination,event_at,received_at,generation) "
                        "VALUES(?,?,?,?,?,?,?,?,?)", (fid, area, kind, headcode, origin, dest, at, now, generation))
        if kind == "CA":
            self.db.execute("INSERT INTO edges VALUES(?,?,?,'observed CA',1,?) ON CONFLICT(area,origin,destination,source) "
                            "DO UPDATE SET observations=observations+1,last_at=MAX(last_at,excluded.last_at)",
                            (area, origin, dest, at))
            previous_track = self.db.execute("SELECT * FROM tracks WHERE area=? AND headcode=?", (area, headcode)).fetchone()
            if (previous_track and previous_track["berth"] == origin and previous_track["generation"] == generation
                    and 3 <= at - previous_track["event_at"] <= 1200
                    and now - at <= self.config.stale_seconds and health["connected"]
                    and previous_track["event_at"] >= health["connected_at"]):
                self._sample(area, origin, dest, headcode, at - previous_track["event_at"], at, fid)
            self.db.execute("INSERT OR REPLACE INTO tracks VALUES(?,?,?,?,?)", (area, headcode, dest, at, generation))
        else:
            # Interposes/cancels do not establish a running-time baseline.
            self.db.execute("DELETE FROM tracks WHERE area=? AND (headcode=? OR berth=?)", (area, headcode, origin or dest))
        return True

    def _sample(self, area, origin, dest, headcode, seconds, at, fid):
        from .analysis import match_restrictions, segment_locations
        cfg = self.config
        names, direction, line = segment_locations(self, area, origin, dest)
        restrictions = match_restrictions(self, names, at, direction=direction, line=line)
        baseline = [r[0] for r in self.db.execute(
            "SELECT seconds FROM samples WHERE area=? AND origin=? AND destination=? AND train_class=? "
            "AND restricted=0 AND event_at>=? AND event_at<? ORDER BY event_at DESC LIMIT 200",
            (area, origin, dest, headcode[:1], at - cfg.retention_days * 86400, at))]
        self.db.execute("INSERT INTO samples(area,origin,destination,train_class,headcode,seconds,event_at,restricted) VALUES(?,?,?,?,?,?,?,?)",
                        (area, origin, dest, headcode[:1], headcode, seconds, at, int(bool(restrictions))))
        if len(baseline) < cfg.baseline_min:
            return
        typical = statistics.median(baseline)
        if seconds < typical * cfg.slow_ratio or seconds - typical < cfg.slow_extra:
            return
        self.finding(fid, "td_slow", area, headcode, at, f"{area}:{origin} → {dest}", {
            "origin": origin, "destination": dest, "seconds": round(seconds, 1),
            "baseline_seconds": round(typical, 1), "baseline_samples": len(baseline),
            "extra_seconds": round(seconds - typical, 1), "locations": names,
            "restriction_matches": restrictions,
            "summary": f"Berth movement took {seconds:.0f}s; recent median {typical:.0f}s ({len(baseline)} samples).",
            "confidence": "Observed slower berth movement; physical speed and cause are not confirmed.",
        })

    def finding(self, fid, kind, area, headcode, at, section, details):
        self.db.execute("INSERT INTO findings VALUES(?,?,?,?,?,?,?,NULL) ON CONFLICT(id) DO UPDATE SET details=excluded.details,event_at=excluded.event_at",
                        (fid, kind, area, headcode, at, section, json.dumps(details)))

    def prune(self, now=None):
        now = now or time.time()
        cutoff = now - self.config.retention_days * 86400
        with self.lock, self.db:
            for table, col in (("dedup", "received_at"), ("td_events", "event_at"), ("samples", "event_at"),
                               ("findings", "event_at"), ("trust_events", "event_at"), ("runs", "origin_at"),
                               ("schedules", "updated_at")):
                self.db.execute(f"DELETE FROM {table} WHERE {col}<?", (cutoff,))
            self.db.execute("DELETE FROM tsr_batches WHERE valid_to<?", (cutoff,))
            self.db.execute("DELETE FROM restrictions WHERE batch_id NOT IN (SELECT id FROM tsr_batches)")
            self.db.execute("PRAGMA optimize")

    def backup(self, path: Path):
        with self.lock, sqlite3.connect(path) as dest:
            self.db.backup(dest)
