#!/usr/bin/env python3
"""Read-only, mutually authenticated HTTPS snapshot API for T3 berth state.

The API deliberately exposes positive evidence only.  C-Class berth messages
are delta based, so an untouched persisted occupation cannot be treated as
current after a feed reconnect.  Every berth update is therefore stamped with
the live connection generation and only rows from the current generation are
returned.
"""
from __future__ import annotations

import asyncio
import contextlib
import csv
import datetime as dt
import hashlib
import ipaddress
import json
import logging
import re
import sqlite3
import ssl
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from aiohttp import web

LOGGER = logging.getLogger(__name__)
UTC = dt.timezone.utc
METRO_HEADCODE_RE = re.compile(r"^2I([0-9]{2})$", re.IGNORECASE)


def normalize_berth(value: Any) -> str:
    """Match the canonical berth format used by the T3 learner database."""
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text


def iso_utc(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return dt.datetime.fromtimestamp(float(timestamp), UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def metro_tdn_for_headcode(headcode: str) -> str | None:
    """Convert the Metro convention 2I01 -> T101, 2I51 -> T151."""
    match = METRO_HEADCODE_RE.fullmatch(str(headcode or "").strip())
    if not match:
        return None
    return f"T1{match.group(1)}"


def _append_unique(target: dict[Any, list[str]], key: Any, value: str) -> None:
    cleaned = str(value or "").strip()
    if cleaned and cleaned not in target[key]:
        target[key].append(cleaned)


@dataclass(frozen=True)
class BerthCatalogue:
    """Human labels and expected movements supplied for the T3 area."""

    descriptions: dict[str, tuple[str, ...]]
    source_ids: dict[str, tuple[str, ...]]
    step_descriptions: dict[tuple[str, str], tuple[str, ...]]
    next_berths: dict[str, tuple[str, ...]]

    @classmethod
    def load(cls, berth_path: Path, step_path: Path) -> "BerthCatalogue":
        descriptions: dict[str, list[str]] = defaultdict(list)
        source_ids: dict[str, list[str]] = defaultdict(list)
        steps: dict[tuple[str, str], list[str]] = defaultdict(list)
        nexts: dict[str, list[str]] = defaultdict(list)

        with berth_path.open("r", newline="", encoding="utf-8-sig") as handle:
            for line_number, row in enumerate(csv.reader(handle), start=1):
                if not row or not any(str(value).strip() for value in row):
                    continue
                if len(row) != 2:
                    raise ValueError(
                        f"{berth_path.name}:{line_number}: expected 2 columns, received {len(row)}"
                    )
                source_id, description = (str(value).strip() for value in row)
                canonical = normalize_berth(source_id)
                if not canonical or not description:
                    raise ValueError(
                        f"{berth_path.name}:{line_number}: berth and description are required"
                    )
                _append_unique(descriptions, canonical, description)
                _append_unique(source_ids, canonical, source_id.upper())

        with step_path.open("r", newline="", encoding="utf-8-sig") as handle:
            for line_number, row in enumerate(csv.reader(handle), start=1):
                if not row or not any(str(value).strip() for value in row):
                    continue
                if len(row) != 3:
                    raise ValueError(
                        f"{step_path.name}:{line_number}: expected 3 columns, received {len(row)}"
                    )
                raw_from, raw_to, description = (str(value).strip() for value in row)
                from_berth = normalize_berth(raw_from)
                to_berth = normalize_berth(raw_to)
                if not description or (not from_berth and not to_berth):
                    raise ValueError(
                        f"{step_path.name}:{line_number}: a movement endpoint and description are required"
                    )
                _append_unique(steps, (from_berth, to_berth), description)
                if from_berth and to_berth:
                    _append_unique(nexts, from_berth, to_berth)

        if not descriptions:
            raise ValueError(f"{berth_path.name}: no berth mappings were loaded")
        if not steps:
            raise ValueError(f"{step_path.name}: no berth-step mappings were loaded")

        return cls(
            descriptions={key: tuple(values) for key, values in descriptions.items()},
            source_ids={key: tuple(values) for key, values in source_ids.items()},
            step_descriptions={key: tuple(values) for key, values in steps.items()},
            next_berths={key: tuple(values) for key, values in nexts.items()},
        )

    @property
    def scope(self) -> frozenset[str]:
        step_endpoints = {
            berth
            for movement in self.step_descriptions
            for berth in movement
            if berth
        }
        return frozenset(set(self.descriptions) | step_endpoints)

    @property
    def ambiguous_berths(self) -> tuple[str, ...]:
        return tuple(sorted(key for key, values in self.descriptions.items() if len(values) > 1))

    @property
    def ambiguous_steps(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                f"{from_berth or '∅'}->{to_berth or '∅'}"
                for (from_berth, to_berth), values in self.step_descriptions.items()
                if len(values) > 1
            )
        )

    def descriptions_for(self, berth: str) -> tuple[str, ...]:
        return self.descriptions.get(normalize_berth(berth), ())

    def step_texts_for(self, from_berth: str, to_berth: str) -> tuple[str, ...]:
        return self.step_descriptions.get(
            (normalize_berth(from_berth), normalize_berth(to_berth)),
            (),
        )


class T3SnapshotBuilder:
    """Build a conservative API response from a WAL-mode learner database."""

    schema_version = 1

    def __init__(
        self,
        *,
        db_path: Path,
        catalogue: BerthCatalogue,
        area: str = "T3",
        stale_seconds: float = 180.0,
        busy_timeout_ms: int = 5000,
    ) -> None:
        self.db_path = Path(db_path)
        self.catalogue = catalogue
        self.area = str(area or "").strip().upper()
        self.stale_seconds = max(10.0, float(stale_seconds))
        self.busy_timeout_ms = max(250, int(busy_timeout_ms))

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path}?mode=ro"
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only=ON")
        return connection

    @staticmethod
    def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None

    def _base_payload(self, now: float) -> dict[str, Any]:
        return {
            "api_version": self.schema_version,
            "generated_at": iso_utc(now),
            "signal_area": self.area,
            "snapshot_mode": "conservative_positive_evidence",
            "absence_is_evidence": False,
            "catalogue": {
                "mapped_berths": len(self.catalogue.scope),
                "mapped_steps": len(self.catalogue.step_descriptions),
                "ambiguous_berths": list(self.catalogue.ambiguous_berths),
                "ambiguous_steps": list(self.catalogue.ambiguous_steps),
            },
            "feed": {
                "connected": False,
                "fresh": False,
                "available": False,
                "complete": False,
                "reason": "snapshot not built",
                "connection_generation": 0,
                "connection_started_at": None,
                "last_message_at": None,
                "last_message_age_seconds": None,
                "last_t3_event_at": None,
                "last_t3_event_age_seconds": None,
                "stale_after_seconds": self.stale_seconds,
            },
            "positions": [],
            "withheld": {
                "previous_connection_occupations": 0,
                "unmapped_occupations": 0,
            },
        }

    def _last_step(
        self,
        connection: sqlite3.Connection,
        *,
        berth: str,
        headcode: str,
        occupied_ts: float,
    ) -> sqlite3.Row | None:
        if not self._table_exists(connection, "berth_steps"):
            return None
        return connection.execute(
            """
            SELECT id,event_ts,from_berth,to_berth,descr,source_msg_type,topology_valid
            FROM berth_steps
            WHERE area=? AND to_berth=? AND UPPER(TRIM(COALESCE(descr,'')))=?
              AND event_ts <= ?
            ORDER BY event_ts DESC,id DESC
            LIMIT 1
            """,
            (
                self.area,
                normalize_berth(berth),
                str(headcode or "").strip().upper(),
                float(occupied_ts) + 0.001,
            ),
        ).fetchone()

    def _position(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now: float,
        generation: int,
    ) -> dict[str, Any]:
        berth = normalize_berth(row["berth"])
        headcode = str(row["descr"] or "").strip().upper()
        occupied_ts = float(row["updated_ts"])
        descriptions = self.catalogue.descriptions_for(berth)
        last_step = self._last_step(
            connection,
            berth=berth,
            headcode=headcode,
            occupied_ts=occupied_ts,
        )

        from_berth: str | None = None
        last_step_at: float | None = None
        step_texts: tuple[str, ...] = ()
        if last_step is not None and abs(float(last_step["event_ts"]) - occupied_ts) <= 1.0:
            from_berth = normalize_berth(last_step["from_berth"]) or None
            last_step_at = float(last_step["event_ts"])
            step_texts = self.catalogue.step_texts_for(
                str(last_step["from_berth"] or ""),
                str(last_step["to_berth"] or ""),
            )

        return {
            "headcode": headcode,
            "tdn": metro_tdn_for_headcode(headcode),
            "is_metro": bool(METRO_HEADCODE_RE.fullmatch(headcode)),
            "berth": berth,
            "source_berth_ids": list(self.catalogue.source_ids.get(berth, (berth,))),
            "description": descriptions[0] if descriptions else None,
            "description_alternatives": list(descriptions[1:]),
            "occupied_since": iso_utc(occupied_ts),
            "occupied_for_seconds": max(0.0, round(now - occupied_ts, 3)),
            "source_message_type": str(row["source_msg_type"] or ""),
            "connection_generation": generation,
            "quality": "live_current_connection",
            "last_step": {
                "from_berth": from_berth,
                "to_berth": berth,
                "at": iso_utc(last_step_at),
                "description": step_texts[0] if step_texts else None,
                "description_alternatives": list(step_texts[1:]),
            }
            if last_step_at is not None
            else None,
            "next_berths": list(self.catalogue.next_berths.get(berth, ())),
        }

    def build(
        self,
        *,
        connected: bool,
        last_message_ts: float | None,
        now: float | None = None,
    ) -> dict[str, Any]:
        now = float(now if now is not None else time.time())
        payload = self._base_payload(now)
        last_age = None if last_message_ts is None else now - float(last_message_ts)
        payload["feed"].update(
            {
                "connected": bool(connected),
                "last_message_at": iso_utc(last_message_ts),
                "last_message_age_seconds": None if last_age is None else round(last_age, 3),
            }
        )

        if self.area != "T3":
            payload["feed"]["reason"] = f"API is restricted to T3, not {self.area or 'blank'}"
            return self._finish(payload)
        if not self.db_path.exists():
            payload["feed"]["reason"] = "T3 learner database is not available yet"
            return self._finish(payload)

        try:
            with contextlib.closing(self._connect()) as connection:
                if not self._table_exists(connection, "feed_state") or not self._table_exists(
                    connection, "berth_state"
                ):
                    payload["feed"]["reason"] = "learner database has no live berth state yet"
                    return self._finish(payload)

                feed_columns = self._columns(connection, "feed_state")
                berth_columns = self._columns(connection, "berth_state")
                feed_row = connection.execute(
                    "SELECT * FROM feed_state WHERE area=?",
                    (self.area,),
                ).fetchone()
                if feed_row is None:
                    payload["feed"]["reason"] = "waiting for the first T3 feed connection"
                    return self._finish(payload)

                connection_started = (
                    float(feed_row["last_connected_ts"])
                    if feed_row["last_connected_ts"] is not None
                    else None
                )
                generation = (
                    int(feed_row["connection_generation"] or 0)
                    if "connection_generation" in feed_columns
                    else 0
                )
                event_timestamps = [
                    float(feed_row[column])
                    for column in ("last_s_event_ts", "last_c_event_ts")
                    if column in feed_columns and feed_row[column] is not None
                ]
                last_t3_event = max(event_timestamps) if event_timestamps else None
                last_t3_event_age = (
                    None if last_t3_event is None else now - last_t3_event
                )
                feed_fresh = bool(
                    connected
                    and last_age is not None
                    and -30.0 <= last_age <= self.stale_seconds
                    and connection_started is not None
                    and last_t3_event_age is not None
                    and -30.0 <= last_t3_event_age <= self.stale_seconds
                )
                if not connected:
                    reason = "Network Rail feed is disconnected"
                elif last_age is None:
                    reason = "connected but waiting for the first T3 message"
                elif last_age < -30.0:
                    reason = "latest received T3 frame timestamp is implausibly in the future"
                elif last_age > self.stale_seconds:
                    reason = (
                        f"last received T3 frame is {last_age:.1f}s old; "
                        f"limit is {self.stale_seconds:.1f}s"
                    )
                elif last_t3_event_age is None:
                    reason = "connected but no valid T3 event timestamp has been committed"
                elif last_t3_event_age < -30.0:
                    reason = "latest T3 event timestamp is implausibly in the future"
                elif last_t3_event_age > self.stale_seconds:
                    reason = (
                        f"latest committed T3 event is {last_t3_event_age:.1f}s old; "
                        "the durable feed may still be catching up"
                    )
                elif generation <= 0 and "connection_generation" in feed_columns:
                    reason = "waiting for a stamped T3 connection generation"
                    feed_fresh = False
                else:
                    reason = (
                        "live conservative delta snapshot; only current-connection "
                        "occupations are positive evidence"
                    )

                payload["feed"].update(
                    {
                        "fresh": feed_fresh,
                        "available": feed_fresh,
                        "complete": False,
                        "reason": reason,
                        "connection_generation": generation,
                        "connection_started_at": iso_utc(connection_started),
                        "last_t3_event_at": iso_utc(last_t3_event),
                        "last_t3_event_age_seconds": (
                            None
                            if last_t3_event_age is None
                            else round(last_t3_event_age, 3)
                        ),
                    }
                )

                generation_select = (
                    ",connection_generation"
                    if "connection_generation" in berth_columns
                    else ""
                )
                occupied_rows = connection.execute(
                    "SELECT berth,descr,updated_ts,source_msg_type"
                    f"{generation_select} FROM berth_state WHERE occupied=1 "
                    "ORDER BY berth"
                ).fetchall()

                confirmed: list[sqlite3.Row] = []
                previous_count = 0
                unmapped_count = 0
                for row in occupied_rows:
                    berth = normalize_berth(row["berth"])
                    if berth not in self.catalogue.scope:
                        unmapped_count += 1
                        continue
                    if "connection_generation" in berth_columns:
                        current_connection = generation > 0 and int(
                            row["connection_generation"] or 0
                        ) == generation
                    else:
                        # Compatibility for a database opened during a rolling
                        # upgrade.  Once Store starts, explicit generations take over.
                        current_connection = bool(
                            connection_started is not None
                            and float(row["updated_ts"]) >= connection_started - 0.001
                        )
                    if not current_connection:
                        previous_count += 1
                        continue
                    confirmed.append(row)

                payload["withheld"] = {
                    "previous_connection_occupations": previous_count,
                    "unmapped_occupations": unmapped_count,
                }
                if feed_fresh:
                    positions = [
                        self._position(
                            connection,
                            row,
                            now=now,
                            generation=generation,
                        )
                        for row in confirmed
                    ]
                    positions.sort(
                        key=lambda item: (
                            item.get("tdn") is None,
                            item.get("tdn") or item.get("headcode") or "",
                            item.get("berth") or "",
                        )
                    )
                    payload["positions"] = positions
        except (OSError, sqlite3.Error, ValueError) as exc:
            payload["feed"].update(
                {
                    "fresh": False,
                    "available": False,
                    "reason": f"snapshot database error: {type(exc).__name__}",
                }
            )
            LOGGER.exception("Could not build T3 snapshot")

        return self._finish(payload)

    @staticmethod
    def _finish(payload: dict[str, Any]) -> dict[str, Any]:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        payload["snapshot_id"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
        return payload


class T3EventBuilder:
    """Read the durable C-Class log through a monotonic cursor."""

    schema_version = 1

    def __init__(
        self,
        *,
        db_path: Path,
        catalogue: BerthCatalogue,
        area: str = "T3",
        busy_timeout_ms: int = 5000,
        maximum_limit: int = 500,
    ) -> None:
        self.db_path = Path(db_path)
        self.catalogue = catalogue
        self.area = str(area or "").strip().upper()
        self.busy_timeout_ms = max(250, int(busy_timeout_ms))
        self.maximum_limit = max(1, min(1000, int(maximum_limit)))
        self.last_stream_id = ""
        self.last_retained_events = 0
        self.last_retained_from: int | None = None
        self.last_retained_to: int | None = None
        self.last_error = ""

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"file:{self.db_path}?mode=ro",
            uri=True,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA query_only=ON")
        return connection

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone() is not None

    def _fallback_stream_id(self) -> str:
        try:
            stat = self.db_path.stat()
            identity = (
                f"{self.db_path.resolve()}|{stat.st_dev}|{stat.st_ino}|"
                f"{stat.st_ctime_ns}"
            )
        except OSError:
            identity = f"{self.db_path.resolve()}|not-created"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]

    def _event_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        headcode = str(row["headcode"] or "").strip().upper()
        from_berth = normalize_berth(row["from_berth"]) or None
        to_berth = normalize_berth(row["to_berth"]) or None
        from_descriptions = self.catalogue.descriptions_for(from_berth or "")
        to_descriptions = self.catalogue.descriptions_for(to_berth or "")
        step_texts = self.catalogue.step_texts_for(
            from_berth or "",
            to_berth or "",
        )
        alternatives: list[str] = []
        for candidate in (
            *from_descriptions[1:],
            *to_descriptions[1:],
            *step_texts[1:],
        ):
            if candidate not in alternatives:
                alternatives.append(candidate)
        return {
            "event_id": int(row["id"]),
            "event_type": str(row["event_type"] or "").strip().upper(),
            "headcode": headcode,
            "tdn": metro_tdn_for_headcode(headcode),
            "is_metro": bool(METRO_HEADCODE_RE.fullmatch(headcode)),
            "from_berth": from_berth,
            "to_berth": to_berth,
            "from_description": from_descriptions[0] if from_descriptions else None,
            "to_description": to_descriptions[0] if to_descriptions else None,
            "description_alternatives": alternatives,
            "at": iso_utc(float(row["event_ts"])),
            "received_at": iso_utc(float(row["received_ts"])),
            "connection_generation": int(row["connection_generation"] or 0),
            "quality": "live_current_connection",
            "direction": None,
            "route_context": step_texts[0] if step_texts else None,
            "next_berths": list(
                self.catalogue.next_berths.get(to_berth or "", ())
            ),
            "conflict_groups": [],
        }

    def build(
        self,
        *,
        after_event_id: int = 0,
        limit: int = 200,
        now: float | None = None,
    ) -> dict[str, Any]:
        now = float(now if now is not None else time.time())
        after = max(0, int(after_event_id))
        page_limit = max(1, min(self.maximum_limit, int(limit)))
        stream_id = self._fallback_stream_id()
        rows: list[sqlite3.Row] = []
        cursor_expired = False
        has_more = False
        retained_from: int | None = None
        retained_to: int | None = None
        retained_events = 0
        next_after = after

        if self.area == "T3" and self.db_path.exists():
            try:
                with contextlib.closing(self._connect()) as connection:
                    if self._table_exists(connection, "t3_bridge_event_meta"):
                        meta = connection.execute(
                            "SELECT stream_id FROM t3_bridge_event_meta WHERE area=?",
                            (self.area,),
                        ).fetchone()
                        if meta and str(meta["stream_id"] or "").strip():
                            stream_id = str(meta["stream_id"]).strip()

                    if self._table_exists(connection, "t3_bridge_events"):
                        range_row = connection.execute(
                            """
                            SELECT MIN(id) AS minimum_id,MAX(id) AS maximum_id,
                                   COUNT(*) AS event_count
                            FROM t3_bridge_events WHERE area=?
                            """,
                            (self.area,),
                        ).fetchone()
                        retained_events = int(range_row["event_count"] or 0)
                        retained_from = (
                            int(range_row["minimum_id"])
                            if range_row["minimum_id"] is not None
                            else None
                        )
                        retained_to = (
                            int(range_row["maximum_id"])
                            if range_row["maximum_id"] is not None
                            else None
                        )
                        cursor_expired = bool(
                            after > 0
                            and (
                                retained_from is None
                                or after < retained_from - 1
                                or (retained_to is not None and after > retained_to)
                            )
                        )
                        effective_after = (
                            max(0, retained_from - 1)
                            if cursor_expired and retained_from is not None
                            else 0 if cursor_expired else after
                        )
                        rows = list(
                            connection.execute(
                                """
                                SELECT * FROM t3_bridge_events
                                WHERE area=? AND id>?
                                ORDER BY id ASC LIMIT ?
                                """,
                                (self.area, effective_after, page_limit + 1),
                            ).fetchall()
                        )
                        has_more = len(rows) > page_limit
                        rows = rows[:page_limit]
                        if rows:
                            next_after = int(rows[-1]["id"])
                        elif cursor_expired:
                            next_after = effective_after
            except (OSError, sqlite3.Error, ValueError) as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                LOGGER.exception("Could not build T3 event page")
                raise RuntimeError("T3 event database is temporarily unavailable") from exc

        self.last_stream_id = stream_id
        self.last_retained_events = retained_events
        self.last_retained_from = retained_from
        self.last_retained_to = retained_to
        self.last_error = ""
        return {
            "api_version": self.schema_version,
            "generated_at": iso_utc(now),
            "signal_area": self.area,
            "event_mode": "confirmed_movement_log",
            "absence_is_evidence": False,
            "stream_id": stream_id,
            "after_event_id": after,
            "next_after": next_after,
            "has_more": has_more,
            "cursor_expired": cursor_expired,
            "retained_from": retained_from,
            "retained_to": retained_to,
            "retained_events": retained_events,
            "events": [self._event_payload(row) for row in rows],
        }


def create_server_ssl_context(
    *,
    certificate: Path,
    private_key: Path,
    client_ca: Path,
) -> ssl.SSLContext:
    """Create a TLS server context which cannot fall back to unauthenticated HTTP."""
    for label, path in (
        ("server certificate", certificate),
        ("server private key", private_key),
        ("client CA certificate", client_ca),
    ):
        if not Path(path).is_file():
            raise FileNotFoundError(f"{label} not found: {path}")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=str(certificate), keyfile=str(private_key))
    context.load_verify_locations(cafile=str(client_ca))
    if hasattr(ssl, "OP_NO_COMPRESSION"):
        context.options |= ssl.OP_NO_COMPRESSION
    return context


def certificate_common_names(peer_certificate: dict[str, Any] | None) -> set[str]:
    names: set[str] = set()
    if not peer_certificate:
        return names
    for relative_name in peer_certificate.get("subject", ()):
        for key, value in relative_name:
            if str(key).lower() == "commonname" and str(value).strip():
                names.add(str(value).strip())
    return names


def validate_private_bind_address(host: str) -> None:
    """Reject accidental public/wildcard binds.

    DNS names are accepted because deployments may use a private hosts entry.
    Literal IPs must be private, loopback, or link-local.
    """
    cleaned = str(host or "").strip()
    if not cleaned:
        raise ValueError("T3_API_BIND is blank")
    if cleaned in {"0.0.0.0", "::", "*"}:
        raise ValueError("T3_API_BIND must name the private bridge address, not a wildcard")
    try:
        address = ipaddress.ip_address(cleaned)
    except ValueError:
        return
    if not (address.is_private or address.is_loopback or address.is_link_local):
        raise ValueError(f"T3_API_BIND must be private; {cleaned} is publicly routable")


class T3SnapshotAPIService:
    """Small mTLS bridge service hosted inside the existing NR bot process."""

    def __init__(
        self,
        *,
        enabled: bool,
        bind_host: str,
        bind_port: int,
        certificate: Path,
        private_key: Path,
        client_ca: Path,
        allowed_client_cn: str,
        builder: T3SnapshotBuilder,
        feed_status: Callable[[], dict[str, Any]],
        event_builder: T3EventBuilder | None = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.bind_host = str(bind_host or "").strip()
        self.bind_port = int(bind_port)
        self.certificate = Path(certificate)
        self.private_key = Path(private_key)
        self.client_ca = Path(client_ca)
        self.allowed_client_cn = str(allowed_client_cn or "").strip()
        self.builder = builder
        self.event_builder = event_builder or T3EventBuilder(
            db_path=builder.db_path,
            catalogue=builder.catalogue,
            area=builder.area,
            busy_timeout_ms=builder.busy_timeout_ms,
        )
        self.feed_status = feed_status
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.started = False
        self.started_at: float | None = None
        self.last_error = ""
        self.requests = 0
        self.snapshot_requests = 0
        self.event_requests = 0
        self.last_request_at: float | None = None

    async def start(self) -> None:
        if not self.enabled or self.started:
            return
        try:
            validate_private_bind_address(self.bind_host)
            if not 1 <= self.bind_port <= 65535:
                raise ValueError(f"invalid T3_API_PORT {self.bind_port}")
            if not self.allowed_client_cn:
                raise ValueError("T3_API_ALLOWED_CLIENT_CN must not be blank")
            tls_context = create_server_ssl_context(
                certificate=self.certificate,
                private_key=self.private_key,
                client_ca=self.client_ca,
            )
            application = web.Application(client_max_size=64 * 1024)
            application.router.add_get("/v1/t3/snapshot", self._snapshot)
            application.router.add_get("/v1/t3/events", self._events)
            self._runner = web.AppRunner(
                application,
                access_log=LOGGER,
                access_log_format='%a "%r" %s %Tf',
            )
            await self._runner.setup()
            self._site = web.TCPSite(
                self._runner,
                host=self.bind_host,
                port=self.bind_port,
                ssl_context=tls_context,
                shutdown_timeout=5.0,
            )
            await self._site.start()
            self.started = True
            self.started_at = time.time()
            self.last_error = ""
            LOGGER.info(
                "T3 mTLS bridge API listening on https://%s:%s/v1/t3/snapshot and /v1/t3/events",
                self.bind_host,
                self.bind_port,
            )
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            LOGGER.exception("T3 mTLS snapshot API did not start")
            if self._runner is not None:
                await self._runner.cleanup()
            self._runner = None
            self._site = None
            self.started = False

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
        self._runner = None
        self._site = None
        self.started = False

    def _authorise(self, request: web.Request) -> None:
        peer_certificate = request.transport.get_extra_info("peercert") if request.transport else None
        common_names = certificate_common_names(peer_certificate)
        if self.allowed_client_cn not in common_names:
            LOGGER.warning("Rejected T3 bridge client certificate CNs=%s", sorted(common_names))
            raise web.HTTPForbidden(text="client certificate identity is not authorised")

    def _mark_request(self, *, event: bool) -> None:
        self.requests += 1
        if event:
            self.event_requests += 1
        else:
            self.snapshot_requests += 1
        self.last_request_at = time.time()

    async def _snapshot(self, request: web.Request) -> web.Response:
        self._authorise(request)

        status = self.feed_status()
        snapshot = await asyncio.to_thread(
            self.builder.build,
            connected=bool(status.get("connected")),
            last_message_ts=status.get("last_message_ts"),
        )
        self._mark_request(event=False)
        return web.json_response(
            snapshot,
            headers={
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def _events(self, request: web.Request) -> web.Response:
        self._authorise(request)
        raw_after = str(request.query.get("after", "0")).strip()
        raw_limit = str(request.query.get("limit", "200")).strip()
        if not raw_after.isdigit():
            raise web.HTTPBadRequest(text="after must be a non-negative integer")
        if not raw_limit.isdigit():
            raise web.HTTPBadRequest(text="limit must be a positive integer")
        after = int(raw_after)
        limit = int(raw_limit)
        if after < 0:
            raise web.HTTPBadRequest(text="after must be a non-negative integer")
        if not 1 <= limit <= self.event_builder.maximum_limit:
            raise web.HTTPBadRequest(
                text=f"limit must be between 1 and {self.event_builder.maximum_limit}"
            )
        try:
            page = await asyncio.to_thread(
                self.event_builder.build,
                after_event_id=after,
                limit=limit,
            )
            self.last_error = ""
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            raise web.HTTPServiceUnavailable(
                text="T3 event stream is temporarily unavailable"
            ) from exc
        self._mark_request(event=True)
        return web.json_response(
            page,
            headers={
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
                "X-Content-Type-Options": "nosniff",
            },
        )

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "started": self.started,
            "bind": f"{self.bind_host}:{self.bind_port}",
            "started_at": self.started_at,
            "requests": self.requests,
            "snapshot_requests": self.snapshot_requests,
            "event_requests": self.event_requests,
            "last_request_at": self.last_request_at,
            "event_stream_id": self.event_builder.last_stream_id,
            "retained_events": self.event_builder.last_retained_events,
            "retained_from": self.event_builder.last_retained_from,
            "retained_to": self.event_builder.last_retained_to,
            "event_error": self.event_builder.last_error,
            "last_error": self.last_error,
        }


def describe_catalogue(catalogue: BerthCatalogue) -> Iterable[str]:
    """Small diagnostic helper used by tests and startup logging."""
    yield f"mapped berths={len(catalogue.scope)}"
    yield f"mapped steps={len(catalogue.step_descriptions)}"
    if catalogue.ambiguous_berths:
        yield f"ambiguous berths={','.join(catalogue.ambiguous_berths)}"
    if catalogue.ambiguous_steps:
        yield f"ambiguous steps={','.join(catalogue.ambiguous_steps)}"
