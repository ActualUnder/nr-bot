#!/usr/bin/env python3
"""
Cleaner T3 TD S-Class / C-Class bit-map learner.

Goals
-----
- Keep the known_bits.csv workflow.
- Make day-to-day commands shorter and clearer.
- Keep learning evidence in SQLite.
- Warn/export when the live TD feed shows a CA movement involving a berth/signal
  that is not in the configured topology.
- Stay small enough to extend into bot commands later.

Typical commands
----------------
Live learning:
    python t3_learner_clean.py live --all

Live watch known bits for one signal:
    python t3_learner_clean.py watch --signal 6232

Offline report from SQLite:
    python t3_learner_clean.py report --all

Show missing topology observations:
    python t3_learner_clean.py missing

Known bits check:
    python t3_learner_clean.py known --signal 6232
"""

from __future__ import annotations

import argparse
import collections
import csv
import dataclasses
import datetime as dt
import json
import os
import re
import signal as signal_module
import sqlite3
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    import stomp  # type: ignore
except ImportError:
    stomp = None  # type: ignore


# =============================================================================
# Topology
# =============================================================================

METRO_UP_PELAW_TO_SOUTH_HYLTON = [
    "6282", "6276", "6274", "6272", "6268", "6266", "6264", "6254",
    "6252", "6248", "6246", "6242", "6238", "6236", "6234", "6232",
    "6228", "6226", "6224", "6222", "6208", "6206", "6298", "6294",
    "6292", "6290", "6288", "6286", "6284", "6280",
]

METRO_DOWN_SOUTH_HYLTON_TO_PELAW = [
    "6285", "6287", "6289", "6291", "6293", "6295", "6297", "6299",
    "6207", "6217", "6219", "6223", "6225", "6227", "6229", "6233",
    "6237", "6239", "6243", "6245", "6247", "6249", "6265", "6267",
    "6269", "6273", "6275", "6277", "6279", "P764",
]

SUNDERLAND_UP_HEWORTH_TO_SEAHAM = [
    "64", "58", "54", "6278", "6276", "6274", "6272", "6268", "6266",
    "6264", "6254", "6252", "6248", "6246", "6242", "6238", "6236",
    "6234", "6232", "6228", "6226", "6224", "6222", "6208", "6206",
    "6204", "7178", "7174", "7172", "7170", "7166",
]

SUNDERLAND_DOWN_SEAHAM_TO_HEWORTH = [
    "7168", "7173", "7175", "7177", "7183", "6203", "6205", "6207",
    "6209", "6217", "6219", "6223", "6225", "6227", "6229", "6233",
    "6237", "6239", "6243", "6245", "6247", "6249", "6265", "6267",
    "6269", "6273", "6275", "6277", "6279", "19", "47", "53", "57", "63",
]

SOUTH_HYLTON_BRANCH_TO_SUN = [
    "6285", "6287", "6289", "6291", "6293", "6295", "6297", "6299",
]

# Known real signal/berth ids from the project notes. Some have no confirmed
# next berth yet; they should still count as "known to topology" for missing
# topology detection.
ALL_T3_SIGNAL_IDS = {
    "19", "40", "42", "45", "47", "49", "51", "52", "53", "54", "57", "58", "63", "64",
    "5303", "5305", "5306", "5308", "5309", "5310", "5312", "5313", "5314", "5315", "5318", "5332", "5333",
    "6203", "6204", "6205", "6206", "6207", "6208", "6209", "6211", "6212", "6213", "6214", "6215", "6217", "6219",
    "6221", "6222", "6223", "6224", "6225", "6226", "6227", "6228", "6229",
    "6232", "6233", "6234", "6236", "6237", "6238", "6239",
    "6241", "6242", "6243", "6244", "6245", "6246", "6247", "6248", "6249",
    "6251", "6252", "6254",
    "6263", "6264", "6265", "6266", "6267", "6268", "6269",
    "6272", "6273", "6274", "6275", "6276", "6277", "6278", "6279",
    "6280", "6282", "6284", "6285", "6286", "6287", "6288", "6289",
    "6290", "6291", "6292", "6293", "6294", "6295", "6296", "6297", "6298", "6299",
    "7166", "7168", "7170", "7172", "7173", "7174", "7175", "7176", "7177", "7178", "7179", "7181", "7183",
    "P764",
}

# Extra route choices, shunts, loops, sidings and reverse moves. These prevent
# strict mode from discarding known special movements.
EXTRA_NEXTS = {
    "6222": {"6208", "6214"},
    "6206": {"6298", "6296", "5303", "5305", "6204"},
    "6207": {"6217", "6212"},
    "6209": {"6217", "6214"},
    "6212": {"6296", "6298", "5303", "5305", "6204"},
    "6214": {"6212"},
    "6248": {"6246", "6244"},
    "6244": {"6241", "6242"},
    "6241": {"6242", "6244"},
    "6247": {"6249", "6263"},
    "6263": {"6252", "6249"},
    "6279": {"P764", "19"},
    "19": {"45", "47", "5312"},
    "5312": {"6278"},
    "54": {"5314", "51"},
    "5314": {"42"},
    "42": {"6278"},
    "5308": {"6254"},
    "5332": {"6241"},
    "5306": {"6222"},
    "5303": {"6206", "6212"},
    "5305": {"6206", "6212"},
    "40": set(),
    "45": {"53"},
    "49": {"54"},
    "51": {"53"},
    "52": {"5318"},
    "63": set(),
    "6211": {"6213", "6215", "6206", "6208"},
    "6213": {"6215", "6217"},
    "6215": {"6217"},
    "6221": {"6222", "6214", "6208"},
    "6251": {"6249", "6252"},
    "6296": {"6206", "6212", "6298"},
    "7166": set(),
    "7176": {"7174", "7179", "7183"},
    "7179": {"7176", "7183"},
    "7181": {"7183"},
    "5309": set(),
    "5310": {"47"},
    "5313": {"5315", "5310", "5312", "47"},
    "5315": set(),
    "5318": {"45", "5312", "5310"},
    "5333": {"5309", "6241", "6242"},
    "P764": set(),
    "6280": set(),
}

SPECIAL_NON_LEARNING_MOVES = {
    ("6280", "6285"): "South Hylton internal platform turnback/headcode transfer",
    ("6207", "6212"): "Sunderland P3 internal turnback/berth-block transfer",
}


def normalize_berth(value: Any) -> str:
    s = str(value or "").strip().upper()
    if not s:
        return ""
    if s.isdigit():
        return s.lstrip("0") or "0"
    return s

# Expected external/boundary berth chains observed around the edge of the T3
# learning area. These are useful for movement context and missing-topology
# reports, but they are NOT treated as real T3 signals to learn with --all.
EXPECTED_EXTERNAL_NEXTS = {
    # Pelaw / Nexus-facing boundary into the Sunderland/Pelaw chord.
    "P751": {"P765"},
    "P765": {"6282"},

    # Other observed P-berth chains from the feed. Keep these out of the real
    # signal topology unless they are later confirmed as actual T3 signals.
    "P773": {"P775"},
    "P775": {"P789"},
    "P789": {"P803"},
    "P803": {"P807"},
    "P804": {"P802"},
    "P802": {"P790"},
    "P790": {"P776"},
    "P776": {"P770"},

    # Heworth / mainline-facing approach chain into signal 64.
    "105": {"96"},
    "96": {"84"},
    "84": {"76"},
    "76": {"64"},
}

EXPECTED_EXTERNAL_BERTHS = {
    normalize_berth(k) for k in EXPECTED_EXTERNAL_NEXTS
} | {
    normalize_berth(v) for vals in EXPECTED_EXTERNAL_NEXTS.values() for v in vals
}


def is_expected_external_move(from_berth: str, to_berth: str) -> bool:
    """True when a missing-topology row is just a known external boundary chain."""
    frm = normalize_berth(from_berth)
    to = normalize_berth(to_berth)
    return to in {normalize_berth(v) for v in EXPECTED_EXTERNAL_NEXTS.get(frm, set())}


def external_expected_nexts(from_berth: str) -> Set[str]:
    return {normalize_berth(v) for v in EXPECTED_EXTERNAL_NEXTS.get(normalize_berth(from_berth), set())}


def build_topology(extra_json: Optional[Path] = None) -> Dict[str, Set[str]]:
    nexts: Dict[str, Set[str]] = collections.defaultdict(set)

    def add_chain(chain: Sequence[str]) -> None:
        for a, b in zip(chain, chain[1:]):
            nexts[normalize_berth(a)].add(normalize_berth(b))

    for chain in [
        METRO_UP_PELAW_TO_SOUTH_HYLTON,
        METRO_DOWN_SOUTH_HYLTON_TO_PELAW,
        SUNDERLAND_UP_HEWORTH_TO_SEAHAM,
        SUNDERLAND_DOWN_SEAHAM_TO_HEWORTH,
        SOUTH_HYLTON_BRANCH_TO_SUN,
    ]:
        add_chain(chain)

    for k, vals in EXTRA_NEXTS.items():
        nexts[normalize_berth(k)].update(normalize_berth(v) for v in vals)

    for s in ALL_T3_SIGNAL_IDS:
        nexts.setdefault(normalize_berth(s), set())

    if extra_json:
        data = json.loads(extra_json.read_text(encoding="utf-8"))
        for k, vals in data.items():
            if isinstance(vals, str):
                vals = [vals]
            nexts[normalize_berth(k)].update(normalize_berth(v) for v in vals)

    return {k: set(v) for k, v in sorted(nexts.items())}


# =============================================================================
# Basic parsing
# =============================================================================

@dataclasses.dataclass(frozen=True)
class BitKey:
    address: str
    bit: int

    @property
    def label(self) -> str:
        return f"{self.address}:{self.bit}"


@dataclasses.dataclass
class KnownBit:
    key: BitKey
    element_type: str
    description: str
    signal: str = ""
    route_from: str = ""
    route_to: str = ""
    active_state: str = ""
    ignore: bool = True
    confidence: str = ""
    notes: str = ""

    @property
    def described(self) -> bool:
        return bool((self.description or "").strip())

    def summary(self) -> str:
        parts = [self.element_type or "Known", self.description or "(blank description)"]
        if self.signal:
            parts.append(f"signal={self.signal}")
        if self.route_from or self.route_to:
            parts.append(f"route={self.route_from}->{self.route_to}")
        if self.active_state:
            parts.append(f"active={self.active_state}")
        if self.confidence:
            parts.append(f"conf={self.confidence}")
        return " | ".join(parts)


class KnownBits:
    def __init__(self, rows: Sequence[KnownBit]):
        self.rows = list(rows)
        self.by_key: Dict[BitKey, List[KnownBit]] = collections.defaultdict(list)
        self.by_signal: Dict[str, List[KnownBit]] = collections.defaultdict(list)

        for row in self.rows:
            self.by_key[row.key].append(row)
            for sig in self._signals_for_row(row):
                self.by_signal[sig].append(row)

    @staticmethod
    def _signals_for_row(row: KnownBit) -> Set[str]:
        out = set()
        if row.signal:
            out.add(normalize_berth(row.signal))
        # The compact known_bits.csv usually has the signal id in Description.
        if (row.element_type or "").strip().lower() == "signal":
            for m in re.finditer(r"\b(P[0-9]{2,4}|[0-9]{2,4})\b", row.description or "", flags=re.I):
                out.add(normalize_berth(m.group(1)))
        return {x for x in out if x}

    def described(self, key: BitKey) -> bool:
        return any(row.described for row in self.by_key.get(key, []))

    def ignored(self, key: BitKey) -> bool:
        return any(row.described and row.ignore for row in self.by_key.get(key, []))

    def describe(self, key: BitKey) -> str:
        rows = self.by_key.get(key, [])
        if not rows:
            return "UNKNOWN"
        return " || ".join(r.summary() for r in rows)

    def signals_for_key(self, key: BitKey) -> Set[str]:
        """Return known signal IDs mapped to this bit, if any.

        This is used by the `bit` command. A raw S-Class bit flip does not
        itself carry a headcode or signal name. If the CSV maps 25:3 to 6244,
        the command must label that bit as signal 6244, not whichever nearby
        CA/pass window happened to be closest.
        """
        out: Set[str] = set()
        for row in self.by_key.get(key, []):
            if not row.described:
                continue
            out.update(self._signals_for_row(row))
        return {normalize_berth(x) for x in out if x}

    def keys_for_signal(self, signal_id: str) -> Set[BitKey]:
        return {r.key for r in self.by_signal.get(normalize_berth(signal_id), [])}


def normalise_address(value: str) -> str:
    s = str(value).strip().upper()
    if s.startswith("0X"):
        s = s[2:]
    s = re.sub(r"[^0-9A-F]", "", s)
    if not s:
        raise ValueError("empty address")
    n = int(s, 16)
    if not 0 <= n <= 0xFF:
        raise ValueError(f"address out of range: {value!r}")
    return f"{n:02X}"


def parse_bit_spec(value: str) -> BitKey:
    s = str(value).strip().upper()
    s = s.replace("ADDRESS", "").replace("ADDR", "")
    s = re.sub(r"\bBIT\b", "", s)
    s = s.replace(".", ":").replace("-", ":").replace("/", ":")
    s = re.sub(r"\s+", " ", s).strip()
    for pat in [r"^(?:0X)?([0-9A-F]{1,2})\s*[: ]\s*B?([0-7])$", r"^(?:0X)?([0-9A-F]{1,2})B([0-7])$"]:
        m = re.match(pat, s)
        if m:
            return BitKey(normalise_address(m.group(1)), int(m.group(2)))
    raise ValueError(f"cannot parse bit spec {value!r}; use 24:1")


def row_get(row: Dict[str, str], *names: str) -> str:
    lowered = {str(k).strip().lower(): ("" if v is None else str(v).strip()) for k, v in row.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return ""


def truthy(value: str, default: bool = True) -> bool:
    s = str(value or "").strip().lower()
    if not s:
        return default
    return s in {"1", "true", "yes", "y", "on", "ignore", "ignored"}


def load_known_bits(path: Path) -> KnownBits:
    if not path.exists():
        return KnownBits([])
    rows: List[KnownBit] = []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for line_no, row in enumerate(reader, start=2):
            spec = row_get(row, "Address:Bit", "Address Bit", "address_bit", "bit", "addr:bit")
            if not spec:
                address = row_get(row, "Address", "addr")
                bit = row_get(row, "Bit", "bit_no")
                spec = f"{address}:{bit}" if address and bit else ""
            if not spec:
                continue
            try:
                key = parse_bit_spec(spec)
            except Exception as exc:
                print(f"[KNOWN] Skipping line {line_no}: {exc}", file=sys.stderr)
                continue
            rows.append(KnownBit(
                key=key,
                element_type=row_get(row, "Element type", "Type", "element_type") or "Known",
                description=row_get(row, "Description", "Desc", "Name"),
                signal=row_get(row, "Signal", "Signal ID", "signal_id"),
                route_from=row_get(row, "Route From", "From", "from_berth", "route_from"),
                route_to=row_get(row, "Route To", "To", "to_berth", "route_to"),
                active_state=row_get(row, "Active State", "Active", "active_state"),
                ignore=truthy(row_get(row, "Ignore In Tracker", "Ignore In Learner", "Ignore", "ignore_in_tracker"), True),
                confidence=row_get(row, "Confidence", "Conf"),
                notes=row_get(row, "Notes", "Note"),
            ))
    return KnownBits(rows)


def parse_nr_time_ms(value: Any) -> float:
    try:
        return int(value) / 1000.0
    except Exception:
        return time.time()


def fmt_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


def iso_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(float(ts)).isoformat(timespec="seconds")


def split_hex_bytes(data: Any) -> List[int]:
    s = str(data or "").strip().replace(" ", "")
    if len(s) % 2:
        s = "0" + s
    return [int(s[i:i + 2], 16) for i in range(0, len(s), 2)]


def parse_hex_address(address: Any) -> int:
    return int(str(address).strip().upper(), 16)


def bit_value(byte: int, bit: int) -> int:
    return 1 if (byte & (1 << bit)) else 0


def iter_message_objects(body: str) -> Iterable[Tuple[str, Dict[str, Any]]]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return
    items = [parsed] if isinstance(parsed, dict) else parsed if isinstance(parsed, list) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        if len(item) == 1:
            key, payload = next(iter(item.items()))
            if isinstance(payload, dict):
                yield str(key), payload
        elif item.get("msg_type"):
            msg_type = str(item["msg_type"])
            yield (msg_type if msg_type.endswith("_MSG") else f"{msg_type}_MSG"), item


# =============================================================================
# Storage
# =============================================================================

@dataclasses.dataclass(frozen=True)
class BitEvent:
    ts: float
    area: str
    address: int
    bit: int
    old_bit: Optional[int]
    new_bit: int
    old_byte: Optional[int]
    new_byte: int
    msg_type: str

    @property
    def key(self) -> BitKey:
        return BitKey(f"{self.address:02X}", self.bit)

    @property
    def direction(self) -> str:
        if self.old_bit is None:
            return f"?->{self.new_bit}"
        return f"{self.old_bit}->{self.new_bit}"

    def compact(self, pass_ts: Optional[float] = None) -> str:
        delta = ""
        if pass_ts is not None:
            delta = f" {self.ts - pass_ts:+.1f}s"
        old_byte = "??" if self.old_byte is None else f"{self.old_byte:02X}"
        return (
            f"{fmt_ts(self.ts)}{delta} {self.msg_type} "
            f"{self.address:02X}:{self.bit} byte {old_byte}->{self.new_byte:02X} bit {self.direction}"
        )


@dataclasses.dataclass
class PassWindow:
    pass_id: int
    signal: str
    from_berth: str
    to_berth: str
    descr: str
    ts: float
    pre_events: List[BitEvent]


class Store:
    def __init__(self, path: Path, missing_dir: Path):
        self.path = path
        self.missing_dir = missing_dir
        self.missing_dir.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function("norm_berth", 1, normalize_berth)
        self.lock = threading.RLock()
        self.read_only = False
        try:
            self._init_schema()
        except sqlite3.OperationalError as exc:
            # Uploaded/reference DBs can be read-only. Report/known/check mode
            # should still work against the existing v11 tables; live mode will
            # fail later if it needs to write.
            if "readonly" in str(exc).lower() or "read-only" in str(exc).lower():
                self.read_only = True
                print(f"[DB] read-only database opened: {path}", file=sys.stderr)
            else:
                raise

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        with self.lock, self.conn:
            self.conn.executescript("""
                PRAGMA busy_timeout=5000;
                PRAGMA wal_checkpoint(TRUNCATE);
                PRAGMA journal_mode=DELETE;

                CREATE TABLE IF NOT EXISTS s_bytes (
                    area TEXT NOT NULL,
                    address INTEGER NOT NULL,
                    value INTEGER NOT NULL,
                    msg_type TEXT,
                    updated_ts REAL NOT NULL,
                    PRIMARY KEY(area, address)
                );


                CREATE TABLE IF NOT EXISTS berth_state (
                    berth TEXT PRIMARY KEY,
                    descr TEXT,
                    occupied INTEGER NOT NULL DEFAULT 0,
                    updated_ts REAL NOT NULL,
                    source_msg_type TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_berth_state_updated ON berth_state(updated_ts);

                CREATE TABLE IF NOT EXISTS pass_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal TEXT NOT NULL,
                    from_berth TEXT NOT NULL,
                    to_berth TEXT NOT NULL,
                    descr TEXT,
                    pass_ts REAL NOT NULL,
                    finalised_ts REAL,
                    event_count INTEGER DEFAULT 0,
                    special_reason TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS pass_bit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pass_id INTEGER NOT NULL,
                    signal TEXT NOT NULL,
                    area TEXT NOT NULL,
                    address INTEGER NOT NULL,
                    bit INTEGER NOT NULL,
                    old_bit INTEGER,
                    new_bit INTEGER NOT NULL,
                    old_byte INTEGER,
                    new_byte INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    delta_seconds REAL NOT NULL,
                    msg_type TEXT,
                    event_ts REAL NOT NULL
                );

                -- Raw S-Class bit changes as they arrive from TD.
                -- pass_bit_events can contain the same S event attached to many nearby
                -- pass windows; this table is the actual unique bit-change history.
                CREATE TABLE IF NOT EXISTS s_bit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts REAL NOT NULL,
                    area TEXT NOT NULL,
                    address INTEGER NOT NULL,
                    bit INTEGER NOT NULL,
                    old_bit INTEGER,
                    new_bit INTEGER NOT NULL,
                    old_byte INTEGER,
                    new_byte INTEGER NOT NULL,
                    msg_type TEXT
                );

                CREATE TABLE IF NOT EXISTS missing_topology_moves (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts REAL NOT NULL,
                    area TEXT NOT NULL,
                    descr TEXT,
                    from_berth TEXT NOT NULL,
                    to_berth TEXT NOT NULL,
                    missing_from INTEGER NOT NULL,
                    missing_to INTEGER NOT NULL,
                    route_missing INTEGER NOT NULL,
                    expected_nexts TEXT,
                    reason TEXT,
                    raw_json TEXT
                );

                CREATE TABLE IF NOT EXISTS missing_topology_summary (
                    area TEXT NOT NULL,
                    from_berth TEXT NOT NULL,
                    to_berth TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    first_ts REAL NOT NULL,
                    last_ts REAL NOT NULL,
                    count INTEGER NOT NULL,
                    example_descr TEXT,
                    expected_nexts TEXT,
                    PRIMARY KEY(area, from_berth, to_berth, reason)
                );

                CREATE INDEX IF NOT EXISTS idx_pbe_signal_time ON pass_bit_events(signal, event_ts);
                CREATE INDEX IF NOT EXISTS idx_pbe_bit ON pass_bit_events(address, bit);
                CREATE INDEX IF NOT EXISTS idx_sbe_bit_time ON s_bit_events(address, bit, event_ts);
                CREATE INDEX IF NOT EXISTS idx_pass_signal ON pass_log(signal, to_berth, finalised_ts);
                CREATE INDEX IF NOT EXISTS idx_missing_last ON missing_topology_summary(last_ts);
            """)

    def load_bytes(self, area: str) -> Dict[int, int]:
        with self.lock:
            rows = self.conn.execute("SELECT address,value FROM s_bytes WHERE area=?", (area,)).fetchall()
            return {int(r["address"]): int(r["value"]) for r in rows}

    def save_byte(self, area: str, address: int, value: int, msg_type: str, ts: float) -> None:
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO s_bytes(area,address,value,msg_type,updated_ts)
                VALUES(?,?,?,?,?)
                ON CONFLICT(area,address) DO UPDATE SET
                    value=excluded.value, msg_type=excluded.msg_type, updated_ts=excluded.updated_ts
            """, (area, address, value, msg_type, ts))

    def record_berth_state(self, berth: str, descr: str, occupied: bool, ts: float, msg_type: str) -> None:
        """Store a simple latest berth/headcode occupancy state for Discord /signal."""
        if getattr(self, "read_only", False):
            return
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO berth_state(berth, descr, occupied, updated_ts, source_msg_type)
                VALUES(?,?,?,?,?)
                ON CONFLICT(berth) DO UPDATE SET
                    descr=excluded.descr,
                    occupied=excluded.occupied,
                    updated_ts=excluded.updated_ts,
                    source_msg_type=excluded.source_msg_type
            """, (
                normalize_berth(berth),
                str(descr or "").strip(),
                1 if occupied else 0,
                float(ts),
                str(msg_type or ""),
            ))

    def record_raw_bit_event(self, event: BitEvent) -> None:
        """Store the real S-Class bit change once.

        This is different from pass_bit_events. The same raw S-Class event may
        be attached to several pass windows when multiple trains move nearby;
        raw history should not repeat it per pass_id.
        """
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO s_bit_events(
                    event_ts, area, address, bit, old_bit, new_bit,
                    old_byte, new_byte, msg_type
                ) VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                event.ts, event.area, event.address, event.bit,
                event.old_bit, event.new_bit, event.old_byte, event.new_byte,
                event.msg_type,
            ))

    def create_pass(self, obs: PassWindow) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute("""
                INSERT INTO pass_log(signal,from_berth,to_berth,descr,pass_ts,special_reason)
                VALUES(?,?,?,?,?,?)
            """, (obs.signal, obs.from_berth, obs.to_berth, obs.descr, obs.ts, ""))
            return int(cur.lastrowid)

    def finalise_pass(self, pass_id: int, event_count: int) -> None:
        with self.lock, self.conn:
            self.conn.execute("UPDATE pass_log SET finalised_ts=?, event_count=? WHERE id=?",
                              (time.time(), event_count, pass_id))

    def record_pass_event(self, pass_id: int, signal_id: str, event: BitEvent, phase: str, delta: float) -> None:
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO pass_bit_events(
                    pass_id, signal, area, address, bit, old_bit, new_bit,
                    old_byte, new_byte, phase, delta_seconds, msg_type, event_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                pass_id, signal_id, event.area, event.address, event.bit,
                event.old_bit, event.new_bit, event.old_byte, event.new_byte,
                phase, delta, event.msg_type, event.ts,
            ))

    def record_missing_topology(
        self,
        *,
        ts: float,
        area: str,
        descr: str,
        from_berth: str,
        to_berth: str,
        missing_from: bool,
        missing_to: bool,
        route_missing: bool,
        expected_nexts: Sequence[str],
        reason: str,
        raw: Dict[str, Any],
    ) -> None:
        if getattr(self, "read_only", False):
            print("[DB] cannot record missing topology because database is read-only", file=sys.stderr)
            return
        expected_text = ",".join(sorted(expected_nexts))
        raw_text = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO missing_topology_moves(
                    event_ts,area,descr,from_berth,to_berth,missing_from,missing_to,
                    route_missing,expected_nexts,reason,raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ts, area, descr, from_berth, to_berth, int(missing_from), int(missing_to),
                int(route_missing), expected_text, reason, raw_text,
            ))
            self.conn.execute("""
                INSERT INTO missing_topology_summary(
                    area,from_berth,to_berth,reason,first_ts,last_ts,count,example_descr,expected_nexts
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(area,from_berth,to_berth,reason) DO UPDATE SET
                    last_ts=excluded.last_ts,
                    count=count+1,
                    example_descr=CASE
                        WHEN excluded.example_descr != '' THEN excluded.example_descr
                        ELSE example_descr
                    END,
                    expected_nexts=excluded.expected_nexts
            """, (
                area, from_berth, to_berth, reason, ts, ts, 1, descr, expected_text,
            ))

        self._append_missing_detail_csv(ts, area, descr, from_berth, to_berth, missing_from, missing_to,
                                        route_missing, expected_text, reason)
        self.export_missing_summary(self.missing_dir / "missing_topology_summary.csv")

    def _append_missing_detail_csv(
        self,
        ts: float,
        area: str,
        descr: str,
        from_berth: str,
        to_berth: str,
        missing_from: bool,
        missing_to: bool,
        route_missing: bool,
        expected_nexts: str,
        reason: str,
    ) -> None:
        path = self.missing_dir / "missing_topology_moves.csv"
        exists = path.exists()
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not exists:
                writer.writerow([
                    "first_seen", "area", "descr", "from_berth", "to_berth",
                    "missing_from", "missing_to", "route_missing", "expected_nexts", "reason",
                ])
            writer.writerow([
                iso_ts(ts), area, descr, from_berth, to_berth,
                "yes" if missing_from else "no",
                "yes" if missing_to else "no",
                "yes" if route_missing else "no",
                expected_nexts,
                reason,
            ])

    def export_missing_summary(self, path: Path) -> None:
        rows = self.missing_summary_rows()
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "first_seen", "last_seen", "count", "area", "from_berth", "to_berth",
                "reason", "example_descr", "expected_nexts",
            ])
            for r in rows:
                writer.writerow([
                    iso_ts(float(r["first_ts"])), iso_ts(float(r["last_ts"])), int(r["count"]),
                    r["area"], r["from_berth"], r["to_berth"], r["reason"],
                    r["example_descr"] or "", r["expected_nexts"] or "",
                ])

    def missing_summary_rows(self) -> List[sqlite3.Row]:
        with self.lock:
            try:
                return list(self.conn.execute("""
                    SELECT * FROM missing_topology_summary
                    ORDER BY last_ts DESC, count DESC, from_berth, to_berth
                """).fetchall())
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc).lower():
                    return []
                raise

    def route_counts(self, signal_id: str) -> List[sqlite3.Row]:
        with self.lock:
            return list(self.conn.execute("""
                SELECT norm_berth(to_berth) AS to_berth, COUNT(*) AS pass_count
                FROM pass_log
                WHERE signal=? AND finalised_ts IS NOT NULL
                GROUP BY norm_berth(to_berth)
                ORDER BY pass_count DESC, to_berth
            """, (normalize_berth(signal_id),)).fetchall())

    def pass_count(self, signal_id: str, to_berth: Optional[str] = None) -> int:
        route = normalize_berth(to_berth) if to_berth else None
        with self.lock:
            row = self.conn.execute("""
                SELECT COUNT(*) AS c FROM pass_log
                WHERE signal=? AND finalised_ts IS NOT NULL
                  AND (? IS NULL OR norm_berth(to_berth)=?)
            """, (normalize_berth(signal_id), route, route)).fetchone()
            return int(row["c"] if row else 0)

    def candidate_rows(self, signal_id: str, *, score_window: float, to_berth: Optional[str] = None, limit: int = 40) -> List[sqlite3.Row]:
        route = normalize_berth(to_berth) if to_berth else None
        with self.lock:
            return list(self.conn.execute("""
                WITH per_pass AS (
                    SELECT
                        e.pass_id, e.signal, e.area, e.address, e.bit,
                        MAX(CASE WHEN e.phase='before' AND e.old_bit=0 AND e.new_bit=1 THEN 1 ELSE 0 END) AS before_on,
                        MAX(CASE WHEN e.phase='before' AND e.old_bit=1 AND e.new_bit=0 THEN 1 ELSE 0 END) AS before_off,
                        MAX(CASE WHEN e.phase='after' AND e.old_bit=0 AND e.new_bit=1 THEN 1 ELSE 0 END) AS after_on,
                        MAX(CASE WHEN e.phase='after' AND e.old_bit=1 AND e.new_bit=0 THEN 1 ELSE 0 END) AS after_off,
                        MIN(ABS(e.delta_seconds)) AS closest_abs
                    FROM pass_bit_events e
                    JOIN pass_log p ON p.id=e.pass_id
                    WHERE e.signal=?
                      AND p.finalised_ts IS NOT NULL
                      AND (? IS NULL OR norm_berth(p.to_berth)=?)
                      AND e.old_bit IS NOT NULL
                      AND e.old_bit != e.new_bit
                      AND ABS(e.delta_seconds) <= ?
                    GROUP BY e.pass_id,e.signal,e.area,e.address,e.bit
                )
                SELECT
                    signal, area, address, bit,
                    COUNT(*) AS support_passes,
                    SUM(before_on) AS before_on,
                    SUM(before_off) AS before_off,
                    SUM(after_on) AS after_on,
                    SUM(after_off) AS after_off,
                    AVG(closest_abs) AS avg_abs,
                    MIN(closest_abs) AS closest_abs
                FROM per_pass
                GROUP BY signal,area,address,bit
                ORDER BY support_passes DESC, avg_abs ASC
                LIMIT ?
            """, (normalize_berth(signal_id), route, route, float(score_window), int(limit))).fetchall())

    def movement_rows(self, berth: str, *, limit: int = 50) -> List[sqlite3.Row]:
        """Return learned/pass-log movements involving a berth.

        This searches pass_log, not the raw STOMP feed. It will include CA
        movements that were captured as learning pass windows, normally from
        live --all or live --signals.
        """
        b = normalize_berth(berth)
        with self.lock:
            return list(self.conn.execute("""
                SELECT
                    id, signal, norm_berth(from_berth) AS from_berth,
                    norm_berth(to_berth) AS to_berth, descr, pass_ts,
                    finalised_ts, event_count, '' AS special_reason
                FROM pass_log
                WHERE norm_berth(signal)=?
                   OR norm_berth(from_berth)=?
                   OR norm_berth(to_berth)=?
                ORDER BY pass_ts DESC
                LIMIT ?
            """, (b, b, b, int(limit))).fetchall())

    def movement_summary_rows(self, berth: str) -> List[sqlite3.Row]:
        b = normalize_berth(berth)
        with self.lock:
            return list(self.conn.execute("""
                SELECT
                    norm_berth(from_berth) AS from_berth,
                    norm_berth(to_berth) AS to_berth,
                    COUNT(*) AS count,
                    MIN(pass_ts) AS first_ts,
                    MAX(pass_ts) AS last_ts,
                    SUM(CASE WHEN finalised_ts IS NOT NULL THEN 1 ELSE 0 END) AS finalised_count,
                    SUM(COALESCE(event_count,0)) AS bit_event_count
                FROM pass_log
                WHERE norm_berth(signal)=?
                   OR norm_berth(from_berth)=?
                   OR norm_berth(to_berth)=?
                GROUP BY norm_berth(from_berth), norm_berth(to_berth)
                ORDER BY last_ts DESC, count DESC
            """, (b, b, b)).fetchall())

    def pass_events(self, pass_id: int, *, limit: int = 80) -> List[sqlite3.Row]:
        with self.lock:
            return list(self.conn.execute("""
                SELECT
                    event_ts, phase, delta_seconds, area, address, bit,
                    old_bit, new_bit, old_byte, new_byte, msg_type
                FROM pass_bit_events
                WHERE pass_id=?
                ORDER BY ABS(delta_seconds), event_ts
                LIMIT ?
            """, (int(pass_id), int(limit))).fetchall())

    def raw_bit_history_rows(self, key: BitKey, *, limit: int = 80) -> List[sqlite3.Row]:
        """Return true raw S-Class bit changes for a byte:bit.

        New databases/runs populate s_bit_events. Older databases created before
        this table existed will simply return no rows.
        """
        with self.lock:
            try:
                return list(self.conn.execute("""
                    SELECT event_ts, area, address, bit, old_bit, new_bit, old_byte, new_byte, msg_type
                    FROM s_bit_events
                    WHERE address=? AND bit=?
                    ORDER BY event_ts DESC
                    LIMIT ?
                """, (int(key.address, 16), int(key.bit), int(limit))).fetchall())
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc).lower():
                    return []
                raise

    def bit_evidence_history_rows(self, key: BitKey, *, signal_id: Optional[str] = None, limit: int = 80) -> List[sqlite3.Row]:
        """Return pass-window evidence attachments for a byte:bit.

        This is the old noisy view: the same raw bit change may appear once for
        every pass window it fell inside. Use it only when debugging correlation.
        """
        sig = normalize_berth(signal_id) if signal_id else None
        with self.lock:
            return list(self.conn.execute("""
                SELECT
                    e.event_ts, e.phase, e.delta_seconds, e.signal,
                    p.from_berth, p.to_berth, p.descr, e.pass_id,
                    e.old_bit, e.new_bit, e.old_byte, e.new_byte, e.msg_type
                FROM pass_bit_events e
                JOIN pass_log p ON p.id=e.pass_id
                WHERE e.address=? AND e.bit=?
                  AND (? IS NULL OR norm_berth(e.signal)=?)
                ORDER BY e.event_ts DESC, e.pass_id DESC
                LIMIT ?
            """, (int(key.address, 16), int(key.bit), sig, sig, int(limit))).fetchall())

    def bit_deduped_evidence_rows(self, key: BitKey, *, signal_id: Optional[str] = None, limit: int = 80) -> List[sqlite3.Row]:
        """Return de-duplicated bit changes reconstructed from pass_bit_events.

        This is useful for old databases that do not have s_bit_events. It groups
        by the S-Class event itself, then shows how many pass windows attached to
        that one change.
        """
        sig = normalize_berth(signal_id) if signal_id else None
        with self.lock:
            return list(self.conn.execute("""
                SELECT
                    e.event_ts, e.area, e.address, e.bit, e.old_bit, e.new_bit,
                    e.old_byte, e.new_byte, e.msg_type,
                    COUNT(*) AS attachment_count,
                    GROUP_CONCAT(DISTINCT e.signal) AS signals,
                    GROUP_CONCAT(DISTINCT norm_berth(p.from_berth) || '->' || norm_berth(p.to_berth)) AS routes,
                    MIN(ABS(e.delta_seconds)) AS closest_abs_delta
                FROM pass_bit_events e
                JOIN pass_log p ON p.id=e.pass_id
                WHERE e.address=? AND e.bit=?
                  AND (? IS NULL OR norm_berth(e.signal)=?)
                GROUP BY e.event_ts, e.area, e.address, e.bit, e.old_bit, e.new_bit, e.old_byte, e.new_byte, e.msg_type
                ORDER BY e.event_ts DESC
                LIMIT ?
            """, (int(key.address, 16), int(key.bit), sig, sig, int(limit))).fetchall())

    def nearest_pass_for_signal(self, signal_id: str, event_ts: float, *, window_seconds: float = 180.0) -> Optional[sqlite3.Row]:
        """Find the nearest stored CA pass for a mapped signal around a bit flip.

        This is deliberately restricted to the mapped/known signal. It must not
        label a known bit as an unrelated nearby signal just because another CA
        movement happened closer in time.
        """
        sig = normalize_berth(signal_id)
        with self.lock:
            return self.conn.execute("""
                SELECT
                    id, signal, from_berth, to_berth, descr, pass_ts,
                    pass_ts - ? AS delta_seconds,
                    ABS(pass_ts - ?) AS abs_delta
                FROM pass_log
                WHERE norm_berth(signal)=?
                  AND ABS(pass_ts - ?) <= ?
                ORDER BY ABS(pass_ts - ?), pass_ts DESC
                LIMIT 1
            """, (float(event_ts), float(event_ts), sig, float(event_ts), float(window_seconds), float(event_ts))).fetchone()

    def byte_summary_rows(self, *, include_pass_fallback: bool = True) -> List[Dict[str, Any]]:
        """Summarise S-Class byte addresses seen by the learner.

        s_bytes contains the latest byte values from live S-Class snapshots.
        s_bit_events contains true raw bit flips. Older databases may only have
        pass_bit_events, so this can fall back to those for bit/change counts.
        """
        with self.lock:
            current: Dict[int, sqlite3.Row] = {}
            try:
                for r in self.conn.execute("""
                    SELECT address, value, msg_type, updated_ts
                    FROM s_bytes
                    ORDER BY address
                """):
                    current[int(r["address"])] = r
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise

            per_bit: Dict[int, Dict[int, Dict[str, Any]]] = collections.defaultdict(dict)
            source = "s_bit_events"
            try:
                rows = list(self.conn.execute("""
                    SELECT
                        address, bit,
                        COUNT(*) AS changes,
                        MIN(event_ts) AS first_ts,
                        MAX(event_ts) AS last_ts,
                        GROUP_CONCAT(DISTINCT printf('%02X', new_byte)) AS values_seen
                    FROM s_bit_events
                    GROUP BY address, bit
                    ORDER BY address, bit
                """))
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc).lower():
                    rows = []
                else:
                    raise

            if not rows and include_pass_fallback:
                source = "pass_bit_events_fallback"
                try:
                    rows = list(self.conn.execute("""
                        SELECT
                            address, bit,
                            COUNT(*) AS changes,
                            MIN(event_ts) AS first_ts,
                            MAX(event_ts) AS last_ts,
                            GROUP_CONCAT(DISTINCT printf('%02X', new_byte)) AS values_seen
                        FROM pass_bit_events
                        GROUP BY address, bit
                        ORDER BY address, bit
                    """))
                except sqlite3.OperationalError as exc:
                    if "no such table" in str(exc).lower():
                        rows = []
                    else:
                        raise

            for r in rows:
                addr = int(r["address"])
                bit = int(r["bit"])
                per_bit[addr][bit] = {
                    "changes": int(r["changes"] or 0),
                    "first_ts": r["first_ts"],
                    "last_ts": r["last_ts"],
                    "values_seen": r["values_seen"] or "",
                }

            addresses = sorted(set(current.keys()) | set(per_bit.keys()))
            out: List[Dict[str, Any]] = []
            for addr in addresses:
                cur = current.get(addr)
                bits = per_bit.get(addr, {})
                out.append({
                    "address": addr,
                    "address_hex": f"{addr:02X}",
                    "latest_value": None if cur is None else int(cur["value"]),
                    "latest_msg_type": None if cur is None else cur["msg_type"],
                    "latest_ts": None if cur is None else cur["updated_ts"],
                    "bits": bits,
                    "changes": sum(int(v.get("changes") or 0) for v in bits.values()),
                    "source": source,
                })
            return out

    def missing_rows_for_berth(self, berth: str, *, limit: int = 50) -> List[sqlite3.Row]:
        b = normalize_berth(berth)
        with self.lock:
            try:
                return list(self.conn.execute("""
                    SELECT *
                    FROM missing_topology_summary
                    WHERE norm_berth(from_berth)=? OR norm_berth(to_berth)=?
                    ORDER BY last_ts DESC, count DESC
                    LIMIT ?
                """, (b, b, int(limit))).fetchall())
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc).lower():
                    return []
                raise


# =============================================================================
# Learner
# =============================================================================

class Learner:
    def __init__(
        self,
        *,
        area: str,
        store: Store,
        topology: Dict[str, Set[str]],
        known: KnownBits,
        watch_signals: Set[str],
        pre: float,
        post: float,
        recent_keep: float,
        strict: bool,
        learn_special: bool,
        ignore_known: bool,
        show_known: bool,
        print_s: bool,
        print_c: bool,
        watch_bits: Set[BitKey],
        watch_unknown: bool,
        watch_all_bits: bool,
        record_unmapped_routes: bool,
    ):
        self.area = area.upper()
        self.store = store
        self.topology = topology
        self.known = known
        self.watch_signals = {normalize_berth(x) for x in watch_signals}
        self.pre = float(pre)
        self.post = float(post)
        self.recent_keep = max(float(recent_keep), self.pre + self.post + 60.0)
        self.strict = bool(strict)
        self.learn_special = bool(learn_special)
        self.ignore_known = bool(ignore_known)
        self.show_known = bool(show_known)
        self.print_s = bool(print_s)
        self.print_c = bool(print_c)
        self.watch_bits = set(watch_bits)
        self.watch_unknown = bool(watch_unknown)
        self.watch_all_bits = bool(watch_all_bits)
        self.record_unmapped_routes = bool(record_unmapped_routes)

        self.current_bytes = self.store.load_bytes(self.area)
        self.recent_events: Deque[BitEvent] = collections.deque(maxlen=25000)
        self.pending: List[PassWindow] = []
        self.lock = threading.RLock()

    def start_banner(self) -> None:
        print(f"[INIT] area={self.area} db={self.store.path}")
        print(f"[INIT] topology signals/berths={len(self.topology)} watch_signals={len(self.watch_signals)}")
        print(f"[INIT] known_bits rows={len(self.known.rows)} described={sum(1 for r in self.known.rows if r.described)}")
        print(f"[INIT] missing topology CSVs: {self.store.missing_dir}")
        if self.strict:
            print("[INIT] strict pass mode: CA from->to must match topology nexts")
        if self.record_unmapped_routes:
            print("[INIT] missing topology will also record known-from/known-to CA routes not present in next-map")

    def handle_message(self, key: str, msg: Dict[str, Any]) -> None:
        area = str(msg.get("area_id", "")).upper()
        if area != self.area:
            return

        msg_type = str(msg.get("msg_type") or key.replace("_MSG", "")).upper()
        if msg_type in {"SF", "SG", "SH"} or key in {"SF_MSG", "SG_MSG", "SH_MSG"}:
            self._handle_s(msg_type, msg)
        elif msg_type in {"CA", "CB", "CC", "CT"} or key in {"CA_MSG", "CB_MSG", "CC_MSG", "CT_MSG"}:
            self._handle_c(msg_type, msg)

    def _handle_s(self, msg_type: str, msg: Dict[str, Any]) -> None:
        ts = parse_nr_time_ms(msg.get("time"))
        try:
            start_address = parse_hex_address(msg.get("address"))
            data_bytes = split_hex_bytes(msg.get("data"))
        except Exception as exc:
            print(f"[WARN] Bad S-Class ignored: {exc} {msg!r}", file=sys.stderr)
            return

        with self.lock:
            for offset, new_value in enumerate(data_bytes):
                address = start_address + offset
                old_value = self.current_bytes.get(address)
                self.current_bytes[address] = new_value
                self.store.save_byte(self.area, address, new_value, msg_type, ts)

                if old_value is None:
                    continue
                if old_value == new_value:
                    continue

                changed = []
                for bit in range(8):
                    old_bit = bit_value(old_value, bit)
                    new_bit = bit_value(new_value, bit)
                    if old_bit == new_bit:
                        continue
                    event = BitEvent(
                        ts=ts, area=self.area, address=address, bit=bit,
                        old_bit=old_bit, new_bit=new_bit,
                        old_byte=old_value, new_byte=new_value, msg_type=msg_type,
                    )
                    self.recent_events.append(event)
                    self.store.record_raw_bit_event(event)
                    self._maybe_print_watch(event)
                    changed.append(f"b{bit}:{old_bit}->{new_bit}")

                if self.print_s and changed:
                    print(f"[S] {fmt_ts(ts)} {msg_type} addr {address:02X} {old_value:02X}->{new_value:02X} {' '.join(changed)}")

            self._prune_locked(time.time())

    def _maybe_print_watch(self, event: BitEvent) -> None:
        key = event.key
        should = self.watch_all_bits or key in self.watch_bits or (self.watch_unknown and not self.known.described(key))
        if not should:
            return
        state = "KNOWN" if self.known.described(key) else "UNKNOWN"
        print(f"[WATCH] {event.compact()} {state} :: {self.known.describe(key)}", flush=True)

    def _handle_c(self, msg_type: str, msg: Dict[str, Any]) -> None:
        ts = parse_nr_time_ms(msg.get("time"))
        if self.print_c and msg_type != "CT":
            print(f"[C] {fmt_ts(ts)} {msg_type} {msg}")

        # Keep the latest berth/headcode state from all normal TD berth events,
        # not just CA step messages. Without this, a train that is interposed
        # directly into a berth (CC) or cancelled from a berth (CB) will never be
        # reflected in /signal until a later CA happens. That was the main reason
        # some berths appeared permanently clear or stale in Discord.
        if msg_type == "CB":
            berth = normalize_berth(msg.get("from") or msg.get("to") or msg.get("berth") or msg.get("address") or "")
            descr = str(msg.get("descr", "") or "").strip()
            if berth:
                self.store.record_berth_state(berth, "", False, ts, msg_type)
                self._check_missing_topology(ts, descr, berth, berth, msg)
            return

        if msg_type == "CC":
            berth = normalize_berth(msg.get("to") or msg.get("from") or msg.get("berth") or msg.get("address") or "")
            descr = str(msg.get("descr", "") or "").strip()
            if berth:
                self.store.record_berth_state(berth, descr, bool(descr), ts, msg_type)
                self._check_missing_topology(ts, descr, berth, berth, msg)
            return

        if msg_type != "CA":
            return

        from_berth = normalize_berth(msg.get("from", ""))
        to_berth = normalize_berth(msg.get("to", ""))
        descr = str(msg.get("descr", "") or "").strip()

        if not from_berth or not to_berth:
            return

        # CA means the description moved from one berth to another.
        self.store.record_berth_state(from_berth, "", False, ts, msg_type)
        self.store.record_berth_state(to_berth, descr, True, ts, msg_type)

        self._check_missing_topology(ts, descr, from_berth, to_berth, msg)

        if from_berth not in self.watch_signals:
            return

        special_reason = SPECIAL_NON_LEARNING_MOVES.get((from_berth, to_berth), "")
        if special_reason and not self.learn_special:
            print(f"[PASS-SKIP] {fmt_ts(ts)} {from_berth}->{to_berth} {descr or '----'} special: {special_reason}")
            return

        expected = self.topology.get(from_berth, set())
        if self.strict and (not expected or to_berth not in expected):
            print(f"[PASS-SKIP] {fmt_ts(ts)} {from_berth}->{to_berth} not in topology nexts={sorted(expected)}")
            return

        with self.lock:
            pre_events = [e for e in self.recent_events if ts - self.pre <= e.ts <= ts]
            obs = PassWindow(
                pass_id=0,
                signal=from_berth,
                from_berth=from_berth,
                to_berth=to_berth,
                descr=descr,
                ts=ts,
                pre_events=pre_events,
            )
            pass_id = self.store.create_pass(obs)
            obs.pass_id = pass_id
            self.pending.append(obs)

        print(f"[PASS] {fmt_ts(ts)} id={pass_id} {from_berth}->{to_berth} {descr or '----'} pre_events={len(pre_events)}")

    def _check_missing_topology(self, ts: float, descr: str, from_berth: str, to_berth: str, raw: Dict[str, Any]) -> None:
        from_real = from_berth in self.topology
        to_real = to_berth in self.topology
        from_external = from_berth in EXPECTED_EXTERNAL_BERTHS
        to_external = to_berth in EXPECTED_EXTERNAL_BERTHS

        # Expected boundary/external moves are not real T3 signal-learning berths,
        # but they are not "missing signal topology" either. Suppress them from
        # new missing reports unless the move is a genuinely unknown direction.
        if is_expected_external_move(from_berth, to_berth):
            return

        expected = set(self.topology.get(from_berth, set()))
        expected.update(external_expected_nexts(from_berth))

        from_known = from_real or from_external
        to_known = to_real or to_external
        route_missing = from_real and to_real and bool(expected) and to_berth not in expected

        if not from_known or not to_known:
            missing_bits = []
            if not from_known:
                missing_bits.append("from_berth_not_in_topology")
            if not to_known:
                missing_bits.append("to_berth_not_in_topology")
            reason = "+".join(missing_bits)
        elif route_missing and self.record_unmapped_routes:
            reason = "route_not_in_next_map"
        else:
            return

        self.store.record_missing_topology(
            ts=ts,
            area=self.area,
            descr=descr,
            from_berth=from_berth,
            to_berth=to_berth,
            missing_from=not from_known,
            missing_to=not to_known,
            route_missing=route_missing,
            expected_nexts=sorted(expected),
            reason=reason,
            raw=raw,
        )
        print(f"[MISSING-TOPOLOGY] {fmt_ts(ts)} {from_berth}->{to_berth} {descr or '----'} reason={reason}")

    def tick(self) -> None:
        with self.lock:
            now = time.time()
            ready = [p for p in self.pending if now >= p.ts + self.post]
            self.pending = [p for p in self.pending if now < p.ts + self.post]
            self._prune_locked(now)

        for obs in ready:
            self._finalise(obs)

    def _finalise(self, obs: PassWindow) -> None:
        with self.lock:
            after_events = [e for e in self.recent_events if obs.ts < e.ts <= obs.ts + self.post]
            events = list(obs.pre_events) + after_events

        stored = 0
        hidden_by_default = 0
        for event in events:
            # Store known and unknown events. Reports hide described known bits by
            # default, but --show-known can only audit them later if they were
            # actually written to pass_bit_events.
            if self.known.ignored(event.key):
                hidden_by_default += 1
            phase = "before" if event.ts <= obs.ts else "after"
            self.store.record_pass_event(obs.pass_id, obs.signal, event, phase, event.ts - obs.ts)
            stored += 1

        self.store.finalise_pass(obs.pass_id, stored)
        print(f"[RESULT] id={obs.pass_id} {obs.signal}->{obs.to_berth} events={stored} known_hidden_by_default={hidden_by_default}")

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self.recent_keep
        while self.recent_events and self.recent_events[0].ts < cutoff:
            self.recent_events.popleft()


# =============================================================================
# Reports
# =============================================================================

def candidate_best(row: sqlite3.Row, pass_count: int) -> Dict[str, Any]:
    before_on = int(row["before_on"] or 0)
    before_off = int(row["before_off"] or 0)
    after_on = int(row["after_on"] or 0)
    after_off = int(row["after_off"] or 0)
    choices = [
        (before_on, "before 0->1", "likely proceed/route set before pass"),
        (before_off, "before 1->0", "likely danger cleared before pass"),
        (after_on, "after 0->1", "likely restored/track occupied after pass"),
        (after_off, "after 1->0", "likely proceed removed/route released after pass"),
    ]
    best_count, bucket, guess = max(choices, key=lambda x: x[0])
    pct = best_count / pass_count if pass_count else 0.0
    return {
        "best_count": best_count,
        "bucket": bucket,
        "guess": guess,
        "pct": pct,
        "before_on": before_on,
        "before_off": before_off,
        "after_on": after_on,
        "after_off": after_off,
    }


def print_signal_report(
    store: Store,
    known: KnownBits,
    signal_id: str,
    *,
    score_window: float,
    show_known: bool,
    show_cross_known: bool,
    min_pct: float,
    min_pass_count: int,
    max_avg_delta: Optional[float],
    limit: int,
) -> None:
    signal_id = normalize_berth(signal_id)
    pass_count = store.pass_count(signal_id)
    print(f"\n=== Signal {signal_id} | finalised passes={pass_count} | score window +/-{score_window:.0f}s ===")

    known_for_signal = sorted(known.keys_for_signal(signal_id), key=lambda k: (int(k.address, 16), k.bit))
    known_key_set = set(known_for_signal)
    if known_for_signal:
        print("Known CSV bit(s) for this signal: " + ", ".join(k.label for k in known_for_signal))
    else:
        print("Known CSV bit(s) for this signal: none")

    if pass_count <= 0:
        print("No evidence yet.")
        return

    rows = store.candidate_rows(signal_id, score_window=score_window, limit=max(limit * 12, 200))
    row_by_key: Dict[BitKey, sqlite3.Row] = {}
    for r in rows:
        row_by_key[BitKey(f"{int(r['address']):02X}", int(r["bit"]))] = r

    def format_candidate(r: sqlite3.Row, key: BitKey, best: Dict[str, Any]) -> str:
        status = "KNOWN" if known.described(key) else "UNKNOWN"
        return (
            f"{key.label:<5} {status:<7} {best['bucket']:<13} "
            f"{best['best_count']}/{pass_count} ({best['pct']*100:>3.0f}%) "
            f"avg_delta={float(r['avg_abs'] or 0.0):>4.1f}s "
            f"bo={best['before_on']} bf={best['before_off']} ao={best['after_on']} af={best['after_off']} | "
            f"{best['guess']} | {known.describe(key)}"
        )

    if show_known and known_for_signal:
        print("Known bit evidence audit:")
        for key in known_for_signal:
            r = row_by_key.get(key)
            if r is None:
                print(
                    f"  {key.label:<5} KNOWN   not stored in pass-window evidence for this DB/run | "
                    f"{known.describe(key)}"
                )
                print(
                    "        reason: older live runs may have suppressed known bits before storing; "
                    "new v7 live runs store them and reports hide them by default."
                )
                continue
            best = candidate_best(r, pass_count)
            print("  " + format_candidate(r, key, best))

    printable = []
    hidden_own_known = 0
    hidden_cross_known = 0
    for r in rows:
        key = BitKey(f"{int(r['address']):02X}", int(r["bit"]))
        is_known = known.ignored(key)
        is_own_known = key in known_key_set

        # Known rows do not belong in the normal candidate list.
        # --show-known audits this signal's own known bit(s) above.
        # Known bits belonging to other signals are cross-signal contamination and
        # are suppressed by default, not shown as candidates for this signal.
        if is_known:
            if is_own_known:
                hidden_own_known += 1
                continue
            if not show_cross_known:
                hidden_cross_known += 1
                continue

        best = candidate_best(r, pass_count)
        if best["pct"] < min_pct:
            continue
        if int(best["best_count"]) < int(min_pass_count):
            continue
        if max_avg_delta is not None and float(r["avg_abs"] or 0.0) > float(max_avg_delta):
            continue
        printable.append((r, key, best))

    if hidden_own_known and not show_known:
        print(f"Own known bit row(s) hidden from candidate list: {hidden_own_known} (use --show-known to audit this signal's known bit)")
    if hidden_cross_known:
        print(f"Other-signal known bit row(s) suppressed: {hidden_cross_known} (use --show-cross-known to inspect contamination)")

    printable.sort(key=lambda item: (item[2]["pct"], item[2]["best_count"], -float(item[0]["avg_abs"] or 9999)), reverse=True)
    if not printable:
        print("No candidates above threshold after known-bit filtering.")
    for r, key, best in printable[:limit]:
        print(format_candidate(r, key, best))

    routes = store.route_counts(signal_id)
    if len(routes) > 1:
        print("Routes seen: " + ", ".join(f"{signal_id}->{r['to_berth']} ({int(r['pass_count'])})" for r in routes))



def collect_candidate_summary(
    store: Store,
    known: KnownBits,
    signals: Sequence[str],
    *,
    score_window: float,
    min_pct: float,
    min_pass_count: int,
    max_avg_delta: Optional[float],
    include_known: bool,
    per_signal_limit: int = 80,
) -> List[Tuple[str, int, BitKey, sqlite3.Row, Dict[str, Any]]]:
    """Collect filtered candidate rows across signals for compact progress output."""
    out: List[Tuple[str, int, BitKey, sqlite3.Row, Dict[str, Any]]] = []
    for sig in signals:
        sig = normalize_berth(sig)
        pc = store.pass_count(sig)
        if pc <= 0:
            continue
        for r in store.candidate_rows(sig, score_window=score_window, limit=per_signal_limit):
            key = BitKey(f"{int(r['address']):02X}", int(r["bit"]))
            if known.ignored(key) and not include_known:
                continue
            best = candidate_best(r, pc)
            if best["pct"] < min_pct:
                continue
            if int(best["best_count"]) < int(min_pass_count):
                continue
            if max_avg_delta is not None and float(r["avg_abs"] or 0.0) > float(max_avg_delta):
                continue
            out.append((sig, pc, key, r, best))
    out.sort(key=lambda item: (item[4]["best_count"], item[4]["pct"], -float(item[3]["avg_abs"] or 9999)), reverse=True)
    return out


def print_progress_report(
    store: Store,
    topology: Dict[str, Set[str]],
    known: KnownBits,
    *,
    score_window: float,
    min_pct: float,
    min_pass_count: int,
    max_avg_delta: Optional[float],
    limit: int,
    include_known: bool,
) -> None:
    signals = sorted(topology.keys())
    with_evidence: List[Tuple[str, int]] = []
    no_evidence: List[str] = []
    for sig in signals:
        pc = store.pass_count(sig)
        if pc > 0:
            with_evidence.append((sig, pc))
        else:
            no_evidence.append(sig)

    total_passes = sum(pc for _, pc in with_evidence)
    known_signal_count = sum(1 for sig in signals if known.keys_for_signal(sig))
    print("[PROGRESS] T3 learner database summary")
    print(f"  topology entries: {len(signals)}")
    print(f"  entries with evidence: {len(with_evidence)}")
    print(f"  entries with no evidence yet: {len(no_evidence)}")
    print(f"  finalised pass rows across topology: {total_passes}")
    print(f"  topology signals with known CSV bit(s): {known_signal_count}")
    print(
        f"  candidate filter: support>={min_pass_count}, pct>={min_pct*100:.0f}%, window=+/-{score_window:.0f}s"
        + (f", avg_delta<={max_avg_delta:.1f}s" if max_avg_delta is not None else "")
    )

    top_passes = sorted(with_evidence, key=lambda x: (-x[1], x[0]))[:20]
    if top_passes:
        print("\n[PROGRESS] Busiest learned signals:")
        print("  " + ", ".join(f"{sig}({pc})" for sig, pc in top_passes))

    no_evidence_known = [sig for sig in no_evidence if known.keys_for_signal(sig)]
    if no_evidence_known:
        print("\n[PROGRESS] Known CSV signals still waiting for CA pass evidence:")
        print("  " + ", ".join(no_evidence_known[:80]) + (f" ... +{len(no_evidence_known)-80}" if len(no_evidence_known) > 80 else ""))

    candidates = collect_candidate_summary(
        store,
        known,
        signals,
        score_window=score_window,
        min_pct=min_pct,
        min_pass_count=min_pass_count,
        max_avg_delta=max_avg_delta,
        include_known=include_known,
    )
    print(f"\n[PROGRESS] Strong {'known+unknown' if include_known else 'unknown'} candidate rows: {len(candidates)}")
    for sig, pc, key, r, best in candidates[:limit]:
        status = "KNOWN" if known.described(key) else "UNKNOWN"
        print(
            f"  signal={sig:<5} passes={pc:<3} {key.label:<5} {status:<7} "
            f"{best['bucket']:<13} {best['best_count']}/{pc} ({best['pct']*100:>3.0f}%) "
            f"avg_delta={float(r['avg_abs'] or 0.0):>4.1f}s | {best['guess']} | {known.describe(key)}"
        )
    if len(candidates) > limit:
        print(f"  ... {len(candidates) - limit} more candidates hidden by --limit")



def known_bits_for_address(known: KnownBits, address_hex: str) -> List[str]:
    out: List[str] = []
    addr = normalise_address(address_hex)
    for row in known.rows:
        if row.key.address != addr or not row.described:
            continue
        sigs = sorted(known.signals_for_key(row.key))
        if sigs:
            out.append(f"b{row.key.bit}=" + "/".join(sigs))
        else:
            out.append(f"b{row.key.bit}={row.element_type or 'Known'}")
    return out


def print_bytes_report(
    store: Store,
    known: KnownBits,
    *,
    address_filter: Sequence[str],
    show_values: bool,
    known_only: bool,
    export_path: Optional[Path],
) -> None:
    rows = store.byte_summary_rows(include_pass_fallback=True)
    wanted: Set[str] = {normalise_address(x) for x in address_filter}
    if wanted:
        rows = [r for r in rows if str(r["address_hex"]) in wanted]

    if known_only:
        rows = [r for r in rows if known_bits_for_address(known, str(r["address_hex"]))]

    if export_path:
        with export_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["address", "latest_value", "latest_msg_type", "latest_ts", "bits_seen", "changes", "known_mappings", "values_seen"])
            for r in rows:
                bits = r["bits"]
                bit_labels = ",".join(f"b{b}" for b in sorted(bits))
                vals: Set[str] = set()
                for binfo in bits.values():
                    for v in str(binfo.get("values_seen") or "").split(","):
                        if v:
                            vals.add(v)
                latest = "" if r["latest_value"] is None else f"{int(r['latest_value']):02X}"
                writer.writerow([
                    r["address_hex"], latest, r["latest_msg_type"] or "", fmt_ts(float(r["latest_ts"])) if r["latest_ts"] else "",
                    bit_labels, int(r["changes"]), "; ".join(known_bits_for_address(known, str(r["address_hex"]))),
                    ",".join(sorted(vals)),
                ])
        print(f"[BYTES] exported to {export_path}")

    print("[BYTES] S-Class byte addresses seen by learner")
    print(f"  rows={len(rows)}")
    print("  meaning: address = S-Class byte address; b0-b7 = bit positions seen changing")
    for r in rows:
        addr = str(r["address_hex"])
        bits = r["bits"]
        bit_labels = ",".join(f"b{b}" for b in sorted(bits)) if bits else "-"
        latest = "--" if r["latest_value"] is None else f"{int(r['latest_value']):02X}"
        last = "-" if not r["latest_ts"] else fmt_ts(float(r["latest_ts"]))
        known_txt = "; ".join(known_bits_for_address(known, addr)) or "-"
        print(f"  {addr} latest={latest} bits_seen={bit_labels:<23} changes={int(r['changes']):<5} known={known_txt} last={last}")
        if show_values and bits:
            for b in sorted(bits):
                info = bits[b]
                values = str(info.get("values_seen") or "-")
                print(f"      b{b}: changes={int(info.get('changes') or 0):<4} values_seen={values}")



def print_missing_report(store: Store, *, export_path: Optional[Path] = None, show_boundary: bool = False) -> None:
    rows = store.missing_summary_rows()
    if export_path:
        store.export_missing_summary(export_path)
        print(f"[MISSING] exported summary to {export_path}")

    if not rows:
        print("[MISSING] No missing topology observations stored.")
        return

    boundary_rows = [r for r in rows if is_expected_external_move(str(r["from_berth"]), str(r["to_berth"]))]
    real_rows = [r for r in rows if not is_expected_external_move(str(r["from_berth"]), str(r["to_berth"]))]

    print("[MISSING] Missing topology observations:")
    if not real_rows:
        print("  none after filtering expected external/boundary berth chains")
    for r in real_rows[:50]:
        print(
            f"  {r['from_berth']}->{r['to_berth']} count={int(r['count'])} "
            f"last={fmt_ts(float(r['last_ts']))} reason={r['reason']} "
            f"descr={r['example_descr'] or '----'} expected={r['expected_nexts'] or '-'}"
        )
    if len(real_rows) > 50:
        print(f"  ... {len(real_rows) - 50} more rows in CSV/SQLite")

    if boundary_rows and not show_boundary:
        print(
            f"[MISSING] Suppressed {len(boundary_rows)} expected external/boundary row(s); "
            "use missing --show-boundary to list them."
        )
    if boundary_rows and show_boundary:
        print("[MISSING] Expected external/boundary rows, not real T3 signal-learning gaps:")
        for r in boundary_rows[:50]:
            print(
                f"  {r['from_berth']}->{r['to_berth']} count={int(r['count'])} "
                f"last={fmt_ts(float(r['last_ts']))} descr={r['example_descr'] or '----'}"
            )
        if len(boundary_rows) > 50:
            print(f"  ... {len(boundary_rows) - 50} more boundary rows")



def print_movement_report(store: Store, known: KnownBits, berth: str, *, limit: int, show_events: bool, event_limit: int) -> None:
    berth = normalize_berth(berth)
    print(f"[MOVES] Learned/pass-log movements involving berth/signal {berth}")

    summary = store.movement_summary_rows(berth)
    if summary:
        print("[MOVES] Route summary:")
        for r in summary:
            print(
                f"  {r['from_berth']}->{r['to_berth']} count={int(r['count'])} "
                f"finalised={int(r['finalised_count'] or 0)} bit_events={int(r['bit_event_count'] or 0)} "
                f"last={fmt_ts(float(r['last_ts']))}"
            )
    else:
        print("[MOVES] No pass_log movements found for this berth.")

    missing = store.missing_rows_for_berth(berth, limit=limit)
    if missing:
        print("[MOVES] Missing/unmapped topology observations involving this berth:")
        for r in missing:
            print(
                f"  {r['from_berth']}->{r['to_berth']} count={int(r['count'])} "
                f"last={fmt_ts(float(r['last_ts']))} reason={r['reason']} "
                f"expected={r['expected_nexts'] or '-'}"
            )

    rows = store.movement_rows(berth, limit=limit)
    if not rows:
        return

    print("[MOVES] Recent pass_log rows:")
    for r in rows:
        final = "finalised" if r["finalised_ts"] is not None else "pending"
        print(
            f"  id={int(r['id'])} {fmt_ts(float(r['pass_ts']))} "
            f"{r['from_berth']}->{r['to_berth']} {r['descr'] or '----'} "
            f"signal={r['signal']} events={int(r['event_count'] or 0)} {final}"
        )
        if show_events:
            events = store.pass_events(int(r["id"]), limit=event_limit)
            for e in events:
                key = BitKey(f"{int(e['address']):02X}", int(e["bit"]))
                status = "KNOWN" if known.described(key) else "UNKNOWN"
                old_byte = "??" if e["old_byte"] is None else f"{int(e['old_byte']):02X}"
                print(
                    f"      {e['phase']:<6} {float(e['delta_seconds']):>+5.1f}s "
                    f"{key.label:<5} {status:<7} "
                    f"{e['old_bit']}->{e['new_bit']} byte {old_byte}->{int(e['new_byte']):02X} "
                    f"{e['msg_type'] or ''} | {known.describe(key)}"
                )


def print_bit_history(
    store: Store,
    known: KnownBits,
    bit_spec: str,
    *,
    signal_id: Optional[str],
    limit: int,
    evidence: bool = False,
    details: bool = False,
    link_window: float = 30.0,
) -> None:
    key = parse_bit_spec(bit_spec)
    mapped_signals = {normalize_berth(signal_id)} if signal_id else known.signals_for_key(key)

    def bit_line_from_attachment(r: sqlite3.Row, *, mapped_signal: Optional[str] = None) -> str:
        ts_text = fmt_ts(float(r["event_ts"]))
        sig = mapped_signal or normalize_berth(r["signal"]) or "-"
        frm = normalize_berth(r["from_berth"]) or "-"
        to = normalize_berth(r["to_berth"]) or "-"
        headcode = str(r["descr"] or "----").strip() or "----"
        return (
            f"{ts_text} signal={sig} {frm}->{to} {headcode} "
            f"addr {int(key.address, 16):02X} b{key.bit} changed {r['old_bit']}->{r['new_bit']}"
        )

    def bit_line_no_context(event_ts: float, old_bit: Any, new_bit: Any, *, mapped_signal: Optional[str]) -> str:
        sig = mapped_signal or "-"
        return (
            f"{fmt_ts(float(event_ts))} signal={sig} no-linked-pass ---- "
            f"addr {int(key.address, 16):02X} b{key.bit} changed {old_bit}->{new_bit}"
        )

    # Default output:
    # - one line per actual bit flip
    # - if the CSV maps the bit to a signal, label it with that mapped signal
    # - only show from->to/headcode when that mapped signal has an attached pass
    # This avoids the previous bug where 25:3 (known as 6244) could be printed as
    # signal=6209/6234/etc just because those CA moves were nearby.
    if not evidence and not details:
        attachment_rows = store.bit_evidence_history_rows(
            key,
            signal_id=None,  # pull all attachments; choose correct context below
            limit=max(limit * 200, 1000),
        )

        grouped: Dict[Tuple[Any, ...], List[sqlite3.Row]] = collections.defaultdict(list)
        for r in attachment_rows:
            group_key = (
                float(r["event_ts"]),
                int(key.address, 16),
                int(key.bit),
                r["old_bit"],
                r["new_bit"],
                r["old_byte"],
                r["new_byte"],
                r["msg_type"],
            )
            grouped[group_key].append(r)

        if grouped:
            ordered_items = sorted(grouped.items(), key=lambda item: float(item[0][0]), reverse=True)[:limit]
            mapped_label = ",".join(sorted(mapped_signals)) if mapped_signals else None

            for group_key, rows in ordered_items:
                event_ts, _address, _bit, old_bit, new_bit, _old_byte, _new_byte, _msg_type = group_key

                chosen: Optional[sqlite3.Row] = None

                if mapped_signals:
                    # Prefer a real pass for the mapped signal around this bit flip.
                    # This can find the headcode even if the old pass-window attachment
                    # table did not directly attach the bit to that signal.
                    mapped_matches: List[sqlite3.Row] = []
                    for mapped in mapped_signals:
                        nearest = store.nearest_pass_for_signal(mapped, float(event_ts), window_seconds=float(link_window))
                        if nearest is not None:
                            mapped_matches.append(nearest)

                    if mapped_matches:
                        nearest = min(mapped_matches, key=lambda r: abs(float(r["delta_seconds"] or 999999)))
                        ts_text = fmt_ts(float(event_ts))
                        sig = normalize_berth(nearest["signal"]) or mapped_label or "-"
                        frm = normalize_berth(nearest["from_berth"]) or "-"
                        to = normalize_berth(nearest["to_berth"]) or "-"
                        headcode = str(nearest["descr"] or "----").strip() or "----"
                        delta = float(nearest["delta_seconds"] or 0.0)
                        print(
                            f"{ts_text} signal={sig} {frm}->{to} {headcode} "
                            f"pass_delta={delta:+.0f}s "
                            f"addr {int(key.address, 16):02X} b{key.bit} changed {old_bit}->{new_bit}"
                        )
                    else:
                        # The bit is mapped, but this DB has no nearby pass for the mapped
                        # signal. Do NOT lie by printing an unrelated nearest pass.
                        print(bit_line_no_context(event_ts, old_bit, new_bit, mapped_signal=mapped_label))
                    continue

                # Unknown/unmapped bit: choose the nearest CA/pass only as context.
                chosen = min(rows, key=lambda r: abs(float(r["delta_seconds"] or 999999)))
                print(bit_line_from_attachment(chosen))
            return

        # New DBs store raw S-Class rows even when no CA pass window attached.
        raw_rows = store.raw_bit_history_rows(key, limit=limit)
        if raw_rows:
            mapped_label = ",".join(sorted(mapped_signals)) if mapped_signals else None
            for r in raw_rows:
                print(bit_line_no_context(float(r["event_ts"]), r["old_bit"], r["new_bit"], mapped_signal=mapped_label))
            return

        print(f"No stored bit changes for addr {key.address} b{key.bit}")
        return

    sig_text = f" on signal {normalize_berth(signal_id)}" if signal_id else ""
    print(f"[BIT] {key.label}{sig_text}")
    print(f"[BIT] Known CSV: {known.describe(key)}")

    if evidence:
        print("[BIT] Pass-window evidence attachments, not raw history. The same S-Class flip can appear multiple times if it sat inside multiple pass windows.")
        rows = store.bit_evidence_history_rows(key, signal_id=signal_id, limit=limit)
        if not rows:
            print("[BIT] No stored pass_bit_events found for this bit.")
            return
        for r in rows:
            old_byte = "??" if r["old_byte"] is None else f"{int(r['old_byte']):02X}"
            print(
                f"  {fmt_ts(float(r['event_ts']))} pass_id={int(r['pass_id'])} "
                f"signal={r['signal']} {r['from_berth']}->{r['to_berth']} {r['descr'] or '----'} "
                f"{r['phase']} {float(r['delta_seconds']):+5.1f}s "
                f"addr {int(key.address,16):02X} b{key.bit} changed {r['old_bit']}->{r['new_bit']} "
                f"full_byte {old_byte}->{int(r['new_byte']):02X} {r['msg_type'] or ''}"
            )
        return

    raw_rows = store.raw_bit_history_rows(key, limit=limit)
    if raw_rows:
        print("[BIT] Raw S-Class bit changes:")
        for r in raw_rows:
            old_byte = "??" if r["old_byte"] is None else f"{int(r['old_byte']):02X}"
            mapped_text = ",".join(sorted(mapped_signals)) if mapped_signals else "-"
            print(
                f"  {fmt_ts(float(r['event_ts']))} signal={mapped_text} no-linked-pass ---- "
                f"addr {int(r['address']):02X} b{int(r['bit'])} changed {r['old_bit']}->{r['new_bit']} "
                f"full_byte {old_byte}->{int(r['new_byte']):02X} {r['msg_type'] or ''}"
            )
        if signal_id:
            print("[BIT] Note: raw S-Class events are not signal-specific. The signal filter only applies to the fallback/evidence view.")
        return

    print("[BIT] No raw S-Class history found in s_bit_events. This DB was probably collected before raw bit history was added.")
    print("[BIT] Showing de-duplicated pass-window evidence instead. attachment_count shows how many pass windows reused the same S-Class flip.")
    rows = store.bit_deduped_evidence_rows(key, signal_id=signal_id, limit=limit)
    if not rows:
        print("[BIT] No stored evidence found for this bit.")
        return
    for r in rows:
        old_byte = "??" if r["old_byte"] is None else f"{int(r['old_byte']):02X}"
        routes = str(r["routes"] or "")
        if len(routes) > 90:
            routes = routes[:87] + "..."
        signals = str(r["signals"] or "")
        if len(signals) > 60:
            signals = signals[:57] + "..."
        print(
            f"  {fmt_ts(float(r['event_ts']))} "
            f"addr {int(r['address']):02X} b{int(r['bit'])} changed {r['old_bit']}->{r['new_bit']} "
            f"full_byte {old_byte}->{int(r['new_byte']):02X} {r['msg_type'] or ''} | "
            f"attachments={int(r['attachment_count'])} closest_delta={float(r['closest_abs_delta'] or 0):.1f}s "
            f"signals={signals} routes={routes}"
        )


# =============================================================================
# STOMP connection
# =============================================================================

class Listener(stomp.ConnectionListener if stomp is not None else object):
    def __init__(self, learner: Learner):
        self.learner = learner
        self.messages = 0
        self.last_error = ""

    def on_message(self, frame):
        self.messages += 1
        try:
            for key, payload in iter_message_objects(frame.body):
                self.learner.handle_message(key, payload)
        except Exception:
            print("[ERROR] failed to handle STOMP frame", file=sys.stderr)
            traceback.print_exc()

    def on_error(self, frame):
        self.last_error = f"{getattr(frame, 'headers', {})} {getattr(frame, 'body', '')}"
        print(f"[STOMP-ERROR] {self.last_error}", file=sys.stderr)

    def on_disconnected(self):
        print("[STOMP] disconnected")


def read_credentials(args: argparse.Namespace) -> Tuple[str, str]:
    if args.username and args.password:
        return args.username, args.password
    if os.environ.get("NROD_USER") and os.environ.get("NROD_PASS"):
        return os.environ["NROD_USER"], os.environ["NROD_PASS"]
    secrets = Path(args.secrets)
    if secrets.exists():
        data = json.loads(secrets.read_text(encoding="utf-8"))
        if isinstance(data, list) and len(data) >= 2:
            return str(data[0]), str(data[1])
        if isinstance(data, dict):
            return str(data["username"]), str(data["password"])
    raise SystemExit("No credentials. Use secrets.json, NROD_USER/NROD_PASS, or --username/--password.")


def run_live(args: argparse.Namespace, learner: Learner) -> None:
    if stomp is None:
        raise SystemExit("Missing dependency: install stomp.py")

    username, password = read_credentials(args)
    stop_event = threading.Event()

    def stop_handler(signum, frame):
        stop_event.set()
        print("[STOP] stopping...")

    signal_module.signal(signal_module.SIGINT, stop_handler)
    signal_module.signal(signal_module.SIGTERM, stop_handler)

    listener = Listener(learner)
    delay = args.reconnect_delay

    learner.start_banner()

    while not stop_event.is_set():
        conn = None
        try:
            print(f"[STOMP] connecting {args.host}:{args.port} topic={args.topic}")
            conn = stomp.Connection12(
                host_and_ports=[(args.host, args.port)],
                keepalive=True,
                heartbeats=(args.heartbeat_ms, args.heartbeat_ms),
            )
            conn.set_listener("t3-clean-learner", listener)
            conn.connect(username=username, passcode=password, wait=True)
            conn.subscribe(destination=args.topic, id=args.subscription_id, ack="auto")
            print("[STOMP] subscribed")

            last_status = time.time()
            while conn.is_connected() and not stop_event.is_set():
                learner.tick()
                if time.time() - last_status >= args.status_every:
                    last_status = time.time()
                    print(f"[STATUS] {fmt_ts(time.time())} messages={listener.messages} pending={len(learner.pending)} recent_s={len(learner.recent_events)}")
                time.sleep(0.5)

        except Exception as exc:
            print(f"[STOMP] error: {exc!r}", file=sys.stderr)
            if args.debug_tracebacks:
                traceback.print_exc()
        finally:
            try:
                if conn is not None and conn.is_connected():
                    conn.disconnect()
            except Exception:
                pass

        if stop_event.is_set():
            break
        print(f"[STOMP] reconnect in {delay:.0f}s")
        end = time.time() + delay
        while time.time() < end and not stop_event.is_set():
            time.sleep(0.5)
        delay = min(args.max_reconnect_delay, delay * 2)


# =============================================================================
# CLI
# =============================================================================

def split_csv_values(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    for value in values:
        for part in str(value).split(","):
            part = part.strip()
            if part:
                out.append(part)
    return out


def parse_bit_values(values: Sequence[str]) -> Set[BitKey]:
    return {parse_bit_spec(x) for x in split_csv_values(values)}


def build_common(args: argparse.Namespace) -> Tuple[Store, Dict[str, Set[str]], KnownBits]:
    topology = build_topology(Path(args.topology_json) if args.topology_json else None)
    known = load_known_bits(Path(args.known))
    store = Store(Path(args.db), Path(args.missing_dir))
    return store, topology, known


def choose_signals(args: argparse.Namespace, topology: Dict[str, Set[str]]) -> Set[str]:
    if getattr(args, "all", False):
        return set(topology.keys())
    signals = set()
    for s in split_csv_values(getattr(args, "signals", []) or []):
        signals.add(normalize_berth(s))
    return signals


def make_learner(args: argparse.Namespace, *, mode: str) -> Learner:
    store, topology, known = build_common(args)

    watch_signals = choose_signals(args, topology)
    if not watch_signals and mode == "live":
        # Sensible default for learning: all known topology. Watch mode defaults
        # to no pass learning unless --signals/--all is explicitly supplied.
        watch_signals = set(topology.keys())

    watch_bits = parse_bit_values(getattr(args, "bits", []) or [])
    for signal_id in split_csv_values(getattr(args, "signal", []) or []):
        watch_bits.update(known.keys_for_signal(signal_id))

    return Learner(
        area=args.area,
        store=store,
        topology=topology,
        known=known,
        watch_signals=watch_signals,
        pre=args.pre,
        post=args.post,
        recent_keep=args.recent_keep,
        strict=getattr(args, "strict", False),
        learn_special=getattr(args, "learn_special", False),
        ignore_known=not getattr(args, "show_known", False),
        show_known=getattr(args, "show_known", False),
        print_s=getattr(args, "print_s", False),
        print_c=getattr(args, "print_c", False),
        watch_bits=watch_bits,
        watch_unknown=getattr(args, "unknown", False),
        watch_all_bits=getattr(args, "all_bits", False),
        record_unmapped_routes=getattr(args, "record_unmapped_routes", False),
    )


def add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--area", default="T3")
    p.add_argument("--db", default="td_signal_bit_learner.sqlite")
    p.add_argument("--known", "--known-csv", dest="known", default="known_bits.csv", help="Known bit CSV. Compact and full V11 formats are supported.")
    p.add_argument("--topology-json", default="", help="Optional JSON {from:[to,to]} topology extension.")
    p.add_argument("--missing-dir", default="missing_topology", help="Where missing_topology_moves.csv and summary CSV are written.")


def add_live_common(p: argparse.ArgumentParser) -> None:
    add_common(p)
    p.add_argument("--signals", action="append", default=[], help="Comma-separated signal/berth ids to learn. Omit in live mode to learn all topology.")
    p.add_argument("--all", action="store_true", help="Learn/watch all topology berths.")
    p.add_argument("--strict", action="store_true", help="Only treat CA as pass when from->to is in topology.")
    p.add_argument("--learn-special", action="store_true", help="Allow special internal turnback moves to be used for learning.")
    p.add_argument("--record-unmapped-routes", action="store_true", help="Also log known from/to movements where from->to is not in next-map.")
    p.add_argument("--pre", type=float, default=30.0)
    p.add_argument("--post", type=float, default=30.0)
    p.add_argument("--recent-keep", type=float, default=300.0)
    p.add_argument("--print-s", action="store_true", help="Print all S-Class changes. Very noisy.")
    p.add_argument("--print-c", action="store_true", help="Print C-Class messages.")
    p.add_argument("--show-known", action="store_true", help="Do not suppress described known bits from new learning evidence.")
    p.add_argument("--username")
    p.add_argument("--password")
    p.add_argument("--secrets", default="secrets.json")
    p.add_argument("--host", default="publicdatafeeds.networkrail.co.uk")
    p.add_argument("--port", type=int, default=61618)
    p.add_argument("--topic", default="/topic/TD_ALL_SIG_AREA")
    p.add_argument("--heartbeat-ms", type=int, default=10000)
    p.add_argument("--subscription-id", default="t3-clean-learner")
    p.add_argument("--status-every", type=float, default=60.0)
    p.add_argument("--reconnect-delay", type=float, default=10.0)
    p.add_argument("--max-reconnect-delay", type=float, default=300.0)
    p.add_argument("--debug-tracebacks", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Clean T3 TD S-Class/C-Class bit-map learner.")
    sub = p.add_subparsers(dest="command", required=True)

    live = sub.add_parser("live", help="Connect to TD feed and learn pass/bit evidence.")
    add_live_common(live)

    watch = sub.add_parser("watch", help="Connect to TD feed and print selected bits with known/unknown labels.")
    add_live_common(watch)
    watch.add_argument("--bits", action="append", default=[], help="Byte:bit values, e.g. 24:1 or 24:1,23:5")
    watch.add_argument("--signal", action="append", default=[], help="Watch known bits for signal(s), e.g. 6232")
    watch.add_argument("--unknown", action="store_true", help="Print only S-Class bit changes not described in known CSV.")
    watch.add_argument("--all-bits", action="store_true", help="Print all S-Class bit changes with known/unknown labels.")

    report = sub.add_parser("report", help="Offline evidence report from SQLite.")
    add_common(report)
    report.add_argument("--signals", action="append", default=[])
    report.add_argument("--all", action="store_true")
    report.add_argument("--score-window", type=float, default=12.0)
    report.add_argument("--min-pct", "--min-percent", dest="min_pct", type=float, default=0.40, help="Minimum candidate consistency. Use 0.8 or 80 for 80%%. Default 0.40")
    report.add_argument("--min-pass-count", type=int, default=1, help="Minimum number of passes supporting the best bucket. Default 1")
    report.add_argument("--max-avg-delta", type=float, default=None, help="Only show rows whose average timing delta is <= this many seconds")
    report.add_argument("--limit", type=int, default=12)
    report.add_argument("--show-known", action="store_true", help="Audit this signal's own known CSV bit(s) separately from unknown candidates.")
    report.add_argument("--show-cross-known", action="store_true", help="Also show known bits belonging to other signals when they appear in this signal's pass window.")

    progress = sub.add_parser("progress", help="Compact summary of learning progress and strongest candidates.")
    add_common(progress)
    progress.add_argument("--score-window", type=float, default=12.0)
    progress.add_argument("--min-pct", "--min-percent", dest="min_pct", type=float, default=0.80, help="Minimum candidate consistency. Use 0.8 or 80 for 80%%. Default 0.80")
    progress.add_argument("--min-pass-count", type=int, default=3)
    progress.add_argument("--max-avg-delta", type=float, default=3.0)
    progress.add_argument("--limit", type=int, default=50)
    progress.add_argument("--show-known", action="store_true", help="Include known CSV bit rows in strong candidate list")

    known = sub.add_parser("known", help="Show known bit CSV rows.")
    add_common(known)
    known.add_argument("--signal", action="append", default=[])
    known.add_argument("--bits", action="append", default=[])

    missing = sub.add_parser("missing", help="Show/export missing topology CSV summary.")
    add_common(missing)
    missing.add_argument("--export", default="", help="Export summary CSV to this path.")
    missing.add_argument("--show-boundary", action="store_true", help="Also show expected external/boundary berth chains that are normally suppressed.")

    moves = sub.add_parser("moves", aliases=["movements"], help="Search stored pass/movement rows for a berth/signal.")
    add_common(moves)
    moves.add_argument("--berth", "--signal", required=True, help="Berth/signal to search, e.g. 6244")
    moves.add_argument("--limit", type=int, default=50)
    moves.add_argument("--show-events", action="store_true", help="Also print S-Class bit events attached to each pass row.")
    moves.add_argument("--event-limit", type=int, default=40)

    bit = sub.add_parser("bit", help="Search stored pass evidence for one byte:bit.")
    add_common(bit)
    bit.add_argument("--bit", required=True, help="Byte:bit value, e.g. 25:3")
    bit.add_argument("--signal", default="", help="Optional signal filter, e.g. 6244")
    bit.add_argument("--limit", type=int, default=80)
    bit.add_argument("--evidence", action="store_true", help="Show noisy pass-window attachments instead of raw/de-duplicated bit history.")
    bit.add_argument("--details", action="store_true", help="Show full-byte hex/debug context. Default prints: time signal route headcode addr XX bN changed A->B")
    bit.add_argument("--link-window", type=float, default=30.0, help="Seconds either side used to link a known bit to a pass for its mapped signal. Default: 30")

    bytes_p = sub.add_parser("bytes", aliases=["byte-map", "byte-summary"], help="List S-Class byte addresses/bits the learner has seen, without using sqlite3 manually.")
    add_common(bytes_p)
    bytes_p.add_argument("--address", action="append", default=[], help="Optional byte address filter, e.g. 25 or 25,2A")
    bytes_p.add_argument("--values", action="store_true", help="Also show values observed per bit. More verbose.")
    bytes_p.add_argument("--known-only", action="store_true", help="Only show byte addresses with mappings in known_bits.csv")
    bytes_p.add_argument("--export", default="", help="Export the same summary to CSV")

    check = sub.add_parser("check", help="Check topology and known CSV load cleanly.")
    add_common(check)

    return p


def cmd_known(args: argparse.Namespace) -> int:
    store, _, known = build_common(args)
    signals = {normalize_berth(x) for x in split_csv_values(args.signal)}
    bits = parse_bit_values(args.bits)

    rows = known.rows
    if signals:
        rows = [r for r in rows if signals.intersection(KnownBits._signals_for_row(r))]
    if bits:
        rows = [r for r in rows if r.key in bits]

    print(f"[KNOWN] rows={len(rows)}")
    for r in rows:
        state = "described" if r.described else "placeholder"
        ignore = "ignored" if known.ignored(r.key) else "visible"
        print(f"  {r.key.label} | {r.summary()} | {state} | {ignore}")
    store.close()
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    store, topology, known = build_common(args)
    signals = choose_signals(args, topology)
    if not signals:
        raise SystemExit("Use report --all or report --signals 6232,6239")
    try:
        for s in sorted(signals):
            min_pct = float(args.min_pct) / 100.0 if float(args.min_pct) > 1.0 else float(args.min_pct)
            print_signal_report(
                store,
                known,
                s,
                score_window=args.score_window,
                show_known=args.show_known,
                show_cross_known=bool(getattr(args, "show_cross_known", False)),
                min_pct=min_pct,
                min_pass_count=max(1, int(getattr(args, "min_pass_count", 1))),
                max_avg_delta=getattr(args, "max_avg_delta", None),
                limit=int(args.limit),
            )
    finally:
        store.close()
    return 0



def cmd_progress(args: argparse.Namespace) -> int:
    store, topology, known = build_common(args)
    try:
        min_pct = float(args.min_pct) / 100.0 if float(args.min_pct) > 1.0 else float(args.min_pct)
        print_progress_report(
            store,
            topology,
            known,
            score_window=float(args.score_window),
            min_pct=min_pct,
            min_pass_count=max(1, int(args.min_pass_count)),
            max_avg_delta=getattr(args, "max_avg_delta", None),
            limit=max(1, int(args.limit)),
            include_known=bool(getattr(args, "show_known", False)),
        )
    finally:
        store.close()
    return 0



def cmd_missing(args: argparse.Namespace) -> int:
    store, _, _ = build_common(args)
    export = Path(args.export) if args.export else None
    try:
        print_missing_report(store, export_path=export, show_boundary=bool(getattr(args, "show_boundary", False)))
    finally:
        store.close()
    return 0


def cmd_moves(args: argparse.Namespace) -> int:
    store, _, known = build_common(args)
    try:
        print_movement_report(
            store,
            known,
            args.berth,
            limit=max(1, int(args.limit)),
            show_events=bool(args.show_events),
            event_limit=max(1, int(args.event_limit)),
        )
    finally:
        store.close()
    return 0


def cmd_bit(args: argparse.Namespace) -> int:
    store, _, known = build_common(args)
    try:
        print_bit_history(
            store,
            known,
            args.bit,
            signal_id=args.signal or None,
            limit=max(1, int(args.limit)),
            evidence=bool(args.evidence),
            details=bool(getattr(args, "details", False)),
            link_window=max(0.0, float(getattr(args, "link_window", 30.0))),
        )
    finally:
        store.close()
    return 0


def cmd_bytes(args: argparse.Namespace) -> int:
    store, _, known = build_common(args)
    try:
        print_bytes_report(
            store,
            known,
            address_filter=split_csv_values(getattr(args, "address", []) or []),
            show_values=bool(getattr(args, "values", False)),
            known_only=bool(getattr(args, "known_only", False)),
            export_path=Path(args.export) if getattr(args, "export", "") else None,
        )
    finally:
        store.close()
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    store, topology, known = build_common(args)
    print(f"[CHECK] topology entries={len(topology)}")
    print(f"[CHECK] known rows={len(known.rows)} described={sum(1 for r in known.rows if r.described)}")
    print(f"[CHECK] db={store.path}")
    print(f"[CHECK] missing dir={store.missing_dir}")
    print(f"[CHECK] special non-learning moves={len(SPECIAL_NON_LEARNING_MOVES)}")
    store.close()
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "known":
        return cmd_known(args)
    if args.command == "report":
        return cmd_report(args)
    if args.command == "progress":
        return cmd_progress(args)
    if args.command == "missing":
        return cmd_missing(args)
    if args.command in {"moves", "movements"}:
        return cmd_moves(args)
    if args.command == "bit":
        return cmd_bit(args)
    if args.command in {"bytes", "byte-map", "byte-summary"}:
        return cmd_bytes(args)
    if args.command == "check":
        return cmd_check(args)
    if args.command in {"live", "watch"}:
        learner = make_learner(args, mode=args.command)
        run_live(args, learner)
        return 0
    raise SystemExit(f"unknown command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
