#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

import t3_learner_clean as learner_mod

load_dotenv("/etc/metro-bot.env")
load_dotenv()

UK_TIMEZONE = ZoneInfo("Europe/London")


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None and str(raw).strip() else default
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def env_float(name: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    raw = os.getenv(name)
    try:
        value = float(str(raw).strip()) if raw is not None and str(raw).strip() else default
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "never"
    return dt.datetime.fromtimestamp(float(ts), UK_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")


def short_ts(ts: Optional[float]) -> str:
    if not ts:
        return "never"
    return dt.datetime.fromtimestamp(float(ts), UK_TIMEZONE).strftime("%H:%M:%S")


def uptime_text(start_ts: float) -> str:
    seconds = max(0, int(time.time() - start_ts))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"


def process_memory_text() -> str:
    """Best-effort current RSS without adding psutil as a dependency."""
    with contextlib.suppress(Exception):
        page_size = os.sysconf("SC_PAGE_SIZE")
        statm = Path("/proc/self/statm").read_text(encoding="utf-8").split()
        rss_bytes = int(statm[1]) * int(page_size)
        return f"{rss_bytes / (1024 * 1024):.1f} MiB RSS"
    return "unavailable"


@dataclass(frozen=True)
class Config:
    token: str
    guild_id: Optional[int]
    app_dir: Path
    learner_dir: Path
    db_path: Path
    known_path: Path
    missing_dir: Path
    exports_dir: Path
    uploads_dir: Path
    backups_dir: Path
    logs_dir: Path
    nr_enabled: bool
    nr_area: str
    nr_host: str
    nr_port: int
    nr_topic: str
    nr_username: str
    nr_password: str
    nr_subscription_id: str
    nr_client_id: str
    nr_durable_name: str
    nr_durable: bool
    nr_snapshot_stale_seconds: float
    cli_timeout_seconds: int
    cli_concurrency: int
    db_busy_timeout_ms: int
    db_read_cache_kib: int
    db_mmap_mib: int
    learner_recent_keep_seconds: float
    nr_feed_tick_seconds: float
    nr_reconnect_initial_seconds: float
    nr_reconnect_max_seconds: float
    default_command_limit: int
    max_command_limit: int


def load_config() -> Config:
    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN is missing")

    data_root = Path(os.getenv("METRO_BOT_DATA_DIR", "/var/lib/metro-bot")).expanduser()
    learner_dir = data_root / "t3_learner"
    guild_raw = os.getenv("DISCORD_GUILD_ID", "").strip()

    return Config(
        token=token,
        guild_id=int(guild_raw) if guild_raw else None,
        app_dir=Path(__file__).resolve().parent,
        learner_dir=learner_dir,
        db_path=learner_dir / "td_signal_bit_learner.sqlite",
        known_path=learner_dir / "known_bits.csv",
        missing_dir=learner_dir / "missing_topology",
        exports_dir=learner_dir / "exports",
        uploads_dir=learner_dir / "uploads",
        backups_dir=learner_dir / "backups",
        logs_dir=learner_dir / "logs",
        nr_enabled=env_bool("NR_ENABLED", True),
        nr_area=os.getenv("NR_AREA", "T3").strip().upper(),
        nr_host=os.getenv("NR_HOST", "publicdatafeeds.networkrail.co.uk").strip(),
        nr_port=env_int("NR_PORT", 61618, minimum=1, maximum=65535),
        nr_topic=os.getenv("NR_TOPIC", "/topic/TD_ALL_SIG_AREA").strip(),
        nr_username=os.getenv("NROD_USER") or os.getenv("NR_USERNAME", ""),
        nr_password=os.getenv("NROD_PASS") or os.getenv("NR_PASSWORD", ""),
        nr_subscription_id=os.getenv("NR_SUBSCRIPTION_ID", "metro-nr-bot-t3").strip(),
        nr_client_id=(os.getenv("NR_CLIENT_ID") or os.getenv("NROD_USER") or os.getenv("NR_USERNAME", "")).strip(),
        nr_durable_name=os.getenv("NR_DURABLE_NAME", "metro-nr-bot-t3").strip(),
        nr_durable=env_bool("NR_DURABLE_SUBSCRIPTION", True),
        nr_snapshot_stale_seconds=env_float("NR_SNAPSHOT_STALE_SECONDS", 600.0, minimum=30.0, maximum=86400.0),
        cli_timeout_seconds=env_int("NR_CLI_TIMEOUT_SECONDS", 60, minimum=10, maximum=600),
        cli_concurrency=env_int("NR_CLI_CONCURRENCY", 1, minimum=1, maximum=4),
        db_busy_timeout_ms=env_int("NR_DB_BUSY_TIMEOUT_MS", 5000, minimum=250, maximum=60000),
        db_read_cache_kib=env_int("NR_DB_READ_CACHE_KIB", 4096, minimum=512, maximum=65536),
        db_mmap_mib=env_int("NR_DB_MMAP_MIB", 0, minimum=0, maximum=512),
        learner_recent_keep_seconds=env_float("NR_LEARNER_RECENT_KEEP_SECONDS", 180.0, minimum=30.0, maximum=900.0),
        nr_feed_tick_seconds=env_float("NR_FEED_TICK_SECONDS", 1.0, minimum=0.2, maximum=10.0),
        nr_reconnect_initial_seconds=env_float("NR_RECONNECT_INITIAL_SECONDS", 10.0, minimum=1.0, maximum=300.0),
        nr_reconnect_max_seconds=env_float("NR_RECONNECT_MAX_SECONDS", 300.0, minimum=10.0, maximum=1800.0),
        default_command_limit=env_int("NR_COMMAND_DEFAULT_LIMIT", 25, minimum=5, maximum=100),
        max_command_limit=env_int("NR_COMMAND_MAX_LIMIT", 100, minimum=10, maximum=500),
    )


CFG = load_config()

# Evidence needed before the Discord bot turns pass-window statistics into a
# user-facing signal-state interpretation.  One pass can produce many perfectly
# timed but unrelated S-Class changes, so never derive live state from 1/1 rows.
DERIVE_MIN_SUPPORT = env_int("NR_DERIVE_MIN_SUPPORT", 3, minimum=1, maximum=50)
DERIVE_MIN_PCT = env_float("NR_DERIVE_MIN_PCT", 0.80, minimum=0.0, maximum=1.0)
DERIVE_MAX_AVG_DELTA = env_float("NR_DERIVE_MAX_AVG_DELTA", 3.0, minimum=0.1, maximum=30.0)
DERIVE_FLICKER_WINDOW_SECONDS = env_float("NR_DERIVE_FLICKER_WINDOW_SECONDS", 2 * 60 * 60, minimum=60.0, maximum=24 * 60 * 60)
DERIVE_FLICKER_WARN_CHANGES = env_int("NR_DERIVE_FLICKER_WARN_CHANGES", 8, minimum=2, maximum=500)
# For a signal-specific bit, raw changes should mostly line up with C-Class movements
# for that signal. Low-use sidings make wrong/shared mappings obvious: a bit that
# toggles ten times while the siding has one CA move is not a reliable live aspect.
DERIVE_CORRELATION_WINDOW_SECONDS = env_float("NR_DERIVE_CORRELATION_WINDOW_SECONDS", 2 * 60 * 60, minimum=60.0, maximum=24 * 60 * 60)
DERIVE_CORRELATION_MATCH_SECONDS = env_float("NR_DERIVE_CORRELATION_MATCH_SECONDS", 180.0, minimum=10.0, maximum=900.0)


def ensure_dirs() -> None:
    for path in [CFG.learner_dir, CFG.missing_dir, CFG.exports_dir, CFG.uploads_dir, CFG.backups_dir, CFG.logs_dir]:
        path.mkdir(parents=True, exist_ok=True)

    repo_known = CFG.app_dir / "known_bits.csv"
    if not CFG.known_path.exists() and repo_known.exists():
        shutil.copy2(repo_known, CFG.known_path)

    if not CFG.known_path.exists():
        CFG.known_path.write_text(
            "Address:Bit,Element type,Description,Signal,Route From,Route To,Active State,Ignore In Tracker,Confidence,Notes,Provenance,Verified,Element Group\n",
            encoding="utf-8",
        )


def trim(text: str, limit: int = 1900) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 90] + "\n\n...output trimmed. Use /database export for full files."


def split_text(text: str, limit: int = 1900) -> list[str]:
    text = (text or "").strip() or "(no output)"
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < max(120, limit // 4):
            split_at = remaining.rfind(" ", 0, limit)
        if split_at < max(120, limit // 4):
            split_at = limit
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    return chunks


async def send_text(interaction: discord.Interaction, text: str, *, paged: bool = False) -> None:
    chunks = split_text(text) if paged else [trim(text)]
    for idx, chunk in enumerate(chunks):
        prefix = "" if idx == 0 else "continued\n"
        await interaction.followup.send(f"```text\n{prefix}{chunk}\n```")


def cli_args(command: str) -> list[str]:
    return [
        command,
        "--db", str(CFG.db_path),
        "--known", str(CFG.known_path),
        "--missing-dir", str(CFG.missing_dir),
        "--area", CFG.nr_area,
    ]


def run_cli(args: list[str], timeout: Optional[int] = None) -> str:
    cmd = [sys.executable, str(CFG.app_dir / "t3_learner_clean.py"), *args]
    proc = subprocess.run(
        cmd,
        cwd=str(CFG.app_dir),
        text=True,
        capture_output=True,
        timeout=timeout or CFG.cli_timeout_seconds,
    )
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}")
    return out or err or "No output."


CLI_SEMAPHORE = asyncio.Semaphore(CFG.cli_concurrency)


async def run_cli_async(args: list[str], timeout: Optional[int] = None) -> str:
    async with CLI_SEMAPHORE:
        return await asyncio.to_thread(run_cli, args, timeout or CFG.cli_timeout_seconds)


class KnownBitsCache:
    """Small mtime cache so every command does not re-read known_bits.csv."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.RLock()
        self._mtime_ns: int | None = None
        self._size: int | None = None
        self._value: learner_mod.KnownBits | None = None

    def get(self) -> learner_mod.KnownBits:
        with self.lock:
            try:
                stat = self.path.stat()
                signature = (stat.st_mtime_ns, stat.st_size)
            except FileNotFoundError:
                signature = (None, None)
            if self._value is None or signature != (self._mtime_ns, self._size):
                self._value = learner_mod.load_known_bits(self.path)
                self._mtime_ns, self._size = signature
            return self._value

    def invalidate(self) -> None:
        with self.lock:
            self._mtime_ns = None
            self._size = None
            self._value = None


KNOWN_CACHE = KnownBitsCache(CFG.known_path)


def known_bits() -> learner_mod.KnownBits:
    return KNOWN_CACHE.get()


_TOPOLOGY_CACHE: dict[str, set[str]] | None = None
_TOPOLOGY_LOCK = threading.RLock()


def topology() -> dict[str, set[str]]:
    global _TOPOLOGY_CACHE
    with _TOPOLOGY_LOCK:
        if _TOPOLOGY_CACHE is None:
            _TOPOLOGY_CACHE = learner_mod.build_topology(None)
        return _TOPOLOGY_CACHE


def clamp_limit(limit: Optional[int]) -> int:
    if limit is None:
        return CFG.default_command_limit
    return max(1, min(int(limit), CFG.max_command_limit))


def db_connect(*, readonly: bool = True) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{CFG.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=CFG.db_busy_timeout_ms / 1000)
    else:
        conn = sqlite3.connect(str(CFG.db_path), timeout=CFG.db_busy_timeout_ms / 1000)
    conn.row_factory = sqlite3.Row
    with contextlib.suppress(sqlite3.DatabaseError):
        conn.execute(f"PRAGMA busy_timeout={CFG.db_busy_timeout_ms}")
    with contextlib.suppress(sqlite3.DatabaseError):
        conn.execute(f"PRAGMA cache_size=-{CFG.db_read_cache_kib}")
    with contextlib.suppress(sqlite3.DatabaseError):
        conn.execute("PRAGMA temp_store=FILE")
    if CFG.db_mmap_mib > 0:
        with contextlib.suppress(sqlite3.DatabaseError):
            conn.execute(f"PRAGMA mmap_size={CFG.db_mmap_mib * 1024 * 1024}")
    if readonly:
        with contextlib.suppress(sqlite3.DatabaseError):
            conn.execute("PRAGMA query_only=ON")
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


def bit_filter_sql(keys: Iterable[learner_mod.BitKey]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for key in keys:
        clauses.append("(address=? AND bit=?)")
        params.extend([int(key.address, 16), int(key.bit)])
    if not clauses:
        return "", []
    return "(" + " OR ".join(clauses) + ")", params


def known_desc_for_key(known: learner_mod.KnownBits, address: int, bit: int) -> str:
    desc = known.describe(learner_mod.BitKey(f"{int(address):02X}", int(bit)))
    return "" if desc == "UNKNOWN" else desc


def current_bit_value(conn: sqlite3.Connection, key: learner_mod.BitKey) -> tuple[int | None, float | None, str | None]:
    """Return the safest current value for one S-Class bit.

    s_bytes is the latest byte snapshot table, but older bot versions could let
    an out-of-order S-Class snapshot overwrite a newer byte. When raw bit-event
    history has a newer event for this exact bit, prefer that event so /bit and
    /signal do not report impossible stale states.
    """
    row = conn.execute(
        "SELECT value, updated_ts, msg_type FROM s_bytes WHERE area=? AND address=?",
        (CFG.nr_area, int(key.address, 16)),
    ).fetchone()

    current_value: int | None = None
    current_ts: float | None = None
    current_msg: str | None = None
    if row:
        current_value = 1 if int(row["value"]) & (1 << key.bit) else 0
        current_ts = float(row["updated_ts"])
        current_msg = row["msg_type"]

    if table_exists(conn, "s_bit_events"):
        latest = conn.execute(
            """
            SELECT event_ts, new_bit, msg_type
            FROM s_bit_events
            WHERE area=? AND address=? AND bit=?
              AND UPPER(COALESCE(msg_type,''))='SF'
            ORDER BY event_ts DESC
            LIMIT 1
            """,
            (CFG.nr_area, int(key.address, 16), int(key.bit)),
        ).fetchone()
        if latest:
            event_ts = float(latest["event_ts"])
            if current_ts is None or event_ts > current_ts + 0.001:
                return int(latest["new_bit"]), event_ts, f"{latest['msg_type'] or '?'} raw-history"

    return current_value, current_ts, current_msg


def _active_state_text(active_state: str, raw_value: int) -> str | None:
    """Interpret a bit only when known_bits.csv explicitly declares polarity.

    The old bot assumed 1=red and 0=proceed for every S-Class bit. That is not
    safe: some CSV mappings are unverified, some rows are route/track bits, and
    a raw 0 on a red-proving bit only means "not red", not which proceed aspect.
    """
    text = str(active_state or "").strip().lower()
    if not text:
        return None

    # Accept common ways of writing the CSV polarity, e.g. "red=1", "1=red",
    # "proceed=1", "active high proceed", or simply "red" / "proceed".
    red_words = {"red", "danger", "on"}
    proceed_words = {"proceed", "clear", "cleared", "off"}

    def has_any(words: set[str]) -> bool:
        return any(w in text for w in words)

    if "=1" in text or "1=" in text or "high" in text or text in red_words | proceed_words:
        active_value = 1
    elif "=0" in text or "0=" in text or "low" in text:
        active_value = 0
    else:
        return None

    if has_any(red_words):
        return "red/danger" if raw_value == active_value else "not red/cleared"
    if has_any(proceed_words):
        return "proceed/off indication" if raw_value == active_value else "not proved proceed/off"
    return None


def feed_snapshot_status(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return whether the current S-Class snapshot is safe to present as live."""
    if not table_exists(conn, "feed_state"):
        return {
            "valid": False,
            "generation": 0,
            "last_refresh": None,
            "last_s_event": None,
            "last_c_event": None,
            "last_feed_event": None,
            "reason": "database predates protocol-level feed state",
            "refresh_in_progress": False,
        }
    row = conn.execute("SELECT * FROM feed_state WHERE area=?", (CFG.nr_area,)).fetchone()
    if row is None:
        return {
            "valid": False,
            "generation": 0,
            "last_refresh": None,
            "last_s_event": None,
            "last_c_event": None,
            "last_feed_event": None,
            "reason": "waiting for first complete SG...SH refresh",
            "refresh_in_progress": False,
        }
    last_s = float(row["last_s_event_ts"]) if row["last_s_event_ts"] is not None else None
    last_c = float(row["last_c_event_ts"]) if row["last_c_event_ts"] is not None else None
    feed_times = [value for value in (last_s, last_c) if value is not None]
    last_feed_event = max(feed_times) if feed_times else None
    valid = bool(int(row["snapshot_valid"] or 0))
    reason = str(row["last_reason"] or "")
    if valid and last_feed_event is None:
        valid = False
        reason = "snapshot has no subsequent TD heartbeat/event timestamp"
    elif valid and time.time() - last_feed_event > CFG.nr_snapshot_stale_seconds:
        valid = False
        reason = (
            f"last TD event/heartbeat is {time.time() - last_feed_event:.0f}s old; "
            f"stale threshold is {CFG.nr_snapshot_stale_seconds:.0f}s"
        )
    return {
        "valid": valid,
        "generation": int(row["snapshot_generation"] or 0),
        "last_refresh": float(row["last_complete_refresh_ts"]) if row["last_complete_refresh_ts"] is not None else None,
        "last_s_event": last_s,
        "last_c_event": last_c,
        "last_feed_event": last_feed_event,
        "reason": reason,
        "refresh_in_progress": bool(int(row["refresh_in_progress"] or 0)),
        "invalid_messages": int(row["invalid_messages"] or 0),
        "duplicate_messages": int(row["duplicate_messages"] or 0),
    }


def trusted_live_mapping_rows(
    known: learner_mod.KnownBits,
    key: learner_mod.BitKey,
    signal_id: str | None = None,
) -> list[learner_mod.KnownBit]:
    rows = [row for row in known.by_key.get(key, []) if row.trusted_for_live_aspect]
    if signal_id is None:
        return rows
    signal_norm = learner_mod.normalize_berth(signal_id)
    return [row for row in rows if signal_norm in known._signals_for_row(row)]


def describe_raw_bit_state(known: learner_mod.KnownBits, key: learner_mod.BitKey, raw_value: int | None) -> str:
    if raw_value is None:
        return "no current S-Class byte snapshot"
    for row in known.by_key.get(key, []):
        interpreted = _active_state_text(getattr(row, "active_state", ""), int(raw_value))
        if interpreted:
            return f"raw {raw_value} ({interpreted}; polarity from CSV active_state)"
    return f"raw {raw_value} (uninterpreted; CSV has no polarity, so not assuming red/proceed)"


def pass_count_for_signal(conn: sqlite3.Connection, signal_id: str) -> int:
    if not table_exists(conn, "pass_log"):
        return 0
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM pass_log WHERE signal=? AND finalised_ts IS NOT NULL",
        (learner_mod.normalize_berth(signal_id),),
    ).fetchone()
    return int(row["c"] if row else 0)


def learned_candidate_rows(
    conn: sqlite3.Connection,
    signal_id: str,
    *,
    score_window: float = 12.0,
    limit: int = 8,
) -> list[sqlite3.Row]:
    if not table_exists(conn, "pass_bit_events") or not table_exists(conn, "pass_log"):
        return []
    return list(conn.execute(
        """
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
        """,
        (learner_mod.normalize_berth(signal_id), float(score_window), int(limit)),
    ).fetchall())


def candidate_best_for_display(row: sqlite3.Row, pass_count: int) -> dict[str, Any]:
    before_on = int(row["before_on"] or 0)
    before_off = int(row["before_off"] or 0)
    after_on = int(row["after_on"] or 0)
    after_off = int(row["after_off"] or 0)
    choices = [
        (before_on, "before 0->1", "likely proceed/route set before pass", "proceed_active_high"),
        (before_off, "before 1->0", "likely red/danger cleared before pass", "danger_active_high"),
        (after_on, "after 0->1", "likely red/danger restored after pass", "danger_active_high"),
        (after_off, "after 1->0", "likely proceed/route released after pass", "proceed_active_high"),
    ]
    best_count, bucket, guess, polarity = max(choices, key=lambda x: x[0])
    pct = (best_count / pass_count) if pass_count else 0.0
    avg_abs = float(row["avg_abs"] or 0.0)
    confidence_ok = (
        int(best_count) >= DERIVE_MIN_SUPPORT
        and float(pct) >= DERIVE_MIN_PCT
        and avg_abs <= DERIVE_MAX_AVG_DELTA
    )
    if polarity == "proceed_active_high":
        polarity_text = "1=proceed/route set, 0=no proceed proved"
    else:
        polarity_text = "1=red/danger, 0=not red/cleared"
    return {
        "best_count": best_count,
        "bucket": bucket,
        "guess": guess,
        "polarity": polarity,
        "polarity_text": polarity_text,
        "pct": pct,
        "avg_abs": avg_abs,
        "confidence_ok": confidence_ok,
    }


def confidence_text(best: dict[str, Any], pass_count: int) -> str:
    return (
        f"{best['best_count']}/{pass_count} ({best['pct']*100:.0f}%), "
        f"avg_delta={best['avg_abs']:.1f}s"
    )


def describe_candidate_current(
    raw_value: int | None,
    best: dict[str, Any],
    *,
    signal_view: bool = True,
    allow_low_evidence: bool = False,
) -> str:
    if raw_value is None:
        return "current unknown"

    low_evidence = not bool(best.get("confidence_ok"))
    if low_evidence and not allow_low_evidence:
        return f"current raw {raw_value}; low evidence, not deriving state"

    prefix = "low-evidence suggests " if low_evidence else "current likely "
    polarity = str(best.get("polarity", ""))
    if polarity == "danger_active_high":
        return (prefix + "RED/DANGER") if raw_value == 1 else (prefix + "not red/cleared")
    if polarity == "proceed_active_high":
        if raw_value == 1:
            return prefix + "PROCEED / route set"
        # In a signal-specific view this is the useful operational conclusion:
        # the learned proceed/proof bit is not active. It still does not tell
        # us the exact red/yellow/double-yellow/green aspect.
        if signal_view:
            return prefix + "RED/DANGER or no route set (proceed bit not active)"
        return prefix + "proceed bit not active"
    return f"current raw {raw_value}"


def recent_bit_change_count(conn: sqlite3.Connection, key: learner_mod.BitKey, *, seconds: float) -> int:
    if not table_exists(conn, "s_bit_events"):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM s_bit_events
        WHERE area=? AND address=? AND bit=? AND event_ts >= ?
          AND UPPER(COALESCE(msg_type,''))='SF'
        """,
        (CFG.nr_area, int(key.address, 16), int(key.bit), time.time() - float(seconds)),
    ).fetchone()
    return int(row["c"] if row else 0)


def recent_moves_for_berth(conn: sqlite3.Connection, berth: str, *, limit: int = 3) -> list[sqlite3.Row]:
    b = learner_mod.normalize_berth(berth)
    if table_exists(conn, "berth_steps"):
        return list(conn.execute(
            """
            SELECT event_ts AS pass_ts, from_berth AS signal, from_berth, to_berth,
                   descr, event_ts AS finalised_ts
            FROM berth_steps
            WHERE from_berth=? OR to_berth=?
            ORDER BY event_ts DESC LIMIT ?
            """,
            (b, b, int(limit)),
        ).fetchall())
    if not table_exists(conn, "pass_log"):
        return []
    return list(conn.execute(
        """
        SELECT pass_ts, signal, from_berth, to_berth, descr, finalised_ts
        FROM pass_log
        WHERE signal=? OR from_berth=? OR to_berth=?
        ORDER BY pass_ts DESC LIMIT ?
        """,
        (b, b, b, int(limit)),
    ).fetchall())


def recent_signal_move_count(conn: sqlite3.Connection, signal_id: str, *, seconds: float) -> int:
    """Count recent canonical C-Class berth steps involving one berth."""
    sig = learner_mod.normalize_berth(signal_id)
    if table_exists(conn, "berth_steps"):
        row = conn.execute(
            """
            SELECT COUNT(*) AS c FROM berth_steps
            WHERE (from_berth=? OR to_berth=?) AND event_ts >= ?
            """,
            (sig, sig, time.time() - float(seconds)),
        ).fetchone()
        return int(row["c"] if row else 0)
    if not table_exists(conn, "pass_log"):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM pass_log
        WHERE (signal=? OR from_berth=? OR to_berth=?) AND pass_ts >= ?
        """,
        (sig, sig, sig, time.time() - float(seconds)),
    ).fetchone()
    return int(row["c"] if row else 0)


def nearest_pass_for_signal(
    conn: sqlite3.Connection,
    signal_id: str,
    event_ts: float,
    *,
    window_seconds: float,
) -> sqlite3.Row | None:
    """Nearest canonical CA berth step from a selected from-berth.

    The legacy function name is retained for compatibility. The returned time
    is a berth-step time, not an independently proven physical signal pass.
    """
    sig = learner_mod.normalize_berth(signal_id)
    if table_exists(conn, "berth_steps"):
        return conn.execute(
            """
            SELECT from_berth AS signal, from_berth, to_berth, descr,
                   event_ts AS pass_ts, event_ts - ? AS delta_seconds,
                   ABS(event_ts - ?) AS abs_delta
            FROM berth_steps
            WHERE from_berth=? AND ABS(event_ts - ?) <= ?
            ORDER BY ABS(event_ts - ?), event_ts DESC LIMIT 1
            """,
            (float(event_ts), float(event_ts), sig, float(event_ts), float(window_seconds), float(event_ts)),
        ).fetchone()
    if not table_exists(conn, "pass_log"):
        return None
    return conn.execute(
        """
        SELECT signal,from_berth,to_berth,descr,pass_ts,
               pass_ts - ? AS delta_seconds,ABS(pass_ts - ?) AS abs_delta
        FROM pass_log
        WHERE signal=? AND ABS(pass_ts - ?) <= ?
        ORDER BY ABS(pass_ts - ?), pass_ts DESC LIMIT 1
        """,
        (float(event_ts), float(event_ts), sig, float(event_ts), float(window_seconds), float(event_ts)),
    ).fetchone()


def bit_signal_correlation_counts(
    conn: sqlite3.Connection,
    key: learner_mod.BitKey,
    signal_id: str,
    *,
    seconds: float,
    match_window_seconds: float,
) -> tuple[int, int, int]:
    """Return raw bit changes, changes near this signal's passes, and recent moves."""
    if not table_exists(conn, "s_bit_events"):
        return 0, 0, recent_signal_move_count(conn, signal_id, seconds=seconds)
    rows = conn.execute(
        """
        SELECT event_ts
        FROM s_bit_events
        WHERE area=? AND address=? AND bit=? AND event_ts >= ?
          AND UPPER(COALESCE(msg_type,''))='SF'
        ORDER BY event_ts DESC
        """,
        (CFG.nr_area, int(key.address, 16), int(key.bit), time.time() - float(seconds)),
    ).fetchall()
    matched = 0
    for row in rows:
        if nearest_pass_for_signal(conn, signal_id, float(row["event_ts"]), window_seconds=match_window_seconds) is not None:
            matched += 1
    move_count = recent_signal_move_count(conn, signal_id, seconds=seconds)
    return len(rows), matched, move_count


def correlation_warning_line(
    conn: sqlite3.Connection,
    key: learner_mod.BitKey,
    signal_id: str,
    *,
    seconds: float = DERIVE_CORRELATION_WINDOW_SECONDS,
    match_window_seconds: float = DERIVE_CORRELATION_MATCH_SECONDS,
) -> str | None:
    changes, matched, moves = bit_signal_correlation_counts(
        conn,
        key,
        signal_id,
        seconds=seconds,
        match_window_seconds=match_window_seconds,
    )
    if changes <= 0:
        return None

    unmatched = max(0, changes - matched)
    # Expected rough pattern for a proceed/route bit is 0->1 then 1->0 per move.
    # Allow a little slack for cancellations/retries, but flag heavy excess.
    expected_slack = max(4, moves * 2 + 2)
    if changes >= DERIVE_FLICKER_WARN_CHANGES and changes > expected_slack and unmatched >= max(3, moves + 1):
        return (
            f"    Correlation warning: {key.label} changed {changes} times in the last "
            f"{seconds/60:.0f}m, but only {moves} recent movement(s) involving {signal_id} were captured; "
            f"{unmatched} change(s) were not within +/-{match_window_seconds:.0f}s of a {signal_id} CA move. "
            "Treat this CSV mapping as shared/noisy/wrong until checked on the panel."
        )
    return None



def bit_global_correlation_rows(
    conn: sqlite3.Connection,
    key: learner_mod.BitKey,
    *,
    seconds: float,
    match_window_seconds: float,
    limit: int,
) -> tuple[int, list[dict[str, Any]]]:
    """Exploratory correlation against canonical berth steps.

    This deliberately remains a ranking tool, not a signal-aspect learner.
    Only precisely timed SF edges are included; historical SG/SH refresh rows
    from older database versions are ignored.
    """
    if not table_exists(conn, "s_bit_events"):
        return 0, []
    use_steps = table_exists(conn, "berth_steps")
    if not use_steps and not table_exists(conn, "pass_log"):
        return 0, []

    since_ts = time.time() - float(seconds)
    total_row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM s_bit_events
        WHERE area=? AND address=? AND bit=? AND event_ts >= ?
          AND UPPER(COALESCE(msg_type,''))='SF'
        """,
        (CFG.nr_area, int(key.address, 16), int(key.bit), since_ts),
    ).fetchone()
    total_changes = int(total_row["c"] if total_row else 0)
    if total_changes <= 0:
        return 0, []

    step_cte = (
        "SELECT id,from_berth AS signal,from_berth,to_berth,descr,event_ts AS pass_ts "
        "FROM berth_steps WHERE area='" + CFG.nr_area.replace("'", "''") + "'"
        if use_steps else
        "SELECT id,signal,from_berth,to_berth,descr,pass_ts FROM pass_log"
    )
    sql = f"""
        WITH bit_events AS (
            SELECT id,event_ts,old_bit,new_bit
            FROM s_bit_events
            WHERE area=? AND address=? AND bit=? AND event_ts >= ?
              AND UPPER(COALESCE(msg_type,''))='SF'
        ), steps AS (
            {step_cte}
        ), nearby AS (
            SELECT e.id AS event_id,e.event_ts AS bit_ts,e.old_bit,e.new_bit,
                   p.id AS pass_id,p.signal,p.from_berth,p.to_berth,p.descr,p.pass_ts,
                   (p.pass_ts-e.event_ts) AS delta_seconds
            FROM bit_events e
            JOIN steps p ON p.pass_ts BETWEEN e.event_ts-? AND e.event_ts+?
        ), roles AS (
            SELECT event_id,pass_id,'signal' AS role,signal AS candidate,
                   from_berth,to_berth,descr,delta_seconds,old_bit,new_bit
            FROM nearby WHERE signal IS NOT NULL AND signal<>''
            UNION ALL
            SELECT event_id,pass_id,'from' AS role,from_berth AS candidate,
                   from_berth,to_berth,descr,delta_seconds,old_bit,new_bit
            FROM nearby WHERE from_berth IS NOT NULL AND from_berth<>''
            UNION ALL
            SELECT event_id,pass_id,'to' AS role,to_berth AS candidate,
                   from_berth,to_berth,descr,delta_seconds,old_bit,new_bit
            FROM nearby WHERE to_berth IS NOT NULL AND to_berth<>''
        )
        SELECT candidate,role,COUNT(*) AS match_rows,
               COUNT(DISTINCT event_id) AS matched_changes,
               COUNT(DISTINCT pass_id) AS matched_moves,
               AVG(ABS(delta_seconds)) AS avg_abs_delta,
               AVG(delta_seconds) AS avg_signed_delta,
               SUM(CASE WHEN delta_seconds>=0 THEN 1 ELSE 0 END) AS before_move_rows,
               SUM(CASE WHEN delta_seconds<0 THEN 1 ELSE 0 END) AS after_move_rows,
               SUM(CASE WHEN old_bit=0 AND new_bit=1 THEN 1 ELSE 0 END) AS up_rows,
               SUM(CASE WHEN old_bit=1 AND new_bit=0 THEN 1 ELSE 0 END) AS down_rows,
               GROUP_CONCAT(DISTINCT from_berth || '->' || to_berth) AS routes
        FROM roles
        GROUP BY candidate,role
        ORDER BY matched_changes DESC,matched_moves DESC,avg_abs_delta ASC,candidate ASC
        LIMIT ?
    """
    rows = conn.execute(
        sql,
        (
            CFG.nr_area,
            int(key.address, 16),
            int(key.bit),
            since_ts,
            float(match_window_seconds),
            float(match_window_seconds),
            int(limit),
        ),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        candidate = learner_mod.normalize_berth(row["candidate"])
        movement_count = recent_signal_move_count(conn, candidate, seconds=seconds)
        matched_changes = int(row["matched_changes"] or 0)
        before_rows = int(row["before_move_rows"] or 0)
        after_rows = int(row["after_move_rows"] or 0)
        up_rows = int(row["up_rows"] or 0)
        down_rows = int(row["down_rows"] or 0)
        routes = [x for x in str(row["routes"] or "").split(",") if x]
        out.append({
            "candidate": candidate,
            "role": str(row["role"] or "?"),
            "matched_changes": matched_changes,
            "matched_moves": int(row["matched_moves"] or 0),
            "match_rows": int(row["match_rows"] or 0),
            "movement_count": movement_count,
            "match_pct": matched_changes / total_changes if total_changes else 0.0,
            "coverage_pct": matched_changes / movement_count if movement_count else 0.0,
            "avg_abs_delta": float(row["avg_abs_delta"] or 0.0),
            "avg_signed_delta": float(row["avg_signed_delta"] or 0.0),
            "timing": "mostly before step" if before_rows > after_rows else "mostly after step" if after_rows > before_rows else "mixed timing",
            "edge": "mostly 0->1" if up_rows > down_rows else "mostly 1->0" if down_rows > up_rows else "mixed edges",
            "routes": routes[:4],
        })
    return total_changes, out


def signal_bit_interpretation_line(
    conn: sqlite3.Connection,
    key: learner_mod.BitKey,
    val: int | None,
    updated_ts: float | None,
    msg_type: str | None,
    best: dict[str, Any] | None,
    pass_count: int,
) -> str:
    if val is None:
        return f"  {key.label}: no current S-Class byte snapshot"
    raw_prefix = f"  {key.label}: raw {val}"
    suffix = f" at {fmt_ts(updated_ts)} (src {msg_type or '?'})"
    if best and best.get("confidence_ok"):
        return (
            f"{raw_prefix} -> {describe_candidate_current(val, best)}; "
            f"learned {best['bucket']} {confidence_text(best, pass_count)}; "
            f"{best['polarity_text']}{suffix}"
        )
    if best:
        low_guess = describe_candidate_current(val, best, allow_low_evidence=True)
        return (
            f"{raw_prefix} -> {low_guess}; "
            f"LOW EVIDENCE ONLY, own evidence {confidence_text(best, pass_count)}, below threshold "
            f"{DERIVE_MIN_SUPPORT} hits/{DERIVE_MIN_PCT*100:.0f}%/{DERIVE_MAX_AVG_DELTA:.1f}s; "
            f"not used for automation/live confidence{suffix}"
        )
    return f"{raw_prefix} (uninterpreted; no pass-window evidence for this signal bit yet){suffix}"


class Status:
    def __init__(self) -> None:
        self.started_ts = time.time()
        self.nr_running = False
        self.nr_connected = False
        self.nr_messages = 0
        self.nr_last_message_ts: Optional[float] = None
        self.nr_last_connect_ts: Optional[float] = None
        self.nr_last_disconnect_ts: Optional[float] = None
        self.nr_last_error = ""
        self.nr_last_connect_duration: Optional[float] = None
        self.lock = threading.RLock()


STATUS = Status()


class DiscordFeedListener:
    def __init__(self, learner: learner_mod.Learner, connection: Any, subscription_id: str):
        self.learner = learner
        self.connection = connection
        self.subscription_id = subscription_id

    @staticmethod
    def _ack_id(frame: Any) -> str:
        headers = getattr(frame, "headers", {}) or {}
        return str(headers.get("ack") or headers.get("message-id") or "")

    def on_message(self, frame: Any) -> None:
        with STATUS.lock:
            STATUS.nr_messages += 1
            STATUS.nr_last_message_ts = time.time()
        ack_id = self._ack_id(frame)
        try:
            messages = list(learner_mod.iter_message_objects(frame.body))
            for key, payload in messages:
                self.learner.handle_message(key, payload)
            # STOMP 1.2 ACK uses the MESSAGE frame's `ack` id. Acknowledge only
            # after every inner TD message has been committed; database failures
            # therefore remain eligible for durable redelivery.
            if ack_id:
                self.connection.ack(id=ack_id)
        except learner_mod.InvalidTDMessage as exc:
            # A permanently malformed poison frame would otherwise block the
            # durable subscription forever. Record it, then consume it.
            self.learner.store.mark_invalid_message(self.learner.area, f"STOMP frame: {exc}")
            with STATUS.lock:
                STATUS.nr_last_error = f"Malformed TD frame discarded: {exc}"
            if ack_id:
                with contextlib.suppress(Exception):
                    self.connection.ack(id=ack_id)
        except Exception:
            with STATUS.lock:
                STATUS.nr_last_error = traceback.format_exc()

    def on_error(self, frame: Any) -> None:
        with STATUS.lock:
            STATUS.nr_last_error = f"{getattr(frame, 'headers', {})} {getattr(frame, 'body', '')}"

    def on_disconnected(self) -> None:
        self.learner.mark_feed_gap("STOMP disconnected; waiting for a new complete refresh")
        with STATUS.lock:
            STATUS.nr_connected = False
            STATUS.nr_last_disconnect_ts = time.time()


class NRFeedService:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.conn: Any = None
        self.lock = threading.RLock()

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start(self) -> str:
        with self.lock:
            if self.is_alive():
                return "NR feed is already running."
            if not CFG.nr_username or not CFG.nr_password:
                return "NR credentials are missing. Set NROD_USER/NROD_PASS or NR_USERNAME/NR_PASSWORD."
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._run, name="nr-feed", daemon=True)
            self.thread.start()
        return "NR feed starting."

    def stop(self, *, join: bool = False, timeout: float = 5.0) -> str:
        self.stop_event.set()
        with contextlib.suppress(Exception):
            if self.conn is not None and self.conn.is_connected():
                self.conn.disconnect()
        if join and self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=timeout)
        with STATUS.lock:
            STATUS.nr_running = False
            STATUS.nr_connected = False
            STATUS.nr_last_disconnect_ts = time.time()
        return "NR feed stopping."

    def _make_learner(self) -> learner_mod.Learner:
        current_topology = topology()
        store = learner_mod.Store(CFG.db_path, CFG.missing_dir)
        return learner_mod.Learner(
            area=CFG.nr_area,
            store=store,
            topology=current_topology,
            known=known_bits(),
            watch_signals=set(current_topology.keys()),
            pre=30.0,
            post=30.0,
            recent_keep=CFG.learner_recent_keep_seconds,
            strict=False,
            learn_special=False,
            ignore_known=True,
            show_known=False,
            print_s=False,
            print_c=False,
            watch_bits=set(),
            watch_unknown=False,
            watch_all_bits=False,
            record_unmapped_routes=False,
        )

    def _run(self) -> None:
        if learner_mod.stomp is None:
            with STATUS.lock:
                STATUS.nr_last_error = "stomp.py is not installed"
                STATUS.nr_running = False
            return

        delay = CFG.nr_reconnect_initial_seconds
        with STATUS.lock:
            STATUS.nr_running = True

        while not self.stop_event.is_set():
            store_to_close = None
            live_learner: learner_mod.Learner | None = None
            connected_at: float | None = None
            try:
                live_learner = self._make_learner()
                store_to_close = live_learner.store
                conn = learner_mod.stomp.Connection12(
                    host_and_ports=[(CFG.nr_host, CFG.nr_port)],
                    keepalive=True,
                    heartbeats=(10000, 10000),
                )
                self.conn = conn
                listener = DiscordFeedListener(live_learner, conn, CFG.nr_subscription_id)
                conn.set_listener("discord-t3-learner", listener)
                connect_headers = {"client-id": CFG.nr_client_id} if CFG.nr_durable else {}
                conn.connect(
                    username=CFG.nr_username,
                    passcode=CFG.nr_password,
                    wait=True,
                    headers=connect_headers,
                )
                live_learner.mark_connected()
                subscribe_headers = (
                    {"activemq.subscriptionName": CFG.nr_durable_name}
                    if CFG.nr_durable else {}
                )
                conn.subscribe(
                    destination=CFG.nr_topic,
                    id=CFG.nr_subscription_id,
                    ack="client-individual",
                    headers=subscribe_headers,
                )

                connected_at = time.time()
                delay = CFG.nr_reconnect_initial_seconds
                with STATUS.lock:
                    STATUS.nr_connected = True
                    STATUS.nr_last_connect_ts = connected_at
                    STATUS.nr_last_error = ""

                while conn.is_connected() and not self.stop_event.is_set():
                    live_learner.tick()
                    time.sleep(CFG.nr_feed_tick_seconds)

            except Exception:
                with STATUS.lock:
                    STATUS.nr_last_error = traceback.format_exc()
                    STATUS.nr_connected = False
            finally:
                with contextlib.suppress(Exception):
                    if live_learner is not None:
                        live_learner.mark_feed_gap(
                            "feed stopped" if self.stop_event.is_set() else "connection ended"
                        )
                with contextlib.suppress(Exception):
                    if self.conn is not None and self.conn.is_connected():
                        self.conn.disconnect()
                with contextlib.suppress(Exception):
                    if store_to_close is not None:
                        store_to_close.close()
                now = time.time()
                with STATUS.lock:
                    STATUS.nr_connected = False
                    STATUS.nr_last_disconnect_ts = now
                    if connected_at is not None:
                        STATUS.nr_last_connect_duration = now - connected_at

            if self.stop_event.is_set():
                break

            end = time.time() + delay
            while time.time() < end and not self.stop_event.is_set():
                time.sleep(min(1.0, CFG.nr_feed_tick_seconds))
            delay = min(CFG.nr_reconnect_max_seconds, max(delay * 2, CFG.nr_reconnect_initial_seconds))

        with STATUS.lock:
            STATUS.nr_running = False
            STATUS.nr_connected = False


NR_SERVICE = NRFeedService()

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

feed_group = app_commands.Group(
    name="feed",
    description="Start, stop and restart the live Network Rail feed.",
)
signal_group = app_commands.Group(
    name="signal",
    description="Inspect, analyse and physically verify a signal or TD berth.",
)
td_group = app_commands.Group(
    name="td",
    description="Inspect Train Describer berth and headcode movements.",
)
raw_group = app_commands.Group(
    name="raw",
    description="Low-level S-Class byte and bit diagnostics.",
)
diagnostics_group = app_commands.Group(
    name="diagnostics",
    description="Protocol health, topology and learner diagnostics.",
)
database_group = app_commands.Group(
    name="database",
    description="Database maintenance, import and export tools.",
)

_COMMAND_GROUPS = (
    feed_group, signal_group, td_group, raw_group, diagnostics_group, database_group
)
_READY_DONE = False


@bot.event
async def on_ready() -> None:
    global _READY_DONE
    ensure_dirs()
    if not _READY_DONE:
        if CFG.guild_id:
            guild = discord.Object(id=CFG.guild_id)
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
        else:
            await bot.tree.sync()

        if CFG.nr_enabled:
            NR_SERVICE.start()
        _READY_DONE = True

    print(f"Logged in as {bot.user} | slash commands synced | uptime {uptime_text(STATUS.started_ts)}")


@bot.tree.command(name="status", description="Show Discord bot, data files and NR feed status.")
async def status_cmd(interaction: discord.Interaction) -> None:
    ensure_dirs()
    with STATUS.lock:
        nr_running = STATUS.nr_running
        nr_connected = STATUS.nr_connected
        nr_messages = STATUS.nr_messages
        nr_last_message_ts = STATUS.nr_last_message_ts
        nr_last_connect_ts = STATUS.nr_last_connect_ts
        nr_last_error = STATUS.nr_last_error
        nr_last_duration = STATUS.nr_last_connect_duration

    try:
        known_count = len(known_bits().rows)
    except Exception:
        known_count = 0

    db_size = CFG.db_path.stat().st_size if CFG.db_path.exists() else 0
    snapshot_info: dict[str, Any] = {"valid": False, "reason": "database unavailable"}
    if CFG.db_path.exists():
        with contextlib.suppress(Exception):
            with db_connect(readonly=True) as conn:
                snapshot_info = feed_snapshot_status(conn)
    embed = discord.Embed(title="T3 Protocol Bot Status", color=0x2ECC71 if nr_connected and snapshot_info.get("valid") else 0xE67E22)
    embed.add_field(
        name="Discord/runtime",
        value=(
            f"Online as `{bot.user}`\n"
            f"Latency `{bot.latency * 1000:.0f} ms`\n"
            f"Uptime `{uptime_text(STATUS.started_ts)}`\n"
            f"Memory `{process_memory_text()}`\n"
            f"Threads `{threading.active_count()}`"
        ),
        inline=False,
    )
    duration_text = (
        f"{nr_last_duration:.0f}s" if nr_last_duration is not None else "n/a"
    )
    embed.add_field(
        name="NR Feed",
        value=(
            f"Enabled: `{CFG.nr_enabled}`\n"
            f"Running: `{nr_running}`\n"
            f"Connected: `{nr_connected}`\n"
            f"Messages: `{nr_messages}`\n"
            f"Last message: `{fmt_ts(nr_last_message_ts)}`\n"
            f"Last connect: `{fmt_ts(nr_last_connect_ts)}`\n"
            f"Last connected duration: `{duration_text}`"
        ),
        inline=False,
    )
    embed.add_field(
        name="S-Class snapshot",
        value=(
            f"Live valid: `{snapshot_info.get('valid', False)}`\n"
            f"Generation: `{snapshot_info.get('generation', 0)}`\n"
            f"Refresh in progress: `{snapshot_info.get('refresh_in_progress', False)}`\n"
            f"Last complete refresh: `{fmt_ts(snapshot_info.get('last_refresh'))}`\n"
            f"Last S event: `{fmt_ts(snapshot_info.get('last_s_event'))}`\n"
            f"Invalid messages: `{snapshot_info.get('invalid_messages', 0)}`\n"
            f"Duplicate deliveries ignored: `{snapshot_info.get('duplicate_messages', 0)}`\n"
            f"Reason: `{trim(str(snapshot_info.get('reason') or '-'), 300)}`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Files/settings",
        value=(
            f"DB: `{CFG.db_path}`\n"
            f"DB size: `{db_size:,}` bytes\n"
            f"Known CSV rows: `{known_count}`\n"
            f"Recent keep: `{CFG.learner_recent_keep_seconds:.0f}s`\n"
            f"Feed tick: `{CFG.nr_feed_tick_seconds:.1f}s`\n"
            f"Read cache: `{CFG.db_read_cache_kib} KiB`"
        ),
        inline=False,
    )
    if nr_last_error:
        embed.add_field(name="Last NR error", value=f"```text\n{trim(nr_last_error, 900)}\n```", inline=False)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="help", description="Show the simplified NR Bot command layout and examples.")
async def help_cmd(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="NR Bot commands",
        description=(
            "Commands are grouped by purpose. Start with `/status` and `/signal show`. "
            "The low-level raw tools are mainly for mapping and fault-finding."
        ),
        color=0x3498DB,
    )
    embed.add_field(
        name="Everyday",
        value=(
            "`/status` — feed and snapshot health\n"
            "`/signal show signal:6239` — current berth and verified signal evidence\n"
            "`/td berths` — occupied TD berths/headcodes\n"
            "`/td moves signal:6239` — recent berth movements"
        ),
        inline=False,
    )
    embed.add_field(
        name="Signal mapping",
        value=(
            "`/signal observe` — capture RED, OFF and post-pass states\n"
            "`/signal observations` — review physical evidence\n"
            "`/signal analyse` — detailed candidate analysis\n"
            "`/signal mappings` and `/signal routes` — reference/route evidence"
        ),
        inline=False,
    )
    embed.add_field(
        name="Operations and diagnostics",
        value=(
            "`/feed start|stop|restart` — control the NR connection\n"
            "`/diagnostics progress|check|missing` — health and topology checks\n"
            "`/raw bit|recent|trace|correlate|bytes` — low-level S-Class tools\n"
            "`/database stats|optimise|export|import` — data maintenance"
        ),
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@feed_group.command(name="start", description="Start the live Network Rail TD feed.")
async def nr_start_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    await send_text(interaction, await asyncio.to_thread(NR_SERVICE.start))


@feed_group.command(name="stop", description="Stop the live Network Rail TD feed.")
async def nr_stop_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    await send_text(interaction, await asyncio.to_thread(NR_SERVICE.stop, join=True))


@feed_group.command(name="restart", description="Restart the live Network Rail TD feed.")
async def nr_restart_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    stopped = await asyncio.to_thread(NR_SERVICE.stop, join=True)
    await asyncio.sleep(1)
    KNOWN_CACHE.invalidate()
    started = await asyncio.to_thread(NR_SERVICE.start)
    await send_text(interaction, stopped + "\n" + started)


@signal_group.command(name="analyse", description="Analyse protocol evidence and candidate bits for a signal.")
@app_commands.describe(
    signal="Signal/berth, e.g. 6239",
    max_steps="Maximum recent canonical CA berth steps to analyse",
    limit="Maximum candidates per classification",
)
async def report_cmd(
    interaction: discord.Interaction,
    signal: str,
    max_steps: int = 250,
    limit: int = 10,
) -> None:
    await interaction.response.defer()
    sig = learner_mod.normalize_berth(signal)
    try:
        with db_connect(readonly=True) as conn:
            rows = learner_mod.protocol_candidate_analysis(
                conn, sig, area=CFG.nr_area,
                max_steps=max(10, min(int(max_steps), 1000)),
            )
            manual = learner_mod.manual_observation_candidates(conn, sig) if table_exists(conn, "signal_observation_sessions") else []
        known = known_bits()
        mapped = sorted(known.keys_for_signal(sig), key=lambda k: (int(k.address, 16), k.bit))
        lines = [f"Protocol evidence report for signal/berth {sig}"]
        lines.append(
            "CA timestamps are treated as berth-step times, not exact physical signal-passage times. "
            "SG/SH refresh differences are excluded from timed-edge learning; only SF edges are analysed."
        )
        if mapped:
            lines.append("CSV/reference mappings:")
            for key in mapped:
                for row in known.by_key.get(key, []):
                    lines.append(f"  {key.label}: {row.summary()}")
        else:
            lines.append("CSV/reference mappings: none")

        groups = [
            ("movement_pulse", "Rejected movement/track-shaped pulses"),
            ("pre_step_control", "Pre-step control cycles (signal OR route candidates)"),
            ("correlated_control", "Weaker/general correlations"),
        ]
        shown_limit = max(1, min(int(limit), 50))
        for classification, heading in groups:
            group = [r for r in rows if r["classification"] == classification]
            lines.append(heading + ":")
            if not group:
                lines.append("  none")
                continue
            for row in group[:shown_limit]:
                lead = "?" if row["median_lead"] is None else f"{row['median_lead']:.1f}s"
                pulse = "?" if row["median_pulse"] is None else f"{row['median_pulse']:.1f}s"
                lines.append(
                    f"  {row['key'].label}: class={classification}; direction={row['direction']}; "
                    f"steps={row['target_steps']}; pre={row['pre_hits']} ({row['pre_rate']*100:.0f}%); "
                    f"cycles={row['cycle_hits']} ({row['cycle_rate']*100:.0f}%); "
                    f"near-step={row['near_hits']} ({row['near_rate']*100:.0f}%); "
                    f"median_lead={lead}; median_pulse={pulse}; controls={row['control_rate']*100:.0f}%; "
                    f"lift={row['lift']*100:+.0f}pp"
                )
                lines.append(f"    {row['explanation']}")

        lines.append("Paired physical observations:")
        if not manual:
            lines.append("  none/insufficient")
        else:
            for row in manual[:shown_limit]:
                lines.append(
                    f"  {row['key'].label}: RED->OFF {row['direction']} in {row['support']}/{row['pair_count']} "
                    f"pair(s), consistency={row['consistency']*100:.0f}%, post-pass return={row['return_rate']*100:.0f}%"
                )
        lines.append(
            "No automated candidate is allowed to produce RED/OFF output until the CSV mapping is explicitly verified with provenance and polarity."
        )
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@diagnostics_group.command(name="progress", description="Show protocol ingestion and learner progress.")
async def progress_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        known = known_bits()
        verified = sum(1 for row in known.rows if row.trusted_for_live_aspect)
        reference_only = sum(1 for row in known.rows if row.described and not row.trusted_for_live_aspect)
        lines = ["T3 protocol-level progress"]
        if not CFG.db_path.exists():
            lines.append("Database does not exist yet.")
            await send_text(interaction, "\n".join(lines))
            return
        with db_connect(readonly=True) as conn:
            snapshot = feed_snapshot_status(conn)
            lines.append(
                f"S-Class snapshot: {'VALID' if snapshot['valid'] else 'NOT LIVE'}; "
                f"generation={snapshot.get('generation', 0)}; "
                f"refresh_in_progress={snapshot.get('refresh_in_progress', False)}"
            )
            lines.append(
                f"last complete refresh={fmt_ts(snapshot.get('last_refresh'))}; "
                f"last S event={fmt_ts(snapshot.get('last_s_event'))}; "
                f"reason={snapshot.get('reason') or '-'}"
            )
            counts = {}
            for table in [
                "raw_td_messages", "berth_steps", "s_bit_events",
                "s_snapshot_differences", "signal_observation_sessions",
                "signal_observations", "pass_log", "pass_bit_events",
            ]:
                if table_exists(conn, table):
                    counts[table] = int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])
                else:
                    counts[table] = 0
            lines.append(
                f"raw TD messages={counts['raw_td_messages']:,}; duplicates ignored={snapshot.get('duplicate_messages', 0):,}; "
                f"invalid rejected={snapshot.get('invalid_messages', 0):,}"
            )
            lines.append(
                f"canonical CA berth steps={counts['berth_steps']:,}; precisely timed SF bit edges={counts['s_bit_events']:,}; "
                f"SG/SH observational differences={counts['s_snapshot_differences']:,}"
            )
            lines.append(
                f"physical observation sessions={counts['signal_observation_sessions']:,}; snapshots={counts['signal_observations']:,}"
            )
            lines.append(
                f"known CSV rows={len(known.rows):,}; verified live-aspect mappings={verified}; reference-only mappings={reference_only}"
            )
            lines.append(
                f"legacy pass-window rows retained for diagnostics={counts['pass_log']:,}; attached rows={counts['pass_bit_events']:,}. "
                "They are no longer treated as proof of a physical aspect."
            )
            if table_exists(conn, "refresh_history"):
                refreshes = conn.execute(
                    """
                    SELECT status,COUNT(*) AS c FROM refresh_history GROUP BY status ORDER BY status
                    """
                ).fetchall()
                if refreshes:
                    lines.append("refresh history: " + ", ".join(f"{r['status']}={r['c']}" for r in refreshes))
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@signal_group.command(name="mappings", description="Show CSV mappings, provenance and verification for a signal.")
async def known_cmd(interaction: discord.Interaction, signal: str) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("known") + ["--signal", signal])
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@td_group.command(name="moves", description="Show canonical berth movements involving a signal or berth.")
@app_commands.describe(
    signal="Signal/berth to inspect, e.g. 6244",
    limit="Maximum rows to show",
    show_events="Also include S-Class events attached to each movement/pass",
    event_limit="Maximum attached S-Class events when show_events is enabled",
)
async def moves_cmd(
    interaction: discord.Interaction,
    signal: str,
    limit: int = 20,
    show_events: bool = False,
    event_limit: int = 40,
) -> None:
    await interaction.response.defer()
    try:
        args = cli_args("moves") + ["--berth", signal, "--limit", str(clamp_limit(limit))]
        if show_events:
            args += ["--show-events", "--event-limit", str(clamp_limit(event_limit))]
        text = await run_cli_async(args)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@td_group.command(name="berths", description="Show current stored TD berth and headcode states.")
@app_commands.describe(
    berth="Optional exact berth or prefix, e.g. 6263 or 62",
    occupied_only="Only show occupied berths",
    limit="Maximum rows to show",
)
async def berths_cmd(
    interaction: discord.Interaction,
    berth: Optional[str] = None,
    occupied_only: bool = True,
    limit: int = 40,
) -> None:
    await interaction.response.defer()
    try:
        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return
        max_rows = clamp_limit(limit)
        with db_connect(readonly=True) as conn:
            if not table_exists(conn, "berth_state"):
                await send_text(interaction, "No berth_state table yet.")
                return
            where = []
            params: list[Any] = []
            title = "Stored berth/headcode states"
            if berth:
                q = learner_mod.normalize_berth(berth)
                title += f" matching {q}"
                if len(q) >= 4:
                    where.append("berth=?")
                    params.append(q)
                else:
                    where.append("berth LIKE ?")
                    params.append(q + "%")
            if occupied_only:
                where.append("occupied=1")
            sql = "SELECT berth, descr, occupied, updated_ts, source_msg_type FROM berth_state"
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY occupied DESC, updated_ts DESC LIMIT ?"
            params.append(max_rows)
            rows = conn.execute(sql, params).fetchall()

        lines = [title]
        if not rows:
            lines.append("No matching stored berth state.")
            lines.append("Note: TD C-Class is delta based. After a bot restart, a train already sitting in a berth may not appear until the next CA/CB/CC update for that berth.")
        else:
            for r in rows:
                state = "occupied" if int(r["occupied"]) else "clear"
                descr = r["descr"] or "-"
                lines.append(f"{r['berth']}: {state} {descr} | {fmt_ts(float(r['updated_ts']))} via {r['source_msg_type'] or '?'}")
            if len(rows) >= max_rows:
                lines.append(f"Showing first {max_rows} rows. Increase limit or add a berth/prefix filter.")
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@raw_group.command(name="bytes", description="Show S-Class byte addresses seen by the learner.")
async def bytes_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("bytes"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@diagnostics_group.command(name="missing", description="Show berth movements missing from configured topology.")
async def missing_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("missing"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@diagnostics_group.command(name="check", description="Check topology, known CSV and database loading.")
async def check_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("check"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@raw_group.command(name="bit", description="Show one raw S-Class bit, provenance and classification.")
async def bit_cmd(interaction: discord.Interaction, bit: str) -> None:
    await interaction.response.defer()
    try:
        key = learner_mod.parse_bit_spec(bit)
        known = known_bits()
        lines = [f"S-Class bit {key.label}"]
        current_value = None
        current_ts = None
        current_msg = None
        latest_change = None
        snapshot: dict[str, Any] = {"valid": False, "reason": "database unavailable"}

        if CFG.db_path.exists():
            with db_connect(readonly=True) as conn:
                snapshot = feed_snapshot_status(conn)
                if snapshot["valid"]:
                    current_value, current_ts, current_msg = current_bit_value(conn, key)
                if table_exists(conn, "s_bit_events"):
                    latest_change = conn.execute(
                        """
                        SELECT event_ts, old_bit, new_bit, msg_type, old_byte, new_byte
                        FROM s_bit_events
                        WHERE area=? AND address=? AND bit=?
                          AND UPPER(COALESCE(msg_type,''))='SF'
                        ORDER BY event_ts DESC LIMIT 1
                        """,
                        (CFG.nr_area, int(key.address, 16), int(key.bit)),
                    ).fetchone()

        if snapshot.get("valid"):
            if current_value is None:
                lines.append("Live snapshot is valid, but this byte has not been seen.")
            else:
                lines.append(
                    f"Current raw value: {current_value} at {fmt_ts(current_ts)} via {current_msg or '?'} "
                    f"(snapshot generation {snapshot.get('generation', 0)})"
                )
        else:
            lines.append(
                f"Current raw value withheld: S-Class snapshot is not live/valid. "
                f"{snapshot.get('reason') or 'Waiting for complete SG...SH refresh.'}"
            )

        if latest_change:
            old_byte = "??" if latest_change["old_byte"] is None else f"{int(latest_change['old_byte']):02X}"
            new_byte = f"{int(latest_change['new_byte']):02X}"
            lines.append(
                f"Latest precisely timed SF edge: {latest_change['old_bit']}->{latest_change['new_bit']} "
                f"byte {old_byte}->{new_byte} at {fmt_ts(float(latest_change['event_ts']))}"
            )
        else:
            lines.append("No precisely timed SF edge stored for this bit.")

        rows = known.by_key.get(key, [])
        if rows:
            lines.append("CSV/reference mapping(s):")
            for row in rows:
                live = "trusted for live aspect" if row.trusted_for_live_aspect else "reference only"
                lines.append(f"  {row.summary()} | {live}")
        else:
            lines.append("CSV/reference mapping: none")

        mapped_signals = sorted(known.signals_for_key(key))
        if mapped_signals and CFG.db_path.exists():
            with db_connect(readonly=True) as conn:
                for sig in mapped_signals[:5]:
                    matches = [
                        row for row in learner_mod.protocol_candidate_analysis(
                            conn, sig, area=CFG.nr_area, max_steps=250
                        ) if row["key"] == key
                    ]
                    if not matches:
                        lines.append(f"Protocol evidence for {sig}: none/insufficient")
                        continue
                    row = matches[0]
                    if row["classification"] == "movement_pulse":
                        pulse = "?" if row["median_pulse"] is None else f"{row['median_pulse']:.1f}s"
                        lines.append(
                            f"Protocol evidence for {sig}: REJECTED as aspect candidate; movement/track-shaped "
                            f"pulse in {row['near_hits']}/{row['target_steps']} steps, median {pulse}."
                        )
                    else:
                        lead = "?" if row["median_lead"] is None else f"{row['median_lead']:.1f}s"
                        lines.append(
                            f"Protocol evidence for {sig}: {row['classification']}; {row['direction']} with "
                            f"{row['cycle_hits']}/{row['target_steps']} complete cycles, median lead {lead}; "
                            "this may be signal OR route and is not a physical aspect proof."
                        )

        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@signal_group.command(name="show", description="Show berth state and cautious evidence for a signal.")
async def signal_cmd(interaction: discord.Interaction, signal: str) -> None:
    await interaction.response.defer()
    try:
        sig = learner_mod.normalize_berth(signal)
        known = known_bits()
        keys = sorted(known.keys_for_signal(sig), key=lambda k: (int(k.address, 16), k.bit))
        lines = [f"Signal/berth {sig}"]

        route_nexts = sorted(topology().get(sig, set()), key=lambda x: (not x.isdigit(), x))
        lines.append("Configured next berth(s): " + (", ".join(route_nexts) if route_nexts else "none"))
        lines.append(
            "Important: a C-Class CA is a berth step. It does not independently prove the exact physical signal-passage time."
        )

        if not CFG.db_path.exists():
            lines.append("Database does not exist yet.")
            await send_text(interaction, "\n".join(lines), paged=True)
            return

        with db_connect(readonly=True) as conn:
            snapshot = feed_snapshot_status(conn)
            if snapshot["valid"]:
                lines.append(
                    f"S-Class snapshot: VALID generation {snapshot['generation']}; "
                    f"last complete SG...SH refresh {fmt_ts(snapshot['last_refresh'])}; "
                    f"last TD event/heartbeat {fmt_ts(snapshot['last_feed_event'])}"
                )
            else:
                state = "refresh in progress" if snapshot.get("refresh_in_progress") else "not live"
                lines.append(
                    f"S-Class snapshot: {state.upper()}; {snapshot.get('reason') or 'waiting for a complete refresh'}. "
                    "Persisted raw bytes are not used as current signal state."
                )

            if table_exists(conn, "berth_state"):
                berth = conn.execute("SELECT * FROM berth_state WHERE berth=?", (sig,)).fetchone()
            else:
                berth = None
            if berth and int(berth["occupied"]):
                lines.append(
                    f"TD berth: occupied by {berth['descr'] or 'unknown'}; "
                    f"updated {fmt_ts(float(berth['updated_ts']))} via {berth['source_msg_type'] or '?'}"
                )
            elif berth:
                lines.append(
                    f"TD berth: clear; last update {fmt_ts(float(berth['updated_ts']))} "
                    f"via {berth['source_msg_type'] or '?'}"
                )
            else:
                lines.append("TD berth: no stored state yet")

            # Canonical C-Class history.
            if table_exists(conn, "berth_steps"):
                steps = conn.execute(
                    """
                    SELECT event_ts,descr,from_berth,to_berth,source_msg_type
                    FROM berth_steps
                    WHERE from_berth=? OR to_berth=?
                    ORDER BY event_ts DESC LIMIT 4
                    """,
                    (sig, sig),
                ).fetchall()
            else:
                steps = []
            if steps:
                lines.append("Recent C-Class berth steps involving this berth:")
                for row in steps:
                    lines.append(
                        f"  {fmt_ts(float(row['event_ts']))} {row['descr'] or '----'} "
                        f"{row['from_berth']} -> {row['to_berth']} ({row['source_msg_type'] or 'CA'})"
                    )

            lines.append("Physical aspect:")
            trusted_aspect_seen = False
            if not keys:
                lines.append("  UNKNOWN — no CSV/reference bit is mapped to this signal.")
            else:
                for key in keys:
                    rows = known.by_key.get(key, [])
                    sources = sorted({(r.provenance or "reference-unverified") for r in rows})
                    verified = trusted_live_mapping_rows(known, key, sig)
                    raw_value: int | None = None
                    raw_ts: float | None = None
                    raw_src: str | None = None
                    if snapshot["valid"]:
                        raw_value, raw_ts, raw_src = current_bit_value(conn, key)
                    if verified and raw_value is not None:
                        interpretations = []
                        for row in verified:
                            interpreted = _active_state_text(row.active_state, raw_value)
                            if interpreted:
                                interpretations.append(interpreted)
                        if interpretations:
                            trusted_aspect_seen = True
                            lines.append(
                                f"  {key.label}: raw {raw_value} -> {', '.join(sorted(set(interpretations)))}; "
                                f"verified source {', '.join(sorted({r.provenance for r in verified}))}; "
                                f"updated {fmt_ts(raw_ts)} via {raw_src or '?'}"
                            )
                        else:
                            lines.append(
                                f"  {key.label}: raw {raw_value}; mapping is verified but Active State/polarity is missing, "
                                "so RED/OFF is not inferred."
                            )
                    else:
                        raw_text = (
                            f"raw {raw_value} at {fmt_ts(raw_ts)} via {raw_src or '?'}; "
                            if raw_value is not None else "raw live state unavailable; "
                        )
                        lines.append(
                            f"  {key.label}: {raw_text}reference source={','.join(sources)}; "
                            "NOT verified for live aspect use."
                        )
                if not trusted_aspect_seen:
                    lines.append(
                        "  Result: UNKNOWN. Existing CSV rows are references only until verified by SOP/ECS or repeated paired physical observations."
                    )

            protocol_rows = learner_mod.protocol_candidate_analysis(
                conn, sig, area=CFG.nr_area, max_steps=250
            )
            rejected = [r for r in protocol_rows if r["classification"] == "movement_pulse"]
            controls = [r for r in protocol_rows if r["classification"] == "pre_step_control"]
            correlated = [r for r in protocol_rows if r["classification"] == "correlated_control"]

            if rejected:
                lines.append("Rejected movement/track-shaped mappings:")
                for row in rejected[:5]:
                    pulse = "?" if row["median_pulse"] is None else f"{row['median_pulse']:.1f}s"
                    lines.append(
                        f"  {row['key'].label}: near step {row['near_hits']}/{row['target_steps']} "
                        f"({row['near_rate']*100:.0f}%); median pulse {pulse}; {row['explanation']}"
                    )

            if controls:
                lines.append("Pre-step S-Class control candidates (not proof of physical aspect):")
                for row in controls[:5]:
                    lead = "?" if row["median_lead"] is None else f"{row['median_lead']:.1f}s"
                    lines.append(
                        f"  {row['key'].label}: {row['direction']} before "
                        f"{row['cycle_hits']}/{row['target_steps']} step(s); median lead {lead}; "
                        f"target {row['pre_rate']*100:.0f}% vs unrelated controls {row['control_rate']*100:.0f}% "
                        f"(lift {row['lift']*100:+.0f}pp). Signal OR route candidate only."
                    )
            elif correlated:
                lines.append("Only weak/general movement correlations found; none are safe as signal-aspect candidates.")
            elif not protocol_rows:
                lines.append("Protocol learner: not enough canonical berth-step/SF edge evidence yet.")

            if table_exists(conn, "signal_observation_sessions"):
                manual = learner_mod.manual_observation_candidates(conn, sig)
            else:
                manual = []
            if manual:
                lines.append("Paired physical RED/OFF observations:")
                for row in manual[:5]:
                    lines.append(
                        f"  {row['key'].label}: RED->OFF {row['direction']} in "
                        f"{row['support']}/{row['pair_count']} pair(s) "
                        f"({row['consistency']*100:.0f}%); returned after pass in {row['return_rate']*100:.0f}% of supported sessions."
                    )
                lines.append(
                    "Observation candidates remain non-authoritative until enough clean sessions are reviewed and the CSV is explicitly marked verified."
                )
            else:
                lines.append(
                    "Physical observations: none/insufficient. Use /signal observe with red, off and post_pass during the same approach."
                )

        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@signal_group.command(name="observe", description="Capture a physical RED, OFF or post-pass observation.")
@app_commands.describe(
    signal="Signal/berth, e.g. 6239",
    state="Use red, off, post_pass or cancel",
    headcode="Optional train/headcode for this approach",
    notes="Optional short observation note",
)
@app_commands.choices(state=[
    app_commands.Choice(name="RED / on", value="red"),
    app_commands.Choice(name="OFF / proceed", value="off"),
    app_commands.Choice(name="Post-pass / restored", value="post_pass"),
    app_commands.Choice(name="Cancel current session", value="cancel"),
])
async def observe_signal_cmd(
    interaction: discord.Interaction,
    signal: str,
    state: app_commands.Choice[str],
    headcode: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    await interaction.response.defer()
    sig = learner_mod.normalize_berth(signal)
    chosen = state.value
    try:
        store = learner_mod.Store(CFG.db_path, CFG.missing_dir)
        try:
            if chosen == "cancel":
                session = store.latest_open_observation_session(sig)
                if session is None:
                    await send_text(interaction, f"No open observation session for {sig}.")
                    return
                store.cancel_observation_session(int(session["id"]), notes or "cancelled by Discord command")
                await send_text(interaction, f"Cancelled observation session {int(session['id'])} for {sig}.")
                return

            valid, generation, snapshot, last_refresh, reason = store.current_snapshot(CFG.nr_area)
            snapshot_status = feed_snapshot_status(store.conn)
            valid = bool(valid and snapshot_status.get("valid"))
            if not valid:
                await send_text(
                    interaction,
                    f"Cannot capture {chosen}: the S-Class snapshot is not live/valid. "
                    f"{snapshot_status.get('reason') or reason or 'Wait for a complete SG...SH refresh.'}",
                )
                return
            if not snapshot:
                await send_text(interaction, "Cannot capture observation: live snapshot contains no bytes.")
                return

            if chosen == "red":
                session_id = store.start_observation_session(
                    sig, descr=(headcode or "").strip(), notes=(notes or "").strip()
                )
            else:
                session = store.latest_open_observation_session(sig)
                if session is None:
                    await send_text(
                        interaction,
                        f"No open {sig} observation session. Capture RED first, then OFF, then post_pass.",
                    )
                    return
                session_id = int(session["id"])
                existing = store.conn.execute(
                    "SELECT state FROM signal_observations WHERE session_id=?",
                    (session_id,),
                ).fetchall()
                existing_states = {str(r["state"]).lower() for r in existing}
                if chosen == "off" and "red" not in existing_states:
                    await send_text(interaction, "This session has no RED snapshot. Start again with state RED.")
                    return
                if chosen == "post_pass" and not {"red", "off"}.issubset(existing_states):
                    await send_text(interaction, "Capture both RED and OFF before the post-pass snapshot.")
                    return

            observation_id = store.add_signal_observation(
                session_id=session_id,
                signal_id=sig,
                state=chosen,
                snapshot=snapshot,
                generation=generation,
                notes=(notes or "").strip(),
            )
            next_action = {
                "red": "Capture OFF as soon as you physically see the signal clear.",
                "off": "Capture post_pass after the train has passed and the signal/route has restored.",
                "post_pass": "Session complete. Repeating this on several separate approaches builds usable evidence.",
            }[chosen]
            await send_text(
                interaction,
                f"Captured {chosen.upper()} for signal {sig} in session {session_id} "
                f"(observation {observation_id}, snapshot generation {generation}, "
                f"last refresh {fmt_ts(last_refresh)}).\n{next_action}",
            )
        finally:
            store.close()
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@signal_group.command(name="observations", description="Review paired physical observations for a signal.")
async def observations_cmd(interaction: discord.Interaction, signal: str) -> None:
    await interaction.response.defer()
    sig = learner_mod.normalize_berth(signal)
    try:
        with db_connect(readonly=True) as conn:
            if not table_exists(conn, "signal_observation_sessions"):
                await send_text(interaction, "No observation tables exist yet. Restart the updated bot once to migrate the database.")
                return
            sessions = conn.execute(
                """
                SELECT s.*,
                       SUM(CASE WHEN o.state='red' THEN 1 ELSE 0 END) AS red_count,
                       SUM(CASE WHEN o.state='off' THEN 1 ELSE 0 END) AS off_count,
                       SUM(CASE WHEN o.state='post_pass' THEN 1 ELSE 0 END) AS post_count
                FROM signal_observation_sessions s
                LEFT JOIN signal_observations o ON o.session_id=s.id
                WHERE s.signal=?
                GROUP BY s.id
                ORDER BY s.started_ts DESC LIMIT 20
                """,
                (sig,),
            ).fetchall()
            candidates = learner_mod.manual_observation_candidates(conn, sig)

        lines = [f"Physical observation evidence for {sig}"]
        if not sessions:
            lines.append("No sessions yet. Use /signal observe state:red during an approach.")
        else:
            lines.append(f"Sessions: {len(sessions)} shown")
            for row in sessions[:8]:
                lines.append(
                    f"  #{row['id']} {row['status']} started {fmt_ts(float(row['started_ts']))}; "
                    f"RED={int(row['red_count'] or 0)} OFF={int(row['off_count'] or 0)} "
                    f"POST={int(row['post_count'] or 0)} headcode={row['descr'] or '-'}"
                )
        if candidates:
            lines.append("Consistent RED->OFF candidates:")
            for row in candidates[:10]:
                lines.append(
                    f"  {row['key'].label}: {row['direction']} in {row['support']}/{row['pair_count']} pair(s) "
                    f"({row['consistency']*100:.0f}%); post-pass return {row['return_rate']*100:.0f}%"
                )
        else:
            lines.append("No bit has at least two consistent paired RED/OFF observations yet.")
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@raw_group.command(name="recent", description="Show recent precisely timed SF bit changes.")
@app_commands.describe(
    signal="Optional signal to filter by known_bits.csv mapping, e.g. 6244",
    bit="Optional byte:bit filter, e.g. 25:3",
    limit="Maximum rows to show",
    known_only="Only show bits described in known_bits.csv",
    since_minutes="Only show changes newer than this many minutes",
)
async def recent_bits_cmd(
    interaction: discord.Interaction,
    signal: Optional[str] = None,
    bit: Optional[str] = None,
    limit: int = 30,
    known_only: bool = False,
    since_minutes: Optional[int] = None,
) -> None:
    await interaction.response.defer()
    try:
        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return

        known = known_bits()
        params: list[Any] = [CFG.nr_area]
        where = ["area=?", "UPPER(COALESCE(msg_type,''))='SF'"]
        title_parts = ["Recent precisely timed SF bit changes"]

        if bit:
            key = learner_mod.parse_bit_spec(bit)
            where.append("address=? AND bit=?")
            params.extend([int(key.address, 16), int(key.bit)])
            title_parts.append(key.label)

        if signal:
            sig = learner_mod.normalize_berth(signal)
            keys = sorted(known.keys_for_signal(sig), key=lambda k: (int(k.address, 16), k.bit))
            if not keys:
                await send_text(interaction, f"No known_bits.csv bit mappings for signal {sig}.")
                return
            sql, key_params = bit_filter_sql(keys)
            where.append(sql)
            params.extend(key_params)
            title_parts.append(f"signal {sig}")

        if known_only:
            all_known_keys = sorted(known.by_key.keys(), key=lambda k: (int(k.address, 16), k.bit))
            sql, key_params = bit_filter_sql(all_known_keys)
            if not sql:
                await send_text(interaction, "known_bits.csv has no mapped bits.")
                return
            where.append(sql)
            params.extend(key_params)
            title_parts.append("known only")

        if since_minutes is not None and since_minutes > 0:
            where.append("event_ts>=?")
            params.append(time.time() - (int(since_minutes) * 60))
            title_parts.append(f"last {since_minutes}m")

        row_limit = clamp_limit(limit)
        params.append(row_limit)
        with db_connect(readonly=True) as conn:
            if not table_exists(conn, "s_bit_events"):
                await send_text(interaction, "s_bit_events table does not exist yet.")
                return
            rows = conn.execute(
                f"""
                SELECT event_ts, address, bit, old_bit, new_bit, old_byte, new_byte, msg_type
                FROM s_bit_events
                WHERE {' AND '.join(where)}
                ORDER BY event_ts DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        lines = [" | ".join(title_parts)]
        if not rows:
            lines.append("No matching bit changes found.")
        for row in rows:
            addr = int(row["address"])
            b = int(row["bit"])
            old_byte = "??" if row["old_byte"] is None else f"{int(row['old_byte']):02X}"
            new_byte = f"{int(row['new_byte']):02X}"
            desc = known_desc_for_key(known, addr, b)
            suffix = f" | {desc}" if desc else ""
            lines.append(
                f"{fmt_ts(float(row['event_ts']))} {row['msg_type'] or '?'} "
                f"{addr:02X}:{b} bit {row['old_bit']}->{row['new_bit']} byte {old_byte}->{new_byte}{suffix}"
            )
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")





@raw_group.command(name="correlate", description="Correlate one SF bit with canonical CA berth steps.")
@app_commands.describe(
    bit="Byte:bit filter, e.g. 25:3",
    since_minutes="Only analyse bit changes newer than this many minutes",
    match_window_seconds="Seconds either side used to compare an SF edge with canonical CA berth steps",
    limit="Maximum candidate signal/berth rows to show",
)
async def bit_correlate_cmd(
    interaction: discord.Interaction,
    bit: str,
    since_minutes: int = 240,
    match_window_seconds: int = 180,
    limit: int = 20,
) -> None:
    await interaction.response.defer()
    try:
        key = learner_mod.parse_bit_spec(bit)
        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return

        known = known_bits()
        seconds = max(1, int(since_minutes)) * 60
        match_window = max(1.0, float(match_window_seconds))
        row_limit = clamp_limit(limit)

        with db_connect(readonly=True) as conn:
            total_changes, rows = bit_global_correlation_rows(
                conn,
                key,
                seconds=seconds,
                match_window_seconds=match_window,
                limit=row_limit,
            )

        lines = [
            f"Bit correlation scan {key.label} | last {since_minutes}m | match window +/-{match_window:.0f}s",
            f"Known CSV: {known.describe(key)}",
            f"Raw changes in window: {total_changes}",
        ]
        if not rows:
            lines.append("No nearby canonical C-Class berth steps found for this bit in that window.")
        else:
            lines.append(
                "Columns: matched_changes/raw_changes, matched_moves, all recent moves involving candidate, avg_delta, timing, edge, routes"
            )
            mapped = sorted(known.signals_for_key(key))
            if mapped:
                lines.append(f"CSV mapped signal(s): {', '.join(mapped)}")
            for idx, row in enumerate(rows, start=1):
                marker = " CSV" if row["candidate"] in mapped else ""
                routes = "; ".join(row["routes"]) if row["routes"] else "n/a"
                signed = float(row["avg_signed_delta"])
                lines.append(
                    f"{idx:02d}. {row['candidate']} ({row['role']}){marker}: "
                    f"{row['matched_changes']}/{total_changes} changes ({row['match_pct']*100:.0f}%), "
                    f"{row['matched_moves']} matched move(s), {row['movement_count']} recent involved move(s), "
                    f"avg_abs={row['avg_abs_delta']:.1f}s, avg_signed={signed:+.1f}s, "
                    f"{row['timing']}, {row['edge']}; routes: {routes}"
                )
            lines.append(
                "Note: shared bits can correlate with several adjacent berths. This is correlation only, not aspect proof. A specific control should have high matched changes, low timing spread, and few unrelated changes."
            )

        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@raw_group.command(name="trace", description="Trace one SF bit against canonical CA berth steps.")
@app_commands.describe(
    bit="Byte:bit filter, e.g. 25:3",
    signal="Signal/berth to compare against, e.g. 6244. Defaults to known_bits.csv mapping if unique.",
    since_minutes="Only show changes newer than this many minutes",
    match_window_seconds="Seconds either side used to compare the SF edge with that berth's CA step",
    limit="Maximum raw bit changes to show",
)
async def bit_trace_cmd(
    interaction: discord.Interaction,
    bit: str,
    signal: Optional[str] = None,
    since_minutes: int = 180,
    match_window_seconds: int = 180,
    limit: int = 30,
) -> None:
    await interaction.response.defer()
    try:
        key = learner_mod.parse_bit_spec(bit)
        known = known_bits()
        mapped = sorted(known.signals_for_key(key))
        if signal:
            sig = learner_mod.normalize_berth(signal)
        elif len(mapped) == 1:
            sig = mapped[0]
        else:
            await send_text(interaction, f"Please provide signal:. known_bits.csv maps {key.label} to: {', '.join(mapped) if mapped else 'none'}")
            return

        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return

        seconds = max(1, int(since_minutes)) * 60
        match_window = max(1.0, float(match_window_seconds))
        row_limit = clamp_limit(limit)
        with db_connect(readonly=True) as conn:
            if not table_exists(conn, "s_bit_events"):
                await send_text(interaction, "s_bit_events table does not exist yet.")
                return
            rows = conn.execute(
                """
                SELECT event_ts, old_bit, new_bit, old_byte, new_byte, msg_type
                FROM s_bit_events
                WHERE area=? AND address=? AND bit=? AND event_ts >= ?
                  AND UPPER(COALESCE(msg_type,''))='SF'
                ORDER BY event_ts DESC
                LIMIT ?
                """,
                (CFG.nr_area, int(key.address, 16), int(key.bit), time.time() - seconds, row_limit),
            ).fetchall()
            total_changes, matched_count, move_count = bit_signal_correlation_counts(
                conn,
                key,
                sig,
                seconds=seconds,
                match_window_seconds=match_window,
            )

            lines = [
                f"Bit trace {key.label} vs signal {sig} | last {since_minutes}m | match window +/-{match_window:.0f}s",
                f"Known CSV: {known.describe(key)}",
                f"Summary: {total_changes} SF edge(s), {matched_count} near a {sig} CA berth step, {move_count} canonical movement(s) involving {sig}.",
            ]
            warn = correlation_warning_line(conn, key, sig, seconds=seconds, match_window_seconds=match_window)
            if warn:
                lines.append(warn.strip())

            if not rows:
                lines.append("No raw bit changes in this period.")
            for row in rows:
                old_byte = "??" if row["old_byte"] is None else f"{int(row['old_byte']):02X}"
                new_byte = f"{int(row['new_byte']):02X}"
                nearest = nearest_pass_for_signal(conn, sig, float(row["event_ts"]), window_seconds=match_window)
                if nearest:
                    delta = float(nearest["delta_seconds"] or 0.0)
                    context = (
                        f"MATCH {nearest['from_berth']}->{nearest['to_berth']} "
                        f"{nearest['descr'] or '----'} delta={delta:+.0f}s"
                    )
                else:
                    context = "NO MATCH to this berth's CA steps"
                lines.append(
                    f"{fmt_ts(float(row['event_ts']))} {row['msg_type'] or '?'} "
                    f"{key.label} {row['old_bit']}->{row['new_bit']} byte {old_byte}->{new_byte} | {context}"
                )

        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@signal_group.command(name="routes", description="Show route-specific pre-step controls for a signal.")
@app_commands.describe(
    signal="From berth/signal, e.g. 6248",
    to="Optional exact next berth/route, e.g. 6244",
    max_steps="Maximum recent canonical berth steps to analyse",
    limit="Maximum rows to show",
)
async def route_bits_cmd(
    interaction: discord.Interaction,
    signal: str,
    to: Optional[str] = None,
    max_steps: int = 250,
    limit: int = 20,
) -> None:
    await interaction.response.defer()
    try:
        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return
        sig = learner_mod.normalize_berth(signal)
        to_norm = learner_mod.normalize_berth(to) if to else None
        with db_connect(readonly=True) as conn:
            rows = learner_mod.protocol_candidate_analysis(
                conn,
                sig,
                area=CFG.nr_area,
                max_steps=max(10, min(int(max_steps), 1000)),
                to_berth=to_norm,
            )
        heading = f"Route-specific S-Class controls for {sig}" + (f" -> {to_norm}" if to_norm else "")
        lines = [heading]
        lines.append(
            "Only precisely timed SF edges are used. A pre-step cycle can be a route indication or a signal indication; this command does not promote it to a physical aspect."
        )
        controls = [r for r in rows if r["classification"] == "pre_step_control"]
        pulses = [r for r in rows if r["classification"] == "movement_pulse"]
        if controls:
            lines.append("Pre-step control cycles:")
            for row in controls[:max(1, min(int(limit), 50))]:
                lead = "?" if row["median_lead"] is None else f"{row['median_lead']:.1f}s"
                duration = "?" if row["median_cycle"] is None else f"{row['median_cycle']:.1f}s"
                lines.append(
                    f"  {row['key'].label}: {row['direction']}; cycles {row['cycle_hits']}/{row['target_steps']} "
                    f"({row['cycle_rate']*100:.0f}%); median lead {lead}; median full cycle {duration}; "
                    f"unrelated-control rate {row['control_rate']*100:.0f}% (lift {row['lift']*100:+.0f}pp)"
                )
        else:
            lines.append("No sufficiently specific pre-step control cycle found.")
        if pulses:
            lines.append("Rejected track/step/release-shaped pulses:")
            for row in pulses[:5]:
                pulse = "?" if row["median_pulse"] is None else f"{row['median_pulse']:.1f}s"
                lines.append(
                    f"  {row['key'].label}: near step {row['near_hits']}/{row['target_steps']}; median pulse {pulse}"
                )
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@database_group.command(name="stats", description="Show database size, row counts and runtime statistics.")
async def db_stats_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return
        with db_connect(readonly=True) as conn:
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            page_count = conn.execute("PRAGMA page_count").fetchone()[0]
            freelist_count = conn.execute("PRAGMA freelist_count").fetchone()[0]
            rows: list[str] = []
            for table in [
                "feed_state", "raw_td_messages", "refresh_history", "s_bytes",
                "s_bit_events", "s_snapshot_differences", "berth_state", "berth_steps",
                "signal_observation_sessions", "signal_observations",
                "pass_log", "pass_bit_events", "missing_topology_moves",
                "missing_topology_summary",
            ]:
                if table_exists(conn, table):
                    count = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                    rows.append(f"{table}: {int(count):,}")
            if table_exists(conn, "s_bit_events"):
                span = conn.execute("SELECT MIN(event_ts) AS mn, MAX(event_ts) AS mx FROM s_bit_events").fetchone()
            else:
                span = None
        size_bytes = CFG.db_path.stat().st_size
        lines = [
            f"Database: {CFG.db_path}",
            f"File size: {size_bytes:,} bytes ({size_bytes / (1024 * 1024):.2f} MiB)",
            f"SQLite pages: {page_count:,} x {page_size:,} bytes",
            f"Free pages: {freelist_count:,} ({freelist_count * page_size / (1024 * 1024):.2f} MiB reusable)",
            f"Read cache setting: {CFG.db_read_cache_kib} KiB",
            f"mmap setting: {CFG.db_mmap_mib} MiB",
            "",
            "Rows:",
            *rows,
        ]
        if span and span["mn"] and span["mx"]:
            lines.extend(["", f"S-bit event range: {fmt_ts(float(span['mn']))} -> {fmt_ts(float(span['mx']))}"])
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


def optimise_database(*, purge_days: Optional[int], vacuum: bool) -> str:
    if not CFG.db_path.exists():
        return "Database does not exist yet."
    backup_file(CFG.db_path)
    deleted_lines: list[str] = []
    with db_connect(readonly=False) as conn:
        conn.execute("PRAGMA query_only=OFF")
        if purge_days is not None and purge_days > 0:
            cutoff = time.time() - (int(purge_days) * 86400)
            purge_specs = [
                ("raw_td_messages", "event_ts"),
                ("s_bit_events", "event_ts"),
                ("s_snapshot_differences", "observed_ts"),
                ("berth_steps", "event_ts"),
                ("pass_bit_events", "event_ts"),
                ("pass_log", "pass_ts"),
                ("missing_topology_moves", "event_ts"),
            ]
            for table, col in purge_specs:
                if table_exists(conn, table):
                    before = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                    conn.execute(f"DELETE FROM {table} WHERE {col}<?", (cutoff,))
                    after = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                    deleted_lines.append(f"{table}: deleted {int(before) - int(after):,} rows older than {purge_days} days")
        with contextlib.suppress(sqlite3.DatabaseError):
            conn.execute("ANALYZE")
        with contextlib.suppress(sqlite3.DatabaseError):
            conn.execute("PRAGMA optimize")
        if vacuum:
            conn.execute("VACUUM")
    lines = ["Database optimisation complete.", f"Backup created in {CFG.backups_dir}"]
    if deleted_lines:
        lines.extend(deleted_lines)
    if vacuum:
        lines.append("VACUUM completed.")
    else:
        lines.append("VACUUM skipped.")
    return "\n".join(lines)


@database_group.command(name="optimise", description="Optimise, optionally purge, and compact the database.")
@app_commands.describe(
    purge_days="Optional: delete raw/history rows older than this many days before vacuuming",
    vacuum="Compact the DB file after optimisation/purge",
)
async def db_optimise_cmd(interaction: discord.Interaction, purge_days: Optional[int] = None, vacuum: bool = True) -> None:
    await interaction.response.defer()
    was_running = NR_SERVICE.is_alive()
    if was_running:
        await asyncio.to_thread(NR_SERVICE.stop, join=True)
        await asyncio.sleep(1)
    try:
        text = await asyncio.to_thread(optimise_database, purge_days=purge_days, vacuum=vacuum)
    except Exception as exc:
        text = f"Database optimisation failed: {exc}"
    finally:
        if was_running:
            await asyncio.to_thread(NR_SERVICE.start)
    await send_text(interaction, text, paged=True)


def sqlite_backup_to(src: Path, dest: Path) -> None:
    if dest.exists():
        dest.unlink()
    if not src.exists():
        return
    src_conn = sqlite3.connect(str(src), timeout=CFG.db_busy_timeout_ms / 1000)
    dest_conn = sqlite3.connect(str(dest), timeout=CFG.db_busy_timeout_ms / 1000)
    try:
        src_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        src_conn.close()


def make_download_zip() -> Path:
    out = CFG.exports_dir / f"t3_learner_export_{stamp()}.zip"
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_copy = tmp / "td_signal_bit_learner.sqlite"
        if CFG.db_path.exists():
            sqlite_backup_to(CFG.db_path, db_copy)
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            if db_copy.exists():
                z.write(db_copy, "td_signal_bit_learner.sqlite")
            if CFG.known_path.exists():
                z.write(CFG.known_path, "known_bits.csv")
            if CFG.missing_dir.exists():
                for p in CFG.missing_dir.rglob("*"):
                    if p.is_file():
                        z.write(p, f"missing_topology/{p.relative_to(CFG.missing_dir)}")
            z.writestr(
                "EXPORT_README.txt",
                "T3 learner export\n"
                f"Created: {dt.datetime.now().isoformat(timespec='seconds')}\n"
                "Contains SQLite database snapshot, known_bits.csv, and missing topology CSVs.\n",
            )
    return out


@database_group.command(name="export", description="Export the database, mappings and topology evidence as a ZIP.")
async def download_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    out = await asyncio.to_thread(make_download_zip)
    await interaction.followup.send(content=f"Export created: `{out.name}`", file=discord.File(out))


def backup_file(path: Path) -> None:
    if not path.exists():
        return
    CFG.backups_dir.mkdir(parents=True, exist_ok=True)
    destination = CFG.backups_dir / f"{path.name}.{stamp()}.bak"
    if path.suffix.lower() in {".sqlite", ".db"}:
        sqlite_backup_to(path, destination)
    else:
        shutil.copy2(path, destination)


def vacuum_sqlite_into_single_file(src: Path, dest: Path) -> None:
    if dest.exists():
        dest.unlink()
    conn = sqlite3.connect(str(src))
    try:
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        quoted = str(dest).replace("'", "''")
        conn.execute(f"VACUUM INTO '{quoted}'")
    finally:
        conn.close()


def apply_upload(path: Path) -> list[str]:
    changed: list[str] = []
    suffix = path.suffix.lower()

    if suffix == ".csv" and path.name.lower() == "known_bits.csv":
        backup_file(CFG.known_path)
        shutil.copy2(path, CFG.known_path)
        KNOWN_CACHE.invalidate()
        return ["known_bits.csv replaced"]

    if suffix in {".sqlite", ".db"}:
        backup_file(CFG.db_path)
        tmp_db = CFG.uploads_dir / f"uploaded_single_{stamp()}.sqlite"
        vacuum_sqlite_into_single_file(path, tmp_db)
        shutil.move(str(tmp_db), CFG.db_path)
        return ["SQLite database replaced and compacted into one file"]

    if suffix == ".zip":
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            with zipfile.ZipFile(path, "r") as z:
                for member in z.infolist():
                    name = Path(member.filename)
                    if name.is_absolute() or ".." in name.parts:
                        continue
                    z.extract(member, tmp)

            for p in tmp.rglob("*"):
                if p.is_file() and p.name.lower() == "known_bits.csv":
                    backup_file(CFG.known_path)
                    shutil.copy2(p, CFG.known_path)
                    KNOWN_CACHE.invalidate()
                    changed.append("known_bits.csv replaced from zip")

            dbs = [
                p for p in tmp.rglob("*")
                if p.is_file()
                and p.suffix.lower() in {".sqlite", ".db"}
                and not p.name.endswith("-wal")
                and not p.name.endswith("-shm")
            ]
            if dbs:
                backup_file(CFG.db_path)
                tmp_db = CFG.uploads_dir / f"uploaded_single_{stamp()}.sqlite"
                vacuum_sqlite_into_single_file(dbs[0], tmp_db)
                shutil.move(str(tmp_db), CFG.db_path)
                changed.append(f"SQLite database replaced from zip: {dbs[0].name}")

        return changed

    raise RuntimeError("Unsupported upload. Use known_bits.csv, .sqlite/.db, or a .zip containing them.")


@database_group.command(name="import", description="Import mappings, a database, or a supported ZIP backup.")
async def upload_cmd(interaction: discord.Interaction, attachment: discord.Attachment) -> None:
    await interaction.response.defer()
    ensure_dirs()
    was_running = NR_SERVICE.is_alive()
    if was_running:
        await asyncio.to_thread(NR_SERVICE.stop, join=True)
        await asyncio.sleep(1)

    saved = CFG.uploads_dir / f"{stamp()}_{attachment.filename}"
    await attachment.save(saved)

    try:
        changed = await asyncio.to_thread(apply_upload, saved)
        if was_running:
            await asyncio.to_thread(NR_SERVICE.start)
        if not changed:
            changed = ["Upload accepted, but no supported files were found inside it."]
        await send_text(interaction, "\n".join(changed), paged=True)
    except Exception as exc:
        if was_running:
            await asyncio.to_thread(NR_SERVICE.start)
        await send_text(interaction, f"Upload failed: {exc}")


for _group in _COMMAND_GROUPS:
    bot.tree.add_command(_group)


def main() -> None:
    ensure_dirs()
    bot.run(CFG.token)


if __name__ == "__main__":
    main()
