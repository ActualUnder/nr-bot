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

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

import t3_learner_clean as learner_mod

load_dotenv("/etc/metro-bot.env")
load_dotenv()


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
    return dt.datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


def short_ts(ts: Optional[float]) -> str:
    if not ts:
        return "never"
    return dt.datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S")


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


def ensure_dirs() -> None:
    for path in [CFG.learner_dir, CFG.missing_dir, CFG.exports_dir, CFG.uploads_dir, CFG.backups_dir, CFG.logs_dir]:
        path.mkdir(parents=True, exist_ok=True)

    repo_known = CFG.app_dir / "known_bits.csv"
    if not CFG.known_path.exists() and repo_known.exists():
        shutil.copy2(repo_known, CFG.known_path)

    if not CFG.known_path.exists():
        CFG.known_path.write_text(
            "Address:Bit,Element type,Description,Signal,Route From,Route To,Active State,Ignore In Tracker,Confidence,Notes\n",
            encoding="utf-8",
        )


def trim(text: str, limit: int = 1900) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 90] + "\n\n...output trimmed. Use /download for full files."


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
    proceed_words = {"proceed", "clear", "cleared", "off", "route", "set"}

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
        return "proceed/route set" if raw_value == active_value else "not proved proceed"
    return None


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


def describe_candidate_current(raw_value: int | None, best: dict[str, Any], *, signal_view: bool = True) -> str:
    if raw_value is None:
        return "current unknown"
    if not best.get("confidence_ok"):
        return f"current raw {raw_value}; low evidence, not deriving state"
    polarity = str(best.get("polarity", ""))
    if polarity == "danger_active_high":
        return "current likely RED/DANGER" if raw_value == 1 else "current likely not red/cleared"
    if polarity == "proceed_active_high":
        if raw_value == 1:
            return "current likely PROCEED / route set"
        # In a signal-specific view this is the useful operational conclusion:
        # the learned proceed/proof bit is not active.  It still does not tell
        # us the exact red/yellow/double-yellow/green aspect.
        return "current likely RED/DANGER or no route set (proceed bit not active)" if signal_view else "current proceed bit not active"
    return f"current raw {raw_value}"


def recent_bit_change_count(conn: sqlite3.Connection, key: learner_mod.BitKey, *, seconds: float) -> int:
    if not table_exists(conn, "s_bit_events"):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM s_bit_events
        WHERE area=? AND address=? AND bit=? AND event_ts >= ?
        """,
        (CFG.nr_area, int(key.address, 16), int(key.bit), time.time() - float(seconds)),
    ).fetchone()
    return int(row["c"] if row else 0)


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
        return (
            f"{raw_prefix} (uninterpreted; own evidence only "
            f"{confidence_text(best, pass_count)}, below threshold "
            f"{DERIVE_MIN_SUPPORT} hits/{DERIVE_MIN_PCT*100:.0f}%/{DERIVE_MAX_AVG_DELTA:.1f}s){suffix}"
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
    def __init__(self, learner: learner_mod.Learner):
        self.learner = learner

    def on_message(self, frame: Any) -> None:
        with STATUS.lock:
            STATUS.nr_messages += 1
            STATUS.nr_last_message_ts = time.time()
        try:
            for key, payload in learner_mod.iter_message_objects(frame.body):
                self.learner.handle_message(key, payload)
        except Exception:
            with STATUS.lock:
                STATUS.nr_last_error = traceback.format_exc()

    def on_error(self, frame: Any) -> None:
        with STATUS.lock:
            STATUS.nr_last_error = f"{getattr(frame, 'headers', {})} {getattr(frame, 'body', '')}"

    def on_disconnected(self) -> None:
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
            connected_at: float | None = None
            try:
                live_learner = self._make_learner()
                store_to_close = live_learner.store
                listener = DiscordFeedListener(live_learner)
                conn = learner_mod.stomp.Connection12(
                    host_and_ports=[(CFG.nr_host, CFG.nr_port)],
                    keepalive=True,
                    heartbeats=(10000, 10000),
                )
                self.conn = conn
                conn.set_listener("discord-t3-learner", listener)
                conn.connect(username=CFG.nr_username, passcode=CFG.nr_password, wait=True)
                conn.subscribe(destination=CFG.nr_topic, id="discord-t3-learner", ack="auto")

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
    embed = discord.Embed(title="T3 Learner Bot Status", color=0x2ECC71 if nr_connected else 0xE67E22)
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
    embed.add_field(
        name="NR Feed",
        value=(
            f"Enabled: `{CFG.nr_enabled}`\n"
            f"Running: `{nr_running}`\n"
            f"Connected: `{nr_connected}`\n"
            f"Messages: `{nr_messages}`\n"
            f"Last message: `{fmt_ts(nr_last_message_ts)}`\n"
            f"Last connect: `{fmt_ts(nr_last_connect_ts)}`\n"
            f"Last connected duration: `{nr_last_duration:.0f}s`" if nr_last_duration else "Last connected duration: `n/a`"
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


@bot.tree.command(name="nr_start", description="Start the live Network Rail TD feed learner.")
async def nr_start_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    await send_text(interaction, await asyncio.to_thread(NR_SERVICE.start))


@bot.tree.command(name="nr_stop", description="Stop the live Network Rail TD feed learner.")
async def nr_stop_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    await send_text(interaction, await asyncio.to_thread(NR_SERVICE.stop, join=True))


@bot.tree.command(name="nr_restart", description="Restart the live Network Rail TD feed learner.")
async def nr_restart_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    stopped = await asyncio.to_thread(NR_SERVICE.stop, join=True)
    await asyncio.sleep(1)
    KNOWN_CACHE.invalidate()
    started = await asyncio.to_thread(NR_SERVICE.start)
    await send_text(interaction, stopped + "\n" + started)


@bot.tree.command(name="report", description="Show learner evidence report for a signal.")
@app_commands.describe(
    signal="Signal/berth, e.g. 6232",
    show_known="Audit this signal's known CSV bits",
    show_cross_known="Also show known bits belonging to other signals in the pass window",
    min_pass_count="Minimum supporting pass hits to show a candidate",
    min_pct="Minimum consistency percent, e.g. 80",
    max_avg_delta="Maximum average timing delta in seconds; 0 disables this filter",
)
async def report_cmd(
    interaction: discord.Interaction,
    signal: str,
    show_known: bool = True,
    show_cross_known: bool = False,
    min_pass_count: int = 3,
    min_pct: float = 80.0,
    max_avg_delta: float = 3.0,
) -> None:
    await interaction.response.defer()
    args = cli_args("report") + [
        "--signals", signal,
        "--min-pass-count", str(max(1, int(min_pass_count))),
        "--min-pct", str(float(min_pct)),
    ]
    if float(max_avg_delta) > 0:
        args += ["--max-avg-delta", str(float(max_avg_delta))]
    if show_known:
        args.append("--show-known")
    if show_cross_known:
        args.append("--show-cross-known")
    try:
        text = await run_cli_async(args)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="progress", description="Show compact learning progress summary.")
async def progress_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("progress"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="known", description="Show known_bits.csv mappings for a signal.")
async def known_cmd(interaction: discord.Interaction, signal: str) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("known") + ["--signal", signal])
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="moves", description="Show learned movements involving a signal/berth.")
@app_commands.describe(limit="Maximum rows to show")
async def moves_cmd(interaction: discord.Interaction, signal: str, limit: int = 20) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("moves") + ["--berth", signal, "--limit", str(clamp_limit(limit))])
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="berths", description="Show stored TD berth/headcode states.")
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


@bot.tree.command(name="bytes", description="Show S-Class byte addresses seen by the learner.")
async def bytes_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("bytes"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="missing", description="Show missing topology observations.")
async def missing_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("missing"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="check", description="Check topology, known CSV and database load.")
async def check_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    try:
        text = await run_cli_async(cli_args("check"))
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text, paged=True)


@bot.tree.command(name="bit", description="Show current/latest state for a byte:bit, e.g. 25:3.")
async def bit_cmd(interaction: discord.Interaction, bit: str) -> None:
    await interaction.response.defer()
    try:
        key = learner_mod.parse_bit_spec(bit)
        known = known_bits()
        desc = known.describe(key)
        current_value = None
        current_ts = None
        current_msg = None
        latest_change = None

        if CFG.db_path.exists():
            with db_connect(readonly=True) as conn:
                current_value, current_ts, current_msg = current_bit_value(conn, key)
                if table_exists(conn, "s_bit_events"):
                    latest_change = conn.execute(
                        """
                        SELECT event_ts, old_bit, new_bit, msg_type, old_byte, new_byte
                        FROM s_bit_events
                        WHERE area=? AND address=? AND bit=?
                        ORDER BY event_ts DESC
                        LIMIT 1
                        """,
                        (CFG.nr_area, int(key.address, 16), int(key.bit)),
                    ).fetchone()

        lines = []
        if current_value is None:
            lines.append(f"{key.label} has no current byte snapshot yet.")
        else:
            state_text = describe_raw_bit_state(known, key, current_value)
            lines.append(f"{key.label} = {state_text} at {fmt_ts(current_ts)} (src {current_msg or '?'})")

        if latest_change:
            new = int(latest_change["new_bit"])
            old_byte = "??" if latest_change["old_byte"] is None else f"{int(latest_change['old_byte']):02X}"
            new_byte = f"{int(latest_change['new_byte']):02X}"
            lines.append(
                f"Latest raw change: {latest_change['old_bit']}->{latest_change['new_bit']} "
                f"byte {old_byte}->{new_byte} at {fmt_ts(float(latest_change['event_ts']))} "
                f"via {latest_change['msg_type']}"
            )

        mapped_signals = sorted(known.signals_for_key(key))
        if mapped_signals:
            lines.append(f"Known signal(s): {', '.join(mapped_signals)}")
        lines.append(f"Known CSV: {desc}")
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@bot.tree.command(name="signal", description="Show signal state: berth occupancy, routes and known signal bits.")
async def signal_cmd(interaction: discord.Interaction, signal: str) -> None:
    await interaction.response.defer()
    try:
        sig = learner_mod.normalize_berth(signal)
        known = known_bits()
        keys = sorted(known.keys_for_signal(sig), key=lambda k: (int(k.address, 16), k.bit))
        lines = [f"Signal {sig}"]

        route_nexts = sorted(topology().get(sig, set()), key=lambda x: (not x.isdigit(), x))
        lines.append("Routes/next berths: " + (", ".join(route_nexts) if route_nexts else "none configured"))

        if not CFG.db_path.exists():
            lines.append("Database does not exist yet.")
            await send_text(interaction, "\n".join(lines), paged=True)
            return

        with db_connect(readonly=True) as conn:
            if table_exists(conn, "berth_state"):
                berth = conn.execute("SELECT * FROM berth_state WHERE berth=?", (sig,)).fetchone()
            else:
                berth = None

            if berth and int(berth["occupied"]):
                lines.append(f"Train/headcode in berth: {berth['descr'] or 'unknown'} at {fmt_ts(float(berth['updated_ts']))} via {berth['source_msg_type'] or '?'}")
            elif berth:
                lines.append(f"Berth currently clear, last update {fmt_ts(float(berth['updated_ts']))} via {berth['source_msg_type'] or '?'}")
            else:
                lines.append("Berth occupancy: no stored state yet")

            if route_nexts and table_exists(conn, "berth_state"):
                occupied_nexts = []
                for next_berth in route_nexts:
                    r = conn.execute("SELECT * FROM berth_state WHERE berth=?", (next_berth,)).fetchone()
                    if r and int(r["occupied"]):
                        occupied_nexts.append(f"{next_berth}:{r['descr'] or 'unknown'}")
                if occupied_nexts:
                    lines.append("Train/headcode in next berth(s): " + ", ".join(occupied_nexts))

            pass_count = pass_count_for_signal(conn, sig)
            candidates = learned_candidate_rows(conn, sig, score_window=12.0, limit=25)
            known_key_set = set(keys)
            candidate_by_key: dict[learner_mod.BitKey, tuple[sqlite3.Row, dict[str, Any]]] = {}
            for row in candidates:
                ckey = learner_mod.BitKey(f"{int(row['address']):02X}", int(row["bit"]))
                candidate_by_key[ckey] = (row, candidate_best_for_display(row, pass_count))

            if not keys:
                lines.append("CSV mapped signal bit: none in known_bits.csv")
            else:
                lines.append("CSV mapped bit state:")
                for key in keys:
                    val, updated_ts, msg_type = current_bit_value(conn, key)
                    best = candidate_by_key.get(key, (None, None))[1]
                    lines.append(signal_bit_interpretation_line(conn, key, val, updated_ts, msg_type, best, pass_count))
                    changes = recent_bit_change_count(conn, key, seconds=DERIVE_FLICKER_WINDOW_SECONDS)
                    if changes >= DERIVE_FLICKER_WARN_CHANGES and not (best and best.get("confidence_ok")):
                        lines.append(
                            f"    Warning: {key.label} changed {changes} times in the last "
                            f"{DERIVE_FLICKER_WINDOW_SECONDS/60:.0f}m; this is probably not a steady red-lamp state bit."
                        )

            if pass_count and candidates:
                confident = []
                low = []
                for row in candidates:
                    key = learner_mod.BitKey(f"{int(row['address']):02X}", int(row["bit"]))
                    best = candidate_best_for_display(row, pass_count)
                    if best["best_count"] <= 0:
                        continue
                    (confident if best["confidence_ok"] else low).append((row, key, best))

                lines.append(f"Learned candidates from pass evidence ({pass_count} finalised passes):")
                if confident:
                    for row, key, best in confident[:5]:
                        val, updated_ts, msg_type = current_bit_value(conn, key)
                        current_text = describe_candidate_current(val, best)
                        marker = "CSV" if key in known_key_set else "learned"
                        lines.append(
                            f"  {key.label}: {marker}; {best['bucket']} "
                            f"{confidence_text(best, pass_count)}; "
                            f"{best['guess']}; {best['polarity_text']}; {current_text}"
                        )
                else:
                    lines.append(
                        f"  No high-confidence live-state candidates yet "
                        f"(need >= {DERIVE_MIN_SUPPORT} hits, >= {DERIVE_MIN_PCT*100:.0f}%, avg_delta <= {DERIVE_MAX_AVG_DELTA:.1f}s)."
                    )
                    for row, key, best in low[:3]:
                        marker = "CSV" if key in known_key_set else "learned"
                        lines.append(
                            f"  low-evidence {key.label}: {marker}; {best['bucket']} "
                            f"{confidence_text(best, pass_count)}; not used for live state"
                        )

                if keys and confident and all(item[1] not in known_key_set for item in confident[:3]):
                    lines.append("Warning: the CSV mapped bit is not one of the top high-confidence learned candidates for this signal. Treat the CSV aspect as suspect until checked on the panel.")
            elif not pass_count:
                lines.append("Learned candidates: no finalised CA pass evidence yet.")

        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@bot.tree.command(name="recent_bits", description="Show most recent raw S-Class bit changes.")
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
        where = ["area=?"]
        title_parts = ["Recent S-Class bit changes"]

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


@bot.tree.command(name="route_bits", description="Find likely route bits from pass evidence.")
@app_commands.describe(
    signal="Signal/from berth to analyse, e.g. 6248",
    to="Optional route/to berth, e.g. 6244",
    phase="before, after, or both",
    min_hits="Minimum matching bit hits",
    limit="Maximum candidates to show",
)
async def route_bits_cmd(
    interaction: discord.Interaction,
    signal: str,
    to: Optional[str] = None,
    phase: str = "before",
    min_hits: int = 2,
    limit: int = 20,
) -> None:
    await interaction.response.defer()
    try:
        if not CFG.db_path.exists():
            await send_text(interaction, "Database does not exist yet.")
            return
        sig = learner_mod.normalize_berth(signal)
        to_norm = learner_mod.normalize_berth(to) if to else None
        phase_norm = (phase or "before").strip().lower()
        if phase_norm not in {"before", "after", "both"}:
            phase_norm = "before"
        min_hits = max(1, int(min_hits))
        row_limit = clamp_limit(limit)
        known = known_bits()

        with db_connect(readonly=True) as conn:
            if not table_exists(conn, "pass_bit_events") or not table_exists(conn, "pass_log"):
                await send_text(interaction, "pass_log/pass_bit_events tables do not exist yet.")
                return

            pass_where = ["signal=?"]
            pass_params: list[Any] = [sig]
            if to_norm:
                pass_where.append("to_berth=?")
                pass_params.append(to_norm)
            route_pass_count = conn.execute(
                f"SELECT COUNT(*) AS c FROM pass_log WHERE {' AND '.join(pass_where)}",
                pass_params,
            ).fetchone()["c"]

            where = ["e.signal=?"]
            params: list[Any] = [sig]
            if to_norm:
                where.append("p.to_berth=?")
                params.append(to_norm)
            if phase_norm != "both":
                where.append("e.phase=?")
                params.append(phase_norm)
            params.extend([min_hits, row_limit])

            rows = conn.execute(
                f"""
                SELECT
                    p.to_berth AS to_berth,
                    e.address AS address,
                    e.bit AS bit,
                    e.new_bit AS new_bit,
                    e.phase AS phase,
                    COUNT(*) AS hits,
                    COUNT(DISTINCT e.pass_id) AS pass_hits,
                    AVG(e.delta_seconds) AS avg_delta,
                    MIN(e.delta_seconds) AS min_delta,
                    MAX(e.delta_seconds) AS max_delta
                FROM pass_bit_events e
                JOIN pass_log p ON p.id=e.pass_id
                WHERE {' AND '.join(where)}
                GROUP BY p.to_berth, e.address, e.bit, e.new_bit, e.phase
                HAVING hits>=?
                ORDER BY pass_hits DESC, hits DESC, ABS(avg_delta) ASC
                LIMIT ?
                """,
                params,
            ).fetchall()

            totals_by_route = {
                str(r["to_berth"]): int(r["c"])
                for r in conn.execute(
                    "SELECT to_berth, COUNT(*) AS c FROM pass_log WHERE signal=? GROUP BY to_berth",
                    (sig,),
                ).fetchall()
            }

        lines = [f"Route bit candidates for signal {sig}" + (f" -> {to_norm}" if to_norm else "")]
        lines.append(f"Phase: {phase_norm} | pass windows: {route_pass_count if to_norm else sum(totals_by_route.values())}")
        if not rows:
            lines.append("No matching route-bit candidates found. Let the learner collect more pass windows or lower min_hits.")
        for row in rows:
            route = str(row["to_berth"] or "?")
            route_total = totals_by_route.get(route, 0)
            pass_hits = int(row["pass_hits"] or 0)
            confidence = (pass_hits / route_total) if route_total else 0.0
            key = learner_mod.BitKey(f"{int(row['address']):02X}", int(row["bit"]))
            desc = known.describe(key)
            desc_text = "" if desc == "UNKNOWN" else f" | {desc}"
            lines.append(
                f"{route}: {key.label}={int(row['new_bit'])} [{row['phase']}] "
                f"hits={int(row['hits'])} passes={pass_hits}/{route_total} conf={confidence:.2f} "
                f"avg_delta={float(row['avg_delta']):+.1f}s range={float(row['min_delta']):+.1f}..{float(row['max_delta']):+.1f}s{desc_text}"
            )
        await send_text(interaction, "\n".join(lines), paged=True)
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@bot.tree.command(name="db_stats", description="Show SQLite database size, row counts and useful runtime stats.")
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
                "s_bytes", "s_bit_events", "berth_state", "pass_log", "pass_bit_events",
                "missing_topology_moves", "missing_topology_summary",
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
                ("s_bit_events", "event_ts"),
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


@bot.tree.command(name="db_optimise", description="Run SQLite ANALYZE/optimize, optional purge, and optional VACUUM.")
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


@bot.tree.command(name="download", description="Download known_bits.csv, SQLite DB and missing topology files as one zip.")
async def download_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    out = await asyncio.to_thread(make_download_zip)
    await interaction.followup.send(content=f"Export created: `{out.name}`", file=discord.File(out))


def backup_file(path: Path) -> None:
    if path.exists():
        CFG.backups_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, CFG.backups_dir / f"{path.name}.{stamp()}.bak")


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


@bot.tree.command(name="upload", description="Upload known_bits.csv, SQLite DB, or a zip containing both.")
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


def main() -> None:
    ensure_dirs()
    bot.run(CFG.token)


if __name__ == "__main__":
    main()
