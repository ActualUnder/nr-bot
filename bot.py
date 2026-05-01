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
from typing import Optional

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


def stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "never"
    return dt.datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
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
        nr_port=int(os.getenv("NR_PORT", "61618")),
        nr_topic=os.getenv("NR_TOPIC", "/topic/TD_ALL_SIG_AREA").strip(),
        nr_username=os.getenv("NROD_USER") or os.getenv("NR_USERNAME", ""),
        nr_password=os.getenv("NROD_PASS") or os.getenv("NR_PASSWORD", ""),
    )


CFG = load_config()


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


def cli_args(command: str) -> list[str]:
    return [
        command,
        "--db", str(CFG.db_path),
        "--known", str(CFG.known_path),
        "--missing-dir", str(CFG.missing_dir),
        "--area", CFG.nr_area,
    ]


def run_cli(args: list[str], timeout: int = 60) -> str:
    cmd = [sys.executable, str(CFG.app_dir / "t3_learner_clean.py"), *args]
    proc = subprocess.run(cmd, cwd=str(CFG.app_dir), text=True, capture_output=True, timeout=timeout)
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n\nSTDOUT:\n{out}\n\nSTDERR:\n{err}")
    return out or err or "No output."


async def send_text(interaction: discord.Interaction, text: str) -> None:
    await interaction.followup.send(f"```text\n{trim(text)}\n```")


def known_bits() -> learner_mod.KnownBits:
    return learner_mod.load_known_bits(CFG.known_path)


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(CFG.db_path), timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


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
        self.lock = threading.RLock()


STATUS = Status()


class DiscordFeedListener:
    def __init__(self, learner: learner_mod.Learner):
        self.learner = learner

    def on_message(self, frame):
        with STATUS.lock:
            STATUS.nr_messages += 1
            STATUS.nr_last_message_ts = time.time()
        try:
            for key, payload in learner_mod.iter_message_objects(frame.body):
                self.learner.handle_message(key, payload)
        except Exception:
            with STATUS.lock:
                STATUS.nr_last_error = traceback.format_exc()

    def on_error(self, frame):
        with STATUS.lock:
            STATUS.nr_last_error = f"{getattr(frame, 'headers', {})} {getattr(frame, 'body', '')}"

    def on_disconnected(self):
        with STATUS.lock:
            STATUS.nr_connected = False
            STATUS.nr_last_disconnect_ts = time.time()


class NRFeedService:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.conn = None

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start(self) -> str:
        if self.is_alive():
            return "NR feed is already running."
        if not CFG.nr_username or not CFG.nr_password:
            return "NR credentials are missing. Set NROD_USER/NROD_PASS or NR_USERNAME/NR_PASSWORD."
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name="nr-feed", daemon=True)
        self.thread.start()
        return "NR feed starting."

    def stop(self) -> str:
        self.stop_event.set()
        with contextlib.suppress(Exception):
            if self.conn is not None and self.conn.is_connected():
                self.conn.disconnect()
        with STATUS.lock:
            STATUS.nr_running = False
            STATUS.nr_connected = False
            STATUS.nr_last_disconnect_ts = time.time()
        return "NR feed stopping."

    def _make_learner(self) -> learner_mod.Learner:
        topology = learner_mod.build_topology(None)
        known = learner_mod.load_known_bits(CFG.known_path)
        store = learner_mod.Store(CFG.db_path, CFG.missing_dir)
        return learner_mod.Learner(
            area=CFG.nr_area,
            store=store,
            topology=topology,
            known=known,
            watch_signals=set(topology.keys()),
            pre=30.0,
            post=30.0,
            recent_keep=300.0,
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
            return

        delay = 10.0
        with STATUS.lock:
            STATUS.nr_running = True

        while not self.stop_event.is_set():
            store_to_close = None
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

                with STATUS.lock:
                    STATUS.nr_connected = True
                    STATUS.nr_last_connect_ts = time.time()
                    STATUS.nr_last_error = ""

                while conn.is_connected() and not self.stop_event.is_set():
                    live_learner.tick()
                    time.sleep(0.5)

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
                with STATUS.lock:
                    STATUS.nr_connected = False
                    STATUS.nr_last_disconnect_ts = time.time()

            if self.stop_event.is_set():
                break

            end = time.time() + delay
            while time.time() < end and not self.stop_event.is_set():
                time.sleep(0.5)
            delay = min(300.0, delay * 2)

        with STATUS.lock:
            STATUS.nr_running = False
            STATUS.nr_connected = False


NR_SERVICE = NRFeedService()

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    ensure_dirs()
    if CFG.guild_id:
        guild = discord.Object(id=CFG.guild_id)
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
    else:
        await bot.tree.sync()

    if CFG.nr_enabled:
        NR_SERVICE.start()

    print(f"Logged in as {bot.user} | slash commands synced")


@bot.tree.command(name="status", description="Show Discord bot, data files and NR feed status.")
async def status_cmd(interaction: discord.Interaction):
    ensure_dirs()
    with STATUS.lock:
        nr_running = STATUS.nr_running
        nr_connected = STATUS.nr_connected
        nr_messages = STATUS.nr_messages
        nr_last_message_ts = STATUS.nr_last_message_ts
        nr_last_connect_ts = STATUS.nr_last_connect_ts
        nr_last_error = STATUS.nr_last_error

    try:
        known_count = len(known_bits().rows)
    except Exception:
        known_count = 0

    db_size = CFG.db_path.stat().st_size if CFG.db_path.exists() else 0
    embed = discord.Embed(title="T3 Learner Bot Status", color=0x2ECC71 if nr_connected else 0xE67E22)
    embed.add_field(name="Discord", value=f"Online as `{bot.user}`\nLatency `{bot.latency * 1000:.0f} ms`", inline=False)
    embed.add_field(
        name="NR Feed",
        value=(
            f"Enabled: `{CFG.nr_enabled}`\n"
            f"Running: `{nr_running}`\n"
            f"Connected: `{nr_connected}`\n"
            f"Messages: `{nr_messages}`\n"
            f"Last message: `{fmt_ts(nr_last_message_ts)}`\n"
            f"Last connect: `{fmt_ts(nr_last_connect_ts)}`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Files",
        value=(
            f"DB: `{CFG.db_path}`\n"
            f"DB size: `{db_size:,}` bytes\n"
            f"Known CSV rows: `{known_count}`\n"
            f"Known CSV: `{CFG.known_path}`"
        ),
        inline=False,
    )
    if nr_last_error:
        embed.add_field(name="Last NR error", value=f"```text\n{trim(nr_last_error, 900)}\n```", inline=False)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="nr_start", description="Start the live Network Rail TD feed learner.")
async def nr_start_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    await send_text(interaction, NR_SERVICE.start())


@bot.tree.command(name="nr_stop", description="Stop the live Network Rail TD feed learner.")
async def nr_stop_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    await send_text(interaction, NR_SERVICE.stop())


@bot.tree.command(name="nr_restart", description="Restart the live Network Rail TD feed learner.")
async def nr_restart_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    stopped = NR_SERVICE.stop()
    await asyncio.sleep(2)
    started = NR_SERVICE.start()
    await send_text(interaction, stopped + "\n" + started)


@bot.tree.command(name="report", description="Show learner evidence report for a signal.")
@app_commands.describe(signal="Signal/berth, e.g. 6232", show_known="Audit known CSV bits too")
async def report_cmd(interaction: discord.Interaction, signal: str, show_known: bool = True):
    await interaction.response.defer()
    args = cli_args("report") + ["--signals", signal]
    if show_known:
        args.append("--show-known")
    try:
        text = await asyncio.to_thread(run_cli, args, 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="progress", description="Show compact learning progress summary.")
async def progress_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        text = await asyncio.to_thread(run_cli, cli_args("progress"), 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="known", description="Show known_bits.csv mappings for a signal.")
async def known_cmd(interaction: discord.Interaction, signal: str):
    await interaction.response.defer()
    try:
        text = await asyncio.to_thread(run_cli, cli_args("known") + ["--signal", signal], 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="moves", description="Show learned movements involving a signal/berth.")
async def moves_cmd(interaction: discord.Interaction, signal: str):
    await interaction.response.defer()
    try:
        text = await asyncio.to_thread(run_cli, cli_args("moves") + ["--berth", signal, "--limit", "20"], 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="bytes", description="Show S-Class byte addresses seen by the learner.")
async def bytes_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        text = await asyncio.to_thread(run_cli, cli_args("bytes"), 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="missing", description="Show missing topology observations.")
async def missing_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        text = await asyncio.to_thread(run_cli, cli_args("missing"), 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="check", description="Check topology, known CSV and database load.")
async def check_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        text = await asyncio.to_thread(run_cli, cli_args("check"), 60)
    except Exception as exc:
        text = f"Error: {exc}"
    await send_text(interaction, text)


@bot.tree.command(name="bit", description="Show current/latest state for a byte:bit, e.g. 25:3.")
async def bit_cmd(interaction: discord.Interaction, bit: str):
    await interaction.response.defer()
    try:
        key = learner_mod.parse_bit_spec(bit)
        known = known_bits()
        desc = known.describe(key)
        current_value = None
        current_ts = None
        latest_change = None

        if CFG.db_path.exists():
            with db_connect() as conn:
                row = conn.execute(
                    "SELECT value, updated_ts FROM s_bytes WHERE area=? AND address=?",
                    (CFG.nr_area, int(key.address, 16)),
                ).fetchone()
                if row:
                    current_value = 1 if int(row["value"]) & (1 << key.bit) else 0
                    current_ts = float(row["updated_ts"])
                try:
                    latest_change = conn.execute(
                        """
                        SELECT event_ts, old_bit, new_bit, msg_type
                        FROM s_bit_events
                        WHERE address=? AND bit=?
                        ORDER BY event_ts DESC
                        LIMIT 1
                        """,
                        (int(key.address, 16), int(key.bit)),
                    ).fetchone()
                except sqlite3.OperationalError:
                    latest_change = None

        lines = []
        if current_value is None:
            lines.append(f"{key.label} has no current byte snapshot yet.")
        else:
            human = "ON / red" if current_value == 1 else "OFF / proceed"
            lines.append(f"{key.label} = {current_value} at {fmt_ts(current_ts)} ({human})")

        if latest_change:
            new = int(latest_change["new_bit"])
            human = "ON / red" if new == 1 else "OFF / proceed"
            lines.append(
                f"Latest change: {latest_change['old_bit']}->{latest_change['new_bit']} "
                f"at {fmt_ts(float(latest_change['event_ts']))} via {latest_change['msg_type']} ({human})"
            )

        lines.append(f"Known CSV: {desc}")
        await send_text(interaction, "\n".join(lines))
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


@bot.tree.command(name="signal", description="Show signal state: berth occupancy and known signal bit.")
async def signal_cmd(interaction: discord.Interaction, signal: str):
    await interaction.response.defer()
    try:
        sig = learner_mod.normalize_berth(signal)
        known = known_bits()
        keys = sorted(known.keys_for_signal(sig), key=lambda k: (int(k.address, 16), k.bit))
        lines = [f"Signal {sig}"]

        if not CFG.db_path.exists():
            lines.append("Database does not exist yet.")
            await send_text(interaction, "\n".join(lines))
            return

        with db_connect() as conn:
            try:
                berth = conn.execute("SELECT * FROM berth_state WHERE berth=?", (sig,)).fetchone()
            except sqlite3.OperationalError:
                berth = None

            if berth and int(berth["occupied"]):
                lines.append(f"Train/headcode in berth: {berth['descr'] or 'unknown'} at {fmt_ts(float(berth['updated_ts']))}")
            elif berth:
                lines.append(f"Berth currently clear, last update {fmt_ts(float(berth['updated_ts']))}")
            else:
                lines.append("Berth occupancy: no stored state yet")

            if not keys:
                lines.append("Known signal bit: none in known_bits.csv")
            else:
                for key in keys:
                    row = conn.execute(
                        "SELECT value, updated_ts, msg_type FROM s_bytes WHERE area=? AND address=?",
                        (CFG.nr_area, int(key.address, 16)),
                    ).fetchone()
                    if not row:
                        lines.append(f"{key.label}: no current S-Class byte snapshot")
                        continue
                    val = 1 if int(row["value"]) & (1 << key.bit) else 0
                    human = "ON / red" if val == 1 else "OFF / proceed"
                    lines.append(f"{key.label}: {val} at {fmt_ts(float(row['updated_ts']))} ({human}, src {row['msg_type']})")

        await send_text(interaction, "\n".join(lines))
    except Exception as exc:
        await send_text(interaction, f"Error: {exc}")


def make_download_zip() -> Path:
    out = CFG.exports_dir / f"t3_learner_export_{stamp()}.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        if CFG.db_path.exists():
            z.write(CFG.db_path, "td_signal_bit_learner.sqlite")
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
            "Contains SQLite database, known_bits.csv, and missing topology CSVs.\n",
        )
    return out


@bot.tree.command(name="download", description="Download known_bits.csv, SQLite DB and missing topology files as one zip.")
async def download_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    out = await asyncio.to_thread(make_download_zip)
    await interaction.followup.send(content=f"Export created: `{out.name}`", file=discord.File(out))


def backup_file(path: Path) -> None:
    if path.exists():
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
async def upload_cmd(interaction: discord.Interaction, attachment: discord.Attachment):
    await interaction.response.defer()
    was_running = NR_SERVICE.is_alive()
    if was_running:
        NR_SERVICE.stop()
        await asyncio.sleep(2)

    saved = CFG.uploads_dir / f"{stamp()}_{attachment.filename}"
    await attachment.save(saved)

    try:
        changed = await asyncio.to_thread(apply_upload, saved)
        if was_running:
            NR_SERVICE.start()
        if not changed:
            changed = ["Upload accepted, but no supported files were found inside it."]
        await send_text(interaction, "\n".join(changed))
    except Exception as exc:
        if was_running:
            NR_SERVICE.start()
        await send_text(interaction, f"Upload failed: {exc}")


def main() -> None:
    ensure_dirs()
    bot.run(CFG.token)


if __name__ == "__main__":
    main()
