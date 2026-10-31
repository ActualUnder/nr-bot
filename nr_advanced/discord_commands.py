from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import time
from collections import defaultdict
from typing import Optional

import discord
from discord import app_commands

from .protocol import UK


def when(at):
    return dt.datetime.fromtimestamp(at, UK).strftime("%d %b %H:%M:%S %Z") if at else "not received"


def restriction_line(r):
    return (f"{r.get('TSRReference') or r['id']}: {r['FromLocation']} → {r['ToLocation']} | "
            f"passenger {r.get('PassengerSpeed') or '?'} / freight {r.get('FreightSpeed') or '?'} mph | "
            f"{r.get('LineName') or '?'} ({r.get('Direction') or '?'}) | {r.get('Reason') or 'reason not supplied'}")


async def reply(interaction, text):
    # Keep feed-supplied text literal and prevent mentions/code-fence injection.
    text = text.replace("```", "'''")
    chunks = []
    while len(text) > 1850:
        at = text.rfind("\n", 0, 1850)
        at = at if at > 0 else 1850
        chunks.append(text[:at])
        text = text[at:].lstrip("\n")
    chunks.append(text)
    for chunk in chunks:
        await interaction.followup.send("```text\n" + chunk + "\n```", allowed_mentions=discord.AllowedMentions.none())


def register(group, runtime):
    async def ready(interaction):
        await interaction.response.defer()
        if runtime is None:
            await reply(interaction, "Network monitor disabled. Set NR_ADVANCED_ENABLED=true and restart.")
            return False
        return True

    @group.command(name="status", description="Show wider TD coverage and the health of each data feed.")
    async def status(interaction: discord.Interaction):
        if await ready(interaction):
            await reply(interaction, await asyncio.to_thread(runtime.summary))

    @group.command(name="map", description="Open the live Tyneside berth map.")
    async def map_command(interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if runtime is None or not runtime.config.map_enabled:
            await reply(interaction, "Map disabled. Set NR_ADVANCED_ENABLED=true and NR_MAP_ENABLED=true.")
            return
        if not runtime.web or not runtime.web.runner:
            await reply(interaction, "The map server has not started. Check /network status for the configuration error.")
            return
        url = runtime.config.map_public_url
        if not url:
            host = runtime.config.map_host
            if host in {"0.0.0.0", "::"}:
                await reply(interaction, f"Map is on port {runtime.config.map_port}. Set NR_MAP_PUBLIC_URL to your LXC's private URL for a clickable link.")
                return
            if host in {"127.0.0.1", "localhost", "::1"}:
                await reply(interaction, f"Map: http://127.0.0.1:{runtime.config.map_port}\nThis is on the bot host. For phone/LAN access set NR_MAP_BIND, NR_MAP_TOKEN and NR_MAP_PUBLIC_URL; see ADVANCED_SETUP.md.")
                return
            url = f"http://{'[' + host + ']' if ':' in host else host}:{runtime.config.map_port}"
        await interaction.followup.send(f"Live berth map: <{url}>\nUse your map access key when prompted.", ephemeral=True)

    @group.command(name="berths", description="List area-qualified berth occupations across Tyneside.")
    async def berths(interaction: discord.Interaction, area: Optional[str] = None, headcode: Optional[str] = None):
        if not await ready(interaction):
            return
        data = await asyncio.to_thread(runtime.snapshot)
        nodes = [n for n in data["nodes"] if n.get("headcode")
                 and (not area or n["area"] == area.upper()) and (not headcode or n["headcode"] == headcode.upper())]
        lines = ["TD occupations (LIVE / UNCONFIRMED after feed gaps or old updates)"]
        for n in sorted(nodes, key=lambda n: (n["area"], n["berth"]))[:100]:
            labels = " / ".join(r["description"] for r in n["labels"])
            lines.append(f"{n['id']} {n['headcode']} | {'LIVE' if n['live'] else 'UNCONFIRMED'} | {labels or 'location not mapped'}")
        await reply(interaction, "\n".join(lines + (["No matching occupations received."] if not nodes else [])))

    @group.command(name="train", description="Inspect TD movements and candidate TRUST runs for a headcode.")
    async def train(interaction: discord.Interaction, headcode: str):
        if not await ready(interaction):
            return
        data = await asyncio.to_thread(runtime.train, headcode)
        lines = [f"Train {data['headcode']}", data["identity_note"], "", "Recent TD events:"]
        for e in data["events"][:20]:
            lines.append(f"{when(e['event_at'])} {e['area']} {e['kind']} {e['origin'] or '∅'} → {e['destination'] or '∅'}")
        if not data["events"]:
            lines.append("No TD history received for this description.")
        for r in data["runs"]:
            lines += ["", f"Candidate {r['train_id']} | {r['uid']} | {r['status']} | origin {when(r['origin_at'])}"]
            for e in r["reports"][:6]:
                d = e["details"]
                name = "/".join(e["location_names"]) or e["stanox"]
                delay = d.get("delay_minutes")
                lines.append(f"  {when(e['event_at'])} {name} {d.get('event_type',e['kind'])}" + (f" {delay:+g} min" if delay is not None else ""))
            lines.append(f"  Applicable stored schedule candidates: {len(r['schedules'])}")
        await reply(interaction, "\n".join(lines))

    @group.command(name="delays", description="Show measured slow running and possible restriction matches.")
    async def delays(interaction: discord.Interaction):
        if not await ready(interaction):
            return
        data = await asyncio.to_thread(runtime.snapshot)
        lines = ["Running-time findings (last hour)"]
        for r in data["findings"][:15]:
            d = r["details"]
            lines += ["", f"{r['headcode']} | {r['section']} | {when(r['event_at'])}", d["summary"]]
            for restriction in d["restriction_matches"][:3]:
                lines.append("Possible contributor: " + restriction_line(restriction))
                lines.append(f"  {restriction['publication']} published {when(restriction['published_at'])}; exact line/mileage overlap unverified.")
            if not d["restriction_matches"]:
                lines.append("No matching current TSR in the data received; cause unknown.")
        if not data["findings"]:
            lines.append("No supported slow-running findings yet. TD baselines need repeated movements; TRUST needs CORPUS and paired reports.")
        await reply(interaction, "\n".join(lines))

    @group.command(name="restrictions", description="Show current local restrictions from the weekly TSR snapshot.")
    async def restrictions(interaction: discord.Interaction, location: Optional[str] = None):
        if not await ready(interaction):
            return
        data = await asyncio.to_thread(runtime.snapshot)
        rows = [r for r in data["restrictions"] if not location or location.lower() in json.dumps(r).lower()]
        lines = ["Weekly TSR data — not a complete real-time list of emergency restrictions"]
        for r in rows[:30]:
            lines += [restriction_line(r), f"  {r['publication']} | published {when(r['published_at'])} | WON through {when(r['won_end'])}"]
        if not rows:
            lines.append("No matching applicable TSR received. Check /network status; this does not mean there are no restrictions.")
        await reply(interaction, "\n".join(lines))

    @group.command(name="schedules", description="Inspect stored VSTP and optional SCHEDULE records by UID.")
    async def schedules(interaction: discord.Interaction, uid: str):
        if not await ready(interaction):
            return
        rows = await asyncio.to_thread(runtime.store.rows, "SELECT * FROM schedules WHERE uid=? ORDER BY updated_at DESC LIMIT 10", (uid.strip(),))
        lines = [f"Stored schedules for {uid} (candidates; STP overlays are shown separately)"]
        for r in rows:
            d = json.loads(r["details"])
            lines.append(f"{r['source']} {r['stp']} | {r['start_date']} to {r['end_date']} | {'deleted' if d['deleted'] else 'stored'}")
            segments = d["schedule"].get("schedule_segment", [])
            if isinstance(segments, dict):
                segments = [segments]
            for seg in segments:
                for loc in seg.get("schedule_location", []):
                    code = loc.get("tiploc_code") or loc.get("location", {}).get("tiploc", {}).get("tiploc_id", "?")
                    lines.append(f"  {code}: arr {loc.get('scheduled_arrival_time',loc.get('arrival',''))} dep {loc.get('scheduled_departure_time',loc.get('departure',''))} pass {loc.get('scheduled_pass_time',loc.get('pass',''))}")
        if not rows:
            lines.append("No schedule received. VSTP covers short-notice plans; ordinary schedules need an optional JSON-lines import.")
        await reply(interaction, "\n".join(lines))

    @group.command(name="refresh", description="Refresh CORPUS and SMART reference data.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def refresh(interaction: discord.Interaction):
        if await ready(interaction):
            await reply(interaction, await runtime.refresh_references())


async def network_alert_loop(bot, runtime, channel_id):
    """Opt-in only. Coalesce repeated observations and persist notification state."""
    cooldown = 900
    minimum = max(1, int(os.getenv("NR_ALERT_MIN_TRAINS", "3")))
    while True:
        try:
            now = time.time()
            data = await asyncio.to_thread(runtime.snapshot)
            live = {h["feed"] for h in data["feeds"] if h["state"] == "live"}
            groups = defaultdict(list)
            for f in data["findings"]:
                feed = "trust" if f["kind"] == "trust_delay" else "td:" + f["area"]
                if now - f["event_at"] <= 900 and feed in live:
                    groups[(f["kind"], f["section"])].append(f)
            for (kind, section), items in groups.items():
                if len({f["headcode"] for f in items}) < minimum or not any(f["alert_at"] is None for f in items):
                    continue
                if runtime.store.rows("SELECT 1 FROM findings WHERE kind=? AND section=? AND alert_at>? LIMIT 1", (kind, section, now - cooldown)):
                    continue
                channel = bot.get_channel(channel_id)
                if channel is None:
                    channel = await bot.fetch_channel(channel_id)
                latest = items[0]
                text = f"Slower running: {section}\nObserved on {len(set(f['headcode'] for f in items))} descriptions in the last 15 minutes.\n{latest['details']['summary']}"
                matches = latest["details"]["restriction_matches"]
                if matches:
                    text += "\nPossible contributing restriction: " + restriction_line(matches[0])
                    text += f"\nWeekly snapshot {matches[0]['publication']}; exact line/mileage overlap and cause unconfirmed."
                else:
                    text += "\nCause unknown; no matching current TSR was found in the received data."
                await channel.send(discord.utils.escape_mentions(text[:1950]), allowed_mentions=discord.AllowedMentions.none())
                with runtime.store.lock, runtime.store.db:
                    runtime.store.db.executemany("UPDATE findings SET alert_at=? WHERE id=?", [(now, f["id"]) for f in items])
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("Network alert delivery failed")
        await asyncio.sleep(30)
