from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import logging
import time

import aiohttp

from .analysis import snapshot, train_detail
from .config import AdvancedConfig
from .feeds import import_reference
from .store import NetworkStore

LOG = logging.getLogger(__name__)


class NetworkRuntime:
    def __init__(self, config: AdvancedConfig):
        self.config = config
        self.store = NetworkStore(config)
        self.web = None
        self.tasks = []
        self.reference_lock = asyncio.Lock()
        self.username = self.password = ""

    def subscriptions(self, base_id, durable_name, durable):
        return [(base_id + "-" + name, topic,
                 {"activemq.subscriptionName": durable_name + "-" + name} if durable else {})
                for name, topic in self.config.topics.items()]

    def feed_for_subscription(self, subscription, base_id):
        for name in self.config.topics:
            if subscription == base_id + "-" + name:
                return name
        return "td"

    async def start(self, username="", password=""):
        self.username, self.password = username, password
        if self.tasks:
            return
        if self.config.map_enabled:
            from .web import MapServer
            try:
                self.web = MapServer(self)
                await self.web.start()
            except (ValueError, OSError) as exc:
                self.web = None
                self.store.error("map", str(exc))
                LOG.error("Map could not start: %s", exc)
        self.tasks.append(asyncio.create_task(self._maintenance(), name="nr-network-maintenance"))
        if self.config.reference_enabled:
            self.tasks.append(asyncio.create_task(self._references(), name="nr-reference-refresh"))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        if self.web:
            await self.web.stop()

    async def _maintenance(self):
        while True:
            try:
                await asyncio.to_thread(self.store.prune)
            except Exception:
                LOG.exception("Network database maintenance failed")
            await asyncio.sleep(3600)

    async def _references(self):
        while True:
            await self.refresh_references()
            await asyncio.sleep(86400)

    async def refresh_references(self):
        if not self.username or not self.password:
            for name in ("corpus", "smart"):
                self.store.error(name, "Network Rail credentials unavailable; reference data has not been downloaded")
            return "Reference refresh needs NROD_USER and NROD_PASS."
        results = []
        async with self.reference_lock:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as session:
                for name in ("corpus", "smart"):
                    url = "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=" + name.upper()
                    try:
                        # aiohttp does not forward Basic Auth to a different origin
                        # on redirects. Never attach credentials to data in the UI.
                        async with session.get(url, auth=aiohttp.BasicAuth(self.username, self.password)) as response:
                            response.raise_for_status()
                            chunks, size = [], 0
                            async for chunk in response.content.iter_chunked(65536):
                                size += len(chunk)
                                if size > 128 * 1024 * 1024:
                                    raise ValueError("Reference download exceeds 128 MiB")
                                chunks.append(chunk)
                            raw = b"".join(chunks)
                        if raw.startswith(b"\x1f\x8b"):
                            raw = gzip.decompress(raw)
                        if len(raw) > 256 * 1024 * 1024:
                            raise ValueError("Expanded reference data exceeds 256 MiB")
                        data = json.loads(raw.decode("utf-8-sig"))
                        count = await asyncio.to_thread(import_reference, self.store, name, data, time.time())
                        results.append(f"{name.upper()}: {count} rows loaded")
                    except (aiohttp.ClientError, TimeoutError, ValueError, OSError) as exc:
                        # Avoid logging authenticated request representations.
                        message = f"{name.upper()} refresh failed ({type(exc).__name__}); retained last good data"
                        self.store.error(name, message)
                        results.append(message)
            return "\n".join(results)

    def snapshot(self):
        return snapshot(self.store)

    def train(self, headcode):
        return train_detail(self.store, headcode)

    def summary(self):
        data = self.snapshot()
        occupied = [n for n in data["nodes"] if n.get("headcode")]
        lines = ["Tyneside network monitor", f"Areas: {', '.join(self.config.areas)}",
                 f"Berths: {len(data['nodes'])} | Live occupations: {sum(n.get('live', False) for n in occupied)}",
                 f"Timing samples: {data['baseline_samples']} | Recent findings: {len(data['findings'])}"]
        for f in data["feeds"]:
            lines.append(f"{f['feed']}: {f['state']}" + (f" — {f['error']}" if f["error"] else ""))
        return "\n".join(lines)
