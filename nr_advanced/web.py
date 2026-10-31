from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

WEB_ROOT = Path(__file__).with_name("web")


class MapServer:
    def __init__(self, runtime):
        self.runtime = runtime
        self.config = runtime.config
        self.runner = None
        host = self.config.map_host
        if host == "localhost":
            loopback = True
        else:
            try:
                address = ipaddress.ip_address(host)
            except ValueError as exc:
                raise ValueError("NR_MAP_BIND must be localhost or a private IP literal") from exc
            loopback = address.is_loopback
            tailnet = address.version == 4 and address in ipaddress.ip_network("100.64.0.0/10")
            if not (address.is_private or address.is_loopback or address.is_unspecified or tailnet):
                raise ValueError("Map bind address must be private or loopback")
        if not loopback and len(self.config.map_token) < 24:
            raise ValueError("Set a random NR_MAP_TOKEN of at least 24 characters before binding the map beyond loopback")
        self.cookie = hashlib.sha256(("nr-map-session:" + self.config.map_token).encode()).hexdigest()
        public = urlsplit(self.config.map_public_url)
        self.public_origin = f"{public.scheme}://{public.netloc}" if public.scheme in {"http", "https"} and public.netloc else ""
        self.app = web.Application(middlewares=[self.headers, self.auth], client_max_size=4096)
        self.app.add_routes([
            web.get("/", self.index), web.get("/app.js", self.asset), web.get("/style.css", self.asset),
            web.post("/api/login", self.login), web.post("/api/logout", self.logout),
            web.get("/api/snapshot", self.snapshot), web.get("/api/train/{headcode}", self.train),
        ])

    @web.middleware
    async def headers(self, request, handler):
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc
        response.headers.update({
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
        })
        return response

    @web.middleware
    async def auth(self, request, handler):
        if request.path.startswith("/api/") and request.path not in {"/api/login", "/api/logout"} and self.config.map_token:
            supplied = request.headers.get("Authorization", "").removeprefix("Bearer ")
            cookie = request.cookies.get("nr_map", "")
            if not (hmac.compare_digest(supplied, self.config.map_token) or hmac.compare_digest(cookie, self.cookie)):
                raise web.HTTPUnauthorized(text="Map access key required")
        if request.method == "POST":
            # Prevent cross-site login/logout; API is read-only otherwise.
            origin = request.headers.get("Origin")
            if origin and origin not in {f"{request.scheme}://{request.host}", self.public_origin}:
                raise web.HTTPForbidden(text="Origin mismatch")
        return await handler(request)

    async def index(self, request):
        return web.FileResponse(WEB_ROOT / "index.html")

    async def asset(self, request):
        return web.FileResponse(WEB_ROOT / request.path.lstrip("/"))

    async def login(self, request):
        try:
            payload = await request.json()
            key = payload.get("token", "") if isinstance(payload, dict) else ""
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="Invalid login")
        if not isinstance(key, str) or not hmac.compare_digest(key, self.config.map_token):
            await asyncio.sleep(0.3)
            raise web.HTTPUnauthorized(text="Incorrect access key")
        response = web.json_response({"ok": True})
        response.set_cookie("nr_map", self.cookie, httponly=True, samesite="Strict",
                            secure=request.secure or self.public_origin.startswith("https://"), max_age=86400)
        return response

    async def logout(self, request):
        response = web.json_response({"ok": True})
        response.del_cookie("nr_map")
        return response

    async def snapshot(self, request):
        data = await asyncio.to_thread(self.runtime.snapshot)
        data["auth_required"] = bool(self.config.map_token)
        return web.json_response(data, dumps=lambda x: json.dumps(x, allow_nan=False))

    async def train(self, request):
        headcode = request.match_info["headcode"]
        if not 1 <= len(headcode) <= 16 or not headcode.isalnum():
            raise web.HTTPBadRequest(text="Invalid headcode")
        return web.json_response(await asyncio.to_thread(self.runtime.train, headcode))

    async def start(self):
        self.runner = web.AppRunner(self.app, access_log=None)
        try:
            await self.runner.setup()
            await web.TCPSite(self.runner, self.config.map_host, self.config.map_port).start()
        except Exception:
            await self.runner.cleanup()
            self.runner = None
            raise

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
