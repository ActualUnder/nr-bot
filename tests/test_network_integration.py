from __future__ import annotations

import importlib
import json
import os
import tempfile
import types
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from aiohttp import web
from nr_advanced.config import AdvancedConfig
from nr_advanced.runtime import NetworkRuntime
from nr_advanced.web import MapServer
from test_advanced import NOW, ROOT, td, tsr


class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot_tmp=tempfile.TemporaryDirectory()
        with patch.dict(os.environ,{"DISCORD_TOKEN":"test-only","NRBOT_DATA_DIR":cls.bot_tmp.name,"NR_REFERENCE_AUTO_REFRESH":"false"}):
            cls.bot=importlib.import_module("bot")
        cls.original=cls.bot.ADVANCED

    @classmethod
    def tearDownClass(cls):
        cls.original.store.close();cls.bot_tmp.cleanup()

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.runtime=NetworkRuntime(AdvancedConfig(Path(self.tmp.name),ROOT,reference_enabled=False))
        self.runtime.store.connected(NOW-60);self.bot.ADVANCED=self.runtime
        self.acks=[];self.disconnects=[];self.learned=[]
        self.conn=types.SimpleNamespace(ack=lambda **kw:self.acks.append(kw),disconnect=lambda:self.disconnects.append(True))
        self.learner=types.SimpleNamespace(area="T3",handle_message=lambda *a,**kw:self.learned.append(a),store=types.SimpleNamespace(mark_invalid_message=lambda *a:None))
        self.listener=self.bot.DiscordFeedListener(self.learner,self.conn,"test-sub")

    def tearDown(self):
        self.bot.ADVANCED=self.original;self.runtime.store.close();self.tmp.cleanup()

    def test_auxiliary_feed_dispatch_commits_before_ack(self):
        self.listener.on_message(types.SimpleNamespace(headers={"subscription":"test-sub-tsr","ack":"tsr-1"},body=json.dumps(tsr())))
        self.assertEqual(len(self.runtime.store.rows("SELECT * FROM restrictions")),1)
        self.assertEqual(self.acks,[{"id":"tsr-1"}]);self.assertEqual(self.learned,[])

    def test_td_still_goes_to_legacy_learner_and_new_store(self):
        self.listener.on_message(types.SimpleNamespace(headers={"subscription":"test-sub","ack":"td-1"},body=json.dumps(td("CC",NOW,to="6239",descr="2I01"))))
        self.assertEqual(len(self.learned),1);self.assertEqual(len(self.runtime.store.rows("SELECT * FROM berths")),1)
        self.assertEqual(self.acks,[{"id":"td-1"}])

    def test_write_failure_is_not_acked_and_forces_redelivery(self):
        with patch.object(self.runtime.store,"ingest",side_effect=OSError("disk unavailable")):
            self.listener.on_message(types.SimpleNamespace(headers={"subscription":"test-sub","ack":"td-1"},body="[]"))
        self.assertFalse(self.acks);self.assertEqual(self.disconnects,[True])

    def test_one_connection_distinct_durable_subscriptions(self):
        subs=self.runtime.subscriptions("td-main","td-durable",True)
        self.assertEqual(len(subs),3)
        self.assertEqual(len({s[0] for s in subs}),3)
        self.assertTrue(all(s[2]["activemq.subscriptionName"].startswith("td-durable-") for s in subs))

    def test_network_commands_registered(self):
        group=self.bot.bot.tree.get_command("network")
        self.assertEqual({c.name for c in group.commands},{"map","status","train","berths","delays","restrictions","schedules","refresh"})


class MapSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.cfg=AdvancedConfig(Path(self.tmp.name),ROOT,map_token="a-long-test-access-key-0123456789",reference_enabled=False)
        self.runtime=NetworkRuntime(self.cfg);self.server=MapServer(self.runtime)

    async def asyncTearDown(self):
        self.runtime.store.close();self.tmp.cleanup()

    def req(self,**kwargs):
        base=dict(path="/api/snapshot",method="GET",headers={},cookies={},host="localhost:8790",scheme="http",secure=False)
        base.update(kwargs);return types.SimpleNamespace(**base)

    async def test_auth_required_for_api_and_no_query_token(self):
        async def handler(req):return web.json_response({"ok":True})
        with self.assertRaises(web.HTTPUnauthorized):await self.server.auth(self.req(),handler)
        result=await self.server.auth(self.req(headers={"Authorization":"Bearer "+self.cfg.map_token}),handler)
        self.assertEqual(result.status,200)

    async def test_login_cookie_httponly_and_not_network_rail_credentials(self):
        async def body():return {"token":self.cfg.map_token}
        result=await self.server.login(self.req(json=body))
        self.assertTrue(result.cookies["nr_map"]["httponly"])
        self.assertEqual(result.cookies["nr_map"]["samesite"],"Strict")
        self.assertNotEqual(result.cookies["nr_map"].value,self.cfg.map_token)

    async def test_wrong_token_is_rejected(self):
        async def body():return {"token":"wrong"}
        with self.assertRaises(web.HTTPUnauthorized):await self.server.login(self.req(json=body))

    async def test_cross_origin_post_is_rejected(self):
        async def handler(req):return web.Response()
        with self.assertRaises(web.HTTPForbidden):
            await self.server.auth(self.req(path="/api/login",method="POST",headers={"Origin":"https://example.invalid"}),handler)

    async def test_private_bind_needs_strong_map_key(self):
        self.runtime.config=replace(self.cfg,map_host="0.0.0.0",map_token="")
        with self.assertRaises(ValueError):MapServer(self.runtime)
        self.runtime.config=replace(self.cfg,map_host="8.8.8.8")
        with self.assertRaises(ValueError):MapServer(self.runtime)

    async def test_csp_and_snapshot_no_secrets(self):
        async def handler(req):return await self.server.snapshot(req)
        response=await self.server.headers(self.req(),handler)
        self.assertIn("script-src 'self'",response.headers["Content-Security-Policy"])
        self.assertNotIn(self.cfg.map_token,response.text)

    async def test_tailnet_and_configured_https_proxy_origin(self):
        self.runtime.config=replace(self.cfg,map_host="100.80.1.2",map_public_url="https://rail.example.test")
        server=MapServer(self.runtime)
        async def handler(req):return web.Response()
        response=await server.auth(self.req(path="/api/login",method="POST",headers={"Origin":"https://rail.example.test"}),handler)
        self.assertEqual(response.status,200)


if __name__=="__main__":unittest.main()
