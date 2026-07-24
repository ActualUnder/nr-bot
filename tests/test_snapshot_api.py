from __future__ import annotations

import shutil
import ssl
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from aiohttp import web
import t3_learner_clean as learner
from t3_snapshot_api import (
    BerthCatalogue,
    T3SnapshotAPIService,
    T3SnapshotBuilder,
    certificate_common_names,
    create_server_ssl_context,
    metro_tdn_for_headcode,
    normalize_berth,
    validate_private_bind_address,
)


class SnapshotAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        project = Path(__file__).resolve().parents[1]
        self.catalogue = BerthCatalogue.load(
            project / "berth-to-description.csv",
            project / "berth-step-to-description.csv",
        )
        self.store = learner.Store(self.root / "t3.sqlite", self.root / "missing")
        self.builder = T3SnapshotBuilder(
            db_path=self.root / "t3.sqlite",
            catalogue=self.catalogue,
            area="T3",
            stale_seconds=180,
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_supplied_catalogue_keeps_scope_and_ambiguities(self) -> None:
        self.assertEqual(len(self.catalogue.scope), 86)
        self.assertEqual(len(self.catalogue.step_descriptions), 92)
        self.assertEqual(self.catalogue.ambiguous_berths, ("6217", "6298"))
        self.assertEqual(self.catalogue.ambiguous_steps, ("6209->6217",))
        self.assertEqual(normalize_berth("0019"), "19")
        self.assertIn("P789", self.catalogue.scope)
        self.assertIn("6212", self.catalogue.scope)
        self.assertEqual(
            self.catalogue.next_berths["P789"],
            ("P803",),
        )

    def test_headcode_to_tdn_conversion_is_explicit(self) -> None:
        self.assertEqual(metro_tdn_for_headcode("2I01"), "T101")
        self.assertEqual(metro_tdn_for_headcode("2i51"), "T151")
        self.assertEqual(metro_tdn_for_headcode("2I92"), "T192")
        self.assertIsNone(metro_tdn_for_headcode("4N01"))

    def test_snapshot_returns_only_current_connection_positive_evidence(self) -> None:
        generation = self.store.mark_connected("T3", ts=1_700_000_000)
        self.store.touch_feed_event("T3", "C", 1_700_000_002)
        self.store.record_berth_state(
            "6243",
            "2I10",
            True,
            1_700_000_001,
            "CA",
            generation,
        )
        self.store.record_berth_step(
            area="T3",
            ts=1_700_000_001,
            descr="2I10",
            from_berth="6239",
            to_berth="6243",
            topology_valid=True,
            special_reason="",
            raw={},
        )
        self.store.record_berth_state(
            "UNMAPPED",
            "1N23",
            True,
            1_700_000_001,
            "CC",
            generation,
        )

        payload = self.builder.build(
            connected=True,
            last_message_ts=1_700_000_002,
            now=1_700_000_003,
        )
        self.assertTrue(payload["feed"]["available"])
        self.assertFalse(payload["feed"]["complete"])
        self.assertFalse(payload["absence_is_evidence"])
        self.assertEqual(len(payload["positions"]), 1)
        position = payload["positions"][0]
        self.assertEqual(position["headcode"], "2I10")
        self.assertEqual(position["tdn"], "T110")
        self.assertEqual(position["berth"], "6243")
        self.assertEqual(position["last_step"]["from_berth"], "6239")
        self.assertEqual(
            position["last_step"]["description"],
            "has departed East Boldon P2",
        )
        self.assertEqual(payload["withheld"]["unmapped_occupations"], 1)

        next_generation = self.store.mark_connected("T3", ts=1_700_000_010)
        self.assertEqual(next_generation, generation + 1)
        self.store.touch_feed_event("T3", "C", 1_700_000_011)
        after_reconnect = self.builder.build(
            connected=True,
            last_message_ts=1_700_000_011,
            now=1_700_000_012,
        )
        self.assertEqual(after_reconnect["positions"], [])
        self.assertEqual(
            after_reconnect["withheld"]["previous_connection_occupations"],
            1,
        )

        self.store.record_berth_state(
            "P789",
            "2I10",
            True,
            1_700_000_013,
            "CC",
            next_generation,
        )
        self.store.touch_feed_event("T3", "C", 1_700_000_014)
        refreshed = self.builder.build(
            connected=True,
            last_message_ts=1_700_000_014,
            now=1_700_000_015,
        )
        self.assertEqual([item["berth"] for item in refreshed["positions"]], ["P789"])
        self.assertEqual(refreshed["positions"][0]["description"], "Hebburn P1")

    def test_stale_or_disconnected_feed_returns_no_positions(self) -> None:
        generation = self.store.mark_connected("T3", ts=1_700_000_000)
        self.store.touch_feed_event("T3", "C", 1_700_000_002)
        self.store.record_berth_state(
            "6239",
            "2I10",
            True,
            1_700_000_001,
            "CC",
            generation,
        )
        stale = self.builder.build(
            connected=True,
            # A newly received durable message can still describe an old event.
            # Reception freshness must not hide feed catch-up lag.
            last_message_ts=1_700_000_499,
            now=1_700_000_500,
        )
        self.assertFalse(stale["feed"]["available"])
        self.assertIn("catching up", stale["feed"]["reason"])
        self.assertEqual(stale["positions"], [])

        disconnected = self.builder.build(
            connected=False,
            last_message_ts=1_700_000_499,
            now=1_700_000_500,
        )
        self.assertFalse(disconnected["feed"]["available"])
        self.assertEqual(disconnected["positions"], [])

    def test_peer_certificate_common_name_is_read_strictly(self) -> None:
        certificate = {
            "subject": (
                (("countryName", "GB"),),
                (("commonName", "metro-bot"),),
            )
        }
        self.assertEqual(certificate_common_names(certificate), {"metro-bot"})
        self.assertEqual(certificate_common_names(None), set())

    def test_application_rejects_other_ca_signed_identity(self) -> None:
        service = T3SnapshotAPIService(
            enabled=True,
            bind_host="127.0.0.1",
            bind_port=8765,
            certificate=Path("/unused/server.crt"),
            private_key=Path("/unused/server.key"),
            client_ca=Path("/unused/ca.crt"),
            allowed_client_cn="metro-bot",
            builder=self.builder,
            feed_status=lambda: {"connected": True, "last_message_ts": 0},
        )

        class FakeTransport:
            @staticmethod
            def get_extra_info(name):
                self_cert = {
                    "subject": ((("commonName", "another-authorised-client"),),)
                }
                return self_cert if name == "peercert" else None

        request = SimpleNamespace(transport=FakeTransport())

        async def call_endpoint() -> None:
            await service._snapshot(request)

        with self.assertRaises(web.HTTPForbidden):
            import asyncio

            asyncio.run(call_endpoint())

    def test_private_bind_validation_rejects_wildcard_and_public_ip(self) -> None:
        validate_private_bind_address("10.77.0.1")
        validate_private_bind_address("127.0.0.1")
        validate_private_bind_address("nr-bot")
        with self.assertRaises(ValueError):
            validate_private_bind_address("0.0.0.0")
        with self.assertRaises(ValueError):
            validate_private_bind_address("8.8.8.8")

    @unittest.skipUnless(shutil.which("openssl"), "openssl is not installed")
    def test_generated_server_context_requires_client_certificates(self) -> None:
        project = Path(__file__).resolve().parents[1]
        output = self.root / "certificates"
        subprocess.run(
            [
                "bash",
                str(project / "scripts" / "create_bridge_certs.sh"),
                str(output),
                "nr-bot",
                "10.77.0.1",
                "metro-bot",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        context = create_server_ssl_context(
            certificate=output / "nr-bot" / "server.crt",
            private_key=output / "nr-bot" / "server.key",
            client_ca=output / "nr-bot" / "ca.crt",
        )
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertGreaterEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)


if __name__ == "__main__":
    unittest.main()
