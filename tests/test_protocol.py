from __future__ import annotations

import json
import tempfile
import time
import types
import unittest
from pathlib import Path

import t3_learner_clean as mod


class ProtocolTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.store = mod.Store(root / "test.sqlite", root / "missing")
        self.learner = mod.Learner(
            area="T3",
            store=self.store,
            topology={"6239": {"6243"}},
            known=mod.KnownBits([]),
            watch_signals={"6239"},
            pre=30,
            post=30,
            recent_keep=300,
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

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    @staticmethod
    def msg(msg_type: str, when: float, **kwargs):
        data = {"area_id": "T3", "msg_type": msg_type, "time": str(int(when * 1000))}
        data.update(kwargs)
        return data

    def test_complete_sg_sh_refresh_is_transactional_and_sh_has_data(self) -> None:
        now = time.time()
        self.learner.mark_connected()
        # A full ordered refresh begins at 00. Use enough chunks to include 24
        # so the subsequent SF has a trusted refresh baseline.
        for address in range(0x00, 0x24, 4):
            self.learner.handle_message(
                "SG_MSG", self.msg("SG", now + address / 1000, address=f"{address:02X}", data="00000000")
            )

        state = self.store.feed_state_row("T3")
        self.assertFalse(bool(state["snapshot_valid"]))
        self.assertTrue(bool(state["refresh_in_progress"]))
        self.assertEqual(self.store.load_bytes("T3"), {})

        # SH carries its own final four-byte block and completes the refresh.
        self.learner.handle_message("SH_MSG", self.msg("SH", now + 0.1, address="24", data="00000000"))
        state = self.store.feed_state_row("T3")
        self.assertTrue(bool(state["snapshot_valid"]))
        self.assertFalse(bool(state["refresh_in_progress"]))
        self.assertEqual(len(self.store.load_bytes("T3")), 40)
        self.assertEqual(self.store.load_bytes("T3")[0x24], 0)

        # Now that 24 is trusted by a complete refresh, SF creates a precise edge.
        self.learner.handle_message("SF_MSG", self.msg("SF", now + 1, address="24", data="80"))
        row = self.store.conn.execute(
            "SELECT * FROM s_bit_events WHERE address=0x24 AND bit=7"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual((row["old_bit"], row["new_bit"]), (0, 1))

    def test_partial_or_non_contiguous_refresh_is_rejected(self) -> None:
        now = time.time()
        self.learner.mark_connected()

        # Reconnecting in the middle of an SG sequence must not create a live snapshot.
        self.learner.handle_message("SG_MSG", self.msg("SG", now, address="20", data="00000000"))
        state = self.store.feed_state_row("T3")
        self.assertFalse(bool(state["snapshot_valid"]))
        self.assertFalse(bool(state["refresh_in_progress"]))

        # A gap in an otherwise valid sequence also invalidates the transaction.
        self.learner.handle_message("SG_MSG", self.msg("SG", now + 1, address="00", data="00000000"))
        self.learner.handle_message("SH_MSG", self.msg("SH", now + 2, address="08", data="00000000"))
        state = self.store.feed_state_row("T3")
        self.assertFalse(bool(state["snapshot_valid"]))
        self.assertFalse(bool(state["refresh_in_progress"]))
        self.assertGreaterEqual(int(state["invalid_messages"]), 2)

    def test_orphan_sh_does_not_validate_snapshot(self) -> None:
        now = time.time()
        self.learner.mark_connected()
        self.learner.handle_message("SH_MSG", self.msg("SH", now, address="24", data="00000000"))
        state = self.store.feed_state_row("T3")
        self.assertFalse(bool(state["snapshot_valid"]))
        self.assertGreaterEqual(int(state["invalid_messages"]), 1)

    def test_first_sf_after_restart_is_baseline_not_fake_edge(self) -> None:
        now = time.time()
        self.learner.mark_connected()
        self.learner.handle_message("SF_MSG", self.msg("SF", now, address="24", data="80"))
        count = self.store.conn.execute("SELECT COUNT(*) AS c FROM s_bit_events").fetchone()["c"]
        self.assertEqual(count, 0)
        state = self.store.feed_state_row("T3")
        self.assertFalse(bool(state["snapshot_valid"]))

        self.learner.handle_message("SF_MSG", self.msg("SF", now + 1, address="24", data="00"))
        count = self.store.conn.execute("SELECT COUNT(*) AS c FROM s_bit_events").fetchone()["c"]
        self.assertEqual(count, 1)

    def test_duplicate_message_is_processed_once(self) -> None:
        now = time.time()
        self.learner.mark_connected()
        message = self.msg("SG", now, address="00", data="00000000")
        self.learner.handle_message("SG_MSG", message)
        self.learner.handle_message("SG_MSG", message)
        count = self.store.conn.execute("SELECT COUNT(*) AS c FROM raw_td_messages").fetchone()["c"]
        state = self.store.feed_state_row("T3")
        self.assertEqual(count, 1)
        self.assertEqual(int(state["duplicate_messages"]), 1)

    def test_stomp_listener_uses_stomp12_ack_id_and_consumes_poison_frames(self) -> None:
        class FakeConnection:
            def __init__(self):
                self.calls = []

            def ack(self, **kwargs):
                self.calls.append(kwargs)

        connection = FakeConnection()
        listener = mod.Listener(self.learner, connection, "test-sub")
        now = time.time()
        valid = types.SimpleNamespace(
            headers={"ack": "ack-1"},
            body=json.dumps({"CT_MSG": self.msg("CT", now)}),
        )
        listener.on_message(valid)
        self.assertEqual(connection.calls[-1], {"id": "ack-1"})

        invalid = types.SimpleNamespace(headers={"ack": "ack-2"}, body="not-json")
        before = int(self.store.feed_state_row("T3")["invalid_messages"])
        listener.on_message(invalid)
        after = int(self.store.feed_state_row("T3")["invalid_messages"])
        self.assertEqual(connection.calls[-1], {"id": "ack-2"})
        self.assertEqual(after, before + 1)

    def test_only_verified_signal_mappings_can_drive_live_aspects(self) -> None:
        key = mod.parse_bit_spec("24:7")
        signal = mod.KnownBit(
            key=key, element_type="Signal", description="6239", signal="6239",
            provenance="SOP", verified=True, active_state="off=1", element_group="SIG",
        )
        route = mod.KnownBit(
            key=key, element_type="Route", description="6239 to 6243",
            route_from="6239", route_to="6243", provenance="SOP", verified=True,
            active_state="set=1", element_group="RTE",
        )
        unverified = mod.KnownBit(
            key=key, element_type="Signal", description="6239", signal="6239",
            provenance="reference-unverified", verified=False, active_state="off=1",
        )
        self.assertTrue(signal.trusted_for_live_aspect)
        self.assertFalse(route.trusted_for_live_aspect)
        self.assertFalse(unverified.trusted_for_live_aspect)

    def test_strict_payload_validation(self) -> None:
        with self.assertRaises(mod.InvalidTDMessage):
            mod.split_hex_bytes("0", expected_bytes=1)
        with self.assertRaises(mod.InvalidTDMessage):
            mod.split_hex_bytes("0000", expected_bytes=1)
        with self.assertRaises(mod.InvalidTDMessage):
            mod.parse_nr_time_ms("not-a-time")
        with self.assertRaises(mod.InvalidTDMessage):
            mod.parse_hex_address("100")
        with self.assertRaises(mod.InvalidTDMessage):
            mod.parse_hex_address("GG")

    def test_protocol_matching_does_not_reuse_one_edge_for_multiple_trains(self) -> None:
        rows = [
            {"event_ts": 80.0, "old_bit": 0, "new_bit": 1},
            {"event_ts": 140.0, "old_bit": 1, "new_bit": 0},
        ]
        stats = mod._assign_pre_edges_one_to_one(
            rows, [100.0, 130.0], (0, 1),
            pre_seconds=120.0, post_seconds=180.0, near_step_seconds=2.5,
        )
        self.assertEqual(stats["pre"], 1)
        self.assertEqual(stats["cycles"], 1)

    def test_protocol_classifier_rejects_6239_movement_pulse(self) -> None:
        base = time.time() - 10_000
        for i in range(10):
            step = base + i * 300
            self.store.record_berth_step(
                area="T3", ts=step, descr=f"2T{i:02d}", from_berth="6239", to_berth="6243",
                topology_valid=True, special_reason="", raw={},
            )
            # 24:7 changes at the berth step then reverses 18s later: pulse/track shaped.
            self.store.record_raw_bit_event(mod.BitEvent(step, "T3", 0x24, 7, 0, 1, 0x00, 0x80, "SF"))
            self.store.record_raw_bit_event(mod.BitEvent(step + 18, "T3", 0x24, 7, 1, 0, 0x80, 0x00, "SF"))
            # 30:1 changes well before the step and restores afterwards.
            self.store.record_raw_bit_event(mod.BitEvent(step - 20, "T3", 0x30, 1, 0, 1, 0x00, 0x02, "SF"))
            self.store.record_raw_bit_event(mod.BitEvent(step + 10, "T3", 0x30, 1, 1, 0, 0x02, 0x00, "SF"))
            # Unrelated control movements, without either bit pattern nearby.
            self.store.record_berth_step(
                area="T3", ts=step + 140, descr=f"1N{i:02d}", from_berth="6207", to_berth="6217",
                topology_valid=True, special_reason="", raw={},
            )

        rows = mod.protocol_candidate_analysis(self.store.conn, "6239", area="T3")
        by_key = {row["key"].label: row for row in rows}
        self.assertEqual(by_key["24:7"]["classification"], "movement_pulse")
        self.assertEqual(by_key["30:1"]["classification"], "pre_step_control")

    def test_existing_legacy_database_is_migrated_without_reset(self) -> None:
        import sqlite3

        root = Path(self.tmp.name) / "legacy_case"
        root.mkdir()
        db = root / "legacy.sqlite"
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE pass_log (
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
            CREATE TABLE s_bytes (
                area TEXT NOT NULL,address INTEGER NOT NULL,value INTEGER NOT NULL,
                msg_type TEXT,updated_ts REAL NOT NULL,PRIMARY KEY(area,address)
            );
            INSERT INTO pass_log(signal,from_berth,to_berth,descr,pass_ts,special_reason)
            VALUES('6239','6239','6243','2T10',1700000000,'');
            """
        )
        conn.commit()
        conn.close()

        migrated = mod.Store(db, root / "missing")
        try:
            row = migrated.conn.execute(
                "SELECT * FROM berth_steps WHERE from_berth='6239' AND to_berth='6243'"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["source_msg_type"], "CA-legacy")
            self.assertIsNotNone(migrated.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='feed_state'"
            ).fetchone())
        finally:
            migrated.close()

    def test_paired_manual_observations(self) -> None:
        for _ in range(3):
            session = self.store.start_observation_session("6239")
            self.store.add_signal_observation(
                session_id=session, signal_id="6239", state="red",
                snapshot={0x30: 0x00}, generation=1,
            )
            self.store.add_signal_observation(
                session_id=session, signal_id="6239", state="off",
                snapshot={0x30: 0x02}, generation=1,
            )
            self.store.add_signal_observation(
                session_id=session, signal_id="6239", state="post_pass",
                snapshot={0x30: 0x00}, generation=1,
            )
        rows = mod.manual_observation_candidates(self.store.conn, "6239")
        self.assertEqual(rows[0]["key"].label, "30:1")
        self.assertEqual(rows[0]["support"], 3)
        self.assertEqual(rows[0]["return_rate"], 1.0)


if __name__ == "__main__":
    unittest.main()
