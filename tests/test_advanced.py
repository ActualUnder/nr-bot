from __future__ import annotations

import datetime as dt
import json
import sqlite3
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from nr_advanced.analysis import active_restrictions, match_restrictions, snapshot, train_detail
from nr_advanced.config import AdvancedConfig
from nr_advanced.feeds import import_reference
from nr_advanced.protocol import InvalidFeedMessage, trust_timestamp
from nr_advanced.store import NetworkStore

ROOT = Path(__file__).resolve().parents[1]
NOW = dt.datetime(2026, 10, 2, 10, tzinfo=dt.timezone.utc).timestamp()


def td(kind, at, area="T3", **kwargs):
    return {kind + "_MSG": {"area_id": area, "msg_type": kind, "time": str(int(at * 1000)), **kwargs}}


def tsr(at=NOW, start=NOW-3600, end=NOW+86400, speed="30", name="East Boldon", empty=False):
    rows = [] if empty else [{"TSRID":"test-tsr", "TSRReference":"TEST ONLY", "RouteCode":"TEST",
        "FromLocation":name, "ToLocation":name, "LineName":"Up", "Direction":"up",
        "PassengerSpeed":speed, "FreightSpeed":"20", "Reason":"Test fixture", "ValidFromDate":str(int((NOW-86400)*1000)),
        "ValidToDate":str(int((NOW+864000)*1000))}]
    return {"TSRBatchMsgV1":{"timestamp":str(int(at*1000)), "TSRBatchMsg":{
        "routeGroupCode":"TEST", "routeGroupCoverage":"full", "publishDate":str(int(at*1000)),
        "publishSource":"TEST-WON", "WONStartDate":str(int(start*1000)), "WONEndDate":str(int(end*1000)), "tsr":rows}}}


def trust(kind, at, train="122A01AA02", **kwargs):
    return {"header":{"msg_type":kind, "msg_queue_timestamp":str(int(at*1000)),"original_data_source":"TEST"},
            "body":{"train_id":train, **kwargs}}


class AdvancedTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.cfg=AdvancedConfig(Path(self.tmp.name),ROOT,baseline_min=3,trust_clock="utc")
        self.store=NetworkStore(self.cfg)
        self.store.connected(NOW-7200)

    def tearDown(self):
        self.store.close(); self.tmp.cleanup()

    def put(self,item,now=NOW):
        return self.store.ingest("td",item,now)

    def sample(self,at,seconds=30,headcode="2I01"):
        self.put(td("CA",at-seconds,**{"from":"6237","to":"6239","descr":headcode}),at-seconds)
        self.put(td("CA",at,**{"from":"6239","to":"6243","descr":headcode}),at)

    def test_area_namespaces_and_leading_zeroes(self):
        self.put([td("CC",NOW,"T1",to="0019",descr="1A01"),td("CC",NOW,"T3",to="0019",descr="2I01")])
        rows=self.store.rows("SELECT * FROM berths WHERE berth='0019' ORDER BY area")
        self.assertEqual([r["headcode"] for r in rows],["1A01","2I01"])

    def test_restart_gap_and_backlog_do_not_show_live_occupations(self):
        self.put(td("CC",NOW,to="6239",descr="2I01"))
        node=lambda: next(n for n in snapshot(self.store,NOW+10)["nodes"] if n["id"]=="T3:6239")
        self.assertTrue(node()["live"])
        self.store.disconnected();self.assertFalse(node()["live"])
        self.store.connected(NOW+5);self.put(td("CT",NOW+6),NOW+6)
        self.assertFalse(node()["live"])
        self.put(td("CC",NOW+7,to="6239",descr="2I02"),NOW+7);self.assertTrue(node()["live"])
        self.store.connected(NOW+1000)
        self.put(td("CC",NOW+100,to="6239",descr="2I03"),NOW+1001)
        n=next(n for n in snapshot(self.store,NOW+1002)["nodes"] if n["id"]=="T3:6239")
        self.assertFalse(n["live"])

    def test_duplicate_redelivery_not_double_counted(self):
        event=td("CA",NOW,**{"from":"6239","to":"6243","descr":"2I01"})
        self.assertEqual(self.put(event),1);self.assertEqual(self.put(event),0)
        self.assertEqual(self.store.rows("SELECT observations FROM edges WHERE source='observed CA'")[0]["observations"],1)

    def test_db_failure_rolls_back_dedup_and_state(self):
        event=td("CA",NOW,**{"from":"6239","to":"6243","descr":"2I01"})
        with patch.object(self.store,"_td",side_effect=sqlite3.OperationalError("locked")):
            with self.assertRaises(sqlite3.OperationalError):self.put(event)
        self.assertEqual(self.put(event),1)

    def test_out_of_order_and_mismatching_cancel_do_not_erase(self):
        self.put(td("CC",NOW,to="6239",descr="2I01"))
        self.put(td("CB",NOW-10,**{"from":"6239","descr":"2I01"}))
        self.put(td("CB",NOW+1,**{"from":"6239","descr":"2I02"}),NOW+1)
        self.assertEqual(self.store.rows("SELECT headcode FROM berths WHERE berth='6239'")[0]["headcode"],"2I01")

    def test_poison_item_does_not_discard_neighbours(self):
        self.put([td("CC",NOW,to="A001",descr="1A01"),td("CA",NOW,to="A002",descr="1A01"),td("CC",NOW,to="A003",descr="1A02")])
        self.assertEqual(len(self.store.rows("SELECT * FROM berths")),2)
        self.assertEqual(self.store.rows("SELECT rejected FROM feed_health WHERE feed='td'")[0]["rejected"],1)

    def test_interpose_does_not_create_timing_baseline(self):
        self.put(td("CC",NOW-40,to="6239",descr="2I01"),NOW-40)
        self.put(td("CA",NOW,**{"from":"6239","to":"6243","descr":"2I01"}))
        self.assertEqual(self.store.rows("SELECT * FROM samples"),[])

    def test_slow_running_requires_baseline_and_is_not_physical_speed(self):
        for i in range(3):self.sample(NOW-600+i*60,30,f"2I0{i}")
        self.sample(NOW,120,"2I10")
        findings=snapshot(self.store,NOW)["findings"]
        self.assertEqual(len(findings),1)
        self.assertEqual(findings[0]["details"]["baseline_seconds"],30)
        self.assertIn("physical speed",findings[0]["details"]["confidence"])

    def test_gap_cannot_create_slow_running_sample(self):
        self.put(td("CA",NOW-60,**{"from":"6237","to":"6239","descr":"2I01"}),NOW-60)
        self.store.disconnected();self.store.connected(NOW-30)
        self.put(td("CA",NOW,**{"from":"6239","to":"6243","descr":"2I01"}))
        self.assertEqual(self.store.rows("SELECT * FROM samples"),[])

    def test_tsr_next_week_does_not_replace_current_and_empty_batch_clears(self):
        self.store.ingest("tsr",tsr(at=NOW-100),NOW)
        self.store.ingest("tsr",tsr(at=NOW,start=NOW+86400,end=NOW+8*86400,speed="50"),NOW)
        self.assertEqual(active_restrictions(self.store,NOW)[0]["PassengerSpeed"],"30")
        self.assertEqual(active_restrictions(self.store,NOW+2*86400)[0]["PassengerSpeed"],"50")
        self.store.ingest("tsr",tsr(at=NOW+1,empty=True),NOW+1)
        self.assertEqual(active_restrictions(self.store,NOW+1),[])

    def test_tsr_old_redelivery_does_not_undo_newer_snapshot(self):
        self.store.ingest("tsr",tsr(at=NOW,speed="40"),NOW)
        self.store.ingest("tsr",tsr(at=NOW-30,speed="20"),NOW)
        self.assertEqual(active_restrictions(self.store,NOW)[0]["PassengerSpeed"],"40")

    def test_tsr_malformed_snapshot_retains_last_good_data(self):
        self.store.ingest("tsr",tsr(at=NOW-10),NOW)
        bad=tsr(at=NOW);del bad["TSRBatchMsgV1"]["TSRBatchMsg"]["tsr"][0]["ValidFromDate"]
        self.store.ingest("tsr",bad,NOW)
        self.assertEqual(len(active_restrictions(self.store,NOW)),1)

    def test_tsr_match_needs_location_validity_and_direction(self):
        self.store.ingest("tsr",tsr(),NOW)
        self.assertEqual(len(match_restrictions(self.store,["East Boldon"],NOW)),1)
        self.assertEqual(match_restrictions(self.store,["East Boldon"],NOW,direction="down"),[])
        self.assertEqual(match_restrictions(self.store,["Durham"],NOW),[])
        self.assertEqual(match_restrictions(self.store,["East Boldon"],NOW+2*86400),[])
        self.assertEqual(match_restrictions(self.store,["East Boldon"],NOW)[0]["confidence"],"possible contributor")

    def test_slow_running_matches_tsr_and_excludes_restricted_baseline(self):
        for i in range(3):self.sample(NOW-600+i*60,30,f"2I0{i}")
        self.store.ingest("tsr",tsr(),NOW)
        self.sample(NOW,120,"2I10")
        finding=snapshot(self.store,NOW)["findings"][0]
        self.assertEqual(finding["details"]["restriction_matches"][0]["FromLocation"],"East Boldon")
        self.assertEqual(self.store.rows("SELECT restricted FROM samples ORDER BY id DESC LIMIT 1")[0]["restricted"],1)

    def test_corpus_smart_import_and_no_false_physical_edges(self):
        import_reference(self.store,"corpus",{"TIPLOCDATA":[{"STANOX":"12345","TIPLOC":"TEST","NLCDESC":"East Boldon","3ALPHA":"EBO"}]},NOW)
        import_reference(self.store,"smart",{"BERTHDATA":[{"TD":"T1","FROMBERTH":"1234","TOBERTH":"2345","STANOX":"12345","STANME":"E BOLDON","EVENT":"A","STEPTYPE":"B"}]},NOW)
        self.assertEqual(self.store.rows("SELECT description FROM labels WHERE area='T1'")[0]["description"],"East Boldon")
        self.assertEqual(self.store.rows("SELECT source FROM edges WHERE area='T1'")[0]["source"],"SMART reference")
        with self.assertRaises(InvalidFeedMessage):import_reference(self.store,"smart",{"wrong":[]},NOW)
        self.assertEqual(len(self.store.rows("SELECT * FROM smart")),1)

    def activation(self,at=NOW-1200):
        self.store.ingest("trust",trust("0001",at,origin_dep_timestamp=str(int(at*1000)),creation_timestamp=str(int(at*1000)),train_uid="A12345"),NOW)

    def movement(self,at,planned,stanox,event,queued=None,**kwargs):
        self.store.ingest("trust",trust("0003",queued or at,actual_timestamp=str(int(at*1000)),planned_timestamp=str(int(planned*1000)),loc_stanox=stanox,event_type=event,variation_status="LATE",timetable_variation="3",**kwargs),NOW)

    def route_refs(self):
        import_reference(self.store,"corpus",{"TIPLOCDATA":[{"STANOX":"11111","TIPLOC":"SUN","NLCDESC":"Sunderland"},{"STANOX":"22222","TIPLOC":"NCL","NLCDESC":"Newcastle"}]},NOW)

    def test_trust_delay_gain_matches_intermediate_east_boldon(self):
        self.route_refs();self.activation();self.store.ingest("tsr",tsr(),NOW)
        self.movement(NOW-900,NOW-960,"11111","DEPARTURE")
        self.movement(NOW,NOW-240,"22222","ARRIVAL")
        findings=snapshot(self.store,NOW)["findings"]
        self.assertEqual(len(findings),1)
        self.assertEqual(findings[0]["details"]["extra_seconds"],180)
        self.assertIn("East Boldon",findings[0]["details"]["locations"])
        self.assertEqual(len(findings[0]["details"]["restriction_matches"]),1)

    def test_trust_correction_removes_false_delay_finding(self):
        self.route_refs();self.activation()
        self.movement(NOW-900,NOW-960,"11111","DEPARTURE")
        self.movement(NOW,NOW-240,"22222","ARRIVAL")
        self.assertEqual(len(snapshot(self.store,NOW)["findings"]),1)
        self.movement(NOW-180,NOW-240,"22222","ARRIVAL",queued=NOW+5,correction_ind="true")
        self.assertEqual(snapshot(self.store,NOW)["findings"],[])

    def test_offroute_and_station_dwell_do_not_become_running_loss(self):
        self.route_refs();self.activation()
        self.movement(NOW-900,NOW-960,"11111","ARRIVAL")
        self.movement(NOW,NOW-240,"11111","DEPARTURE")
        self.movement(NOW+600,NOW+60,"22222","ARRIVAL",offroute_ind="true")
        self.assertEqual(snapshot(self.store,NOW+600)["findings"],[])

    def test_trust_early_sign_and_cancellation_reinstatement_identity(self):
        self.activation()
        body=trust("0003",NOW,actual_timestamp=str(int(NOW*1000)),planned_timestamp=str(int((NOW+60)*1000)),loc_stanox="11111",event_type="ARRIVAL",variation_status="EARLY",timetable_variation="1")
        self.store.ingest("trust",body,NOW)
        self.assertEqual(train_detail(self.store,"2A01",NOW)["runs"][0]["reports"][0]["details"]["delay_minutes"],-1)
        self.store.ingest("trust",trust("0002",NOW+1,canx_timestamp=str(int((NOW+1)*1000))),NOW+1)
        self.assertEqual(self.store.rows("SELECT status FROM runs")[0]["status"],"cancelled")
        self.store.ingest("trust",trust("0005",NOW+2,reinstatement_timestamp=str(int((NOW+2)*1000))),NOW+2)
        self.store.ingest("trust",trust("0007",NOW+3,event_timestamp=str(int((NOW+3)*1000)),revised_train_id="120A01AA02"),NOW+3)
        self.assertEqual(self.store.rows("SELECT status,headcode FROM runs")[0],{"status":"reinstated","headcode":"0A01"})

    def test_late_activation_links_orphan_reports(self):
        self.route_refs()
        self.movement(NOW-900,NOW-960,"11111","DEPARTURE")
        self.movement(NOW,NOW-240,"22222","ARRIVAL")
        self.assertEqual(snapshot(self.store,NOW)["findings"],[])
        self.activation()
        self.assertEqual(len(snapshot(self.store,NOW)["findings"]),1)
        self.assertEqual(self.store.rows("SELECT status FROM runs")[0]["status"],"running")
        self.movement(NOW-180,NOW-240,"22222","ARRIVAL",queued=NOW+5,correction_ind="true")
        self.assertEqual(len(self.store.rows("SELECT * FROM trust_events WHERE kind='0003'")),2)
        self.assertEqual(snapshot(self.store,NOW)["findings"],[])

    def test_headcode_collision_returns_candidates_not_one_identity(self):
        self.activation()
        self.store.ingest("trust",trust("0001",NOW-600,train="342A01AB02",origin_dep_timestamp=str(int((NOW-600)*1000)),creation_timestamp=str(int((NOW-600)*1000)),train_uid="B12345"),NOW)
        detail=train_detail(self.store,"2A01",NOW)
        self.assertEqual(len(detail["runs"]),2);self.assertIn("not a unique",detail["identity_note"])

    def test_trust_bst_documented_adjustment_not_applied_to_activation(self):
        self.assertEqual(trust_timestamp(NOW*1000,"network-rail"),NOW-3600)
        jan=dt.datetime(2026,1,2,10,tzinfo=dt.timezone.utc).timestamp()
        self.assertEqual(trust_timestamp(jan*1000,"network-rail"),jan)

    def test_vstp_delete_tombstone_prevents_old_create(self):
        def msg(action,at):return {"VSTPCIFMsgV1":{"timestamp":str(int(at*1000)),"schedule":{
            "CIF_train_uid":"A12345","CIF_stp_indicator":"N","schedule_start_date":"2026-10-02",
            "schedule_end_date":"2026-10-02","transaction_type":action,"schedule_segment":[]}}}
        self.store.ingest("vstp",msg("Delete",NOW),NOW)
        self.store.ingest("vstp",msg("Create",NOW-30),NOW)
        self.assertTrue(json.loads(self.store.rows("SELECT details FROM schedules")[0]["details"])["deleted"])

    def test_no_active_tsr_does_not_claim_no_restrictions(self):
        health=next(h for h in snapshot(self.store,NOW)["feeds"] if h["feed"]=="tsr")
        self.assertIn("waiting",health["state"])

    def test_conflicting_supplied_berth_names_flagged(self):
        n=next(n for n in snapshot(self.store,NOW)["nodes"] if n["id"]=="T3:6298")
        self.assertTrue(n["ambiguous"])

    def test_disabled_feeds_and_removed_areas_not_misreported_live(self):
        self.put(td("CC",NOW,to="6239",descr="2I01"))
        self.store.config=replace(self.cfg,areas=("T1",),trust_enabled=False)
        self.store.connected(NOW+1)
        data=snapshot(self.store,NOW+2)
        self.assertFalse(any(n["area"]=="T3" for n in data["nodes"]))
        self.assertEqual(next(h for h in data["feeds"] if h["feed"]=="trust")["state"],"disabled")


if __name__=="__main__":unittest.main()
