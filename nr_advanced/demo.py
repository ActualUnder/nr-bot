"""Clearly labelled synthetic data. Never contacts Network Rail or Discord."""
from __future__ import annotations

import asyncio
import json
import time

from .analysis import geography


def td(kind, at, area="T3", **values):
    return {kind+"_MSG": {"area_id":area,"msg_type":kind,"time":str(int(at*1000)),**values}}


def seed(runtime):
    s=runtime.store;now=time.time()
    s.set_meta("demo",True);s.connected(now-7200)
    for i in range(10):
        end=now-600+i*40
        s.ingest("td",td("CA",end-30,**{"from":"6237","to":"6239","descr":f"2I{i:02}"}),end-30)
        s.ingest("td",td("CA",end,**{"from":"6239","to":"6243","descr":f"2I{i:02}"}),end)
    s.ingest("tsr",{"TSRBatchMsgV1":{"TSRBatchMsg":{
        "routeGroupCode":"DEMO","routeGroupCoverage":"full","publishDate":str(int((now-120)*1000)),
        "publishSource":"DEMO-WON — synthetic example","WONStartDate":str(int((now-86400)*1000)),
        "WONEndDate":str(int((now+86400)*1000)),"tsr":[{
            "TSRID":"DEMO-1","TSRReference":"DEMO ONLY","FromLocation":"East Boldon","ToLocation":"East Boldon",
            "LineName":"Up","Direction":"up","PassengerSpeed":"30","FreightSpeed":"20",
            "Reason":"Synthetic example — not an actual restriction","ValidFromDate":str(int((now-86400)*1000)),
            "ValidToDate":str(int((now+86400)*1000))}]}}},now)
    s.ingest("td",td("CA",now-125,**{"from":"6237","to":"6239","descr":"2I11"}),now-125)
    s.ingest("td",td("CA",now-5,**{"from":"6239","to":"6243","descr":"2I11"}),now-5)
    routes={"T1":["Newcastle","Manors","Heaton","Benton","Cramlington","Morpeth"],
            "T0":["Northumberland Park","Seaton Delaval","Newsham","Bedlington","Ashington"],
            "T2":["Chester-le-Street","Durham","Darlington"]}
    with s.lock,s.db:
        for area,names in routes.items():
            for i,name in enumerate(names):
                s.db.execute("INSERT INTO labels VALUES(?,?,?,?)",(area,f"D{i:03}",name,"synthetic demo berth; not a real ID"))
                if i:
                    s.db.execute("INSERT INTO edges VALUES(?,?,?,'demo link',0,NULL)",(area,f"D{i-1:03}",f"D{i:03}"))
    for area,headcode in (("T1","1D01"),("T0","2D02"),("T2","1D03")):
        s.ingest("td",td("CC",now,area,to="D001",descr=headcode),now)
    for b,h in (("6242","2I12"),("6227","2I14"),("6280","2I16"),("P807","2I18"),("6254","2I20")):
        s.ingest("td",td("CC",now,to=b,descr=h),now)
    s.ingest("td",td("CC",now-3700,to="6298",descr="2I99"),now)
    for area in s.config.areas:s.ingest("td",td("CT",now,area),now)


async def animate(runtime):
    route="6239 6243 6245 6247 6249 6265 6267 6269 6273 6275 6277 6279 P764".split()
    i=0
    while True:
        now=time.time();s=runtime.store
        for area in s.config.areas:s.ingest("td",td("CT",now,area),now)
        if i==0:s.ingest("td",td("CC",now,to=route[0],descr="2I08"),now)
        else:s.ingest("td",td("CA",now,**{"from":route[i-1],"to":route[i],"descr":"2I08"}),now)
        i+=1
        if i==len(route):
            await asyncio.sleep(5);s.ingest("td",td("CB",time.time(),**{"from":route[-1],"descr":"2I08"}));i=0
        await asyncio.sleep(5)
