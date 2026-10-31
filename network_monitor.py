#!/usr/bin/env python3
"""Offline import/inspection and isolated map demo. Production starts with bot.py."""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from dotenv import load_dotenv

from nr_advanced.analysis import snapshot
from nr_advanced.config import AdvancedConfig
from nr_advanced.feeds import import_reference
from nr_advanced.runtime import NetworkRuntime
from nr_advanced.store import NetworkStore

ROOT=Path(__file__).resolve().parent


async def demo(args):
    from nr_advanced.demo import seed, animate
    # Always use a fresh temporary database and never load the production env.
    with tempfile.TemporaryDirectory(prefix="nr-map-demo-") as folder:
        cfg=AdvancedConfig(Path(folder),ROOT,map_host=args.bind,map_port=args.port,
                           map_token=args.token,reference_enabled=False,trust_clock="utc")
        runtime=NetworkRuntime(cfg)
        seed(runtime)
        await runtime.start()
        if not runtime.web or not runtime.web.runner:
            raise RuntimeError(runtime.summary())
        print(f"SYNTHETIC DEMO ONLY: http://{args.bind}:{args.port}",flush=True)
        try:await animate(runtime)
        finally:await runtime.stop();runtime.store.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest="command",required=True)
    d=sub.add_parser("demo",help="Run the map with labelled synthetic data, no credentials needed")
    d.add_argument("--bind",default="127.0.0.1");d.add_argument("--port",type=int,default=8791)
    d.add_argument("--token",default="",help="Required for a non-loopback bind")
    for name in ("import","status","backup"):
        p=sub.add_parser(name)
        p.add_argument("--data-dir",type=Path,help="Network database directory; default NRBOT_DATA_DIR/network")
        if name=="import":p.add_argument("dataset",choices=["corpus","smart","tsr","vstp","schedule"]);p.add_argument("file",type=Path)
        if name=="backup":p.add_argument("file",type=Path)
    args=parser.parse_args()
    if args.command=="demo":
        try:asyncio.run(demo(args))
        except KeyboardInterrupt:pass
        return
    load_dotenv("/etc/nr-bot.env");load_dotenv("/etc/metro-bot.env");load_dotenv()
    data_root=Path(os.getenv("NRBOT_DATA_DIR") or os.getenv("METRO_BOT_DATA_DIR") or "/var/lib/nr-bot")
    cfg=AdvancedConfig.from_env(data_root,ROOT)
    if args.data_dir:cfg=replace(cfg,data_dir=args.data_dir)
    store=NetworkStore(cfg,reset_connections=False)
    try:
        if args.command=="status":
            data=snapshot(store);print(json.dumps({"feeds":data["feeds"],"berths":len(data["nodes"]),"samples":data["baseline_samples"]},indent=2))
        elif args.command=="backup":
            if args.file.exists():raise ValueError("Backup destination already exists; choose a new filename")
            args.file.parent.mkdir(parents=True,exist_ok=True);store.backup(args.file);print(f"Backup saved: {args.file}")
        else:
            opener=gzip.open if args.file.suffix==".gz" else open
            before=sum(h["rejected"] for h in store.rows("SELECT rejected FROM feed_health"))
            with opener(args.file,"rt",encoding="utf-8-sig") as f:
                if args.dataset=="schedule":
                    count=0
                    for line in f:
                        if line.strip():count+=store.ingest("schedule",line)
                elif args.dataset in {"corpus","smart"}:count=import_reference(store,args.dataset,json.load(f),time.time())
                else:count=store.ingest(args.dataset,json.load(f))
            after=sum(h["rejected"] for h in store.rows("SELECT rejected FROM feed_health"))
            print(f"Processed {count} rows/messages; rejected {after-before}. Existing good data is retained on invalid snapshots.")
            if after>before:raise SystemExit(2)
    finally:store.close()


if __name__=="__main__":main()
