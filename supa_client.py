#!/usr/bin/env python3
"""Simple Python client to call Supabase RPCs created for contacts.

Usage:
  python supa_client.py get_Pune_contacts_200 --export out.csv
  python supa_client.py get_contacts --max all

Reads SUPABASE_URL and SUPABASE_ANON_KEY from environment.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import csv
from typing import Any, Dict, List, Optional

import requests


def parse_limit(cmdname: str, max_arg: Optional[str]) -> Optional[int]:
    # suffix _NNN
    m = re.search(r"_(\d+)$", cmdname)
    limit = 100
    if m:
        try:
            limit = int(m.group(1))
        except Exception:
            limit = 100

    if max_arg is not None:
        if str(max_arg).lower() == "all":
            return 0
        try:
            return int(max_arg)
        except Exception:
            return limit
    return limit


def rpc_name_for(cmdname: str) -> Optional[str]:
    n = cmdname.lower()
    if n.startswith("get_pune_contacts"):
        return "get_pune_contacts"
    if n.startswith("get_mh_contacts"):
        return "get_mh_contacts"
    if n.startswith("get_contacts"):
        return "get_contacts"
    return None


def fetch_rpc(supabase_url: str, key: str, rpc: str, p_limit: Optional[int]) -> List[Dict[str, Any]]:
    base = supabase_url.rstrip('/')
    # ensure caller can provide either root URL or include /rest/v1
    if not base.lower().endswith('/rest/v1'):
        base = base + '/rest/v1'
    url = f"{base}/rpc/{rpc}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    body = {}
    if p_limit is not None:
        # rpc treats 0 or negative as all; pass 0 for all
        body["p_limit"] = p_limit
    resp = requests.post(url, headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()


def to_csv(rows: List[Dict[str, Any]], out_path: Optional[str]):
    if not rows:
        print("No rows returned")
        return
    # determine fieldnames as union of keys
    fieldnames = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    def fmt(v):
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False)
        return str(v)

    if out_path:
        with open(out_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                out = {k: fmt(r.get(k)) for k in fieldnames}
                writer.writerow(out)
        print(f"Wrote {len(rows)} rows to {out_path}")
    else:
        # print first 10 rows pretty
        for i, r in enumerate(rows[:10]):
            print(json.dumps(r, ensure_ascii=False))
        if len(rows) > 10:
            print(f"... {len(rows)} rows total")


def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Command name like get_Pune_contacts_200")
    parser.add_argument("--export", dest="export", help="Write output CSV to this path")
    parser.add_argument("--max", dest="max", help="Override suffix limit; use 'all' for no limit or integer")
    args = parser.parse_args(argv)

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_ANON_KEY")
    if not supabase_url or not supabase_key:
        print("Set SUPABASE_URL and SUPABASE_ANON_KEY environment variables")
        sys.exit(1)

    rpc = rpc_name_for(args.command)
    if not rpc:
        print("Unknown command. Use get_Pune_contacts*, get_MH_contacts*, or get_contacts*")
        sys.exit(1)

    limit = parse_limit(args.command, args.max)
    # convert p_limit: None -> omit, 0 -> all
    p_limit = None if limit is None else limit
    rows = fetch_rpc(supabase_url, supabase_key, rpc, p_limit)
    to_csv(rows, args.export)


if __name__ == '__main__':
    main()
