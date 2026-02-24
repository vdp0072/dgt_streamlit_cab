#!/usr/bin/env python3
"""
Console pipeline to ingest CSVs into a SQLite contacts DB and query contacts.

Usage examples:
  python pipeline.py add path/to/file1.csv [file2.csv ...]
  python pipeline.py get_Pune_contacts --export exports/pune.csv
  python pipeline.py get_MH_contacts --export exports/mh.csv

Defaults:
  - SQLite DB stored at data/contacts.db
  - Phone canonicalization: keep last 10 digits

This script auto-detects common column names and stores unmapped columns in `other` JSON.
"""
from __future__ import annotations

import os
import re
import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import click
import sys
import pandas as pd
from pathlib import Path


DB_PATH = os.path.join("data", "contacts.db")


def ensure_dirs():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs("exports", exist_ok=True)


def get_conn():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            raw_phone TEXT,
            name TEXT,
            addr TEXT,
            loc TEXT,
            city TEXT,
            state TEXT,
            other TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()


def normalize_phone(raw: Optional[str]) -> str:
    if raw is None:
        return ""
    s = re.sub(r"\D+", "", str(raw))
    if not s:
        return ""
    # keep last 10 digits as canonical
    return s[-10:]


COMMON_PHONE_COLS = [
    "phone",
    "mobile",
    "number",
    "contact",
    "phone_number",
    "ph",
    "msisdn",
]

COMMON_NAME_COLS = ["name", "full_name", "fullname"]
COMMON_ADDR_COLS = ["addr", "address", "street"]
COMMON_LOC_COLS = ["loc", "location"]
COMMON_CITY_COLS = ["city", "town"]
COMMON_STATE_COLS = ["state", "region"]


def detect_mapping(columns: List[str]) -> Dict[str, Optional[str]]:
    cols = [c.lower() for c in columns]
    mapping = {"phone": None, "name": None, "addr": None, "loc": None, "city": None, "state": None}

    for c in COMMON_PHONE_COLS:
        if c in cols:
            mapping["phone"] = columns[cols.index(c)]
            break

    for c in COMMON_NAME_COLS:
        if c in cols:
            mapping["name"] = columns[cols.index(c)]
            break

    for c in COMMON_ADDR_COLS:
        if c in cols:
            mapping["addr"] = columns[cols.index(c)]
            break

    for c in COMMON_LOC_COLS:
        if c in cols:
            mapping["loc"] = columns[cols.index(c)]
            break

    for c in COMMON_CITY_COLS:
        if c in cols:
            mapping["city"] = columns[cols.index(c)]
            break

    for c in COMMON_STATE_COLS:
        if c in cols:
            mapping["state"] = columns[cols.index(c)]
            break

    return mapping


def row_to_record(row: pd.Series, mapping: Dict[str, Optional[str]]) -> Optional[Dict]:
    # Extract raw phone
    phone_col = mapping.get("phone")
    raw_phone = None
    if phone_col and phone_col in row:
        raw_phone = row[phone_col]
    else:
        # try to find any column that looks like a number
        for v in row.index:
            if re.search(r"\d", str(row[v] or "")) and len(str(row[v] or "")) >= 6:
                raw_phone = row[v]
                break

    norm = normalize_phone(raw_phone)
    if not norm:
        return None

    def get_field(key):
        col = mapping.get(key)
        if col and col in row:
            val = row[col]
            return val if val != "" else None
        return None

    name = get_field("name")
    addr = get_field("addr")
    loc = get_field("loc")
    city = get_field("city")
    state = get_field("state")

    # other columns
    other = {}
    mapped_cols = {v for v in mapping.values() if v}
    for c in row.index:
        if c not in mapped_cols:
            other[c] = row[c]

    return {
        "phone": norm,
        "raw_phone": raw_phone,
        "name": name,
        "addr": addr,
        "loc": loc,
        "city": city,
        "state": state,
        "other": other,
    }


def upsert_contact(conn: sqlite3.Connection, rec: Dict):
    now = datetime.utcnow().isoformat()
    cur = conn.cursor()
    other_json = json.dumps(rec.get("other") or {})

    # convert empty strings to NULL by using None
    params = (
        rec["phone"],
        rec.get("raw_phone"),
        rec.get("name"),
        rec.get("addr"),
        rec.get("loc"),
        rec.get("city"),
        rec.get("state"),
        other_json,
        now,
        now,
    )

    # Use SQLite UPSERT. On conflict, update NULL fields only (prefer existing non-null values).
    # We'll set columns to COALESCE(excluded.col, contacts.col) so excluded wins if not null.
    cur.execute(
        """
        INSERT INTO contacts (phone, raw_phone, name, addr, loc, city, state, other, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(phone) DO UPDATE SET
            raw_phone = COALESCE(excluded.raw_phone, contacts.raw_phone),
            name = COALESCE(excluded.name, contacts.name),
            addr = COALESCE(excluded.addr, contacts.addr),
            loc = COALESCE(excluded.loc, contacts.loc),
            city = COALESCE(excluded.city, contacts.city),
            state = COALESCE(excluded.state, contacts.state),
            other = COALESCE(excluded.other, contacts.other),
            updated_at = excluded.updated_at
        """,
        params,
    )
    conn.commit()


@click.group()
def cli():
    """Contacts pipeline CLI"""


@cli.command("add")
@click.argument("files", nargs=-1, required=False, type=click.Path(exists=True))
@click.option("--input-folder", "input_folders", multiple=True, type=click.Path(exists=True, file_okay=False), help="Directory containing CSV files to ingest")
@click.option("--pattern", default="*.csv", help="Glob pattern for files in folders (default '*.csv')")
@click.option("--dry-run", is_flag=True, default=False, help="List files to ingest but do not process")
def add(files, input_folders, pattern, dry_run):
    """Add one or more CSV files into the contacts DB. You can pass individual files and/or use --input-folder to ingest all CSVs in a directory."""
    # Build list of files to process
    to_process: List[str] = []
    seen = set()

    # add explicit files first; if a positional arg is a directory, expand it using the same pattern
    for f in files:
        p = Path(f)
        if p.is_dir():
            # expand non-recursive
            for fpath in p.glob(pattern):
                if fpath.is_file():
                    full = str(fpath)
                    if full not in seen:
                        to_process.append(full)
                        seen.add(full)
        else:
            full = str(p)
            if full not in seen:
                to_process.append(full)
                seen.add(full)

    # expand folders (non-recursive by default)
    for folder in input_folders:
        p = Path(folder)
        for fpath in p.glob(pattern):
            if fpath.is_file():
                full = str(fpath)
                if full not in seen:
                    to_process.append(full)
                    seen.add(full)

    if not to_process:
        click.echo("No files found to ingest. Provide files or use --input-folder.")
        return

    click.echo(f"Files to ingest ({len(to_process)}):")
    for f in to_process:
        click.echo(f"  {f}")

    if dry_run:
        click.echo("Dry run: no files were processed.")
        return

    conn = get_conn()
    init_db(conn)
    total_in = 0
    total_skipped = 0

    for path in to_process:
        click.echo(f"Ingesting {path}...")
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        except Exception as e:
            click.echo(f"  Failed to read {path}: {e}")
            continue

        mapping = detect_mapping(list(df.columns))
        click.echo(f"  Detected mapping: {mapping}")

        for _, row in df.iterrows():
            rec = row_to_record(row, mapping)
            if rec is None:
                total_skipped += 1
                continue
            upsert_contact(conn, rec)
            total_in += 1

    click.echo(f"Done. Inserted/updated: {total_in}, skipped (no phone): {total_skipped}")


def query_contacts_sql(filter_terms: List[str], limit: Optional[int] = None) -> List[sqlite3.Row]:
    conn = get_conn()
    init_db(conn)
    cur = conn.cursor()

    params = []
    if filter_terms:
        # build WHERE clause: check each term against concatenation of addr/loc/city/state/other
        checks = []
        for term in filter_terms:
            checks.append("(lower(COALESCE(addr,'') || ' ' || COALESCE(loc,'') || ' ' || COALESCE(city,'') || ' ' || COALESCE(state,'') || ' ' || COALESCE(other,'')) LIKE ?)")
            params.append(f"%{term.lower()}%")
        where = " OR ".join(checks)
        sql = f"SELECT id, phone, raw_phone, name, addr, loc, city, state, other, created_at, updated_at FROM contacts WHERE {where} ORDER BY updated_at DESC, name"
    else:
        sql = "SELECT id, phone, raw_phone, name, addr, loc, city, state, other, created_at, updated_at FROM contacts ORDER BY updated_at DESC, name"

    if limit is not None:
        sql = sql + " LIMIT ?"
        params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()
    return rows


def rows_to_dataframe(rows: List[sqlite3.Row]) -> pd.DataFrame:
    records = []
    for r in rows:
        d = dict(r)
        # expand other JSON into a column
        try:
            other = json.loads(d.get("other") or "{}")
        except Exception:
            other = {}
        d["other_json"] = other
        records.append(d)
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    return df


@cli.command("dynamic_get")
@click.option("--cmd", "cmdname", required=True, help="Command name invoked (e.g. get_Pune_contacts_200)")
@click.option("--export", "export_path", type=click.Path(), help="Write results to CSV path")
@click.option("--max", "max_rows", default=None, help="Use 'all' or an integer to override suffix limit")
def dynamic_get(cmdname: str, export_path: Optional[str], max_rows: Optional[str]):
    """Internal dynamic get handler. Use the public CLI with names like get_Pune_contacts_200."""
    # parse suffix limit
    limit = 100
    m = re.search(r"_(\d+)$", cmdname)
    if m:
        try:
            limit = int(m.group(1))
        except Exception:
            limit = 100

    # handle --max override
    if max_rows is not None:
        if str(max_rows).lower() == "all":
            limit = None
        else:
            try:
                limit = int(max_rows)
            except Exception:
                limit = 100

    # decide which filter to use
    if cmdname.lower().startswith("get_pune_contacts"):
        filters = ["pune"]
    elif cmdname.lower().startswith("get_mh_contacts"):
        filters = ["maharashtra", "mh"]
    elif cmdname.lower().startswith("get_contacts"):
        filters = []
    else:
        click.echo(f"Unknown get command: {cmdname}")
        return

    rows = query_contacts_sql(filters, limit)
    df = rows_to_dataframe(rows)
    click.echo(f"Found {len(df)} contacts matching {cmdname}")
    if df.empty:
        return
    if export_path:
        df.to_csv(export_path, index=False)
        click.echo(f"Exported to {export_path}")
    else:
        for _, r in df.iterrows():
            click.echo(f"{r.get('name') or ''}\t{r.get('phone')}\t{r.get('city') or r.get('state') or ''}\t{(r.get('addr') or '')[:80]}")


if __name__ == "__main__":
    # Allow commands like: get_Pune_contacts_200 as direct subcommands.
    # If the first arg starts with get_, rewrite argv to call the dynamic_get command.
    if len(sys.argv) > 1 and sys.argv[1].lower().startswith("get_"):
        original = sys.argv[1]
        # replace with dynamic_get invocation: pipeline.py dynamic_get --cmd <original> [rest...]
        new_argv = [sys.argv[0], "dynamic_get", "--cmd", original]
        # append remaining args (like --export ... or --max ...)
        new_argv.extend(sys.argv[2:])
        sys.argv = new_argv
    cli()
