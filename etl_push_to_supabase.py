#!/usr/bin/env python3
"""
ETL script: push local SQLite contacts DB to Supabase Postgres (DATABASE_URL in .env)

Usage: python etl_push_to_supabase.py

This reads `data/contacts.db` and upserts rows into the target Postgres DB.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from typing import Optional

import psycopg2
import psycopg2.extras


def load_env(path: str = ".env") -> dict:
    env = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # support KEY=VALUE and KEY = "value"
            m = re.match(r"^([A-Za-z0-9_]+)\s*=\s*(.*)$", line)
            if not m:
                continue
            k, v = m.group(1), m.group(2)
            if v.startswith('"') and v.endswith('"'):
                v = v[1:-1]
            env[k] = v
    return env


def ensure_table(conn):
    create_sql = '''
    CREATE TABLE IF NOT EXISTS contacts (
      id BIGSERIAL PRIMARY KEY,
      phone TEXT UNIQUE NOT NULL,
      raw_phone TEXT,
      name TEXT,
      addr TEXT,
      loc TEXT,
      city TEXT,
      state TEXT,
      other JSONB,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
    '''
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()


def push(sqlite_path: str, database_url: str, batch: int = 1000):
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(sqlite_path)

    # connect to postgres with psycopg2
    pg_conn = psycopg2.connect(database_url)
    ensure_table(pg_conn)

    sconn = sqlite3.connect(sqlite_path)
    sconn.row_factory = sqlite3.Row
    scur = sconn.cursor()

    scur.execute("SELECT count(*) FROM contacts")
    total = scur.fetchone()[0]
    print(f"Total rows in sqlite: {total}")

    select_sql = "SELECT phone, raw_phone, name, addr, loc, city, state, other, created_at, updated_at FROM contacts"
    scur.execute(select_sql)

    # Use DO NOTHING on conflict to ensure existing remote rows are not overwritten
    upsert_sql = '''
    INSERT INTO contacts (phone, raw_phone, name, addr, loc, city, state, other, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
    ON CONFLICT (phone) DO NOTHING
    '''

    inserted = 0
    pg_cur = pg_conn.cursor()
    try:
        while True:
            rows = scur.fetchmany(batch)
            if not rows:
                break
            params = []
            for r in rows:
                other_val = r["other"]
                try:
                    if other_val is None or other_val == "":
                        other_json = {}
                    elif isinstance(other_val, str):
                        other_json = json.loads(other_val)
                    else:
                        other_json = other_val
                except Exception:
                    other_json = {"raw": str(other_val)}

                params.append(
                    (
                        r["phone"],
                        r["raw_phone"],
                        r["name"],
                        r["addr"],
                        r["loc"],
                        r["city"],
                        r["state"],
                        json.dumps(other_json),
                        r["created_at"],
                        r["updated_at"],
                    )
                )

            psycopg2.extras.execute_batch(pg_cur, upsert_sql, params)
            pg_conn.commit()
            inserted += len(params)
    finally:
        pg_cur.close()

    sconn.close()
    pg_conn.close()
    print(f"Finished. Upserted approx {inserted} rows (rows processed).")


if __name__ == "__main__":
    env = load_env(".env")
    dburl = env.get("DATABASE_URL")
    if not dburl:
        print("DATABASE_URL not found in .env. Please set it.")
        raise SystemExit(1)
    # mask display
    print("Using DATABASE_URL from .env (hidden)")
    push(os.path.join("data", "contacts.db"), dburl)
