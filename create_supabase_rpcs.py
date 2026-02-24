#!/usr/bin/env python3
"""
Create RPC functions in Supabase Postgres for get_contacts, get_Pune_contacts, get_MH_contacts.

Reads DATABASE_URL from .env in repo root and executes CREATE OR REPLACE FUNCTION SQL.
"""
import os
import re
import sys
import psycopg2


def load_env(path: str = ".env"):
    env = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^([A-Za-z0-9_]+)\s*=\s*(.*)$", line)
            if not m:
                continue
            k, v = m.group(1), m.group(2)
            if v.startswith('"') and v.endswith('"'):
                v = v[1:-1]
            env[k] = v
    return env


RPC_GET_PUNE = r"""
CREATE OR REPLACE FUNCTION public.get_pune_contacts(p_limit integer DEFAULT 100)
RETURNS SETOF public.contacts
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF p_limit IS NULL OR p_limit <= 0 THEN
    p_limit := 2147483647; -- effectively no limit
  END IF;

  RETURN QUERY
    SELECT * FROM public.contacts
    WHERE lower(coalesce(addr,'') || ' ' || coalesce(loc,'') || ' ' || coalesce(city,'') || ' ' || coalesce(state,'') || ' ' || coalesce(other::text,'')) LIKE '%pune%'
    ORDER BY updated_at DESC, name
    LIMIT p_limit;
END;
$$;
"""

RPC_GET_MH = r"""
CREATE OR REPLACE FUNCTION public.get_mh_contacts(p_limit integer DEFAULT 100)
RETURNS SETOF public.contacts
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF p_limit IS NULL OR p_limit <= 0 THEN
    p_limit := 2147483647;
  END IF;

  RETURN QUERY
    SELECT * FROM public.contacts
    WHERE (
      lower(coalesce(addr,'') || ' ' || coalesce(loc,'') || ' ' || coalesce(city,'') || ' ' || coalesce(state,'') || ' ' || coalesce(other::text,'')) LIKE '%maharashtra%'
      OR lower(coalesce(addr,'') || ' ' || coalesce(loc,'') || ' ' || coalesce(city,'') || ' ' || coalesce(state,'') || ' ' || coalesce(other::text,'')) LIKE '%mh%'
    )
    ORDER BY updated_at DESC, name
    LIMIT p_limit;
END;
$$;
"""

RPC_GET_CONTACTS = r"""
CREATE OR REPLACE FUNCTION public.get_contacts(p_limit integer DEFAULT 100)
RETURNS SETOF public.contacts
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  IF p_limit IS NULL OR p_limit <= 0 THEN
    p_limit := 2147483647;
  END IF;

  RETURN QUERY
    SELECT * FROM public.contacts ORDER BY updated_at DESC, name LIMIT p_limit;
END;
$$;
"""


def main():
    env = load_env()
    dburl = env.get("DATABASE_URL")
    if not dburl:
        print("DATABASE_URL not found in .env")
        sys.exit(1)

    conn = psycopg2.connect(dburl)
    cur = conn.cursor()
    try:
        print("Creating RPC: get_pune_contacts")
        cur.execute(RPC_GET_PUNE)
        print("Creating RPC: get_mh_contacts")
        cur.execute(RPC_GET_MH)
        print("Creating RPC: get_contacts")
        cur.execute(RPC_GET_CONTACTS)
        conn.commit()
        print("RPC functions created successfully.")
    except Exception as e:
        conn.rollback()
        print("Error creating RPCs:", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
