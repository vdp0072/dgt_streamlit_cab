#!/usr/bin/env python3
"""
Create aggregate RPCs on Supabase/Postgres:
 - get_total_contacts()
 - get_daily_added(p_days integer DEFAULT 30)
 - get_distribution()

Reads DATABASE_URL from .env and executes SQL. Grants EXECUTE to anon.
"""
import re
import os
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


RPCS = [
    (
        "get_total_contacts",
        '''
CREATE OR REPLACE FUNCTION public.get_total_contacts()
RETURNS TABLE(total bigint)
LANGUAGE sql STABLE
AS $$
  select count(*)::bigint as total from public.contacts;
$$;
''',
    ),
    (
        "get_daily_added",
        '''
CREATE OR REPLACE FUNCTION public.get_daily_added(p_days integer DEFAULT 30)
RETURNS TABLE(day date, cnt bigint)
LANGUAGE sql STABLE
AS $$
  select
    date(created_at) as day,
    count(*)::bigint as cnt
  from public.contacts
  where created_at >= (current_date - (p_days - 1))
  group by date(created_at)
  order by day;
$$;
''',
    ),
    (
        "get_distribution",
        '''
CREATE OR REPLACE FUNCTION public.get_distribution()
RETURNS TABLE(label text, cnt bigint)
LANGUAGE sql STABLE
AS $$
  with matched as (
    select
      case
        when lower(coalesce(addr,'') || ' ' || coalesce(loc,'') || ' ' || coalesce(city,'') || ' ' || coalesce(state,'') || ' ' || coalesce(other::text,'')) like '%pune%' then 'Pune'
        when lower(coalesce(addr,'') || ' ' || coalesce(loc,'') || ' ' || coalesce(city,'') || ' ' || coalesce(state,'') || ' ' || coalesce(other::text,'')) like '%maharashtra%' then 'Maharashtra'
        when lower(coalesce(addr,'') || ' ' || coalesce(loc,'') || ' ' || coalesce(city,'') || ' ' || coalesce(state,'') || ' ' || coalesce(other::text,'')) like '%mh%' then 'Maharashtra'
        else 'Other'
      end as label
    from public.contacts
  )
  select label, count(*)::bigint as cnt
  from matched
  group by label;
$$;
''',
    ),
]


def main():
    env = load_env()
    dburl = env.get("DATABASE_URL")
    if not dburl:
        print("DATABASE_URL not found in .env")
        sys.exit(1)

    conn = psycopg2.connect(dburl)
    cur = conn.cursor()
    try:
        for name, sql in RPCS:
            print(f"Creating RPC: {name}")
            cur.execute(sql)
            # grant execute to anon
            try:
                cur.execute(f"GRANT EXECUTE ON FUNCTION public.{name}() TO anon;")
            except Exception:
                # try with integer arg for daily_added
                cur.execute(f"GRANT EXECUTE ON FUNCTION public.{name}(integer) TO anon;")
        conn.commit()
        print("RPCs created and granted execute to anon.")
    except Exception as e:
        conn.rollback()
        print("Error creating RPCs:", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
