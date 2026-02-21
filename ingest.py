"""Ingest CSV and upsert into Supabase/Postgres.

Usage:
    python ingest.py [path/to/file.csv]

If DATABASE_URL is not set in environment, falls back to sqlite:///data.db for local testing.
"""
import sys
import os
import json
import argparse
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from db import get_engine, get_dialect_name
from models import Base, Record
from utils import (
    parse_raw_json_field,
    extract_state_country,
    extract_city,
    classify,
    normalize_phone,
)


def ensure_tables(engine):
    Base.metadata.create_all(engine)


def row_to_record_dict(row):
    # row: pandas Series
    raw = parse_raw_json_field(row.get("raw_db1_summary") or row.get("raw"))

    number = str(row.get("number")) if row.get("number") else None
    processed_at = row.get("processed_at")

    address = raw.get("address") or row.get("result_loc") or ""
    result_loc = row.get("result_loc") or raw.get("belong_area") or ""
    belong_area = raw.get("belong_area") or row.get("result_loc") or ""

    state, country = extract_state_country(belong_area)
    city = extract_city(address, result_loc, belong_area)
    is_pune, city_category = classify(city, state)

    record = {
        "phone": number,
        "e164_phone": raw.get("e164_tel_number") or raw.get("format_tel_number"),
        "timestamp_ms": int(processed_at) if processed_at and str(processed_at).isdigit() else None,
        "batch_id": None,
        "uid": row.get("uid"),

        "db1_success": bool(raw.get("status")) if raw.get("status") is not None else None,
        "db1_confidence": raw.get("confidence") or None,
        "db1_latency_ms": None,

        "name": raw.get("name") or row.get("result_name"),
        "operator": raw.get("operator"),
        "phone_type": raw.get("type"),
        "website": raw.get("website"),

        "address": address,
        "result_loc": result_loc,
        "belong_area": belong_area,

        "city": city,
        "state": state,
        "country": country,

        "is_pune": is_pune,
        "city_category": city_category,

        "raw_json": raw,
    }
    return record


def upsert_records(engine, records):
    dialect = get_dialect_name(engine)
    Session = sessionmaker(bind=engine)

    if dialect == "postgresql":
        # use efficient ON CONFLICT upsert
        from sqlalchemy.dialects.postgresql import insert

        with engine.begin() as conn:
            for r in records:
                stmt = insert(Record.__table__).values(r)
                # exclude phone from being overwritten with None
                update_dict = {k: v for k, v in r.items() if k != "phone"}
                stmt = stmt.on_conflict_do_update(index_elements=["phone"], set_=update_dict)
                conn.execute(stmt)
    else:
        # sqlite or others: do a find+update or insert
        session = Session()
        try:
            for r in records:
                phone = r.get("phone")
                if not phone:
                    continue
                existing = session.query(Record).filter_by(phone=phone).first()
                if existing:
                    for k, v in r.items():
                        setattr(existing, k, v)
                else:
                    rec = Record(**r)
                    session.add(rec)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def ingest_file(path):
    engine = get_engine()
    ensure_tables(engine)

    df = pd.read_csv(path, dtype=str)
    df = df.fillna("")

    ingest_dataframe(df, engine=engine)
    print(f"Ingested {len(df)} rows from {path}")


def ingest_dataframe(df, engine=None, batch_size=1000, dry_run=False):
    """Ingest a pandas DataFrame. If engine is None, uses get_engine().

    DataFrame should have columns similar to the CSV (number, result_name, result_loc, uid, processed_at, raw_db1_summary)
    """
    if engine is None:
        engine = get_engine()

    ensure_tables(engine)

    df = df.fillna("")

    # validate & normalize phones; collect failed rows
    valid_records = []
    failed_rows = []
    for _, row in df.iterrows():
        raw_number = row.get("number") or row.get("phone") or row.get("tel_number")
        phone_norm, e164 = normalize_phone(raw_number)
        if not phone_norm:
            failed_rows.append(row.to_dict())
            continue
        r = row_to_record_dict(row)
        r["phone"] = phone_norm
        # prefer e164 from normalization if not present
        if not r.get("e164_phone") and e164:
            r["e164_phone"] = e164
        valid_records.append(r)

    # perform upsert in batches to avoid large transactions
    if dry_run:
        # write normalized preview and exit
        out_df = pd.DataFrame(valid_records)
        out_path = os.getenv("DRY_RUN_OUT", "dry_run_normalized.csv")
        out_df.to_csv(out_path, index=False)
        logging.info("Dry-run: wrote %d normalized rows to %s", len(valid_records), out_path)
    else:
        # batch upserts
        for i in range(0, len(valid_records), batch_size):
            chunk = valid_records[i : i + batch_size]
            upsert_records(engine, chunk)
            logging.info("Upserted batch %d - %d", i, i + len(chunk))

    # write failed rows for inspection
    if failed_rows:
        failed_path = os.getenv("FAILED_ROWS_PATH", "failed_rows.csv")
        pd.DataFrame(failed_rows).to_csv(failed_path, index=False)
        logging.warning("Wrote %d failed rows to %s", len(failed_rows), failed_path)

    return len(valid_records)


def main():
    parser = argparse.ArgumentParser(description="Ingest CSV into Supabase/Postgres")
    parser.add_argument("path", nargs="?", default="input_csv/single_num_1a_part_001.csv")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true", help="Normalize and write output without DB writes")
    args = parser.parse_args()

    path = args.path
    if not os.path.exists(path):
        print(f"File not found: {path}")
        sys.exit(1)

    engine = get_engine()
    df = pd.read_csv(path, dtype=str).fillna("")
    count = ingest_dataframe(df, engine=engine, batch_size=args.batch_size, dry_run=args.dry_run)
    print(f"Processed {count} (valid) rows from {path}")


if __name__ == "__main__":
    main()
