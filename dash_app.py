import sqlite3
from pathlib import Path
from typing import Tuple, List

import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime, timedelta
import sys
import os
import requests
import pandas as pd
from pandas.api import types as pdtypes
import inspect
import json
import matplotlib.pyplot as plt
from io import BytesIO


def make_jsonable_records(df: pd.DataFrame):
    """Convert a DataFrame to a list of JSON-safe Python dicts.
    Dates -> ISO strings, NA -> None, numpy scalars -> Python scalars.
    """
    if df is None or df.empty:
        return []
    s = df.copy()
    # convert datetimes to ISO strings
    for col in s.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(s[col]):
                s[col] = s[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    # replace NA-like with null
    s = s.where(pd.notnull(s), None)
    # use pandas' JSON roundtrip to get plain Python types
    try:
        json_text = s.to_json(orient="records", date_format="iso")
        records = json.loads(json_text)
    except Exception:
        # fallback: coerce to plain python types via iterrows
        records = []
        for _, row in s.iterrows():
            rec = {}
            for k, v in row.items():
                if pd.isna(v):
                    rec[k] = None
                elif isinstance(v, (pd.Timestamp, datetime)):
                    rec[k] = v.isoformat()
                else:
                    try:
                        if hasattr(v, "item"):
                            rec[k] = v.item()
                        else:
                            rec[k] = v
                    except Exception:
                        rec[k] = str(v)
            records.append(rec)
    return records


def render_vega_spec(spec: dict):
    try:
        sig = inspect.signature(st.vega_lite_chart)
        params = sig.parameters
        if 'width' in params:
            st.vega_lite_chart(spec, width='stretch')
        elif 'use_container_width' in params:
            st.vega_lite_chart(spec, use_container_width=True)
        else:
            st.vega_lite_chart(spec)
    except Exception:
        try:
            st.vega_lite_chart(spec)
        except Exception:
            st.write("Failed to render vega spec")


def render_line_matplotlib(records):
    # records: list of dicts with 'day' and 'count'
    if not records:
        return None
    df = pd.DataFrame(records)
    if 'day' in df.columns:
        df['day'] = pd.to_datetime(df['day'], errors='coerce')
    else:
        return None
    df = df.sort_values('day')
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.plot(df['day'], df['count'], marker='o')
    ax.set_xlabel('Date')
    ax.set_ylabel('Added')
    ax.grid(alpha=0.3)
    fig.autofmt_xdate()
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf


def render_pie_matplotlib(records):
    if not records:
        return None
    df = pd.DataFrame(records)
    labels = df['label'].astype(str).tolist()
    sizes = df['count'].astype(int).tolist()
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf
import json


DB_PATH = Path("data") / "contacts.db"


@st.cache_data
def load_contacts(db_path: str = str(DB_PATH)) -> pd.DataFrame:
    """Load contacts from local SQLite into a DataFrame and normalize fields."""
    conn = sqlite3.connect(db_path)
    # read relevant columns
    df = pd.read_sql_query(
        "SELECT phone, raw_phone, name, addr, loc, city, state, other, created_at FROM contacts",
        conn,
    )
    conn.close()

    # parse created_at to datetime
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # build a searchable text field (lowercased)
    def stringify(x):
        if pd.isna(x):
            return ""
        try:
            return str(x)
        except Exception:
            return ""

    for c in ["addr", "loc", "city", "state", "other"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].apply(stringify)

    df["search_text"] = (
        df["addr"].fillna("")
        + " "
        + df["loc"].fillna("")
        + " "
        + df["city"].fillna("")
        + " "
        + df["state"].fillna("")
        + " "
        + df["other"].fillna("")
    ).str.lower()

    return df


def categorize_distribution(df: pd.DataFrame) -> pd.DataFrame:
    s = df["search_text"]
    is_pune = s.str.contains("pune", na=False)
    is_mh = s.str.contains("maharashtra", na=False) | s.str.contains(r"\bmh\b", na=False)

    # prioritize Pune over Maharashtra if both match
    labels = []
    for p, m in zip(is_pune, is_mh):
        if p:
            labels.append("Pune")
        elif m:
            labels.append("Maharashtra")
        else:
            labels.append("Other")

    df_labels = pd.Series(labels, name="label")
    counts = df_labels.value_counts().reindex(["Pune", "Maharashtra", "Other"]).fillna(0).astype(int)
    return counts.reset_index().rename(columns={"index": "label", 0: "count"})


def daily_counts(df: pd.DataFrame, days: int) -> pd.DataFrame:
    today = datetime.utcnow().date()
    start = today - timedelta(days=days - 1)

    dd = df[~df["created_at"].isna()].copy()
    dd["day"] = dd["created_at"].dt.date
    counts = dd.groupby("day").size().rename("count").reset_index()

    # ensure all days present
    all_days = pd.DataFrame({"day": [start + timedelta(days=i) for i in range(days)]})
    merged = all_days.merge(counts, on="day", how="left").fillna(0)
    merged["count"] = merged["count"].astype(int)
    return merged


def main():
    st.set_page_config(page_title="Contacts Dashboard", layout="wide")
    st.title("Contacts Dashboard")

    st.markdown("Hosted on Supabase — for RPC endpoints contact admin: virendra@acadflip.com")

    # Decide mode: local (SQLite) or remote (Supabase). Default: remote.
    # Streamlit passes additional args after `--`; check sys.argv for '--local'.
    local_flag = False
    if "--local" in sys.argv:
        local_flag = True
    # Also allow env override
    if os.environ.get("DASH_LOCAL", "").lower() in ("1", "true", "yes"):
        local_flag = True

    if local_flag:
        st.info("Running in LOCAL mode: reading from local SQLite DB")
    else:
        st.info("Running in REMOTE mode: reading from Supabase RPCs (anon key required in secrets)")

    if local_flag:
        if not DB_PATH.exists():
            st.error(f"Local DB not found at {DB_PATH}. Run the pipeline to ingest data first.")
            return
        df = load_contacts()
    else:
        # remote mode: call RPCs. Use st.secrets first, then env vars.
        SUPABASE_URL = st.secrets.get("SUPABASE_URL") if hasattr(st, "secrets") else None
        SUPABASE_KEY = st.secrets.get("SUPABASE_ANON_KEY") if hasattr(st, "secrets") else None
        # fallback to env
        SUPABASE_URL = SUPABASE_URL or os.environ.get("SUPABASE_URL")
        SUPABASE_KEY = SUPABASE_KEY or os.environ.get("SUPABASE_ANON_KEY")
        if not SUPABASE_URL or not SUPABASE_KEY:
            st.error("SUPABASE_URL and SUPABASE_ANON_KEY must be set in Streamlit secrets or environment for REMOTE mode.")
            return

        rest_base = SUPABASE_URL.rstrip("/")
        if not rest_base.lower().endswith("/rest/v1"):
            rest_base = rest_base + "/rest/v1"

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        }

    # helper to call rpc
        def call_rpc(name: str, payload: dict = None):
            url = f"{rest_base}/rpc/{name}"
            r = requests.post(url, headers=headers, json=payload or {})
            r.raise_for_status()
            return pd.DataFrame(r.json())

        # load total and distribution; daily will be fetched after user selects days
        try:
            df_total = call_rpc("get_total_contacts")
            total = int(df_total.iloc[0]["total"]) if not df_total.empty else 0
        except Exception as e:
            st.error(f"Failed to fetch total from Supabase: {e}")
            return

        try:
            df_dist = call_rpc("get_distribution")
        except Exception:
            df_dist = pd.DataFrame(columns=["label", "cnt"]) 

        # Render using remote frames
        st.metric("Total contacts", f"{total:,}")

        days = st.slider("Lookback days for daily added", min_value=7, max_value=90, value=30)

        # fetch daily with selected days
        try:
            df_daily = call_rpc("get_daily_added", {"p_days": int(days)})
            if not df_daily.empty:
                df_daily = df_daily.rename(columns={"cnt": "count"})
                df_daily["day"] = pd.to_datetime(df_daily["day"])
        except Exception:
            df_daily = pd.DataFrame(columns=["day", "count"])

        # helper to make JSON-serializable records (avoid Arrow extension types)
        def make_jsonable_records(df: pd.DataFrame):
            if df is None or df.empty:
                return []
            s = df.copy()
            # convert datetimes to ISO strings
            for col in s.columns:
                try:
                    if pd.api.types.is_datetime64_any_dtype(s[col]):
                        s[col] = s[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
                except Exception:
                    pass
            # replace NA-like with null
            s = s.where(pd.notnull(s), None)
            # use pandas' JSON roundtrip to get plain Python types
            try:
                json_text = s.to_json(orient="records", date_format="iso")
                records = json.loads(json_text)
            except Exception:
                # fallback: coerce to plain python types via applymap
                records = []
                for _, row in s.iterrows():
                    rec = {}
                    for k, v in row.items():
                        if pd.isna(v):
                            rec[k] = None
                        elif isinstance(v, (pd.Timestamp, datetime)):
                            rec[k] = v.isoformat()
                        else:
                            try:
                                # convert numpy types to python scalars
                                if hasattr(v, "item"):
                                    rec[k] = v.item()
                                else:
                                    rec[k] = v
                            except Exception:
                                rec[k] = str(v)
                    records.append(rec)
            return records

        def render_vega_spec(spec: dict):
            # Render a Vega-Lite spec via Streamlit, choosing compatible kwargs
            try:
                sig = inspect.signature(st.vega_lite_chart)
                params = sig.parameters
                if 'width' in params:
                    st.vega_lite_chart(spec, width='stretch')
                elif 'use_container_width' in params:
                    st.vega_lite_chart(spec, use_container_width=True)
                else:
                    st.vega_lite_chart(spec)
            except Exception:
                try:
                    st.vega_lite_chart(spec)
                except Exception:
                    st.write("Failed to render vega spec")

        st.subheader("Rows added daily")
        if not df_daily.empty:
            records = make_jsonable_records(df_daily)
            buf = render_line_matplotlib(records)
            if buf is not None:
                st.image(buf)
            else:
                st.write("No daily data")
        else:
            st.write("No daily data")

        st.subheader("Contacts distribution")
        if not df_dist.empty:
            df_dist = df_dist.rename(columns={"cnt": "count", "label": "label"})
            dist_records = make_jsonable_records(df_dist)
            buf = render_pie_matplotlib(dist_records)
            if buf is not None:
                st.image(buf)
            else:
                st.write("No distribution data")
        else:
            st.write("No distribution data")

        st.write("---")
        st.caption("Hosted on Supabase — for RPC endpoints contact admin: virendra@acadflip.com")
        return

    total = len(df)

    col1, col2 = st.columns([1, 3])
    with col1:
        st.metric("Total contacts", f"{total:,}")

    with col2:
        days = st.slider("Lookback days for daily added", min_value=7, max_value=90, value=30)

    # Daily added chart
    daily = daily_counts(df, days)
    records = make_jsonable_records(daily)
    line = alt.Chart(alt.Data(values=records)).mark_line(point=True).encode(
        x=alt.X("day:T", title="Date"),
        y=alt.Y("count:Q", title="Added per day"),
        tooltip=[alt.Tooltip("day:T", title="Date"), alt.Tooltip("count:Q", title="Added")],
    ).properties(height=300)

    st.subheader("Rows added daily")
    records = make_jsonable_records(daily)
    buf = render_line_matplotlib(records)
    if buf is not None:
        st.image(buf)
    else:
        st.write("No daily data")

    # Distribution pie
    dist_df = categorize_distribution(df)
    dist_records = make_jsonable_records(dist_df)
    st.subheader("Contacts distribution")
    buf = render_pie_matplotlib(dist_records)
    if buf is not None:
        st.image(buf)
    else:
        st.write("No distribution data")

    st.write("---")
    st.caption("Hosted on Supabase — for RPC endpoints contact admin: virendra@acadflip.com")


if __name__ == "__main__":
    main()
