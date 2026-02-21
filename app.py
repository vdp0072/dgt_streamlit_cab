import streamlit as st
import pandas as pd
from io import StringIO

import ingest
from db import get_engine
from sqlalchemy import text


st.set_page_config(page_title="Supabase Ingest Dashboard", layout="wide")

st.title("Ingest + Dashboard (Supabase)")

engine = get_engine()


def fetch_counts(engine):
    query_total = text("SELECT COUNT(*) FROM records")
    query_pune = text("SELECT COUNT(*) FROM records WHERE is_pune = TRUE")
    try:
        total = pd.read_sql(query_total, engine).iloc[0, 0]
        pune = pd.read_sql(query_pune, engine).iloc[0, 0]
    except Exception:
        total = 0
        pune = 0
    return total, pune


uploaded = st.file_uploader("Upload CSV to ingest", type=["csv"])
if uploaded is not None:
    # read into pandas
    try:
        df = pd.read_csv(uploaded, dtype=str)
    except Exception:
        uploaded.seek(0)
        df = pd.read_csv(StringIO(uploaded.getvalue().decode()), dtype=str)

    cnt = ingest.ingest_dataframe(df, engine=engine)
    st.success(f"Ingested {cnt} rows")


st.markdown("---")

# show warning if using sqlite fallback
if engine.dialect.name == "sqlite":
    st.warning("Using local SQLite fallback — your DATABASE_URL may be unreachable. App is in local mode.")

filter_option = st.selectbox("Filter", ["Pune", "Maharashtra_Other", "Other_State", "All"])

if st.button("Refresh counts"):
    total, pune = fetch_counts(engine)
    st.metric("Total Records", total)
    st.metric("Pune Records", pune)

if st.button("Show sample Pune numbers"):
    try:
        if filter_option == "All":
            df = pd.read_sql(text("SELECT * FROM records LIMIT 200"), engine)
        else:
            q = text("SELECT * FROM records WHERE city_category = :cat LIMIT 200")
            df = pd.read_sql(q, engine, params={"cat": filter_option})
        st.dataframe(df.head(200))
        if "phone" in df.columns:
            csv = df["phone"].to_csv(index=False)
            st.download_button("Download phones CSV", csv)
    except Exception as e:
        st.error(f"Failed to query DB: {e}")
