import os
import streamlit as st
import pandas as pd
from io import StringIO

import ingest
from db import get_engine
from sqlalchemy import text

# Optional: Supabase client for auth
try:
    from supabase import create_client
except Exception:
    create_client = None


st.set_page_config(page_title="Supabase Ingest Dashboard", layout="wide")

st.title("Ingest + Dashboard (Supabase)")

# Create Supabase client if credentials available
SUPABASE_URL = None
SUPABASE_KEY = None
if "SUPABASE_URL" in st.secrets:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets.get("SUPABASE_ANON_KEY") or st.secrets.get("SUPABASE_KEY")
else:
    # local dev: read from environment / .env
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_API_KEY") or os.getenv("SUPABASE_ANON_KEY")

supabase = None
if create_client and SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        supabase = None

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


def fetch_user_role(user_id, token=None):
    """Return role string for given user id by querying profiles table via Supabase client.

    Returns 'user' when unknown or on error.
    """
    if not supabase or not user_id:
        return "user"
    try:
        # try common client API shapes
        try:
            res = supabase.table("profiles").select("role").eq("id", user_id).execute()
            data = getattr(res, "data", None) or (res["data"] if isinstance(res, dict) and "data" in res else None)
        except Exception:
            res = supabase.from_("profiles").select("role").eq("id", user_id).execute()
            data = getattr(res, "data", None) or (res["data"] if isinstance(res, dict) and "data" in res else None)

        if data and len(data) > 0:
            role = data[0].get("role") if isinstance(data[0], dict) else None
            return role or "user"
    except Exception:
        return "user"
    return "user"


uploaded = st.file_uploader("Upload CSV to ingest", type=["csv"])
uploaded = st.file_uploader("Upload CSV to ingest", type=["csv"])

# Authentication UI
if "user" not in st.session_state:
    st.session_state.user = None
    st.session_state.token = None
    st.session_state.role = None


def login_form():
    st.subheader("Sign in")
    with st.form("login"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in")
        if submitted:
            if not supabase:
                st.error("Supabase client not configured. Cannot sign in.")
                return
            try:
                # attempt to sign in
                resp = supabase.auth.sign_in_with_password({"email": email, "password": password})
            except Exception:
                try:
                    resp = supabase.auth.sign_in({"email": email, "password": password})
                except Exception as e:
                    st.error(f"Sign-in failed: {e}")
                    return

            # extract user and token depending on client response shape
            user = None
            token = None
            if isinstance(resp, dict):
                token = resp.get("access_token") or resp.get("session", {}).get("access_token")
                user = resp.get("user") or resp.get("session", {}).get("user")
            else:
                # try attribute access
                token = getattr(resp, "access_token", None) or getattr(resp, "session", {}).get("access_token")
                user = getattr(resp, "user", None) or getattr(resp, "session", {}).get("user")

            if not user:
                st.error("Sign-in failed: no user returned")
                return

            st.session_state.user = user
            st.session_state.token = token
            uid = user.get("id") if isinstance(user, dict) else getattr(user, "id", None)
            st.session_state.role = fetch_user_role(uid, token)
            st.success(f"Signed in as {user.get('email') if isinstance(user, dict) else uid}")


def logout():
    st.session_state.user = None
    st.session_state.token = None
    st.session_state.role = None


if st.session_state.user is None:
    login_form()
else:
    st.write(f"Signed in: {st.session_state.user.get('email') if isinstance(st.session_state.user, dict) else 'user'}")
    st.write(f"Role: {st.session_state.role}")
    if st.button("Logout"):
        logout()

    # handle upload only for admin
    if st.session_state.role == "admin":
        if uploaded is not None:
            # read into pandas
            try:
                df = pd.read_csv(uploaded, dtype=str)
            except Exception:
                uploaded.seek(0)
                df = pd.read_csv(StringIO(uploaded.getvalue().decode()), dtype=str)

            # preview then require confirmation
            st.subheader("Upload preview (first 5 rows)")
            st.dataframe(df.head())
            if st.button("Confirm Ingest"):
                cnt = ingest.ingest_dataframe(df, engine=engine)
                st.success(f"Ingested {cnt} rows")
    else:
        if uploaded is not None:
            st.info("Only admins can ingest data. Please contact an admin for ingestion privileges.")


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
