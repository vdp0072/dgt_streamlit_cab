# Supabase Ingest Project

Phase 1 implementation: ingestion pipeline that normalizes location data and upserts into Supabase Postgres (via DATABASE_URL).

Quick start:

1. Copy `.env.example` to `.env` and set `DATABASE_URL` from your Supabase project.
2. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\pip.exe install -r requirements.txt
```

3. Run ingestion locally:

```bash
python ingest.py input_csv/single_num_1a_part_001.csv
```

If `DATABASE_URL` is not set the code falls back to `sqlite:///data.db` for local testing.

Deployment (Streamlit Cloud)
-----------------------------

1. Do NOT commit secrets. Add your `DATABASE_URL` to Streamlit Cloud Secrets:

   - Go to your app on Streamlit Cloud → Settings → Secrets
   - Add a key `DATABASE_URL` with the Postgres connection string

2. Ensure `app.py` is the Streamlit entrypoint at repository root.

3. Push this repository to GitHub and connect the repo in Streamlit Cloud.

Secrets
-------
- Keep `.env` local and DO NOT commit it. It's already in `.gitignore`.
- For CI or Cloud, prefer using Streamlit secrets or environment variables.
