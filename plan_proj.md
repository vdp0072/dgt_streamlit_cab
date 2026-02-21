

* I already created the Supabase project
* I have `SUPABASE_URL`, `SUPABASE_ANON_KEY` (or service role key)
* I have the `DATABASE_URL` in `.env`

We’ll use **direct Postgres connection via DATABASE_URL** (cleanest for your use case), not the JS SDK model.

---

# ✅ Updated 3-Phase Plan (Supabase-Based)

---

# 🧱 PHASE 1 — Supabase DB Creation (With Dedup Prevention)

## 1️⃣ Use Supabase Postgres (Managed)

Supabase = Hosted PostgreSQL.

You will:

* Use `DATABASE_URL` from Supabase
* Connect via SQLAlchemy / psycopg2
* Manage schema via Supabase SQL Editor

---

## 2️⃣ Create Table in Supabase

Go to:

> Supabase → SQL Editor → New Query

Run:

```sql
CREATE TABLE records (
    id BIGSERIAL PRIMARY KEY,

    phone VARCHAR(20) NOT NULL UNIQUE,
    e164_phone VARCHAR(20),

    timestamp_ms BIGINT,
    batch_id TEXT,
    uid TEXT,

    db1_success BOOLEAN,
    db1_confidence FLOAT,
    db1_latency_ms INT,

    name TEXT,
    operator TEXT,
    phone_type TEXT,
    website TEXT,

    address TEXT,
    result_loc TEXT,
    belong_area TEXT,

    city TEXT,
    state TEXT,
    country TEXT,

    is_pune BOOLEAN DEFAULT FALSE,
    city_category TEXT,

    raw_json JSONB,

    created_at TIMESTAMP DEFAULT NOW()
);
```

---

## 3️⃣ Add Indexes (Performance Critical)

```sql
CREATE INDEX idx_city ON records(city);
CREATE INDEX idx_state ON records(state);
CREATE INDEX idx_is_pune ON records(is_pune);
CREATE INDEX idx_city_category ON records(city_category);
```

Now Supabase handles:

* Storage
* Scaling
* Backups
* SSL security

---

## 4️⃣ Dedup Strategy (Upsert via SQLAlchemy)

Because of:

```sql
phone UNIQUE
```

In Python ingestion:

```python
from sqlalchemy.dialects.postgresql import insert

stmt = insert(Record).values(row_dict)

stmt = stmt.on_conflict_do_update(
    index_elements=["phone"],
    set_=row_dict
)

engine.execute(stmt)
```

✔ No duplicate phones
✔ Latest data overwrites old
✔ No manual duplicate handling needed

Supabase handles constraint enforcement.

---

# 📍 PHASE 2 — Location Extraction & Classification (Before DB Insert)

This logic remains app-side (not DB-side).

---

## 1️⃣ Clean & Normalize

```python
def clean_text(x):
    return str(x).strip().lower() if x else ""
```

---

## 2️⃣ Extract State & Country

```python
def extract_state_country(belong_area):
    parts = clean_text(belong_area).split(",")
    state = parts[0].strip() if len(parts) > 0 else None
    country = parts[1].strip() if len(parts) > 1 else None
    return state, country
```

---

## 3️⃣ Extract City

```python
KNOWN_CITIES = [
    "pune", "mumbai", "nashik", "nagpur",
    "solapur", "kolhapur", "satara",
    "ahmednagar", "jalgaon", "thane"
]

def extract_city(address, result_loc, belong_area):
    text = " ".join([
        clean_text(address),
        clean_text(result_loc),
        clean_text(belong_area)
    ])

    for city in KNOWN_CITIES:
        if city in text:
            return city.title()

    return None
```

---

## 4️⃣ Classification Logic

```python
def classify(city, state):
    if city == "Pune":
        return True, "Pune"

    if state and state.lower() == "maharashtra":
        return False, "Maharashtra_Other"

    return False, "Other_State"
```

---

## Final Stored Columns

Each row inserted into Supabase will contain:

* city
* state
* country
* is_pune
* city_category

Filtering becomes trivial:

```sql
SELECT phone FROM records WHERE is_pune = TRUE;
```

Supabase indexes make this instant.

---

# 📊 PHASE 3 — Streamlit + Supabase Integration

---

## 1️⃣ .env Configuration (Local Dev)

```env
DATABASE_URL=postgresql://postgres:password@db.xxx.supabase.co:5432/postgres
```

---

## 2️⃣ DB Connection Module

```python
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_engine():
    return create_engine(os.getenv("DATABASE_URL"))
```

---

## 3️⃣ Streamlit File Upload (Data Injection)

```python
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    ingest_csv(uploaded_file)
    st.success("Data ingested to Supabase successfully")
```

Ingestion pipeline:

* Parse CSV
* Extract JSON fields
* Normalize location
* Classify
* UPSERT to Supabase

---

## 4️⃣ Dashboard Filtering

```python
filter_option = st.selectbox(
    "Filter",
    ["Pune", "Maharashtra_Other", "Other_State", "All"]
)
```

Query Supabase:

```python
query = """
SELECT * FROM records
WHERE city_category = :category
"""

df = pd.read_sql(query, engine, params={"category": filter_option})
```

---

## 5️⃣ Cloud Deployment (Streamlit Cloud)

In Streamlit Cloud:

Settings → Secrets:

```toml
DATABASE_URL="postgresql://postgres:password@db.xxx.supabase.co:5432/postgres"
```

Then:

```python
def get_engine():
    if "DATABASE_URL" in st.secrets:
        return create_engine(st.secrets["DATABASE_URL"])
    return create_engine(os.getenv("DATABASE_URL"))
```

No Docker required.
No local DB required.
Supabase remains always live.

---

# 🎯 Final Supabase-Based Architecture

```
Streamlit (Local or Cloud)
        ↓
SQLAlchemy
        ↓
Supabase Managed PostgreSQL
        ↓
Indexed records table
```

---

# 🔐 Security Notes

For your project:

* You are using direct Postgres connection
* You are bypassing Supabase RLS
* Access control is handled by your app

For demo showcase → perfectly fine.

---

# 🚀 What This Supabase Plan Guarantees

✔ Managed cloud database
✔ Automatic dedup
✔ Indexed Pune filtering
✔ Clean geo-classification
✔ No local infra dependency
✔ Easily deployable Streamlit app
✔ Scales well beyond 1M rows


