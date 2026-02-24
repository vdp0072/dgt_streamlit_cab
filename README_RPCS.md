RPC Endpoints & Client Usage
===========================

This document explains how to call the Supabase RPCs created for the contacts dataset, with examples for
1) Node and Python client scripts (provided in this repo),
2) curl (PostgREST), and
3) an exhaustive list of available RPCs and alternative REST queries.

Setup (environment)
- Export these environment variables (from your project `.env` or your environment):
  - `SUPABASE_URL` — e.g. `https://<project>.supabase.co` or `https://<project>.supabase.co/rest/v1` (either works)
  - `SUPABASE_ANON_KEY` — anon/public key used for read-only RPC calls

Note on RPCs
- The repo provides three server-side RPC functions (created in the `public` schema):
  - `get_contacts(p_limit integer DEFAULT 100)`
  - `get_pune_contacts(p_limit integer DEFAULT 100)`
  - `get_mh_contacts(p_limit integer DEFAULT 100)`
- Each RPC returns SETOF `public.contacts` and applies these rules:
  - Default limit = 100 rows when `p_limit` omitted.
  - If `p_limit` > 0, that number of rows is returned.
  - If `p_limit` <= 0 (or 0), the function treats it as no limit (returns all rows).
  - Results are ordered by `updated_at DESC, name`.
  - `get_pune_contacts` filters rows where a case-insensitive substring `pune` appears in the concatenated fields `addr`, `loc`, `city`, `state`, or the JSONB `other` column (converted to text).
  - `get_mh_contacts` filters for `maharashtra` OR the abbreviation `mh` across the same concatenation.

Where RPCs are exposed
- PostgREST RPC endpoints live at: `https://<project>.supabase.co/rest/v1/rpc/<function>`
- The client scripts in this repo will automatically add `/rest/v1` if it is not present in `SUPABASE_URL`.

Security note
- Use the anon key for read-only RPCs from client applications. Do NOT embed the `service_role` key in browser or public clients.
- If anon receives `403` when calling RPCs, grant execute rights to anon in the Supabase SQL editor:
  - `GRANT EXECUTE ON FUNCTION public.get_pune_contacts(integer) TO anon;`
  - `GRANT EXECUTE ON FUNCTION public.get_mh_contacts(integer) TO anon;`
  - `GRANT EXECUTE ON FUNCTION public.get_contacts(integer) TO anon;`

1) Node (supabase-js and provided client)
- Option A: use the provided lightweight client `supa_client.js` (no npm packages needed)
  - Usage examples (ensure env vars set):
    - `node supa_client.js get_Pune_contacts_200 --export exports/pune_200.csv`
    - `node supa_client.js get_MH_contacts --export exports/mh_100.csv`
    - `node supa_client.js get_contacts --max all --export exports/all.csv`

- Option B: use `@supabase/supabase-js` library (recommended for richer clients)
  - Example (Node / browser):
    ```js
    import { createClient } from '@supabase/supabase-js'
    const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY)
    // call RPC
    const { data, error } = await supabase.rpc('get_pune_contacts', { p_limit: 200 })
    ```

2) Python (requests and provided client)
- Option A: use provided `supa_client.py` (requires `requests`):
  - `python supa_client.py get_Pune_contacts_200 --export exports/pune_200.csv`
  - `python supa_client.py get_contacts --max all --export exports/all.csv`

- Option B: direct requests example
  - Example requesting Pune 200 via PostgREST:
    ```py
    import os, requests, pandas as pd
    url = f"{os.environ['SUPABASE_URL'].rstrip('/')}/rest/v1/rpc/get_pune_contacts"
    headers = {'apikey': os.environ['SUPABASE_ANON_KEY'], 'Authorization': f"Bearer {os.environ['SUPABASE_ANON_KEY']}", 'Content-Type': 'application/json'}
    r = requests.post(url, headers=headers, json={'p_limit': 200})
    r.raise_for_status()
    rows = r.json()
    df = pd.DataFrame(rows)
    df.to_csv('exports/pune_200.csv', index=False)
    ```

3) curl / HTTP (PostgREST)
- Call RPC with curl (replace values):
  - Get Pune 200:
    ```sh
    curl -X POST "https://<project>.supabase.co/rest/v1/rpc/get_pune_contacts" \
      -H "apikey: <ANON_KEY>" \
      -H "Authorization: Bearer <ANON_KEY>" \
      -H "Content-Type: application/json" \
      -d '{"p_limit":200}'
    ```

- Direct REST table queries (alternative to RPCs)
  - Get first 100 contacts (ordered):
    ```sh
    curl "https://<project>.supabase.co/rest/v1/contacts?select=*&order=updated_at.desc&limit=100" \
      -H "apikey: <ANON_KEY>" -H "Authorization: Bearer <ANON_KEY>"
    ```
  - Get Pune contacts via OR filters (PostgREST ilike):
    ```sh
    curl "https://<project>.supabase.co/rest/v1/contacts?select=*&or=(addr.ilike.*pune*,loc.ilike.*pune*,city.ilike.*pune*,state.ilike.*pune*,other.ilike.*pune*)&order=updated_at.desc&limit=100" \
      -H "apikey: <ANON_KEY>" -H "Authorization: Bearer <ANON_KEY>"
    ```

Exhaustive list of available RPC/endpoints and methods
- RPCs (server-side functions you can call via POST to `/rest/v1/rpc/<name>`):
  - `get_contacts(p_limit integer DEFAULT 100)`
    - Returns the most recently updated contacts (ordered by updated_at desc). `p_limit` controls rows; 0 => all.
  - `get_pune_contacts(p_limit integer DEFAULT 100)`
    - Returns contacts where the substring `pune` appears in addr/loc/city/state/other.
  - `get_mh_contacts(p_limit integer DEFAULT 100)`
    - Returns contacts where `maharashtra` OR `mh` appears in addr/loc/city/state/other.

- Direct PostgREST /rest/v1/contacts table (useful for custom filters):
  - Filtering examples:
    - Field equality: `?city=eq.Pune`
    - ilike substring: `?city=ilike.*pune*` or `?addr=ilike.*pune*`
    - OR expressions: `?or=(city.ilike.*pune*,state.ilike.*maharashtra*)`
    - Ordering: `?order=updated_at.desc` or `?order=city.asc`
    - Pagination: use `limit` and `offset` or `.range(start,end)` via the supabase client.

Recommendations & next steps
- For stable client use, call the RPCs rather than crafting complex OR filters client-side.
- If you will frequently search the JSONB `other` column, consider creating a generated text column `search_text` (e.g. `addr||' '||loc||' '||city||' '||state||' '||other::text`) and add a GIN trigram or full-text index to speed queries.
- For CSV downloads in production prefer server-side pagination + streaming to avoid large memory use on the client.

Support files in this repo
- `supa_client.py` — Python client script that calls RPCs and writes CSV (uses `requests`).
- `supa_client.js` — Node client (no deps) that calls RPCs and writes CSV.
- `create_supabase_rpcs.py` — script used to create the three RPC functions (already ran).

