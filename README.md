# Greenville Crawler

Nightly automated lead pull for Restart Homes, Greenville County only.

## What it does right now

`scripts/tax_sale_ingest.py` pulls the current Greenville County tax sale
list (public, published by the Tax Collector), looks up the full property
record for each parcel, and upserts it into the `leads` table in Supabase
with the tag `tax_sale`. It also computes a transparent score — see the
comment above `rescore_touched_rows()` in that file for the exact formula.

It runs automatically every night via `.github/workflows/nightly.yml`
(GitHub Actions — free, no server needed). You can also trigger it by hand:
go to the "Actions" tab on this repo → "Nightly Greenville lead ingest" →
"Run workflow".

## Checking results

Query the `leads` table in the Supabase SQL Editor:

```sql
select address, owner_name, is_absentee, score, source_tags, updated_at
from leads
order by score desc;
```

## Adding the next source

Each source gets its own script in `scripts/`, following the same pattern:
fetch → parse → upsert into `leads` tagging the source name → rescore →
log to `source_runs`. Add a new step to `nightly.yml` to run it.

## Secrets

`DATABASE_URL` is set as a GitHub Actions repo secret (Settings → Secrets
and variables → Actions) — it's the Supabase session pooler connection
string. Never commit it to a file in this repo.
