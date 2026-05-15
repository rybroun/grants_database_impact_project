# Impact Project — Data Pipeline

## What this project is
Data collection and modeling layer for The Impact Project (theimpactproject.org).
Ingests federal grant data from USASpending bulk archives and IRS Business Master File,
runs entity resolution to match grant recipients to nonprofit registry records,
and loads structured data into a DuckDB analytical database.

## Tech stack
- Python 3.10 pipeline (`/Library/Frameworks/Python.framework/Versions/3.10/bin/python3`)
- Prefect for orchestration + monitoring (self-hosted, `prefect server start`)
- DuckDB database (local, ~20GB for FY2023-2025)
- IRS BMF for nonprofit entity resolution
- USASpending bulk CSV archives as primary data source

## Setup on a new machine
```bash
# 1. Clone the repo
git clone <repo-url> && cd impact_project

# 2. Install dependencies (use Python 3.10)
pip install -r requirements.txt

# 3. Download source data
python -c "from pipeline.bmf_loader import download_bmf; download_bmf()"
# Then manually download USASpending archives from:
# https://www.usaspending.gov/download_center/award_data_archive
# Save to data/raw/usaspending/

# 4. Start Prefect server
prefect server start  # opens UI at http://localhost:4200

# 5. Run the pipeline (in another terminal)
python -m pipeline.flow --zip data/raw/usaspending/usaspending_archive_FY2024.zip

# 6. Run tests
python -m pytest tests/ -v
```

## Running the pipeline
```bash
# Via Prefect flow (recommended — shows progress in UI at localhost:4200):
python -m pipeline.flow --zip data/raw/usaspending/usaspending_archive_FY2024.zip

# Multiple fiscal years:
python -m pipeline.flow --zip data/raw/usaspending/usaspending_archive_FY2023.zip data/raw/usaspending/usaspending_archive_FY2024.zip data/raw/usaspending/usaspending_archive_FY2025.zip

# Limit BMF to specific states (faster for testing):
python -m pipeline.flow --zip data/raw/usaspending/usaspending_archive_FY2024.zip --states WY CO

# Direct loader (no Prefect, no UI):
python -m pipeline.loader --zip data/raw/usaspending/usaspending_archive_FY2024.zip
```

## Key docs
- **Data model spec**: `docs/superpowers/specs/2026-05-12-impact-project-data-model-design.md`
- **ER system spec**: `docs/superpowers/specs/2026-05-13-er-system-spec.md`
- **ER experiment findings**: `docs/superpowers/specs/2026-05-13-er-experiment-findings.md`

## Architecture
```
USASpending Bulk ZIPs → pipeline/flow.py (Prefect) → DuckDB
                              ↑
                    IRS BMF CSVs (ER matching)

Bronze: raw_awards (all 112 CSV columns, unfiltered)
Silver: ER matching (normalize → exact name → token fuzzy → address-only for nonprofits)
Gold:   organizations, grants, grant_grantees, sectors, action_types
```

### Pipeline files
- `pipeline/flow.py` — **Prefect flow** with @task/@flow decorators, the primary entry point
- `pipeline/loader.py` — standalone loader (same logic, no Prefect, for scripting)
- `pipeline/bmf_loader.py` — downloads + indexes IRS BMF state files
- `pipeline/er_matcher.py` — 3-pass entity resolution:
  - Pass 1: exact normalized name + ZIP/city/address confirmation
  - Pass 2: token Jaccard + containment within same city
  - Pass 2.5: address-only match for nonprofit-classified recipients (catches renamed orgs)
  - Pass 3: DISABLED (global scan, O(N*M), needs inverted index for production)
- `pipeline/normalize.py` — name/address/city normalization
- `pipeline/seed_data.py` — sector and action_type reference data
- `pipeline/config.py` — paths and constants

## Directory structure
```
data/
  raw/
    usaspending/    # USASpending bulk archive ZIPs (gitignored)
    bmf/            # IRS BMF state CSV files (gitignored)
  processed/        # DuckDB database output (gitignored)
  logs/             # pipeline run logs (gitignored)
notebooks/          # Jupyter notebooks for analysis
tests/
  fixtures/         # small CSV samples for testing
pipeline/           # all pipeline code
docs/superpowers/   # specs and plans
```

## DuckDB tables
- `raw_awards` — all 112 columns from USASpending CSV, unfiltered (~18M rows for FY2023-2025)
- `bmf_records` — IRS BMF data loaded for joins (~1.95M records)
- `organizations` — grant recipients matched to BMF (with EIN) or er_created (~137K)
- `grants` — deduplicated awards (~3.2M for FY2024, more with additional years)
- `grant_grantees` — links grants to recipient organizations
- `sectors` — 16 category reference records
- `action_types` — 20 event type reference records
- Views: `org_grant_summary`, `funding_by_department`, `funding_by_state`

## org_type classification
Derived from USASpending `business_types_description`:
- `nonprofit` — 501C3 and other nonprofit designations
- `private` — for-profit, small business
- `housing_for_profit` — for-profit housing entities matched to nonprofit BMF records (LIHTC)
- `local_government` — city, county, township, school district, special district, regional
- `state_government` — state govt, housing authority, US territory
- `tribal` — Indian/Native American tribal government
- `private_higher_education` / `public_higher_education` — universities
- `foreign` — non-domestic entities
- `non_classified` — "OTHER" or unknown
- `federal_agency` — awarding agencies (granter side)

## Conventions
- Grant is the core entity; reports are events that happen to grants
- Signed change_amount (negative = cuts, positive = adds)
- Entity resolution via canonical IDs — merge, never delete
- org_type derived from business_types_description, not defaulted
- Normalizer strips suffixes longest-first (CORPORATION before CORP)
- Hyphens become spaces during normalization
- Address-only ER pass scoped to nonprofits only
- ER Pass 3 (global token scan) disabled for performance at scale

## Known issues & next steps
- ER match rate for nonprofits: ~77% (main causes: renamed orgs, new orgs, name too different)
- ~21K "non_classified" orgs (USASpending "OTHER" category) need better classification
- Reports/sources tables exist in schema but no data yet (needs GDELT ingestion)
- FY2020-2022 data not yet loaded (archives available at usaspending.gov)
- Supabase project exists (whsecclvtqmzftrqfgam) with schema created but DuckDB is primary for now
