# Impact Project — Data Pipeline

## What this project is
Data collection and modeling layer for The Impact Project (theimpactproject.org).
Ingests federal grant data from USASpending bulk archives and IRS Business Master File,
runs entity resolution to match grant recipients to nonprofit registry records,
and loads structured data into a DuckDB analytical database.

## Tech stack
- Python pipeline, run locally as batch process
- DuckDB database (local, ~20GB for FY2023-2025)
- IRS BMF for nonprofit entity resolution
- USASpending bulk CSV archives as primary data source

## Running the pipeline
```bash
# Full load from a USASpending archive:
python -m pipeline.loader --zip data/raw/usaspending/FY2024.zip

# Limit BMF to specific states (faster for testing):
python -m pipeline.loader --zip data/raw/usaspending/FY2024.zip --states WY CO
```

## Key docs
- **Data model spec**: `docs/superpowers/specs/2026-05-12-impact-project-data-model-design.md`
- **ER system spec**: `docs/superpowers/specs/2026-05-13-er-system-spec.md`
- **ER experiment findings**: `docs/superpowers/specs/2026-05-13-er-experiment-findings.md`

## Architecture
- `pipeline/loader.py` — main entry point, orchestrates full load into DuckDB
- `pipeline/bmf_loader.py` — downloads + indexes IRS BMF state files
- `pipeline/er_matcher.py` — 2-pass entity resolution (exact name + token fuzzy)
- `pipeline/normalize.py` — name/address/city normalization
- `pipeline/seed_data.py` — sector and action_type reference data
- `pipeline/config.py` — paths and constants

## Directory structure
- `data/raw/usaspending/` — USASpending bulk archive ZIPs
- `data/raw/bmf/` — IRS BMF state CSV files
- `data/processed/` — DuckDB database output
- `data/logs/` — pipeline run logs
- `notebooks/` — Jupyter notebooks for analysis

## DuckDB tables
- `raw_awards` — all 112 columns from USASpending CSV, unfiltered
- `bmf_records` — IRS BMF data loaded for joins
- `organizations` — grant recipients matched to BMF (with EIN) or er_created
- `grants` — deduplicated awards
- `grant_grantees` — links grants to recipient organizations
- `sectors` — 16 category reference records
- `action_types` — 20 event type reference records

## Conventions
- Grant is the core entity; reports are events that happen to grants
- Signed change_amount (negative = cuts, positive = adds)
- Entity resolution via canonical IDs — merge, never delete
- org_type derived from business_types_description, not defaulted
- ER Pass 3 (global token scan) disabled for performance at scale
