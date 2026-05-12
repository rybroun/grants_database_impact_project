# Impact Project — Data Pipeline

## What this project is
Data collection and modeling layer for The Impact Project (theimpactproject.org).
Ingests federal grant changes from news articles (GDELT), government databases,
and nonprofit registries, then serves structured data to power the federal change map.

## Tech stack
- Python pipeline, orchestrated by GitHub Actions
- PostgreSQL on Supabase (PostGIS, pgvector future)
- Medallion schema: raw → staging → public

## Key docs
- **Data model & pipeline spec**: `docs/superpowers/specs/2026-05-12-impact-project-data-model-design.md`

## Conventions
- Grant is the core entity; reports are events that happen to grants
- Signed change_amount (negative = cuts, positive = adds)
- Entity resolution via canonical IDs — merge, never delete
- Reference tables for sectors (16) and action_types (20) — not hardcoded enums
