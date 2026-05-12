# Impact Project Data Model & Ingestion Pipeline — Design Spec

## Overview

The Impact Project currently powers a federal change map with static data. This project builds the dynamic data collection, storage, and modeling layer that replaces the static data and enables new analytical capabilities.

## Goals

1. **Automated ingestion** — ingest grant and federal change data from GDELT (news articles), government databases (USASpending, FPDS, SAM.gov, DOGE), and public nonprofit registries (IRS BMF, IRS 990 e-file, ProPublica Nonprofit Explorer)
2. **Entity resolution** — deduplicate and link grants, organizations, and sources across data providers
3. **Power the existing map** — provide the data layer for the county/state choropleth, dot-based event map, filtering by sector and action type
4. **Enable future analytics** — trend analysis ("transit grants down, religion grants up"), look-alike inference ("city Y handled a similar cut this way"), and demographic enrichment

## Tech Stack

- **Database**: PostgreSQL on Supabase (PostGIS for geospatial, pgvector for future embeddings)
- **Schema architecture**: Medallion pattern with three Postgres schemas:
  - `raw` — unprocessed articles, database dumps, API responses
  - `staging` — LLM-extracted records before entity resolution
  - `public` — production tables (grants, organizations, reports) serving the map
- **Pipeline**: Python, orchestrated by GitHub Actions (free for public repos)
- **News ingestion**: GDELT as article source, LLM-based extraction to produce structured records
- **Nonprofit data**: IRS BMF (bulk download, all tax-exempt orgs), IRS 990 e-file (AWS Open Data, financials), ProPublica Nonprofit Explorer API (EIN lookups), SAM.gov (federal contractors/grantees)
- **Cost target**: Supabase free/Pro tier ($0–$25/mo), GitHub Actions free tier (public repo)
- **Open source**: Entire pipeline and data model are open source. API keys stored in GitHub Actions secrets.

## Data Model

### 8 Tables

```
organizations ←──(granter)── grants ──(grantees)──→ grant_grantees → organizations
                                │
                             reports ──→ action_types
                                │
                          report_sources
                                │
                             sources

                             sectors ←── grants
```

---

### organizations

Nonprofits, federal agencies, state/local government bodies, private entities.

| Field | Type | Notes |
|---|---|---|
| organization_id | UUID | PK |
| canonical_org_id | UUID | Self-FK — points to canonical record if this is a merged duplicate |
| status | enum | `active` / `merged` / `needs_review` |
| name | text | Display name |
| name_aliases | text[] | All alternate names seen across sources |
| org_type | enum | `nonprofit` / `federal_agency` / `state_agency` / `local_government` / `private` |
| ein | text | IRS Employer ID Number (gold standard for nonprofit matching) |
| irs_bmf_id | text | IRS Business Master File reference |
| sam_uei | text | SAM.gov Unique Entity ID |
| duns_number | text | Legacy identifier |
| propublica_url | text | ProPublica Nonprofit Explorer profile URL |
| street_address | text | |
| city | text | |
| state | text | |
| zip | text | |
| latitude | float | Org headquarters location |
| longitude | float | |
| created_at | timestamp | |
| updated_at | timestamp | |

**ER strategy**: EIN is the strongest match key for nonprofits. SAM UEI for federal contractors/grantees. Fall back to fuzzy name + zip match. `name_aliases` accumulates every variant seen so future matching improves over time. Duplicates are merged via `canonical_org_id` — never deleted.

---

### grants

The core entity. Represents a federal grant or award.

| Field | Type | Notes |
|---|---|---|
| grant_id | UUID | PK |
| canonical_grant_id | UUID | Self-FK — points to canonical record if merged |
| status | enum | `active` / `merged` / `needs_review` |
| confidence_score | float | How clean/verified this record is (0.0–1.0) |
| title | text | Grant/program name |
| granter_org_id | UUID | FK → organizations (always one federal agency) |
| sector_id | UUID | FK → sectors |
| department | text | Federal department (e.g., "Department of Agriculture") |
| program | text | Specific program (e.g., "COMMUNITY FACILITIES LOANS AND GRANTS") |
| cfda_number | text | Catalog of Federal Domestic Assistance number |
| award_number | text | Unique federal award/agreement ID |
| external_ids | jsonb | IDs from source systems: `{"usaspending": "...", "fpds": "...", "doge": "..."}` |
| geographic_scope | enum | `national` / `state` / `county` / `city` / `point` |
| geo_state | text | |
| geo_county | text | |
| geo_city | text | |
| latitude | float | Grant impact location |
| longitude | float | |
| original_funding_amount | numeric | Baseline funding level |
| funding_year | int | Year of original funding |
| source_database | text | Where the grant record originated |
| created_at | timestamp | |
| updated_at | timestamp | |

**ER strategy**: `award_number` is the strongest match key when available — it's a unique federal identifier for a specific award. `external_ids` (jsonb) stores IDs from every source system without needing a column per source. Fallback matching: `department + program + grantee org + approximate amount + state`. Duplicates merged via `canonical_grant_id`.

---

### grant_grantees

One-to-many: a grant always has one granter (on the grant record) but can have multiple grantees.

| Field | Type | Notes |
|---|---|---|
| grant_id | UUID | FK → grants |
| organization_id | UUID | FK → organizations |
| allocation | numeric | Their share of the grant amount, if known |

**Composite PK**: (grant_id, organization_id)

---

### reports

An event/change that happened to a grant. This is the "dot on the map" — the unit that gets geocoded, displayed, filtered, and clicked on.

| Field | Type | Notes |
|---|---|---|
| report_id | UUID | PK |
| grant_id | UUID | FK → grants |
| action_type_id | UUID | FK → action_types |
| change_amount | numeric | Signed: negative for cuts, positive for adds |
| new_total | numeric | New funding level after change, if known |
| effective_date | date | When the change took effect |
| date_reported | date | When first reported by a source |
| date_entered | date | When added to the Impact Project map |
| summary | text | Narrative description shown on the map dot |
| scope_of_impact | enum | `statewide` / `local` |
| latitude | float | Report-specific geocode (dot placement) |
| longitude | float | |
| is_testimonial | boolean | For testimonial filter toggle |
| is_doge_data | boolean | For DOGE data filter toggle |
| created_at | timestamp | |

**Sign convention**: `change_amount` is always signed. A $5M grant cancellation is stored as `-5000000`. This allows aggregate queries to use `SUM(change_amount)` without CASE statements.

---

### sources

News articles, government database records, testimonials. The raw evidence backing reports.

| Field | Type | Notes |
|---|---|---|
| source_id | UUID | PK |
| source_type | enum | `news_article` / `government_database` / `testimonial` / `doge` |
| title | text | Article headline or description |
| url | text | Link to source |
| publisher | text | e.g., "flatheadbeacon.com" |
| published_date | date | |
| summary | text | Extracted or provided summary |
| raw_content | text | Full article text if available (for re-extraction) |
| gdelt_id | text | GDELT article ID for dedup |
| created_at | timestamp | |

---

### report_sources

Many-to-many: multiple sources can confirm the same report, and one source (article) can generate multiple reports.

| Field | Type | Notes |
|---|---|---|
| report_id | UUID | FK → reports |
| source_id | UUID | FK → sources |

**Composite PK**: (report_id, source_id)

---

### sectors

Reference table for the 16 category/sector buckets. Replaces hardcoded enums.

| Field | Type | Notes |
|---|---|---|
| sector_id | UUID | PK |
| name | text | Display name |
| color | text | Hex color for map rendering |

**Seed data**:

| Name | Color |
|---|---|
| Defense | #f8ec20 |
| Economy & Employment | #b0b0b8 |
| Education | #70f828 |
| Emergency Services, Public Safety & Law Enforcement | #7070fc |
| Energy | #a82814 |
| Food & Agriculture | #fc4848 |
| Housing | #fc74ec |
| Humanities & the Arts | #e1a886 |
| Immigration | #5d0483 |
| Infrastructure | #1c38ac |
| International Development | #b82cd0 |
| Natural Resources, Environment & Public Lands | #208800 |
| Overarching | #ac581c |
| Public Health & Healthcare | #f8a850 |
| Research & Academic Research | #3cd4e4 |
| Social Services | #68605c |

---

### action_types

Reference table for event action types, grouped into cuts/adds/responses.

| Field | Type | Notes |
|---|---|---|
| action_type_id | UUID | PK |
| name | text | Display name |
| category | enum | `cuts` / `adds` / `responses` |

**Seed data**:

| Name | Category |
|---|---|
| Contract Terminated for Convenience | cuts |
| Federal Worker Resigned | cuts |
| Federal Workers Fired | cuts |
| Federal Workers Put on Leave/In Limbo | cuts |
| Funding Frozen/Paused/Cancelled | cuts |
| Government Building Disposition | cuts |
| Hiring Freeze | cuts |
| Lease Terminated | cuts |
| Other Federal Cut | cuts |
| Program Paused/Under Review/Cancelled | cuts |
| Fed Workers Rehired/Possible Rehiring | adds |
| Funding Unfrozen | adds |
| Lease Cancellation Rescinded | adds |
| New Program/Service/Benefit | adds |
| Other Federal Add | adds |
| Industry Response to Cut | responses |
| Judicial Action | responses |
| Local Response to Cuts | responses |
| NGO Response to Cut | responses |
| State Responses to Cuts | responses |

---

## Key Query: County-Level Percent Change

The core map visualization — percent change in grant funding by county.

```sql
WITH county_baseline AS (
    SELECT
        g.geo_state,
        g.geo_county,
        SUM(g.original_funding_amount) AS total_original_funding
    FROM grants g
    WHERE g.status = 'active'
      AND g.geographic_scope = 'county'
    GROUP BY g.geo_state, g.geo_county
),

county_changes AS (
    SELECT
        g.geo_state,
        g.geo_county,
        SUM(r.change_amount) AS total_change_amount
    FROM reports r
    JOIN grants g ON g.grant_id = r.grant_id
    WHERE g.status = 'active'
      AND g.geographic_scope = 'county'
    GROUP BY g.geo_state, g.geo_county
)

SELECT
    b.geo_state,
    b.geo_county,
    b.total_original_funding,
    COALESCE(c.total_change_amount, 0) AS total_change,
    CASE
        WHEN b.total_original_funding = 0 THEN NULL
        ELSE ROUND(
            (COALESCE(c.total_change_amount, 0) / b.total_original_funding) * 100,
            1
        )
    END AS pct_change
FROM county_baseline b
LEFT JOIN county_changes c
    ON b.geo_state = c.geo_state
    AND b.geo_county = c.geo_county
ORDER BY pct_change ASC;
```

**Scope rule**: Only county-scoped grants appear on the county map. State-level and national grants appear on their respective dashboards. No apportionment between geographic levels for now.

---

## Ingestion Pipeline (High-Level)

```
┌─────────────────────────────────────────────────┐
│  GitHub Actions (cron schedule, e.g. every 6h)  │
└─────────────────────────────────────────────────┘
        │
        ▼
   Data Sources
   - GDELT (news articles)
   - USASpending / FPDS / SAM.gov / DOGE (gov databases)
   - IRS BMF / IRS 990 e-file / ProPublica (nonprofit registries)
        │
        ▼
   ┌─────────────────────────┐
   │  raw schema (Bronze)    │
   │  - raw_articles         │
   │  - raw_gov_records      │
   │  - raw_nonprofit_data   │
   │  Dedup by gdelt_id /    │
   │  external_ids           │
   └─────────────────────────┘
        │
        ▼
   LLM Extraction (Python)
   - Extract: org, department, program, action, amount, location
   - Output structured fields
        │
        ▼
   ┌─────────────────────────┐
   │  staging schema (Silver)│
   │  - extracted_reports    │
   │  - extracted_orgs       │
   │  - extracted_grants     │
   │  Unresolved, pre-ER     │
   └─────────────────────────┘
        │
        ▼
   Entity Resolution (Python)
   - Match orgs by EIN / SAM UEI / fuzzy name+zip
   - Match grants by award_number / external_ids / dept+program+org+amount
   - Create or link to existing records
   - Flag low-confidence matches as needs_review
        │
        ▼
   ┌─────────────────────────┐
   │  public schema (Gold)   │
   │  - organizations        │
   │  - grants               │
   │  - grant_grantees       │
   │  - reports              │
   │  - sources              │
   │  - report_sources       │
   │  - sectors              │
   │  - action_types         │
   │  Clean, deduplicated,   │
   │  serves the map         │
   └─────────────────────────┘
        │
        ▼
   Human Review Queue
   - Review needs_review records
   - Merge duplicates via canonical_*_id
```

---

## Design Decisions

| Decision | Rationale |
|---|---|
| Grants are the core entity | Orgs are participants (granter/grantee), reports are events that happen to grants |
| Reports = dots on map | Each report is geocoded independently and carries its own narrative summary |
| Signed change_amount | Negative for cuts, positive for adds. Enables simple SUM aggregation |
| Reference tables for sectors/actions | Avoids schema migrations when categories change. Carries display metadata (colors) |
| Canonical ID pattern for ER | Merged duplicates point to the canonical record — never deleted. Old links still resolve |
| One granter, many grantees | granter_org_id on grant; grant_grantees table for multiple recipients |
| County-only on county map | No apportionment of state/national grants to counties. Avoids phantom precision |
| external_ids as jsonb | Flexible storage for IDs from any source system without schema changes |
| Medallion schema (raw/staging/public) | Separates unprocessed, extracted, and production data. Enables re-extraction and lineage |
| GitHub Actions as orchestrator | Free for public repos, built-in secrets management, transparent pipeline for open-source project |
| IRS BMF + SAM.gov + ProPublica over Candid | Public, free data sources. EIN from IRS, UEI from SAM.gov, enrichment from ProPublica API |

## Future Considerations

- **Apportionment table** for distributing state/national grants to counties
- **Demographic enrichment** on geographic entities for look-alike inference
- **pgvector embeddings** on report summaries for semantic similarity / look-alike matching
- **LLM extraction prompt design** — what exactly to extract, confidence scoring, structured output format
- **GDELT integration details** — query patterns, filtering, rate limits, update frequency
