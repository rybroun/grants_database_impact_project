# Entity Resolution System — Spec

## What This Does

Links federal grant recipients (from USASpending.gov) to IRS-registered nonprofits (from IRS Business Master File) so that every grant in our system is attached to an identified organization with as much metadata as possible (EIN, NTEE code, financials, address).

## Data Sources

| Source | What It Gives Us | Size | Access |
|---|---|---|---|
| USASpending.gov API | Grant recipients: UEI, name, address, business type, alt names | ~50-100K unique nonprofit recipients nationally | Free JSON API, no auth |
| IRS EO Business Master File | All tax-exempt orgs: EIN, name, address, NTEE code, financials | ~1.95M orgs across 50 state CSV files | Free CSV download, no auth |
| ProPublica Nonprofit Explorer | 990 filing data: revenue, expenses, assets by EIN | ~3M filings | Free JSON API, no auth |

**Key constraint**: No shared identifier exists between USASpending (UEI) and IRS BMF (EIN). SAM.gov has both but EIN is restricted to federal "Sensitive" access. The join is fundamentally name + address based.

## Pipeline Overview

```
USASpending API
  → Fetch nonprofit grant recipients (by state or national)
  → For each recipient, get: name, UEI, address, business_types, alt_names

IRS BMF (all 50 state files)
  → Load into indexed lookup (name, city, zip, address)

Matching (3 passes)
  → Pass 1: Exact normalized name + ZIP/city
  → Pass 2: Token-based fuzzy + address confirmation (same city)
  → Pass 3: Token-based fuzzy + address confirmation (any state)

Output → organizations table
  → Matched orgs: status=active, has EIN + UEI
  → Unmatched orgs: status=er_created, has UEI only

Enrichment (async)
  → ProPublica API lookup by EIN for matched orgs
  → ProPublica/address search for er_created orgs
```

## Name Normalization

Applied to both USASpending and BMF names before comparison:

1. Uppercase
2. Strip legal suffixes: INC, LLC, CORP, CORPORATION, LTD, LIMITED, CO, COMPANY, PC, SERVICES, ASSOCIATES, GROUP, AGENCY
3. Normalize: `&` → `AND`, strip apostrophes, remove leading `THE`
4. Remove parenthetical content: `(DISCIPLES OF CHRIST)` → removed
5. Remove all punctuation, collapse whitespace

## Address Normalization

1. Uppercase
2. Strip PO BOX, Suite/STE, Unit, Apt, Room, Floor, Building
3. Normalize suffixes: ROAD→RD, STREET→ST, AVENUE→AVE, BOULEVARD→BLVD, DRIVE→DR, LANE→LN, HIGHWAY→HWY, NORTH→N, SOUTH→S, EAST→E, WEST→W
4. Remove punctuation, collapse whitespace

## City Normalization

Dictionary mapping BMF abbreviations to full names:

```
COLORADO SPGS → COLORADO SPRINGS
GRAND JCT → GRAND JUNCTION
FT COLLINS → FORT COLLINS
GLENWOOD SPGS → GLENWOOD SPRINGS
PAGOSA SPGS → PAGOSA SPRINGS
(extend as new abbreviations are encountered)
```

## Matching Algorithm

### Pre-filter

Remove government/tribal entities from the match pool. An entity is government if its `business_types` include government tags (`government`, `local_government`, `indian_native_american_tribal_government`, etc.) AND do NOT include `nonprofit` or `corporate_entity_tax_exempt`.

### Pass 1: Exact Name Match

Match normalized name against BMF index. Confirm with geography:
- Name + ZIP → confidence 1.0
- Name + city → confidence 0.95
- Name + address match → confidence 0.90
- Name only (no geo) → confidence 0.82

Also try all USASpending alternate names through the same passes.

### Pass 2: Token-Based Fuzzy Match (same city)

For unmatched recipients, compute Jaccard similarity and token containment against all BMF records in the same normalized city.

Threshold: Jaccard >= 0.5 OR token containment >= 0.75

### Pass 3: Token-Based Fuzzy Match (global)

If no strong city match, expand to all BMF records.

Threshold: Jaccard >= 0.6 OR (containment >= 0.8 AND Jaccard >= 0.4)

### Confidence Score

```
score = jaccard(name) * 0.3
      + token_containment(name) * 0.2
      + address_similarity * 0.25
      + city_match_bonus (0.1)
      + zip_match_bonus (0.05)
      + address_exact_boost (0.1)   // when address_similarity >= 0.7
```

### Decision Tiers

| Confidence | Action | Expected % of matches |
|---|---|---|
| >= 0.90 | Auto-approve | ~80% |
| 0.80 - 0.90 | Auto-approve, flag for review | ~10% |
| 0.60 - 0.80 | Human review queue | ~5% |
| < 0.60 or no match | Create as `er_created` | ~5% |

## Handling Unmatched Recipients

Every grant recipient gets an org record. Unmatched recipients are classified into four categories:

### Cross-State National Orgs

USASpending returns a grant spent in CO but the recipient org is registered in CA (e.g., Salvation Army). Create two records:
- **Parent**: matched to BMF in the org's home state, has EIN, `status = active`
- **Local presence**: ER-created at the grant address, `parent_org_id` → parent, `status = er_created`

Requires loading all 50 state BMF files.

### Rebranded/Renamed Orgs

Name differs between USASpending and BMF but address matches. Handled by expanded suffix stripping and address confirmation. These typically resolve automatically in Pass 2/3.

### Genuinely Missing from BMF

No name match, no address match. Could be: registered under parent org, new org, lost tax-exempt status, or completely different legal name.

Resolution: Create as `er_created` with all USASpending data. Link to parent if identifiable. Queue for ProPublica enrichment.

### Non-Nonprofit Grant Recipients

USASpending classifies as `corporate_entity_not_tax_exempt`. Correctly absent from BMF. Create as `er_created` with `org_type = 'private'`.

## Organizations Table Output

Each org record produced by the ER system:

| Field | Source | Notes |
|---|---|---|
| organization_id | Generated | UUID |
| canonical_org_id | ER system | Points to canonical if merged duplicate |
| parent_org_id | ER system | Links local presence to national parent |
| status | ER system | `active` (BMF matched) or `er_created` (unmatched) |
| confidence_score | ER system | 0.0 - 1.0 |
| name | USASpending | Display name |
| name_aliases | USASpending | All alternate names seen |
| org_type | USASpending business_types | nonprofit / federal_agency / private / etc. |
| ein | IRS BMF | NULL if er_created |
| sam_uei | USASpending | Always present |
| duns_number | USASpending | Legacy, when available |
| street_address | USASpending (primary), BMF (fallback) | |
| city | USASpending | |
| state | USASpending | |
| zip | USASpending | |
| ntee_code | IRS BMF | NULL if er_created |
| asset_amount | IRS BMF | NULL if er_created |
| income_amount | IRS BMF | NULL if er_created |

## Tested Performance

| Metric | Wyoming (small) | Colorado (mid-size) |
|---|---|---|
| BMF records | 5,985 | 36,060 |
| Grant recipients | 92 nonprofits | 542 nonprofits |
| Match rate (v3) | 88% | 80% |
| High confidence (>= 0.9) | 79% of matches | 84% of matches |
| Low confidence (< 0.7) | 1 match | 2 matches |
| False positive rate | ~0% | ~1-2% |

## Scale Estimate

- National: ~50,000-100,000 unique nonprofit grant recipients
- BMF: ~1.95M records across all states (~500MB total CSV)
- In-memory matching runs in seconds per state, minutes nationally
- USASpending API: ~100 recipients/minute fetch rate (rate limit safe)
- Full national run: ~2-3 hours (dominated by API fetch time, not matching)

## What This Spec Does NOT Cover

- GDELT / news article ingestion (separate pipeline)
- LLM extraction of grant data from articles
- Grant and report table population
- Map/frontend integration
- ProPublica enrichment implementation details
