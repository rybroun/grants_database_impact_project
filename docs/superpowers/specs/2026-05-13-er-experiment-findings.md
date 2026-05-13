# Entity Resolution Experiment: Matching Federal Grant Recipients to IRS Nonprofits

## Summary

We tested whether we can reliably link federal grant recipients (from USASpending.gov) to IRS-registered nonprofits (from the IRS Business Master File) using name and address matching. Tested on two states at different scales:

- **Wyoming** (small): 92 nonprofits, 87-88% true match rate
- **Colorado** (mid-size): 542 nonprofits, ~78% true match rate

Match rates decrease at scale due to city name abbreviations, national franchise ambiguity, and short/generic org names. A three-tier confidence strategy (auto-approve / flag / human review) is the recommended approach.

## Methodology

### Data Sources

| Source | Records | Key Fields | Access |
|---|---|---|---|
| IRS EO Business Master File (Wyoming) | 5,985 orgs | EIN, name, address, NTEE code, financials | Free CSV download, no auth |
| USASpending.gov API | 102 unique nonprofit grant recipients in WY (2020-2024) | UEI, name, address, business type | Free JSON API, no auth |

### The Join Problem

Neither dataset shares a common identifier:
- **IRS BMF** has EIN but no UEI
- **USASpending** has UEI but no EIN
- **SAM.gov** has both, but EIN is restricted to federal "Sensitive" access level

We must join on **name + geographic proximity** — inherently fuzzy.

### Matching Pipeline

**v1 (Baseline)**: Exact normalized name matching with three passes:
1. Exact name + ZIP match
2. Exact name + city match
3. Exact name only (no geo confirmation)
4. Alternate names from USASpending (same passes)

Name normalization: uppercase, strip legal suffixes (Inc, LLC, Corp), remove punctuation, collapse whitespace.

**v2 (Improved)**: Token-based fuzzy matching with confidence scoring:
1. Pre-filter government/tribal entities by business type
2. Exact normalized name match (same as v1)
3. Token overlap (Jaccard similarity) within same city
4. Token overlap across all BMF records
5. Confidence score combining Jaccard + token containment + Levenshtein distance + geographic proximity

## Results

### Match Rate Comparison

| Metric | v1 (Baseline) | v2 (Improved) |
|---|---|---|
| Total recipients | 102 | 102 |
| Govt/tribal filtered | 9 (manual) | 10 (automatic) |
| Nonprofits to match | 93 | 92 |
| Raw matches | 77 (82.8%) | 85 (92.4%) |
| False positives | 0 | 4-5 |
| **True match rate** | **82.8%** | **87-88%** |
| Confidence: High (>=0.9) | 77 (all exact) | 75 |
| Confidence: Medium (0.7-0.9) | 0 | 9 |
| Confidence: Low (<0.7) | 0 | 1 |

### What v2 Correctly Recovered (v1 Missed)

These 5 matches were missed by exact matching but correctly found by token matching:

| USASpending Name | BMF Name | Why v1 Missed | Confidence |
|---|---|---|---|
| WYOMING CHILD & FAMILY DEVELOPMENT, INC. | WYOMING CHILD & FAMILY DEVELOPEMENT INC | Typo in BMF ("DEVELOPEMENT") | 0.851 |
| VOLUNTEERS OF AMERICA NORTHERN ROCKIES | VOLUNTEERS OF AMERICA NORTHERN ROCKIES FOUNDATION | Extra word "FOUNDATION" | 0.925 |
| LINCOLN-UINTA CHILD DEVELOPMENT ASSOCIATION | LINCOLN-UINTA CHILD DEVELOPMENT ASSOCIATION INCORPORATED | Extra suffix not in strip list | 0.780 |
| NORTHERN WYOMING MENTAL HEALTH CENTER, INC | NORTHERN WYOMING MENTAL HEALTH | Truncated name in BMF | 0.872 |
| HERITAGE TOWERS OF THE CHRISTIAN CHURCH (DISCIPLES OF CHRIST) | HERITAGE TOWERS OF THE CHRISTIAN CHURCH DOC | Abbreviation "DOC" for denomination | 0.965 |

**Common theme**: Names that are clearly the same organization but differ by typos, truncation, extra suffixes, or abbreviations. Token-based matching handles all of these well.

### False Positives Introduced by v2

| USASpending Name | Wrong BMF Match | Confidence | Why It Failed |
|---|---|---|---|
| WIND RIVER SAGE FUND INC. | WIND RIVER DEVELOPMENT FUND | 0.741 | "Wind River" + "Fund" overlap but different orgs |
| PEAK WELLNESS CENTER, INC. | SOUTHEAST WYOMING MENTAL HEALTH CENTER HOUSING INC | 0.799 | Shared generic tokens ("CENTER"), different city |
| WASHAKIE COUNTY WYOMING | WASHAKIE COUNTY LIBRARY FOUNDATION INC | 0.738 | Govt entity leaked through filter, matched on "WASHAKIE COUNTY" |
| LOVELL, INC. | LOVELL RODEO CLUB | 0.604 | Org name is just a town name, matched to any org in that town |

**Common theme**: False positives cluster below 0.8 confidence and involve either (a) generic/geographic tokens dominating the match, or (b) very short org names. A confidence threshold of **0.80** would eliminate 3 of 4 false positives while keeping 4 of 5 true new matches.

### Unmatched Recipients (7 remaining)

| Name | Why Unmatched |
|---|---|
| CENTRUM FOR DISABILITY SERVICES | No close match in BMF — may be registered in another state or under a different name |
| COTTONWOOD IRRIGATION DISTRICT | Government entity (irrigation district) that passed the filter |
| SLOW FOOD IN THE TETONS | Small org, no close match — may be too new or too small for BMF |
| WHITE HEART FOUNDATION | Address is in Heber City, WY 84032 — this is actually a Utah ZIP code |
| SHERIDAN HEALTH CENTER, INC. | Generic name, no strong token overlap with any BMF record |
| CHEYENNE FAMILY YMCA | BMF has "YOUNG MENS CHRISTIAN ASSOCIATION" variants but in other cities |
| EVANSTON WATER DITCH, INC. | Irrigation infrastructure entity, unlikely to be in BMF as nonprofit |

## Key Findings

### 1. Name Matching is Viable but Has a Precision/Recall Tradeoff

- **Exact matching** gives high precision (0 false positives) but misses ~17% of valid matches
- **Fuzzy matching** recovers most of those misses but introduces ~5% false positive rate
- **Sweet spot**: Use exact matching as primary, fuzzy matching as secondary with confidence threshold of 0.80, and flag matches below 0.80 for human review

### 2. The 14 Suspicious ZIP Mismatches Are Mostly Correct

Wyoming cities span multiple ZIP codes (Cheyenne has 5, Casper has 5). A "ZIP mismatch" within the same city is almost always the same org at a different mailing address. Of 14 flagged matches, all were verified correct by name + city agreement.

### 3. Government/Tribal Entities Are a Significant Noise Source

10 of 102 recipients (9.8%) are government or tribal entities classified as "nonprofit" in USASpending. Pre-filtering by `business_types` removes most, but some edge cases leak through (e.g., "WASHAKIE COUNTY WYOMING" has `nonprofit` in its business types). The filter needs refinement for entities that have both `government` and `nonprofit` tags.

### 4. National/Franchise Orgs Present Ambiguity Risk at Scale

In Wyoming alone, "UNIVERSITY OF WYOMING" has 258 BMF records (chapters/departments). "AMERICAN LEGION" has 49. When scaling nationally, matching "Habitat for Humanity" or "United Way" chapters to the correct BMF record requires **name + city + ZIP** at minimum, and may need address-level matching for orgs with multiple records in the same city.

### 5. Duplication Risks

| Risk | Example | Mitigation |
|---|---|---|
| Same org, multiple BMF records | University of Wyoming (258 records) | Match on most specific name variant + address |
| Same org, different names across sources | "Cheyenne Family YMCA" vs "YOUNG MENS CHRISTIAN ASSOCIATION" | Use USASpending alternate names |
| Parent/subsidiary confusion | "VOLUNTEERS OF AMERICA NORTHERN ROCKIES" vs "...FOUNDATION" | Treat as separate entities with organizational link |
| Cross-state duplicates | Org registered in WY but receiving grants in MT | Match BMF state to recipient state |

### 6. Data Quality Observations

- **IRS BMF**: 100% address completeness, 69% NTEE code coverage, ~35% have financial data. Excellent quality.
- **USASpending**: 100% UEI and address coverage. Alternate names are highly valuable for matching.
- **Key gap**: Neither source provides a reliable cross-reference identifier. The join is fundamentally name-based.

## Recommendations for Production System

1. **Use a three-tier matching strategy**:
   - **Auto-approve** (confidence >= 0.90): Direct name match + geographic confirmation. ~75% of matches.
   - **Auto-approve with flag** (0.80-0.90): Token match with strong signals. ~10% of matches.
   - **Human review queue** (0.60-0.80): Plausible but risky matches. ~5% of matches.
   - **Unmatched** (<0.60 or no candidate): Flag as `needs_review` in organizations table.

2. **Add address-level matching** as a confirmation signal — not a primary match key (addresses change), but as a tiebreaker when multiple BMF candidates exist.

3. **Pre-filter government entities** before matching, but treat the filter as lossy — some govt entities have `nonprofit` business type tags.

4. **Scale estimate**: Wyoming has 102 nonprofit grant recipients. Nationally, expect 50,000-100,000 unique nonprofit recipients. BMF has ~1.95M records. The matching pipeline runs in seconds for one state; national scale would take minutes, not hours.

5. **ProPublica API** as a validation layer: For matched pairs, query ProPublica by EIN to confirm the org exists and pull additional metadata (990 financials, filing history). This serves as an independent confirmation of the match.

---

## Colorado Scale Test (542 nonprofits)

### Scale Comparison

| Metric | Wyoming | Colorado | Ratio |
|---|---|---|---|
| BMF records | 5,985 | 36,060 | 6.0x |
| USASpending recipients | 102 | 558 | 5.5x |
| Govt filtered | 10 | 16 | — |
| Nonprofits to match | 92 | 542 | 5.9x |

### Match Rates at Scale

| Version | Wyoming | Colorado | Delta |
|---|---|---|---|
| v1 (exact) | 82.8% | 73.4% | -9.4% |
| v2 (fuzzy) | 87-88% | 80.4% | -7-8% |
| v2 improvement over v1 | +5% | +7% | — |
| Estimated true match rate | 87-88% | ~78% | -10% |

### Confidence Distribution (Colorado v2)

| Level | Count | % | Estimated Precision |
|---|---|---|---|
| High (>=0.9) | 359 | 82% | ~99% |
| Medium (0.7-0.9) | 54 | 12% | ~95% |
| Low (<0.7) | 23 | 5% | ~55% |

### New Failure Modes at Scale

**1. City Name Abbreviations**
BMF uses abbreviations that differ from USASpending:
- "COLORADO SPRINGS" vs "COLORADO SPGS"
- "GRAND JUNCTION" vs "GRAND JCT"

This breaks city-level matching entirely. Fix: normalize common city abbreviations.

**2. National Org Chapter Confusion**
- "HABITAT FOR HUMANITY OF COLORADO" → matched to HFH International (wrong EIN)
- "COLORADO TROUT UNLIMITED" → matched to national Trout Unlimited (wrong EIN)
- State/local chapters often have different EINs from national orgs

**3. Generic/Short Org Names**
- "PLACE, THE" → matched to a random Colorado Springs nonprofit (wrong)
- "LOVELL, INC." → matched to Lovell Rodeo Club (wrong)
- Orgs with <3 meaningful tokens are high false-positive risk

**4. Subsidiary/LLC Structures**
- "COMMUNITY HOUSING CONCEPTS EAST CENTRAL LLC" → parent org "COMMUNITY HOUSING CONCEPTS INC"
- May be intentional (roll up to parent) or wrong (different tax entity)

### Precision Audit: Fuzzy Matches (15 reviewed)

| Verdict | Count | % |
|---|---|---|
| True positive | 7 | 47% |
| False positive | 4 | 27% |
| Plausible (needs review) | 4 | 27% |

False positives in fuzzy matches are **3x higher** at Colorado scale vs Wyoming. The primary cause is generic token overlap ("Housing", "Center", "Community") that creates false signals.

### Examples of Each Category

**True Positive (fuzzy match is correct):**
- "ICAST (INTERNATIONAL CENTER FOR APPROPRIATE AND SUSTAINABLE)" → "ICAST INTERNATIONL CENTER FOR APPROPRIATE & SUSTANABL TECHNO" — typos in BMF, clearly same org
- "GRAND VALLEY WATER USERS ASSOCIATION" → "GRAND VALLEY WATER USERS" — suffix difference + city abbreviation (GRAND JUNCTION vs GRAND JCT)

**False Positive (fuzzy match is wrong):**
- "PARTNERS IN HOUSING INC" (Colorado Springs) → "PAGOSA HOUSING PARTNERS" (Pagosa Springs) — different org, different city, tokens "HOUSING" + "PARTNERS" overlap
- "HOLY CROSS ELECTRIC ASSOCIATION" → "HOLY CROSS ENERGY ROUND-UP FOUNDATION" — utility vs foundation, "HOLY CROSS" overlap

**Plausible (needs human review):**
- "COLORADO TROUT UNLIMITED" → "TROUT UNLIMITED" — state chapter vs national, may share EIN or may not

### Improvement Recommendations (from CO experiment)

1. **City name normalization dictionary** — map BMF abbreviations to full names (COLORADO SPGS → COLORADO SPRINGS, GRAND JCT → GRAND JUNCTION, etc.)
2. **Minimum token threshold** — reject fuzzy matches where the query name has fewer than 3 significant tokens
3. **Penalize generic tokens** — lower the weight of common words like "CENTER", "FOUNDATION", "COMMUNITY", "HOUSING", "ASSOCIATION" in Jaccard scoring
4. **Chapter vs. national detection** — when matching "X OF COLORADO" to "X", check if both are in BMF and flag as potential chapter/national confusion

---

## v3: Address Matching (Implemented)

After the v2 analysis revealed that address data exists on both sides but was never compared, we implemented v3 which adds:

1. **Address normalization** — `ROAD` → `RD`, `STREET` → `ST`, strip PO BOX/Suite/Unit, collapse whitespace
2. **Address similarity scoring** — exact match (1.0), containment (0.9), same street number + overlapping tokens (0.7-0.9)
3. **City name normalization** — dictionary mapping BMF abbreviations to full names (COLORADO SPGS → COLORADO SPRINGS, etc.)
4. **Revised confidence formula**:

```
score = jaccard(name) * 0.3
      + token_containment(name) * 0.2
      + address_similarity * 0.25        ← NEW
      + city_match_bonus (0.1)
      + zip_match_bonus (0.05)
      + address_exact_boost (0.1)        ← NEW: bonus for exact street match
```

### v1 → v2 → v3 Comparison

| Metric | Wyoming v1 | Wyoming v2 | Wyoming v3 | Colorado v1 | Colorado v2 | Colorado v3 |
|---|---|---|---|---|---|---|
| Match rate | 81.5% | 88% | 88.0% | 74.5% | 80.4% | 80.3% |
| High confidence (>=0.9) | — | 75 | 73 | — | 359 | 367 |
| Low confidence (<0.7) | — | 1 | 1 | — | 23 | **2** |

### Key Result: Same Recall, Much Better Precision

v3 match rates are nearly identical to v2, but **low-confidence matches in Colorado dropped from 23 to 2**. Address matching acts as a confirmation signal — it either boosts fuzzy matches to medium/high confidence or lets them die below threshold.

### False Positives Fixed by v3

| v2 False Positive | v3 Outcome | Why |
|---|---|---|
| "PARTNERS IN HOUSING" → "PAGOSA HOUSING PARTNERS" | Eliminated | City normalization reveals different cities |
| "PLACE, THE" → "URBAN WOODLANDS" | Now correctly matches "THE PLACE" | Address match (addr=1.0) confirms correct BMF record |
| "HOLY CROSS ELECTRIC" → "ROUND-UP FOUNDATION" | Eliminated | Different address |
| "WIND RIVER SAGE FUND" → "DEVELOPMENT FUND" | Confirmed as TRUE POSITIVE (0.78) | Exact same address (3 ETHETE RD), same city, same ZIP |

### Address Matching as Tie-Breaker

Address similarity proved valuable for confirming subsidiary/parent relationships:
- "COMMUNITY HOUSING CONCEPTS EAST CENTRAL LLC" → parent org: addr=1.0 (same physical address → confirmed subsidiary)
- "WESTERN GOVERNORS' ASSOCIATION" → "WESTERN GOVERNORS FOUNDATION": addr=1.0 (same building → related orgs)
- "NATIONAL CONFERENCE OF STATE LEGISLATURES" → "NCSL FOUNDATION FOR STATE LEGISLATURES": addr=1.0 (same office → confirmed)

---

## Resolving Unmatched Organizations

Not every grant recipient will match a BMF record. The remaining ~20% fall into distinct categories, each needing a different resolution strategy.

### Principle: Every Grant Recipient Gets an Org Record

No grant data should be orphaned. If an org can't be matched to BMF, it still enters the `organizations` table with `status = 'er_created'` and no EIN. These are first-class records that can be enriched later.

### Category 1: Cross-State National Orgs (e.g., Salvation Army)

**Problem**: USASpending filters by where the grant was spent (Colorado), but the recipient is registered in another state (California). The BMF record exists but in a different state file.

**Resolution**: Create two org records:
- **Parent org** — matched to BMF in the correct state, has EIN. Status: `active`.
- **Local presence** — ER-created with the grant location address, linked via `parent_org_id`. Status: `er_created`.

The grant attaches to the local presence (correct geography for the map) while maintaining the link to the real entity.

**Example**:
```
Parent:  THE SALVATION ARMY | EIN from CA BMF | 30840 HAWTHORNE BLVD, RANCHO PALOS VERDES, CA
  └─ Local: THE SALVATION ARMY (CO) | no EIN | parent_org_id → parent | grant address in CO
```

**Production fix**: Load ALL 50 state BMF files so cross-state parents can be found.

### Category 2: Rebranded/Renamed Orgs (e.g., Diversus Health)

**Problem**: The org exists in BMF under a slightly different name. "DIVERSUS HEALTH SERVICES" vs "DIVERSUS HEALTH INC" — normalization strips "INC" but not "SERVICES".

**Resolution**: Expand the suffix strip list and use address matching as confirmation. Fixed by adding "SERVICES", "ASSOCIATES", "PARTNERS", "GROUP", "AGENCY", "CENTER", "PROGRAMME" to the normalization list.

**Example**:
```
USASpending: DIVERSUS HEALTH SERVICES | 675 SOUTHPOINTE CT STE 100, Colorado Springs, CO 80906
BMF:         DIVERSUS HEALTH INC      | 675 SOUTHPOINTE CT STE 100, Colorado Spgs, CO 80906
→ Same address, name normalizes to same root. Now matched.
```

### Category 3: Missing from BMF Entirely (e.g., Cheyenne YMCA, Centrum)

**Problem**: No name match, no address match, no alt name match. The org is genuinely absent from the IRS BMF extract.

Possible reasons:
- Registered under a parent org (YMCA of the USA)
- New org not yet in monthly BMF extract
- Lost tax-exempt status
- Different legal name with no overlap to search terms

**Resolution**: Create as `er_created` with all available USASpending data:
- Name, UEI, address from USASpending
- `status = 'er_created'`, `confidence_score = 0`
- No EIN — flagged as "not BMF backed"
- If a likely parent can be identified (e.g., YMCA of the USA), link via `parent_org_id`
- Queue for enrichment: ProPublica API lookup by name/address, manual review

**Example**:
```
CHEYENNE FAMILY YMCA | UEI=... | 1426 E LINCOLNWAY, CHEYENNE, WY 82001
  status: er_created | ein: NULL | confidence_score: 0
  parent_org_id: → YMCA OF THE USA (if identifiable)
```

### Category 4: Not a Traditional Nonprofit (e.g., Tri-State Generation)

**Problem**: USASpending classifies the entity as `corporate_entity_not_tax_exempt`. It receives federal grants but is not tax-exempt, so it's correctly absent from BMF.

**Resolution**: Create as `er_created` with `org_type` reflecting the actual business type from USASpending (e.g., `private` instead of `nonprofit`). These are legitimate grant recipients that belong in the organizations table but will never have a BMF match.

### Data Model Changes

Added to `organizations` table:
- `parent_org_id` (UUID, self-FK) — links local presence/chapter to national/parent org
- `status` enum expanded: `active` / `merged` / `needs_review` / `er_created`

### Enrichment Pipeline for ER-Created Orgs

ER-created orgs are candidates for later resolution:
1. **ProPublica API** — search by name or address to find EIN
2. **Cross-state BMF search** — load all 50 state files and retry matching
3. **DUNS number lookup** — USASpending includes DUNS which may help cross-reference
4. **Manual review queue** — surface to human reviewers with all available context
5. **Future BMF updates** — re-run matching monthly as BMF refreshes
