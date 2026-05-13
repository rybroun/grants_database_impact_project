# Entity Resolution Experiment: Matching Federal Grant Recipients to IRS Nonprofits

## Summary

We tested whether we can reliably link federal grant recipients (from USASpending.gov) to IRS-registered nonprofits (from the IRS Business Master File) using name and address matching. Using Wyoming as a test case, we achieved an **87-88% true match rate** on nonprofit recipients, with clear paths to improve further.

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
