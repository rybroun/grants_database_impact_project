# First-Pass National Data Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a queryable Supabase database with the full data model populated from USASpending (grants + orgs) and IRS BMF (org enrichment via ER), covering all 50 states.

**Architecture:** Python scripts run locally as a one-time batch pipeline using the medallion schema pattern (raw → staging → public). Raw BMF and USASpending data lands in `raw` schema, normalized/pre-ER records go to `staging`, and final ER-resolved orgs + grants go to `public`. No automation — just a static first pass we can query against. The layered approach means we can re-run ER or fix normalization without re-fetching source data.

**Tech Stack:** Python 3.10, pandas, supabase-py, USASpending REST API, IRS BMF CSV downloads, Supabase (Postgres)

---

## File Structure

```
impact_project/
├── pipeline/
│   ├── __init__.py
│   ├── config.py                  # Supabase URL/key, constants
│   ├── normalize.py               # Name, address, city normalization (from ER spec)
│   ├── bmf_loader.py              # Download + index all 50 state IRS BMF files
│   ├── usaspending_client.py      # Fetch grant recipients + award data from API
│   ├── er_matcher.py              # 3-pass matching engine (from ER spec)
│   ├── db_schema.py               # Create all 8 tables + reference data in Supabase
│   ├── db_loader.py               # Insert matched/unmatched orgs, grants, grantees into Supabase
│   └── run_pipeline.py            # Orchestrator: ties all steps together
├── tests/
│   ├── test_normalize.py
│   ├── test_er_matcher.py
│   └── test_db_schema.py
├── pipeline/seed_data.py          # Sectors + action_types seed data
└── .env                           # SUPABASE_URL, SUPABASE_KEY (gitignored)
```

**What we're NOT building in this plan:**
- GDELT / news article ingestion (no reports or sources yet — just the org + grant layer)
- GitHub Actions automation
- ProPublica enrichment

**What we ARE building:**
- Medallion schema: `raw`, `staging`, and `public` Postgres schemas
- `raw.bmf_records` — raw IRS BMF data as ingested
- `raw.usaspending_recipients` — raw USASpending recipient API responses
- `raw.usaspending_awards` — raw USASpending award data
- `staging.normalized_orgs` — normalized org records pre-ER (name/address/city normalized, business type classified)
- `staging.er_candidates` — ER match candidates with confidence scores before final resolution
- `public` schema: all 8 production tables (organizations, grants, grant_grantees, reports, sources, report_sources, sectors, action_types)
- Reference data seeded (16 sectors, 20 action types)
- Organizations table populated nationally via ER (USASpending + IRS BMF)
- Grants table populated from USASpending award data
- Grant-grantee relationships linked
- Granter orgs (federal agencies) created as org records

---

### Task 1: Project Setup + Config

**Files:**
- Create: `pipeline/__init__.py`
- Create: `pipeline/config.py`
- Create: `.env`
- Create: `requirements.txt`

- [ ] **Step 1: Create requirements.txt**

```
pandas>=2.0
requests>=2.28
supabase>=2.0
python-dotenv>=1.0
```

- [ ] **Step 2: Create .env (template — user fills in real values)**

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-role-key
```

- [ ] **Step 3: Create pipeline/config.py**

```python
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "experiments", "data")

US_STATES = [
    "al","ak","az","ar","ca","co","ct","de","dc","fl","ga","hi","id","il","in",
    "ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh",
    "nj","nm","ny","nc","nd","oh","ok","or","pa","pr","ri","sc","sd","tn","tx",
    "ut","vt","va","wa","wv","wi","wy"
]

BMF_URL_TEMPLATE = "https://www.irs.gov/pub/irs-soi/eo_{state}.csv"
USASPENDING_API_BASE = "https://api.usaspending.gov/api/v2"
```

- [ ] **Step 4: Create pipeline/__init__.py**

```python
# Impact Project data pipeline
```

- [ ] **Step 5: Commit**

```bash
git add pipeline/ requirements.txt .env
git commit -m "feat: project setup with config and dependencies"
```

---

### Task 2: Normalization Module

**Files:**
- Create: `pipeline/normalize.py`
- Create: `tests/test_normalize.py`

- [ ] **Step 1: Write failing tests for name normalization**

```python
# tests/test_normalize.py
from pipeline.normalize import normalize_name, normalize_address, normalize_city

def test_normalize_name_strips_suffixes():
    assert normalize_name("PARTNERS IN HOUSING INC") == "PARTNERS IN HOUSING"
    assert normalize_name("HOLY CROSS ELECTRIC ASSOCIATION, INC.") == "HOLY CROSS ELECTRIC ASSOCIATION"
    assert normalize_name("DIVERSUS HEALTH SERVICES") == "DIVERSUS HEALTH"

def test_normalize_name_handles_ampersand():
    assert normalize_name("WYOMING CHILD & FAMILY DEVELOPMENT") == "WYOMING CHILD AND FAMILY DEVELOPMENT"

def test_normalize_name_strips_the():
    assert normalize_name("THE SALVATION ARMY") == "SALVATION ARMY"

def test_normalize_name_removes_parenthetical():
    assert normalize_name("HERITAGE TOWERS OF THE CHRISTIAN CHURCH (DISCIPLES OF CHRIST)") == "HERITAGE TOWERS OF CHRISTIAN CHURCH"

def test_normalize_address_normalizes_suffixes():
    assert normalize_address("3 ETHETE ROAD PO BOX 661") == "3 ETHETE RD"
    assert normalize_address("455 GOLD PASS HTS") == "455 GOLD PASS HTS"

def test_normalize_address_strips_suite():
    assert normalize_address("675 SOUTHPOINTE CT STE 100") == "675 SOUTHPOINTE CT"
    assert normalize_address("1700 BROADWAY STE 500") == "1700 BROADWAY"

def test_normalize_city_maps_abbreviations():
    assert normalize_city("COLORADO SPGS") == "COLORADO SPRINGS"
    assert normalize_city("GRAND JCT") == "GRAND JUNCTION"
    assert normalize_city("DENVER") == "DENVER"  # passthrough
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/ryan/impact_project && python -m pytest tests/test_normalize.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement normalize.py**

```python
# pipeline/normalize.py
import re

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.upper().strip()
    for suffix in [
        ", INC.", ", INC", " INC.", " INC", " INCORPORATED",
        ", LLC", " LLC", ", L.L.C.",
        ", CORP.", ", CORP", " CORP.", " CORP", " CORPORATION",
        ", CO.", ", CO", " COMPANY",
        ", LTD", " LTD", " LIMITED",
        ", P.C.", ", PC",
        " SERVICES", " ASSOCIATES", " GROUP", " AGENCY",
    ]:
        name = name.replace(suffix, "")
    name = name.replace(" & ", " AND ").replace("&", " AND ")
    name = name.replace("'S", "S").replace("'", "")
    name = re.sub(r"^THE\s+", "", name)
    name = re.sub(r"\([^)]*\)", "", name)
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    addr = addr.upper().strip()
    addr = re.sub(r"\bP\.?O\.?\s*BOX\s*\d*", "", addr)
    addr = re.sub(
        r"\b(STE|SUITE|UNIT|APT|RM|ROOM|FL|FLOOR|BLDG|BUILDING)\s*[#]?\s*\w*",
        "", addr,
    )
    replacements = {
        r"\bROAD\b": "RD", r"\bSTREET\b": "ST", r"\bAVENUE\b": "AVE",
        r"\bBOULEVARD\b": "BLVD", r"\bDRIVE\b": "DR", r"\bLANE\b": "LN",
        r"\bCOURT\b": "CT", r"\bCIRCLE\b": "CIR", r"\bPLACE\b": "PL",
        r"\bTERRACE\b": "TER", r"\bHIGHWAY\b": "HWY", r"\bPARKWAY\b": "PKWY",
        r"\bNORTH\b": "N", r"\bSOUTH\b": "S", r"\bEAST\b": "E", r"\bWEST\b": "W",
    }
    for pattern, repl in replacements.items():
        addr = re.sub(pattern, repl, addr)
    addr = re.sub(r"[^A-Z0-9\s]", "", addr)
    return re.sub(r"\s+", " ", addr).strip()


_CITY_ABBREVS = {
    "COLORADO SPGS": "COLORADO SPRINGS", "COLO SPGS": "COLORADO SPRINGS",
    "GRAND JCT": "GRAND JUNCTION", "FT COLLINS": "FORT COLLINS",
    "FT WASHAKIE": "FORT WASHAKIE", "GLENWOOD SPGS": "GLENWOOD SPRINGS",
    "STEAMBT SPGS": "STEAMBOAT SPRINGS", "PAGOSA SPGS": "PAGOSA SPRINGS",
    "IDAHO SPGS": "IDAHO SPRINGS", "MANITOU SPGS": "MANITOU SPRINGS",
    "FT WORTH": "FORT WORTH", "FT LAUDERDALE": "FORT LAUDERDALE",
    "ST LOUIS": "SAINT LOUIS", "ST PAUL": "SAINT PAUL",
}


def normalize_city(city: str) -> str:
    if not city:
        return ""
    city = city.upper().strip()
    return _CITY_ABBREVS.get(city, city)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/ryan/impact_project && python -m pytest tests/test_normalize.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/normalize.py tests/test_normalize.py
git commit -m "feat: name, address, and city normalization"
```

---

### Task 3: BMF Loader

**Files:**
- Create: `pipeline/bmf_loader.py`

- [ ] **Step 1: Implement BMF downloader + indexer**

```python
# pipeline/bmf_loader.py
import os
import pandas as pd
import requests
from pipeline.config import BMF_URL_TEMPLATE, US_STATES, DATA_DIR
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def download_bmf(states: list[str] | None = None, data_dir: str = DATA_DIR) -> None:
    """Download IRS BMF CSV files for the given states."""
    os.makedirs(data_dir, exist_ok=True)
    states = states or US_STATES
    for state in states:
        path = os.path.join(data_dir, f"eo_{state}.csv")
        if os.path.exists(path):
            print(f"  {state}: already downloaded")
            continue
        url = BMF_URL_TEMPLATE.format(state=state)
        print(f"  {state}: downloading from {url}")
        resp = requests.get(url)
        resp.raise_for_status()
        with open(path, "wb") as f:
            f.write(resp.content)


def load_bmf(states: list[str] | None = None, data_dir: str = DATA_DIR) -> list[dict]:
    """Load all BMF CSVs into a list of normalized dicts."""
    states = states or US_STATES
    records = []
    for state in states:
        path = os.path.join(data_dir, f"eo_{state}.csv")
        if not os.path.exists(path):
            print(f"  WARNING: {path} not found, skipping")
            continue
        df = pd.read_csv(path, dtype=str)
        for _, row in df.iterrows():
            r = row.to_dict()
            r["_norm_name"] = normalize_name(r.get("NAME", ""))
            r["_norm_addr"] = normalize_address(r.get("STREET", ""))
            r["_norm_city"] = normalize_city(r.get("CITY", ""))
            r["_zip5"] = str(r.get("ZIP", ""))[:5]
            r["_tokens"] = set(
                w for w in r["_norm_name"].split() if len(w) > 2
            )
            records.append(r)
    print(f"Loaded {len(records)} BMF records from {len(states)} states")
    return records


def build_bmf_index(records: list[dict]) -> dict:
    """Build lookup indices for fast matching."""
    by_exact = {}
    by_city = {}
    for r in records:
        by_exact.setdefault(r["_norm_name"], []).append(r)
        by_city.setdefault(r["_norm_city"], []).append(r)
    return {"by_exact": by_exact, "by_city": by_city, "all": records}
```

- [ ] **Step 2: Test download with 2 states**

Run: `cd /Users/ryan/impact_project && python -c "from pipeline.bmf_loader import download_bmf; download_bmf(['wy', 'co'])"`
Expected: "wy: already downloaded", "co: already downloaded" (from experiments)

- [ ] **Step 3: Commit**

```bash
git add pipeline/bmf_loader.py
git commit -m "feat: BMF downloader and indexer for all 50 states"
```

---

### Task 4: USASpending Client

**Files:**
- Create: `pipeline/usaspending_client.py`

- [ ] **Step 1: Implement API client**

```python
# pipeline/usaspending_client.py
import time
import json
import urllib.request
from pipeline.config import USASPENDING_API_BASE


def fetch_nonprofit_recipients(
    state: str | None = None,
    start_year: int = 2020,
    end_year: int = 2024,
) -> list[dict]:
    """Fetch all unique nonprofit grant recipients from USASpending."""
    # Step 1: Get award records to find unique recipient IDs
    all_results = []
    page = 1
    filters = {
        "award_type_codes": ["02", "03", "04", "05"],
        "time_period": [{"start_date": f"{start_year}-01-01", "end_date": f"{end_year}-12-31"}],
        "recipient_type_names": ["nonprofit"],
    }
    if state:
        filters["recipient_locations"] = [{"country": "USA", "state": state}]

    while page <= 200:  # safety cap
        payload = {
            "filters": filters,
            "fields": ["Recipient Name", "recipient_id", "Award Amount", "CFDA Number"],
            "limit": 100,
            "page": page,
            "sort": "Award Amount",
            "order": "desc",
        }
        req = urllib.request.Request(
            f"{USASPENDING_API_BASE}/search/spending_by_award/",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        resp = json.loads(urllib.request.urlopen(req).read())
        results = resp.get("results", [])
        if not results:
            break
        all_results.extend(results)
        page += 1
        time.sleep(0.3)

    # Deduplicate by recipient_id
    unique = {}
    for r in all_results:
        rid = r.get("recipient_id", "")
        if rid and rid not in unique:
            unique[rid] = r.get("Recipient Name", "")

    print(f"Found {len(all_results)} awards, {len(unique)} unique recipients")
    return _fetch_recipient_details(unique)


def _fetch_recipient_details(unique_recipients: dict) -> list[dict]:
    """Fetch full details for each unique recipient."""
    details = []
    errors = 0
    total = len(unique_recipients)
    for i, (rid, name) in enumerate(unique_recipients.items()):
        try:
            url = f"{USASPENDING_API_BASE}/recipient/duns/{rid}/"
            rdata = json.loads(urllib.request.urlopen(url).read())
            loc = rdata.get("location", {}) or {}
            details.append({
                "name": rdata.get("name", ""),
                "uei": rdata.get("uei", ""),
                "duns": rdata.get("duns", ""),
                "address": loc.get("address_line1", ""),
                "city": loc.get("city_name", ""),
                "state": loc.get("state_code", ""),
                "zip": loc.get("zip", ""),
                "business_types": rdata.get("business_types", []),
                "alt_names": rdata.get("alternate_names", []),
            })
        except Exception:
            errors += 1
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{total} fetched ({errors} errors)")
        time.sleep(0.2)
    print(f"Fetched {len(details)} recipients ({errors} errors)")
    return details


def fetch_awards_for_state(
    state: str,
    start_year: int = 2020,
    end_year: int = 2024,
) -> list[dict]:
    """Fetch all nonprofit grant awards for a state with full award details."""
    all_results = []
    page = 1

    while page <= 200:
        payload = {
            "filters": {
                "award_type_codes": ["02", "03", "04", "05"],
                "recipient_locations": [{"country": "USA", "state": state}],
                "time_period": [{"start_date": f"{start_year}-01-01", "end_date": f"{end_year}-12-31"}],
                "recipient_type_names": ["nonprofit"],
            },
            "fields": [
                "Award ID", "Recipient Name", "recipient_id",
                "Award Amount", "Awarding Agency", "Awarding Sub Agency",
                "CFDA Number", "Award Type", "Start Date", "End Date",
                "Description",
            ],
            "limit": 100,
            "page": page,
            "sort": "Award Amount",
            "order": "desc",
        }
        req = urllib.request.Request(
            f"{USASPENDING_API_BASE}/search/spending_by_award/",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        resp = json.loads(urllib.request.urlopen(req).read())
        results = resp.get("results", [])
        if not results:
            break
        all_results.extend(results)
        page += 1
        time.sleep(0.3)

    print(f"Fetched {len(all_results)} awards for {state}")
    return all_results
```

- [ ] **Step 2: Smoke test with WY**

Run: `cd /Users/ryan/impact_project && python -c "from pipeline.usaspending_client import fetch_nonprofit_recipients; r = fetch_nonprofit_recipients('WY'); print(f'{len(r)} recipients')"`
Expected: ~100 recipients

- [ ] **Step 3: Commit**

```bash
git add pipeline/usaspending_client.py
git commit -m "feat: USASpending API client for recipients and awards"
```

---

### Task 5: ER Matcher

**Files:**
- Create: `pipeline/er_matcher.py`
- Create: `tests/test_er_matcher.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_er_matcher.py
from pipeline.er_matcher import match_recipient


def test_exact_name_zip_match():
    bmf_index = {
        "by_exact": {
            "PARTNERS IN HOUSING": [
                {"EIN": "841188208", "NAME": "PARTNERS IN HOUSING INC",
                 "STREET": "455 GOLD PASS HTS", "CITY": "COLORADO SPGS",
                 "STATE": "CO", "ZIP": "80906-3882",
                 "_norm_name": "PARTNERS IN HOUSING", "_norm_addr": "455 GOLD PASS HTS",
                 "_norm_city": "COLORADO SPRINGS", "_zip5": "80906",
                 "_tokens": {"PARTNERS", "HOUSING"}}
            ]
        },
        "by_city": {},
        "all": [],
    }
    recipient = {
        "name": "PARTNERS IN HOUSING INC",
        "address": "455 GOLD PASS HTS",
        "city": "COLORADO SPRINGS",
        "zip": "80906",
        "alt_names": [],
        "business_types": ["nonprofit"],
    }
    match, score, method = match_recipient(recipient, bmf_index)
    assert match is not None
    assert match["EIN"] == "841188208"
    assert score >= 0.9


def test_no_match_returns_none():
    bmf_index = {"by_exact": {}, "by_city": {}, "all": []}
    recipient = {
        "name": "NONEXISTENT ORG",
        "address": "", "city": "", "zip": "",
        "alt_names": [], "business_types": ["nonprofit"],
    }
    match, score, method = match_recipient(recipient, bmf_index)
    assert match is None
    assert score == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_er_matcher.py -v`
Expected: FAIL

- [ ] **Step 3: Implement er_matcher.py**

```python
# pipeline/er_matcher.py
import re
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def _get_tokens(name: str) -> set[str]:
    return set(w for w in name.split() if len(w) > 2)


def _jaccard(s1: set, s2: set) -> float:
    if not s1 or not s2:
        return 0
    return len(s1 & s2) / len(s1 | s2)


def _token_containment(query: set, candidate: set) -> float:
    if not query:
        return 0
    return len(query & candidate) / len(query)


def _address_similarity(addr1: str, addr2: str) -> float:
    a1 = normalize_address(addr1)
    a2 = normalize_address(addr2)
    if not a1 or not a2:
        return 0.0
    if a1 == a2:
        return 1.0
    if a1 in a2 or a2 in a1:
        return 0.9
    m1 = re.match(r"^(\d+)\s+(.+)", a1)
    m2 = re.match(r"^(\d+)\s+(.+)", a2)
    if m1 and m2 and m1.group(1) == m2.group(1):
        t1 = set(m1.group(2).split())
        t2 = set(m2.group(2).split())
        if t1 and t2:
            overlap = len(t1 & t2) / max(len(t1), len(t2))
            if overlap >= 0.5:
                return 0.7 + overlap * 0.2
    t1 = set(a1.split())
    t2 = set(a2.split())
    if t1 and t2:
        return len(t1 & t2) / len(t1 | t2) * 0.5
    return 0.0


def is_govt_entity(rec: dict) -> bool:
    govt_types = {
        "government", "local_government", "regional_and_state_government",
        "national_government", "indian_native_american_tribal_government",
        "council_of_governments", "authorities_and_commissions",
    }
    biz = set(rec.get("business_types", []))
    has_nonprofit = "nonprofit" in biz or "corporate_entity_tax_exempt" in biz
    has_govt = bool(biz & govt_types)
    return has_govt and not has_nonprofit


def match_recipient(rec: dict, bmf_index: dict) -> tuple[dict | None, float, str | None]:
    """Match a USASpending recipient against BMF index. Returns (match, score, method)."""
    all_names = [rec["name"]] + rec.get("alt_names", [])[:10]
    rzip = str(rec.get("zip", ""))[:5]
    rcity = normalize_city(rec.get("city", ""))
    raddr = rec.get("address", "")

    best_match = None
    best_score = 0.0
    best_method = None

    for name_variant in all_names:
        qname = normalize_name(name_variant)
        qtokens = _get_tokens(qname)
        if not qtokens:
            continue

        # Pass 1: Exact name
        candidates = bmf_index["by_exact"].get(qname, [])
        if candidates:
            for c in candidates:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                if c["_zip5"] == rzip:
                    return c, 1.0, f"exact+zip (addr={addr_sim:.2f})"
            for c in candidates:
                if c["_norm_city"] == rcity:
                    return c, 0.95, "exact+city"
            for c in candidates:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                if addr_sim >= 0.7:
                    return c, 0.90, f"exact+addr (addr={addr_sim:.2f})"
            return candidates[0], 0.82, "exact_only"

        # Pass 2: Token in same city
        for c in bmf_index["by_city"].get(rcity, []):
            j = _jaccard(qtokens, c["_tokens"])
            cont = _token_containment(qtokens, c["_tokens"])
            if j >= 0.5 or cont >= 0.75:
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                score = j * 0.3 + cont * 0.2 + addr_sim * 0.25 + 0.1
                if addr_sim >= 0.7:
                    score += 0.1
                if score > best_score:
                    best_score = score
                    best_match = c
                    best_method = f"token+city (j={j:.2f}, addr={addr_sim:.2f})"

    # Pass 3: Token globally (only if no strong match yet)
    if best_score < 0.7:
        qname = normalize_name(rec["name"])
        qtokens = _get_tokens(qname)
        for c in bmf_index["all"]:
            j = _jaccard(qtokens, c["_tokens"])
            cont = _token_containment(qtokens, c["_tokens"])
            if j >= 0.6 or (cont >= 0.8 and j >= 0.4):
                addr_sim = _address_similarity(raddr, c.get("STREET", ""))
                city_bonus = 0.1 if c["_norm_city"] == rcity else 0
                zip_bonus = 0.05 if c["_zip5"] == rzip else 0
                score = j * 0.3 + cont * 0.2 + addr_sim * 0.25 + city_bonus + zip_bonus
                if addr_sim >= 0.7:
                    score += 0.1
                if score > best_score:
                    best_score = score
                    best_match = c
                    best_method = f"token+global (j={j:.2f}, addr={addr_sim:.2f})"

    if best_match and best_score >= 0.55:
        return best_match, best_score, best_method
    return None, 0, None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_er_matcher.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add pipeline/er_matcher.py tests/test_er_matcher.py
git commit -m "feat: 3-pass ER matcher with address confirmation"
```

---

### Task 6: Supabase Schema + Seed Data

**Files:**
- Create: `pipeline/seed_data.py`
- Create: `pipeline/db_schema.py`

- [ ] **Step 1: Create seed data module**

```python
# pipeline/seed_data.py

SECTORS = [
    ("Defense", "#f8ec20"),
    ("Economy & Employment", "#b0b0b8"),
    ("Education", "#70f828"),
    ("Emergency Services, Public Safety & Law Enforcement", "#7070fc"),
    ("Energy", "#a82814"),
    ("Food & Agriculture", "#fc4848"),
    ("Housing", "#fc74ec"),
    ("Humanities & the Arts", "#e1a886"),
    ("Immigration", "#5d0483"),
    ("Infrastructure", "#1c38ac"),
    ("International Development", "#b82cd0"),
    ("Natural Resources, Environment & Public Lands", "#208800"),
    ("Overarching", "#ac581c"),
    ("Public Health & Healthcare", "#f8a850"),
    ("Research & Academic Research", "#3cd4e4"),
    ("Social Services", "#68605c"),
]

ACTION_TYPES = [
    ("Contract Terminated for Convenience", "cuts"),
    ("Federal Worker Resigned", "cuts"),
    ("Federal Workers Fired", "cuts"),
    ("Federal Workers Put on Leave/In Limbo", "cuts"),
    ("Funding Frozen/Paused/Cancelled", "cuts"),
    ("Government Building Disposition", "cuts"),
    ("Hiring Freeze", "cuts"),
    ("Lease Terminated", "cuts"),
    ("Other Federal Cut", "cuts"),
    ("Program Paused/Under Review/Cancelled", "cuts"),
    ("Fed Workers Rehired/Possible Rehiring", "adds"),
    ("Funding Unfrozen", "adds"),
    ("Lease Cancellation Rescinded", "adds"),
    ("New Program/Service/Benefit", "adds"),
    ("Other Federal Add", "adds"),
    ("Industry Response to Cut", "responses"),
    ("Judicial Action", "responses"),
    ("Local Response to Cuts", "responses"),
    ("NGO Response to Cut", "responses"),
    ("State Responses to Cuts", "responses"),
]
```

- [ ] **Step 2: Create db_schema.py with SQL DDL and seed logic**

This module uses the Supabase client to execute raw SQL via the `rpc` method or direct Postgres connection. Since supabase-py doesn't support raw DDL, we'll use `psycopg2` or the Supabase SQL editor. For this first pass, we'll generate a SQL file that can be run against Supabase.

```python
# pipeline/db_schema.py
import uuid
from pipeline.seed_data import SECTORS, ACTION_TYPES

SCHEMA_SQL = """
-- ============================================================
-- Medallion Architecture: raw → staging → public
-- ============================================================

-- Raw schema: unprocessed data exactly as ingested from sources
CREATE SCHEMA IF NOT EXISTS raw;

-- Raw BMF records (one row per IRS BMF record)
CREATE TABLE IF NOT EXISTS raw.bmf_records (
    id BIGSERIAL PRIMARY KEY,
    ein TEXT,
    name TEXT,
    ico TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    subsection TEXT,
    ntee_cd TEXT,
    asset_amt TEXT,
    income_amt TEXT,
    revenue_amt TEXT,
    ruling TEXT,
    source_file TEXT,  -- e.g., "eo_co.csv"
    ingested_at TIMESTAMPTZ DEFAULT now()
);

-- Raw USASpending recipient data
CREATE TABLE IF NOT EXISTS raw.usaspending_recipients (
    id BIGSERIAL PRIMARY KEY,
    name TEXT,
    uei TEXT,
    duns TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    business_types JSONB,
    alt_names JSONB,
    raw_response JSONB,  -- full API response for lineage
    ingested_at TIMESTAMPTZ DEFAULT now()
);

-- Raw USASpending award data
CREATE TABLE IF NOT EXISTS raw.usaspending_awards (
    id BIGSERIAL PRIMARY KEY,
    award_id TEXT,
    recipient_name TEXT,
    recipient_id TEXT,
    award_amount NUMERIC,
    awarding_agency TEXT,
    awarding_sub_agency TEXT,
    cfda_number TEXT,
    award_type TEXT,
    start_date TEXT,
    end_date TEXT,
    description TEXT,
    internal_id BIGINT,
    generated_internal_id TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_bmf_ein ON raw.bmf_records(ein);
CREATE INDEX IF NOT EXISTS idx_raw_bmf_state ON raw.bmf_records(state);
CREATE INDEX IF NOT EXISTS idx_raw_recipients_uei ON raw.usaspending_recipients(uei);
CREATE INDEX IF NOT EXISTS idx_raw_awards_award_id ON raw.usaspending_awards(award_id);

-- Staging schema: normalized, pre-ER data
CREATE SCHEMA IF NOT EXISTS staging;

-- Normalized org records ready for ER matching
CREATE TABLE IF NOT EXISTS staging.normalized_orgs (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('usaspending', 'bmf')),
    source_id TEXT,  -- raw table PK or external ID
    name TEXT,
    normalized_name TEXT,
    normalized_address TEXT,
    normalized_city TEXT,
    state TEXT,
    zip5 TEXT,
    uei TEXT,
    ein TEXT,
    business_types JSONB,
    alt_names JSONB,
    tokens TEXT[],  -- pre-computed name tokens for matching
    processed_at TIMESTAMPTZ DEFAULT now()
);

-- ER match candidates with scores (audit trail)
CREATE TABLE IF NOT EXISTS staging.er_candidates (
    id BIGSERIAL PRIMARY KEY,
    usaspending_org_id BIGINT,  -- FK to staging.normalized_orgs
    bmf_org_id BIGINT,          -- FK to staging.normalized_orgs
    confidence_score FLOAT,
    match_method TEXT,
    name_similarity FLOAT,
    address_similarity FLOAT,
    accepted BOOLEAN DEFAULT FALSE,  -- promoted to public?
    reviewed_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_staging_orgs_norm_name ON staging.normalized_orgs(normalized_name);
CREATE INDEX IF NOT EXISTS idx_staging_orgs_uei ON staging.normalized_orgs(uei);
CREATE INDEX IF NOT EXISTS idx_staging_orgs_ein ON staging.normalized_orgs(ein);
CREATE INDEX IF NOT EXISTS idx_staging_er_score ON staging.er_candidates(confidence_score);

-- ============================================================
-- Public schema: production tables (Gold layer)
-- ============================================================

-- Organizations
CREATE TABLE IF NOT EXISTS organizations (
    organization_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_org_id UUID REFERENCES organizations(organization_id),
    parent_org_id UUID REFERENCES organizations(organization_id),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'merged', 'needs_review', 'er_created')),
    confidence_score FLOAT,
    name TEXT NOT NULL,
    name_aliases TEXT[],
    org_type TEXT CHECK (org_type IN ('nonprofit', 'federal_agency', 'state_agency', 'local_government', 'private')),
    ein TEXT,
    irs_bmf_id TEXT,
    sam_uei TEXT,
    duns_number TEXT,
    propublica_url TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Sectors
CREATE TABLE IF NOT EXISTS sectors (
    sector_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    color TEXT
);

-- Action types
CREATE TABLE IF NOT EXISTS action_types (
    action_type_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL CHECK (category IN ('cuts', 'adds', 'responses'))
);

-- Grants
CREATE TABLE IF NOT EXISTS grants (
    grant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_grant_id UUID REFERENCES grants(grant_id),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'merged', 'needs_review')),
    confidence_score FLOAT,
    title TEXT,
    granter_org_id UUID REFERENCES organizations(organization_id),
    sector_id UUID REFERENCES sectors(sector_id),
    department TEXT,
    program TEXT,
    cfda_number TEXT,
    award_number TEXT,
    external_ids JSONB,
    geographic_scope TEXT CHECK (geographic_scope IN ('national', 'state', 'county', 'city', 'point')),
    geo_state TEXT,
    geo_county TEXT,
    geo_city TEXT,
    latitude FLOAT,
    longitude FLOAT,
    original_funding_amount NUMERIC,
    funding_year INTEGER,
    source_database TEXT DEFAULT 'usaspending',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Grant grantees
CREATE TABLE IF NOT EXISTS grant_grantees (
    grant_id UUID REFERENCES grants(grant_id),
    organization_id UUID REFERENCES organizations(organization_id),
    allocation NUMERIC,
    PRIMARY KEY (grant_id, organization_id)
);

-- Reports
CREATE TABLE IF NOT EXISTS reports (
    report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    grant_id UUID REFERENCES grants(grant_id),
    action_type_id UUID REFERENCES action_types(action_type_id),
    change_amount NUMERIC,
    new_total NUMERIC,
    effective_date DATE,
    date_reported DATE,
    date_entered DATE DEFAULT CURRENT_DATE,
    summary TEXT,
    scope_of_impact TEXT CHECK (scope_of_impact IN ('statewide', 'local')),
    latitude FLOAT,
    longitude FLOAT,
    is_testimonial BOOLEAN DEFAULT FALSE,
    is_doge_data BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Sources
CREATE TABLE IF NOT EXISTS sources (
    source_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type TEXT CHECK (source_type IN ('news_article', 'government_database', 'testimonial', 'doge')),
    title TEXT,
    url TEXT,
    publisher TEXT,
    published_date DATE,
    summary TEXT,
    raw_content TEXT,
    gdelt_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Report-sources junction
CREATE TABLE IF NOT EXISTS report_sources (
    report_id UUID REFERENCES reports(report_id),
    source_id UUID REFERENCES sources(source_id),
    PRIMARY KEY (report_id, source_id)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_organizations_ein ON organizations(ein);
CREATE INDEX IF NOT EXISTS idx_organizations_sam_uei ON organizations(sam_uei);
CREATE INDEX IF NOT EXISTS idx_organizations_state ON organizations(state);
CREATE INDEX IF NOT EXISTS idx_organizations_status ON organizations(status);
CREATE INDEX IF NOT EXISTS idx_grants_award_number ON grants(award_number);
CREATE INDEX IF NOT EXISTS idx_grants_cfda ON grants(cfda_number);
CREATE INDEX IF NOT EXISTS idx_grants_department ON grants(department);
CREATE INDEX IF NOT EXISTS idx_grants_geo_state ON grants(geo_state);
CREATE INDEX IF NOT EXISTS idx_reports_grant_id ON reports(grant_id);
"""


def generate_seed_sql() -> str:
    lines = []
    for name, color in SECTORS:
        sid = str(uuid.uuid4())
        safe_name = name.replace("'", "''")
        lines.append(
            f"INSERT INTO sectors (sector_id, name, color) "
            f"VALUES ('{sid}', '{safe_name}', '{color}') ON CONFLICT (name) DO NOTHING;"
        )
    for name, category in ACTION_TYPES:
        aid = str(uuid.uuid4())
        safe_name = name.replace("'", "''")
        lines.append(
            f"INSERT INTO action_types (action_type_id, name, category) "
            f"VALUES ('{aid}', '{safe_name}', '{category}') ON CONFLICT (name) DO NOTHING;"
        )
    return "\n".join(lines)


def write_schema_file(path: str = "schema.sql") -> None:
    full_sql = SCHEMA_SQL + "\n\n-- Seed data\n" + generate_seed_sql()
    with open(path, "w") as f:
        f.write(full_sql)
    print(f"Schema written to {path}")
    print("Run this in Supabase SQL Editor or via: psql $DATABASE_URL < schema.sql")
```

- [ ] **Step 3: Generate and review schema.sql**

Run: `cd /Users/ryan/impact_project && python -c "from pipeline.db_schema import write_schema_file; write_schema_file()"`
Expected: `schema.sql` created with DDL + seed inserts

- [ ] **Step 4: Run schema.sql against Supabase**

The user needs to either:
- Paste the contents of `schema.sql` into the Supabase SQL Editor at https://supabase.com/dashboard
- Or run: `psql $DATABASE_URL < schema.sql` if they have direct Postgres access

- [ ] **Step 5: Commit**

```bash
git add pipeline/seed_data.py pipeline/db_schema.py schema.sql
git commit -m "feat: Supabase schema DDL with seed data for sectors and action types"
```

---

### Task 7: Database Loader

**Files:**
- Create: `pipeline/db_loader.py`

- [ ] **Step 1: Implement the loader that inserts orgs, grants, and grantees into Supabase**

```python
# pipeline/db_loader.py
import uuid
from supabase import create_client
from pipeline.config import SUPABASE_URL, SUPABASE_KEY
from pipeline.normalize import normalize_name, normalize_address, normalize_city
from pipeline.er_matcher import is_govt_entity


def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _batch_insert(client, table: str, rows: list[dict], batch_size: int = 500) -> None:
    """Insert rows in batches."""
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        client.table(table).insert(batch).execute()


# ---- Raw layer ----

def load_raw_bmf(bmf_records: list[dict]) -> None:
    """Load raw BMF records into raw.bmf_records."""
    client = get_client()
    rows = [
        {
            "ein": r.get("EIN"),
            "name": r.get("NAME"),
            "ico": r.get("ICO"),
            "street": r.get("STREET"),
            "city": r.get("CITY"),
            "state": r.get("STATE"),
            "zip": r.get("ZIP"),
            "subsection": r.get("SUBSECTION"),
            "ntee_cd": r.get("NTEE_CD"),
            "asset_amt": r.get("ASSET_AMT"),
            "income_amt": r.get("INCOME_AMT"),
            "revenue_amt": r.get("REVENUE_AMT"),
            "ruling": r.get("RULING"),
            "source_file": r.get("_source_file", ""),
        }
        for r in bmf_records
    ]
    _batch_insert(client, "raw.bmf_records", rows)
    print(f"Loaded {len(rows)} raw BMF records")


def load_raw_recipients(recipients: list[dict]) -> None:
    """Load raw USASpending recipients into raw.usaspending_recipients."""
    client = get_client()
    rows = [
        {
            "name": r.get("name"),
            "uei": r.get("uei"),
            "duns": r.get("duns"),
            "address": r.get("address"),
            "city": r.get("city"),
            "state": r.get("state"),
            "zip": r.get("zip"),
            "business_types": r.get("business_types"),
            "alt_names": r.get("alt_names"),
        }
        for r in recipients
    ]
    _batch_insert(client, "raw.usaspending_recipients", rows)
    print(f"Loaded {len(rows)} raw recipients")


def load_raw_awards(awards: list[dict]) -> None:
    """Load raw USASpending awards into raw.usaspending_awards."""
    client = get_client()
    rows = [
        {
            "award_id": a.get("Award ID"),
            "recipient_name": a.get("Recipient Name"),
            "recipient_id": a.get("recipient_id"),
            "award_amount": a.get("Award Amount"),
            "awarding_agency": a.get("Awarding Agency"),
            "awarding_sub_agency": a.get("Awarding Sub Agency"),
            "cfda_number": a.get("CFDA Number"),
            "award_type": a.get("Award Type"),
            "start_date": a.get("Start Date"),
            "end_date": a.get("End Date"),
            "description": a.get("Description"),
            "internal_id": a.get("internal_id"),
            "generated_internal_id": a.get("generated_internal_id"),
        }
        for a in awards
    ]
    _batch_insert(client, "raw.usaspending_awards", rows)
    print(f"Loaded {len(rows)} raw awards")


# ---- Staging layer ----

def load_staging_orgs(recipients: list[dict], bmf_records: list[dict]) -> None:
    """Load normalized org records into staging.normalized_orgs."""
    client = get_client()
    rows = []
    for r in recipients:
        rows.append({
            "source": "usaspending",
            "source_id": r.get("uei"),
            "name": r.get("name"),
            "normalized_name": normalize_name(r.get("name", "")),
            "normalized_address": normalize_address(r.get("address", "")),
            "normalized_city": normalize_city(r.get("city", "")),
            "state": r.get("state"),
            "zip5": str(r.get("zip", ""))[:5],
            "uei": r.get("uei"),
            "business_types": r.get("business_types"),
            "alt_names": r.get("alt_names"),
        })
    for r in bmf_records:
        rows.append({
            "source": "bmf",
            "source_id": r.get("EIN"),
            "name": r.get("NAME"),
            "normalized_name": normalize_name(r.get("NAME", "")),
            "normalized_address": normalize_address(r.get("STREET", "")),
            "normalized_city": normalize_city(r.get("CITY", "")),
            "state": r.get("STATE"),
            "zip5": str(r.get("ZIP", ""))[:5],
            "ein": r.get("EIN"),
        })
    _batch_insert(client, "staging.normalized_orgs", rows)
    print(f"Loaded {len(rows)} staging normalized orgs")


def load_staging_er_candidates(
    matches: dict[str, tuple[dict | None, float, str | None]],
) -> None:
    """Load ER match candidates into staging.er_candidates for audit."""
    client = get_client()
    rows = [
        {
            "confidence_score": round(score, 3),
            "match_method": method,
            "accepted": score >= 0.55,
        }
        for uei, (bmf_match, score, method) in matches.items()
        if bmf_match is not None
    ]
    _batch_insert(client, "staging.er_candidates", rows)
    print(f"Loaded {len(rows)} ER candidates")


# ---- Public layer ----

def load_organizations(
    recipients: list[dict],
    bmf_matches: dict[str, tuple[dict | None, float, str | None]],
) -> dict[str, str]:
    """Insert organizations into Supabase. Returns {uei: organization_id} mapping."""
    client = get_client()
    uei_to_org_id = {}
    batch = []

    for rec in recipients:
        uei = rec.get("uei", "")
        if not uei:
            continue

        bmf_match, score, method = bmf_matches.get(uei, (None, 0, None))
        is_govt = is_govt_entity(rec)

        org_type = "nonprofit"
        if is_govt:
            org_type = "local_government"
        elif "corporate_entity_not_tax_exempt" in rec.get("business_types", []):
            org_type = "private"

        status = "active" if bmf_match else "er_created"
        if is_govt:
            status = "er_created"

        org_id = str(uuid.uuid4())
        uei_to_org_id[uei] = org_id

        row = {
            "organization_id": org_id,
            "status": status,
            "confidence_score": round(score, 3) if score else None,
            "name": rec["name"],
            "name_aliases": rec.get("alt_names", [])[:20] or None,
            "org_type": org_type,
            "ein": bmf_match["EIN"] if bmf_match else None,
            "sam_uei": uei,
            "duns_number": rec.get("duns") or None,
            "street_address": rec.get("address") or None,
            "city": rec.get("city") or None,
            "state": rec.get("state") or None,
            "zip": rec.get("zip") or None,
        }
        batch.append(row)

        if len(batch) >= 500:
            client.table("organizations").insert(batch).execute()
            print(f"  Inserted {len(batch)} orgs")
            batch = []

    if batch:
        client.table("organizations").insert(batch).execute()
        print(f"  Inserted {len(batch)} orgs")

    print(f"Total organizations loaded: {len(uei_to_org_id)}")
    return uei_to_org_id


def _get_or_create_agency(client, agency_name: str, agency_cache: dict) -> str:
    """Get or create a federal agency org record. Returns organization_id."""
    if agency_name in agency_cache:
        return agency_cache[agency_name]

    org_id = str(uuid.uuid4())
    client.table("organizations").insert({
        "organization_id": org_id,
        "status": "active",
        "name": agency_name,
        "org_type": "federal_agency",
    }).execute()
    agency_cache[agency_name] = org_id
    return org_id


def load_grants(
    awards: list[dict],
    uei_to_org_id: dict[str, str],
    recipient_uei_lookup: dict[str, str],
) -> None:
    """Insert grants and grant_grantees into Supabase."""
    client = get_client()
    agency_cache = {}
    grant_batch = []
    grantee_batch = []
    seen_awards = set()

    for award in awards:
        award_id = award.get("Award ID", "")
        if not award_id or award_id in seen_awards:
            continue
        seen_awards.add(award_id)

        recipient_id = award.get("recipient_id", "")
        recipient_uei = recipient_uei_lookup.get(recipient_id, "")
        grantee_org_id = uei_to_org_id.get(recipient_uei)

        agency_name = award.get("Awarding Agency", "Unknown Agency")
        granter_org_id = _get_or_create_agency(client, agency_name, agency_cache)

        grant_id = str(uuid.uuid4())

        grant_batch.append({
            "grant_id": grant_id,
            "status": "active",
            "title": (award.get("Description") or "")[:200] or award.get("Award Type"),
            "granter_org_id": granter_org_id,
            "department": agency_name,
            "program": award.get("Awarding Sub Agency"),
            "cfda_number": award.get("CFDA Number"),
            "award_number": award_id,
            "external_ids": {"usaspending_internal_id": award.get("internal_id")},
            "original_funding_amount": award.get("Award Amount"),
            "source_database": "usaspending",
        })

        if grantee_org_id:
            grantee_batch.append({
                "grant_id": grant_id,
                "organization_id": grantee_org_id,
            })

        if len(grant_batch) >= 500:
            client.table("grants").insert(grant_batch).execute()
            if grantee_batch:
                client.table("grant_grantees").insert(grantee_batch).execute()
            print(f"  Inserted {len(grant_batch)} grants")
            grant_batch = []
            grantee_batch = []

    if grant_batch:
        client.table("grants").insert(grant_batch).execute()
        if grantee_batch:
            client.table("grant_grantees").insert(grantee_batch).execute()
        print(f"  Inserted {len(grant_batch)} grants")

    print(f"Total grants loaded: {len(seen_awards)}")
    print(f"Unique agencies created: {len(agency_cache)}")
```

- [ ] **Step 2: Commit**

```bash
git add pipeline/db_loader.py
git commit -m "feat: Supabase loader for organizations, grants, and grantees"
```

---

### Task 8: Pipeline Orchestrator

**Files:**
- Create: `pipeline/run_pipeline.py`

- [ ] **Step 1: Implement the orchestrator**

```python
# pipeline/run_pipeline.py
"""
First-pass national data load.

Usage:
    # Full national run (all 50 states):
    python -m pipeline.run_pipeline

    # Single state test:
    python -m pipeline.run_pipeline --states WY

    # Multiple states:
    python -m pipeline.run_pipeline --states WY CO CA
"""
import argparse
import json
import os
import time

from pipeline.config import DATA_DIR, US_STATES
from pipeline.bmf_loader import download_bmf, load_bmf, build_bmf_index
from pipeline.usaspending_client import fetch_nonprofit_recipients, fetch_awards_for_state
from pipeline.er_matcher import match_recipient, is_govt_entity
from pipeline.db_loader import (
    load_raw_bmf, load_raw_recipients, load_raw_awards,
    load_staging_orgs, load_staging_er_candidates,
    load_organizations, load_grants,
)


def run(states: list[str] | None = None):
    states = [s.lower() for s in (states or US_STATES)]
    state_codes_upper = [s.upper() for s in states]
    start = time.time()

    # Step 1: Download BMF
    print("\n=== Step 1: Download IRS BMF ===")
    download_bmf(states)

    # Step 2: Load and index BMF
    print("\n=== Step 2: Load and index BMF ===")
    bmf_records = load_bmf(states)
    bmf_index = build_bmf_index(bmf_records)

    # Step 3: Fetch USASpending recipients per state
    print("\n=== Step 3: Fetch USASpending recipients ===")
    all_recipients = []
    all_awards = []
    recipient_id_to_uei = {}

    for state in state_codes_upper:
        print(f"\n--- {state} ---")
        cache_path = os.path.join(DATA_DIR, f"{state.lower()}_nonprofit_recipients.json")

        if os.path.exists(cache_path):
            print(f"  Loading cached recipients from {cache_path}")
            with open(cache_path) as f:
                recipients = json.load(f)
        else:
            recipients = fetch_nonprofit_recipients(state)
            with open(cache_path, "w") as f:
                json.dump(recipients, f)

        all_recipients.extend(recipients)

        awards = fetch_awards_for_state(state)
        all_awards.extend(awards)

        # Build recipient_id → UEI lookup from awards
        for a in awards:
            rid = a.get("recipient_id", "")
            # Find matching recipient by name
            for r in recipients:
                if r["name"] == a.get("Recipient Name"):
                    recipient_id_to_uei[rid] = r.get("uei", "")
                    break

    # Deduplicate recipients by UEI
    seen_uei = set()
    unique_recipients = []
    for r in all_recipients:
        uei = r.get("uei", "")
        if uei and uei not in seen_uei:
            seen_uei.add(uei)
            unique_recipients.append(r)

    print(f"\nTotal unique recipients across {len(states)} states: {len(unique_recipients)}")
    print(f"Total awards: {len(all_awards)}")

    # Step 4: Run ER matching
    print("\n=== Step 4: ER Matching ===")
    bmf_matches = {}
    matched = 0
    er_created = 0
    govt_filtered = 0

    for rec in unique_recipients:
        uei = rec.get("uei", "")
        if is_govt_entity(rec):
            govt_filtered += 1
            bmf_matches[uei] = (None, 0, "govt_filtered")
            continue
        m, score, method = match_recipient(rec, bmf_index)
        bmf_matches[uei] = (m, score, method)
        if m:
            matched += 1
        else:
            er_created += 1

    nonprofit_total = matched + er_created
    print(f"Govt filtered: {govt_filtered}")
    print(f"Matched: {matched}/{nonprofit_total} ({matched/nonprofit_total*100:.1f}%)")
    print(f"ER-created: {er_created}")

    # Step 5a: Load raw layer (Bronze)
    print("\n=== Step 5a: Load raw layer ===")
    load_raw_bmf(bmf_records)
    load_raw_recipients(unique_recipients)
    load_raw_awards(all_awards)

    # Step 5b: Load staging layer (Silver)
    print("\n=== Step 5b: Load staging layer ===")
    load_staging_orgs(unique_recipients, bmf_records)
    load_staging_er_candidates(bmf_matches)

    # Step 5c: Load public layer (Gold)
    print("\n=== Step 5c: Load public layer ===")
    uei_to_org_id = load_organizations(unique_recipients, bmf_matches)
    load_grants(all_awards, uei_to_org_id, recipient_id_to_uei)

    elapsed = time.time() - start
    print(f"\n=== Done in {elapsed/60:.1f} minutes ===")
    print(f"Organizations: {len(uei_to_org_id)}")
    print(f"Grants: {len(all_awards)}")
    print(f"Raw BMF records: {len(bmf_records)}")
    print(f"Raw awards: {len(all_awards)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", nargs="+", help="State codes (e.g., WY CO)")
    args = parser.parse_args()
    run(args.states)
```

- [ ] **Step 2: Test with Wyoming only**

Run: `cd /Users/ryan/impact_project && python -m pipeline.run_pipeline --states WY`

This requires:
1. Supabase project created with schema.sql already run
2. `.env` filled in with real SUPABASE_URL and SUPABASE_KEY

Expected output: ~100 orgs loaded, ~300 grants loaded, done in <5 minutes.

- [ ] **Step 3: If WY works, run CO to test at scale**

Run: `python -m pipeline.run_pipeline --states WY CO`

- [ ] **Step 4: When ready, run nationally**

Run: `python -m pipeline.run_pipeline`

Note: This will take 2-3 hours due to USASpending API fetch time. BMF downloads are ~500MB total.

- [ ] **Step 5: Commit**

```bash
git add pipeline/run_pipeline.py
git commit -m "feat: pipeline orchestrator with state-by-state processing"
```

---

### Task 9: Verify with Queries

After the pipeline runs, verify the data by running these queries in Supabase SQL Editor:

- [ ] **Step 1: Check record counts**

```sql
-- Medallion layer counts
SELECT 'raw.bmf_records' as tbl, count(*) FROM raw.bmf_records
UNION ALL SELECT 'raw.usaspending_recipients', count(*) FROM raw.usaspending_recipients
UNION ALL SELECT 'raw.usaspending_awards', count(*) FROM raw.usaspending_awards
UNION ALL SELECT 'staging.normalized_orgs', count(*) FROM staging.normalized_orgs
UNION ALL SELECT 'staging.er_candidates', count(*) FROM staging.er_candidates
UNION ALL SELECT 'public.organizations', count(*) FROM organizations
UNION ALL SELECT 'public.grants', count(*) FROM grants
UNION ALL SELECT 'public.grant_grantees', count(*) FROM grant_grantees
UNION ALL SELECT 'public.sectors', count(*) FROM sectors
UNION ALL SELECT 'public.action_types', count(*) FROM action_types;
```

- [ ] **Step 2: Check ER quality**

```sql
-- Match rate
SELECT status, count(*), round(count(*)::numeric / sum(count(*)) over() * 100, 1) as pct
FROM organizations
GROUP BY status;

-- Confidence distribution
SELECT
    CASE
        WHEN confidence_score >= 0.9 THEN 'high'
        WHEN confidence_score >= 0.7 THEN 'medium'
        WHEN confidence_score > 0 THEN 'low'
        ELSE 'none'
    END as tier,
    count(*)
FROM organizations
WHERE status != 'er_created'
GROUP BY tier;
```

- [ ] **Step 3: Test the county percent change query from the spec**

```sql
-- Won't have full results without geographic_scope populated,
-- but validates the schema works
SELECT g.department, count(*) as grant_count,
       sum(g.original_funding_amount) as total_funding
FROM grants g
GROUP BY g.department
ORDER BY total_funding DESC NULLS LAST
LIMIT 10;
```

- [ ] **Step 4: Test org-grant join**

```sql
SELECT o.name, o.ein, o.sam_uei, o.city, o.state,
       count(gg.grant_id) as grant_count,
       sum(g.original_funding_amount) as total_funding
FROM organizations o
JOIN grant_grantees gg ON gg.organization_id = o.organization_id
JOIN grants g ON g.grant_id = gg.grant_id
GROUP BY o.organization_id
ORDER BY total_funding DESC NULLS LAST
LIMIT 20;
```

---

## Pre-requisites Before Running

1. **Create Supabase project** at https://supabase.com (free tier is fine)
2. **Get credentials**: Project URL + anon key (or service role key) from Settings → API
3. **Fill in `.env`** with real values
4. **Run schema.sql** in Supabase SQL Editor
5. **Connect Supabase MCP** in Claude Code for querying (optional but recommended)

## What You'll Have After This Plan

- 8 tables in Supabase, all with correct schema and constraints
- 16 sectors + 20 action types seeded
- ~50,000-100,000 organization records (80% BMF-matched with EIN, 20% er_created)
- ~500,000+ grant records from USASpending (2020-2024)
- Grant-grantee relationships linking grants to orgs
- Federal agencies as org records (granter side)
- A queryable database you can explore with SQL or the Supabase dashboard
