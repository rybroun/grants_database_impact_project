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
    all_results = []
    page = 1
    filters = {
        "award_type_codes": ["02", "03", "04", "05"],
        "time_period": [{"start_date": f"{start_year}-01-01", "end_date": f"{end_year}-12-31"}],
        "recipient_type_names": ["nonprofit"],
    }
    if state:
        filters["recipient_locations"] = [{"country": "USA", "state": state}]

    while page <= 200:
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
