import math
import uuid
from supabase import create_client
from pipeline.config import SUPABASE_URL, SUPABASE_KEY
from pipeline.normalize import normalize_name, normalize_address, normalize_city
from pipeline.er_matcher import is_govt_entity


def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _sanitize(value):
    """Replace NaN/Inf floats with None so they serialize to JSON null."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _sanitize_row(row: dict) -> dict:
    return {k: _sanitize(v) for k, v in row.items()}


def _batch_insert(client, table: str, rows: list[dict], batch_size: int = 500, schema: str = "public") -> None:
    """Insert rows in batches, sanitizing non-JSON-compliant floats."""
    rows = [_sanitize_row(r) for r in rows]
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        if schema != "public":
            client.schema(schema).table(table).insert(batch).execute()
        else:
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
    _batch_insert(client, "bmf_records", rows, schema="raw")
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
    _batch_insert(client, "usaspending_recipients", rows, schema="raw")
    print(f"Loaded {len(rows)} raw recipients")


def load_raw_awards(awards: list[dict]) -> None:
    """Load raw USASpending awards into raw.usaspending_awards."""
    client = get_client()
    rows = [
        {
            "award_id": a.get("Award ID"),
            "recipient_name": a.get("Recipient Name"),
            "recipient_uei": a.get("recipient_uei", ""),
            "award_amount": a.get("Award Amount"),
            "awarding_agency": a.get("Awarding Agency"),
            "awarding_sub_agency": a.get("Awarding Sub Agency"),
            "cfda_number": a.get("CFDA Number"),
            "award_type": a.get("Award Type"),
            "start_date": a.get("Start Date"),
            "end_date": a.get("End Date"),
            "description": (a.get("Description") or "")[:2000],
        }
        for a in awards
    ]
    _batch_insert(client, "usaspending_awards", rows, schema="raw")
    print(f"Loaded {len(rows)} raw awards")


# ---- Staging layer ----

def load_staging_orgs(recipients: list[dict]) -> None:
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
    _batch_insert(client, "normalized_orgs", rows, schema="staging")
    print(f"Loaded {len(rows)} staging normalized orgs")


def load_staging_er_candidates(
    matches: dict[str, tuple[dict | None, float, str | None]],
) -> None:
    """Load ER match candidates into staging.er_candidates for audit."""
    client = get_client()
    rows = []
    for uei, (bmf_match, score, method) in matches.items():
        if bmf_match is not None and method != "govt_filtered":
            rows.append({
                "usaspending_uei": uei,
                "usaspending_name": "",
                "bmf_ein": bmf_match.get("EIN"),
                "bmf_name": bmf_match.get("NAME"),
                "confidence_score": round(score, 3),
                "match_method": method,
                "accepted": score >= 0.55,
            })
    _batch_insert(client, "er_candidates", rows, schema="staging")
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
