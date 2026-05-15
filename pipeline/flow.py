"""
Prefect flow for the Impact Project data pipeline.

Usage:
    # Start Prefect server (separate terminal):
    prefect server start

    # Run the pipeline:
    python -m pipeline.flow --zip data/raw/usaspending/usaspending_archive_FY2024.zip

    # Run with specific states for BMF:
    python -m pipeline.flow --zip data/raw/usaspending/FY2024.zip --states WY CO

    # Run all 3 fiscal years:
    python -m pipeline.flow --zip data/raw/usaspending/usaspending_archive_FY2023.zip data/raw/usaspending/usaspending_archive_FY2024.zip data/raw/usaspending/usaspending_archive_FY2025.zip
"""
import argparse
import os
import tempfile
import shutil
import time
import uuid
import zipfile

import duckdb
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from pipeline.config import PROCESSED_DIR, BMF_DIR
from pipeline.bmf_loader import download_bmf, load_bmf, build_bmf_index
from pipeline.er_matcher import match_recipient, is_govt_entity
from pipeline.seed_data import SECTORS, ACTION_TYPES


# ─── Bronze Tasks ───

@task(name="load-raw-awards", log_prints=True, retries=1)
def load_raw_awards(zip_path: str, conn_path: str) -> int:
    """Unzip and load USASpending CSV into raw_awards table."""
    logger = get_run_logger()
    logger.info(f"Loading raw awards from {zip_path}")

    tmp_dir = tempfile.mkdtemp(prefix="usaspending_")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmp_dir)
    csv_glob = os.path.join(tmp_dir, "*.csv")

    conn = duckdb.connect(conn_path)

    # Create or append
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    if "raw_awards" in tables:
        before = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
        conn.execute(f"INSERT INTO raw_awards SELECT * FROM read_csv_auto('{csv_glob}', ignore_errors=true)")
        after = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
        added = after - before
    else:
        conn.execute(f"CREATE TABLE raw_awards AS SELECT * FROM read_csv_auto('{csv_glob}', ignore_errors=true)")
        added = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]

    total = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
    conn.close()
    shutil.rmtree(tmp_dir)

    logger.info(f"Added {added:,} rows (total: {total:,})")
    return total


@task(name="download-bmf", log_prints=True, retries=2)
def download_bmf_task(states: list[str] | None = None) -> str:
    """Download IRS BMF CSV files."""
    logger = get_run_logger()
    states = states or None
    download_bmf(states)
    logger.info("BMF download complete")
    return BMF_DIR


@task(name="load-bmf", log_prints=True)
def load_bmf_task(states: list[str] | None = None) -> list[dict]:
    """Load BMF CSVs into memory and normalize."""
    logger = get_run_logger()
    records = load_bmf(states)
    logger.info(f"Loaded {len(records):,} BMF records")
    return records


@task(name="load-bmf-to-duckdb", log_prints=True)
def load_bmf_to_duckdb(conn_path: str) -> int:
    """Load BMF records into DuckDB for joins."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path)

    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    if "bmf_records" in tables:
        conn.execute("DROP TABLE bmf_records")

    csv_glob = os.path.join(BMF_DIR, "eo_*.csv")
    conn.execute(f"CREATE TABLE bmf_records AS SELECT * FROM read_csv_auto('{csv_glob}', ignore_errors=true)")
    count = conn.execute("SELECT count(*) FROM bmf_records").fetchone()[0]
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bmf_ein ON bmf_records(EIN)")
    conn.close()

    logger.info(f"Loaded {count:,} BMF records into DuckDB")
    return count


# ─── Silver Tasks ───

@task(name="build-bmf-index", log_prints=True)
def build_index_task(bmf_records: list[dict]) -> dict:
    """Build in-memory BMF index for ER matching."""
    logger = get_run_logger()
    index = build_bmf_index(bmf_records)
    logger.info(f"Index: {len(index['by_exact']):,} names, {len(index['by_city']):,} cities")
    return index


@task(name="extract-recipients", log_prints=True)
def extract_recipients(conn_path: str) -> list[dict]:
    """Extract unique recipients from raw_awards."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path, read_only=True)

    df = conn.sql("""
        SELECT DISTINCT ON (recipient_uei)
            recipient_uei as uei,
            recipient_name as name,
            recipient_duns as duns,
            recipient_address_line_1 as address,
            recipient_city_name as city,
            recipient_state_code as state,
            recipient_zip_code as zip,
            business_types_description as biz_desc
        FROM raw_awards
        WHERE recipient_uei IS NOT NULL
          AND recipient_uei != ''
          AND recipient_name NOT LIKE '%REDACTED%'
    """).df()
    conn.close()

    recipients = []
    for _, row in df.iterrows():
        recipients.append({
            "name": row["name"] or "",
            "uei": row["uei"] or "",
            "duns": row["duns"] or "",
            "address": row["address"] or "",
            "city": row["city"] or "",
            "state": row["state"] or "",
            "zip": row["zip"] or "",
            "business_types": [],
            "business_types_description": row["biz_desc"] or "",
            "alt_names": [],
        })

    logger.info(f"Extracted {len(recipients):,} unique recipients")
    return recipients


@task(name="er-matching", log_prints=True)
def run_er_matching(recipients: list[dict], bmf_index: dict) -> list[tuple]:
    """Run entity resolution matching."""
    logger = get_run_logger()
    results = []
    matched = er_created = 0

    for i, rec in enumerate(recipients, 1):
        uei = rec["uei"]
        bmf_match, score, method = match_recipient(rec, bmf_index)
        if bmf_match:
            matched += 1
        else:
            er_created += 1

        # Classify org_type
        desc = rec.get("business_types_description", "").upper()
        if "FOR-PROFIT" in desc or "SMALL BUSINESS" in desc:
            org_type = "private"
        elif "STATE GOVERNMENT" in desc:
            org_type = "state_government"
        elif any(x in desc for x in ["CITY", "COUNTY", "TOWNSHIP", "SPECIAL DISTRICT", "INDEPENDENT SCHOOL"]):
            org_type = "local_government"
        elif any(x in desc for x in ["INDIAN", "TRIBAL"]):
            org_type = "tribal"
        elif any(x in desc for x in ["NONPROFIT", "501C3"]):
            org_type = "nonprofit"
        elif "PRIVATE" in desc and "HIGHER EDUCATION" in desc:
            org_type = "private_higher_education"
        elif "PUBLIC" in desc and "HIGHER EDUCATION" in desc:
            org_type = "public_higher_education"
        elif "NON-DOMESTIC" in desc:
            org_type = "foreign"
        elif desc == "OTHER":
            org_type = "non_classified"
        else:
            org_type = "non_classified"

        status = "active" if bmf_match else "er_created"
        ntee = None
        if bmf_match:
            ntee = bmf_match.get("NTEE_CD")
            if ntee and not isinstance(ntee, str):
                ntee = None

        results.append((
            str(uuid.uuid4()), status, round(score, 3) if score else None,
            rec["name"], org_type,
            bmf_match["EIN"] if bmf_match else None,
            uei, rec.get("duns") or None,
            rec.get("address") or None, rec.get("city") or None,
            rec.get("state") or None, rec.get("zip") or None,
            ntee, rec.get("business_types_description") or None,
            method,
        ))

        if i % 10000 == 0:
            logger.info(f"ER: {i:,}/{len(recipients):,} ({matched:,} matched, {er_created:,} er_created)")

    nonprofit_total = matched + er_created
    if nonprofit_total > 0:
        logger.info(f"ER complete: {matched:,}/{nonprofit_total:,} matched ({matched/nonprofit_total*100:.1f}%), {er_created:,} er_created")

    return results


# ─── Gold Tasks ───

@task(name="seed-reference-data", log_prints=True)
def seed_reference_data(conn_path: str) -> None:
    """Insert sectors and action_types."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path)

    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]

    if "sectors" not in tables:
        conn.execute("CREATE TABLE sectors (sector_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE, color VARCHAR)")
    for name, color in SECTORS:
        conn.execute("INSERT OR IGNORE INTO sectors VALUES (?, ?, ?)", [str(uuid.uuid4()), name, color])

    if "action_types" not in tables:
        conn.execute("CREATE TABLE action_types (action_type_id VARCHAR PRIMARY KEY, name VARCHAR UNIQUE, category VARCHAR)")
    for name, cat in ACTION_TYPES:
        conn.execute("INSERT OR IGNORE INTO action_types VALUES (?, ?, ?)", [str(uuid.uuid4()), name, cat])

    conn.close()
    logger.info(f"Seeded {len(SECTORS)} sectors, {len(ACTION_TYPES)} action types")


@task(name="load-organizations", log_prints=True)
def load_organizations(conn_path: str, er_results: list[tuple]) -> dict[str, str]:
    """Insert organizations into DuckDB. Returns {uei: org_id} mapping."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path)

    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    if "organizations" not in tables:
        conn.execute("""
            CREATE TABLE organizations (
                organization_id VARCHAR PRIMARY KEY, status VARCHAR,
                confidence_score DOUBLE, name VARCHAR, org_type VARCHAR,
                ein VARCHAR, sam_uei VARCHAR, duns_number VARCHAR,
                street_address VARCHAR, city VARCHAR, state VARCHAR,
                zip VARCHAR, ntee_code VARCHAR,
                business_types_description VARCHAR, match_method VARCHAR
            )
        """)

    conn.executemany(
        "INSERT INTO organizations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        er_results,
    )

    uei_to_org = {r[6]: r[0] for r in er_results}  # uei=index 6, org_id=index 0
    conn.close()

    logger.info(f"Loaded {len(er_results):,} organizations")
    return uei_to_org


@task(name="load-agencies", log_prints=True)
def load_agencies(conn_path: str) -> dict[str, str]:
    """Create federal agency org records from distinct awarding agencies."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path)

    agencies = conn.execute("""
        SELECT DISTINCT awarding_agency_name
        FROM raw_awards WHERE awarding_agency_name IS NOT NULL
    """).fetchall()

    agency_map = {}
    for (name,) in agencies:
        oid = str(uuid.uuid4())
        agency_map[name] = oid
        conn.execute(
            "INSERT INTO organizations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [oid, "active", None, name, "federal_agency",
             None, None, None, None, None, None, None, None, None, None],
        )

    conn.close()
    logger.info(f"Created {len(agency_map)} agency records")
    return agency_map


@task(name="load-grants", log_prints=True)
def load_grants(conn_path: str, uei_to_org: dict, agency_map: dict) -> int:
    """Insert deduplicated grants and grant_grantees."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path)

    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    if "grants" not in tables:
        conn.execute("""
            CREATE TABLE grants (
                grant_id VARCHAR PRIMARY KEY, status VARCHAR, title VARCHAR,
                granter_org_id VARCHAR, department VARCHAR, program VARCHAR,
                cfda_number VARCHAR, award_number VARCHAR,
                geo_state VARCHAR, geo_city VARCHAR,
                original_funding_amount DOUBLE, source_database VARCHAR
            )
        """)
    if "grant_grantees" not in tables:
        conn.execute("""
            CREATE TABLE grant_grantees (
                grant_id VARCHAR, organization_id VARCHAR,
                PRIMARY KEY (grant_id, organization_id)
            )
        """)

    awards_df = conn.execute("""
        SELECT DISTINCT ON (award_id_fain)
            award_id_fain as award_id, recipient_uei,
            total_obligated_amount, awarding_agency_name,
            awarding_sub_agency_name, cfda_number,
            assistance_type_description as award_type,
            prime_award_base_transaction_description as description,
            primary_place_of_performance_state_name as pop_state,
            primary_place_of_performance_city_name as pop_city
        FROM raw_awards
        WHERE award_id_fain IS NOT NULL AND award_id_fain != ''
    """).df()
    logger.info(f"Processing {len(awards_df):,} unique awards")

    grant_rows = []
    grantee_rows = []
    for _, a in awards_df.iterrows():
        gid = str(uuid.uuid4())
        agency = a["awarding_agency_name"] or "Unknown Agency"
        granter_id = agency_map.get(agency)

        try:
            amount = float(a["total_obligated_amount"]) if a["total_obligated_amount"] else None
        except (ValueError, TypeError):
            amount = None

        desc = str(a["description"])[:200] if a["description"] else a["award_type"]
        grant_rows.append((
            gid, "active", desc, granter_id, agency,
            a["awarding_sub_agency_name"], a["cfda_number"],
            a["award_id"], a["pop_state"], a["pop_city"],
            amount, "usaspending",
        ))

        uei = a["recipient_uei"]
        org_id = uei_to_org.get(uei)
        if org_id:
            grantee_rows.append((gid, org_id))

    conn.executemany(
        "INSERT INTO grants VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        grant_rows,
    )
    conn.executemany(
        "INSERT INTO grant_grantees VALUES (?,?)",
        grantee_rows,
    )

    conn.close()
    logger.info(f"Loaded {len(grant_rows):,} grants, {len(grantee_rows):,} grantees")
    return len(grant_rows)


@task(name="create-views-and-indexes", log_prints=True)
def create_views_and_indexes(conn_path: str) -> None:
    """Create analytical views and indexes."""
    logger = get_run_logger()
    conn = duckdb.connect(conn_path)

    conn.execute("""
        CREATE OR REPLACE VIEW org_grant_summary AS
        SELECT o.name, o.ein, o.sam_uei, o.city, o.state, o.status,
               o.org_type, o.business_types_description, o.ntee_code,
               o.confidence_score, o.match_method,
               count(gg.grant_id) as grant_count,
               sum(g.original_funding_amount) as total_funding
        FROM organizations o
        LEFT JOIN grant_grantees gg ON gg.organization_id = o.organization_id
        LEFT JOIN grants g ON g.grant_id = gg.grant_id
        WHERE o.org_type != 'federal_agency'
        GROUP BY ALL
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW funding_by_department AS
        SELECT department, count(*) as grant_count,
               sum(original_funding_amount) as total_funding,
               count(DISTINCT gg.organization_id) as recipient_count
        FROM grants g
        LEFT JOIN grant_grantees gg ON gg.grant_id = g.grant_id
        GROUP BY department ORDER BY total_funding DESC
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW funding_by_state AS
        SELECT o.state, count(DISTINCT o.organization_id) as org_count,
               count(gg.grant_id) as grant_count,
               sum(g.original_funding_amount) as total_funding
        FROM organizations o
        JOIN grant_grantees gg ON gg.organization_id = o.organization_id
        JOIN grants g ON g.grant_id = gg.grant_id
        WHERE o.state IS NOT NULL
        GROUP BY o.state ORDER BY total_funding DESC
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_org_ein ON organizations(ein)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_org_uei ON organizations(sam_uei)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_org_state ON organizations(state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_grants_award ON grants(award_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_grants_dept ON grants(department)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_uei ON raw_awards(recipient_uei)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_fain ON raw_awards(award_id_fain)")

    conn.close()
    logger.info("Views and indexes created")


# ─── Main Flow ───

@flow(name="impact-pipeline", log_prints=True)
def impact_pipeline(
    zip_paths: list[str],
    db_path: str | None = None,
    bmf_states: list[str] | None = None,
):
    """Full pipeline: Bronze → Silver → Gold."""
    logger = get_run_logger()
    db_path = db_path or os.path.join(PROCESSED_DIR, "impact.duckdb")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Remove old DB for fresh build
    if os.path.exists(db_path):
        os.remove(db_path)
        logger.info(f"Removed existing {db_path}")
    wal = db_path + ".wal"
    if os.path.exists(wal):
        os.remove(wal)

    logger.info(f"Building {db_path} from {len(zip_paths)} archives")

    # ── Bronze ──
    logger.info("═══ BRONZE LAYER ═══")
    for zp in zip_paths:
        load_raw_awards(zp, db_path)

    download_bmf_task(bmf_states)
    load_bmf_to_duckdb(db_path)

    # ── Silver ──
    logger.info("═══ SILVER LAYER ═══")
    bmf_records = load_bmf_task(bmf_states)
    bmf_index = build_index_task(bmf_records)
    recipients = extract_recipients(db_path)
    er_results = run_er_matching(recipients, bmf_index)

    # ── Gold ──
    logger.info("═══ GOLD LAYER ═══")
    seed_reference_data(db_path)
    uei_to_org = load_organizations(db_path, er_results)
    agency_map = load_agencies(db_path)
    grant_count = load_grants(db_path, uei_to_org, agency_map)
    create_views_and_indexes(db_path)

    # ── Summary artifact ──
    matched = sum(1 for r in er_results if r[1] == "active")
    total = len(er_results)
    db_size = os.path.getsize(db_path) / (1024 ** 3)

    create_markdown_artifact(
        key="pipeline-summary",
        markdown=f"""# Pipeline Run Summary

| Metric | Value |
|---|---|
| Archives loaded | {len(zip_paths)} |
| Raw award rows | {duckdb.connect(db_path, read_only=True).execute('SELECT count(*) FROM raw_awards').fetchone()[0]:,} |
| Organizations | {total:,} |
| ER matched (with EIN) | {matched:,} ({matched/total*100:.1f}%) |
| Grants | {grant_count:,} |
| Database size | {db_size:.1f} GB |
""",
    )

    logger.info(f"Pipeline complete — {db_size:.1f} GB database at {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", nargs="+", required=True, help="USASpending archive ZIPs")
    parser.add_argument("--db", default=None, help="DuckDB output path")
    parser.add_argument("--states", nargs="+", help="BMF states (default: all)")
    args = parser.parse_args()
    impact_pipeline(
        zip_paths=args.zip,
        db_path=args.db,
        bmf_states=[s.lower() for s in args.states] if args.states else None,
    )
