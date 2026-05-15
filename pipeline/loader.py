"""
Load USASpending bulk CSV + IRS BMF into a DuckDB database.

Usage:
    python -m pipeline.loader --zip data/raw/usaspending/usaspending_archive_FY2024.zip
"""
import argparse
import os
import time
import uuid
import zipfile
import tempfile
import shutil

import duckdb

from pipeline.config import PROCESSED_DIR, BMF_DIR
from pipeline.bmf_loader import download_bmf, load_bmf, build_bmf_index
from pipeline.er_matcher import match_recipient, is_govt_entity
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(zip_path: str, db_path: str, bmf_states: list[str] | None = None):
    start = time.time()

    # Unzip CSVs to temp dir
    log("Unzipping CSVs...")
    tmp_dir = tempfile.mkdtemp(prefix="usaspending_")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmp_dir)
    csv_glob = os.path.join(tmp_dir, "*.csv")
    log(f"  Extracted to {tmp_dir}")

    # Create DuckDB
    log(f"Creating database at {db_path}")
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = duckdb.connect(db_path)

    # ─── Raw layer: load all CSV data as parquet-backed table ───
    log("=== Raw layer ===")
    conn.execute(f"""
        CREATE TABLE raw_awards AS
        SELECT * FROM read_csv_auto('{csv_glob}', ignore_errors=true)
    """)
    raw_count = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
    log(f"  raw_awards: {raw_count} rows")

    # Clean up temp CSVs
    shutil.rmtree(tmp_dir)

    # ─── Reference tables ───
    log("=== Reference tables ===")
    from pipeline.seed_data import SECTORS, ACTION_TYPES

    conn.execute("""
        CREATE TABLE sectors (
            sector_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            color VARCHAR
        )
    """)
    for name, color in SECTORS:
        conn.execute("INSERT INTO sectors VALUES (?, ?, ?)",
                     [str(uuid.uuid4()), name, color])

    conn.execute("""
        CREATE TABLE action_types (
            action_type_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            category VARCHAR NOT NULL
        )
    """)
    for name, cat in ACTION_TYPES:
        conn.execute("INSERT INTO action_types VALUES (?, ?, ?)",
                     [str(uuid.uuid4()), name, cat])
    log(f"  sectors: {len(SECTORS)}, action_types: {len(ACTION_TYPES)}")

    # ─── Extract unique recipients from raw ───
    log("=== Extracting unique recipients ===")
    recip_df = conn.execute("""
        SELECT DISTINCT ON (recipient_uei)
            recipient_uei as uei,
            recipient_name as name,
            recipient_duns as duns,
            recipient_address_line_1 as address,
            recipient_city_name as city,
            recipient_state_code as state,
            recipient_zip_code as zip,
            business_types_description as biz_desc,
            business_types_code as biz_code
        FROM raw_awards
        WHERE recipient_uei IS NOT NULL
          AND recipient_uei != ''
          AND recipient_name NOT LIKE '%REDACTED%'
    """).df()
    log(f"  {len(recip_df)} unique recipients")

    # Convert to list of dicts for ER
    recipients = []
    for _, row in recip_df.iterrows():
        recipients.append({
            'name': row['name'] or '',
            'uei': row['uei'] or '',
            'duns': row['duns'] or '',
            'address': row['address'] or '',
            'city': row['city'] or '',
            'state': row['state'] or '',
            'zip': row['zip'] or '',
            'business_types': [],  # not used for ER matching
            'business_types_description': row['biz_desc'] or '',
            'alt_names': [],
        })

    # ─── Load BMF + ER match ───
    log("=== Loading IRS BMF ===")
    if bmf_states:
        download_bmf(bmf_states)
    bmf_records = load_bmf(bmf_states)
    bmf_index = build_bmf_index(bmf_records)
    log(f"  {len(bmf_records)} BMF records, {len(bmf_index['by_exact'])} unique names")

    log("=== ER Matching ===")
    org_rows = []
    matched = er_created = govt_filtered = 0

    for i, rec in enumerate(recipients, 1):
        uei = rec['uei']
        if is_govt_entity(rec):
            govt_filtered += 1
            bmf_match, score, method = None, 0, 'govt_filtered'
        else:
            bmf_match, score, method = match_recipient(rec, bmf_index)
            if bmf_match:
                matched += 1
            else:
                er_created += 1

        is_govt = is_govt_entity(rec)
        desc = rec.get('business_types_description', '').upper()
        if 'FOR-PROFIT' in desc or 'SMALL BUSINESS' in desc:
            org_type = 'private'
        elif 'STATE GOVERNMENT' in desc:
            org_type = 'state_agency'
        elif any(x in desc for x in ['CITY', 'COUNTY', 'TOWNSHIP', 'SPECIAL DISTRICT', 'INDEPENDENT SCHOOL', 'INDIAN', 'TRIBAL']):
            org_type = 'local_government'
        elif any(x in desc for x in ['NONPROFIT', '501C3', 'HIGHER EDUCATION', 'OTHER']):
            org_type = 'nonprofit'
        else:
            org_type = 'private'
        status = 'active' if bmf_match else 'er_created'
        if is_govt:
            status = 'er_created'

        ntee = None
        if bmf_match:
            ntee = bmf_match.get('NTEE_CD')
            if ntee and not isinstance(ntee, str):
                ntee = None

        org_rows.append((
            str(uuid.uuid4()), status, round(score, 3) if score else None,
            rec['name'], org_type,
            bmf_match['EIN'] if bmf_match else None,
            uei, rec.get('duns') or None,
            rec.get('address') or None, rec.get('city') or None,
            rec.get('state') or None, rec.get('zip') or None,
            ntee, rec.get('business_types_description') or None,
            method,
        ))

        if i % 10000 == 0:
            log(f"  ER: {i}/{len(recipients)} ({matched} matched, {er_created} er_created)")

    nonprofit_total = matched + er_created
    if nonprofit_total > 0:
        log(f"  ER done: {matched}/{nonprofit_total} ({matched/nonprofit_total*100:.1f}%), {er_created} er_created, {govt_filtered} govt")

    # ─── Organizations table ───
    log("=== Loading organizations ===")
    conn.execute("""
        CREATE TABLE organizations (
            organization_id VARCHAR PRIMARY KEY,
            status VARCHAR,
            confidence_score DOUBLE,
            name VARCHAR,
            org_type VARCHAR,
            ein VARCHAR,
            sam_uei VARCHAR,
            duns_number VARCHAR,
            street_address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            ntee_code VARCHAR,
            business_types_description VARCHAR,
            match_method VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO organizations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        org_rows
    )

    # Build UEI → org_id lookup
    uei_to_org = {r[6]: r[0] for r in org_rows}  # uei is index 6, org_id is 0
    log(f"  {len(org_rows)} organizations")

    # ─── Agencies ───
    log("=== Loading agencies ===")
    agencies = conn.execute("""
        SELECT DISTINCT awarding_agency_name
        FROM raw_awards
        WHERE awarding_agency_name IS NOT NULL
    """).fetchall()

    agency_map = {}
    for (name,) in agencies:
        oid = str(uuid.uuid4())
        agency_map[name] = oid
        conn.execute(
            "INSERT INTO organizations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [oid, 'active', None, name, 'federal_agency',
             None, None, None, None, None, None, None, None, None, None]
        )
    log(f"  {len(agency_map)} agencies")

    # ─── Grants + grantees (from raw, deduplicated) ───
    log("=== Loading grants ===")

    # Get unique awards with their details
    awards_df = conn.execute("""
        SELECT DISTINCT ON (award_id_fain)
            award_id_fain as award_id,
            recipient_uei,
            total_obligated_amount,
            awarding_agency_name,
            awarding_sub_agency_name,
            cfda_number,
            assistance_type_description as award_type,
            prime_award_base_transaction_description as description,
            primary_place_of_performance_state_name as pop_state,
            primary_place_of_performance_city_name as pop_city,
            recipient_state_code as recipient_state
        FROM raw_awards
        WHERE award_id_fain IS NOT NULL AND award_id_fain != ''
    """).df()
    log(f"  {len(awards_df)} unique awards")

    conn.execute("""
        CREATE TABLE grants (
            grant_id VARCHAR PRIMARY KEY,
            status VARCHAR DEFAULT 'active',
            title VARCHAR,
            granter_org_id VARCHAR,
            department VARCHAR,
            program VARCHAR,
            cfda_number VARCHAR,
            award_number VARCHAR,
            geo_state VARCHAR,
            geo_city VARCHAR,
            original_funding_amount DOUBLE,
            source_database VARCHAR DEFAULT 'usaspending'
        )
    """)
    conn.execute("""
        CREATE TABLE grant_grantees (
            grant_id VARCHAR,
            organization_id VARCHAR,
            PRIMARY KEY (grant_id, organization_id)
        )
    """)

    grant_rows = []
    grantee_rows = []
    for _, a in awards_df.iterrows():
        gid = str(uuid.uuid4())
        agency = a['awarding_agency_name'] or 'Unknown Agency'
        granter_id = agency_map.get(agency)

        try:
            amount = float(a['total_obligated_amount']) if a['total_obligated_amount'] else None
        except (ValueError, TypeError):
            amount = None

        desc = str(a['description'])[:200] if a['description'] else a['award_type']
        grant_rows.append((
            gid, 'active', desc, granter_id, agency,
            a['awarding_sub_agency_name'], a['cfda_number'],
            a['award_id'], a['pop_state'], a['pop_city'],
            amount,
        ))

        uei = a['recipient_uei']
        org_id = uei_to_org.get(uei)
        if org_id:
            grantee_rows.append((gid, org_id))

    conn.executemany(
        "INSERT INTO grants VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], 'usaspending') for r in grant_rows]
    )
    conn.executemany(
        "INSERT INTO grant_grantees VALUES (?,?)",
        grantee_rows
    )
    log(f"  {len(grant_rows)} grants, {len(grantee_rows)} grantees")

    # ─── Create useful views ───
    log("=== Creating views ===")
    conn.execute("""
        CREATE VIEW org_grant_summary AS
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
        CREATE VIEW funding_by_department AS
        SELECT department,
               count(*) as grant_count,
               sum(original_funding_amount) as total_funding,
               count(DISTINCT gg.organization_id) as recipient_count
        FROM grants g
        LEFT JOIN grant_grantees gg ON gg.grant_id = g.grant_id
        GROUP BY department
        ORDER BY total_funding DESC
    """)

    conn.execute("""
        CREATE VIEW funding_by_state AS
        SELECT o.state,
               count(DISTINCT o.organization_id) as org_count,
               count(gg.grant_id) as grant_count,
               sum(g.original_funding_amount) as total_funding
        FROM organizations o
        JOIN grant_grantees gg ON gg.organization_id = o.organization_id
        JOIN grants g ON g.grant_id = gg.grant_id
        WHERE o.state IS NOT NULL
        GROUP BY o.state
        ORDER BY total_funding DESC
    """)

    # ─── Indexes ───
    conn.execute("CREATE INDEX idx_org_ein ON organizations(ein)")
    conn.execute("CREATE INDEX idx_org_uei ON organizations(sam_uei)")
    conn.execute("CREATE INDEX idx_org_state ON organizations(state)")
    conn.execute("CREATE INDEX idx_org_status ON organizations(status)")
    conn.execute("CREATE INDEX idx_grants_award ON grants(award_number)")
    conn.execute("CREATE INDEX idx_grants_dept ON grants(department)")
    conn.execute("CREATE INDEX idx_raw_uei ON raw_awards(recipient_uei)")
    conn.execute("CREATE INDEX idx_raw_fain ON raw_awards(award_id_fain)")
    conn.execute("CREATE INDEX idx_raw_biztype ON raw_awards(business_types_description)")

    # ─── Summary ───
    db_size = os.path.getsize(db_path) / (1024 * 1024)
    elapsed = time.time() - start
    log(f"\n{'='*60}")
    log(f"COMPLETE in {elapsed/60:.1f} minutes")
    log(f"  Database: {db_path} ({db_size:.0f} MB)")
    log(f"  raw_awards: {raw_count}")
    log(f"  organizations: {len(org_rows)} ({matched} with EIN)")
    log(f"  grants: {len(grant_rows)}")
    log(f"  grant_grantees: {len(grantee_rows)}")
    log(f"  agencies: {len(agency_map)}")
    log(f"{'='*60}")
    log(f"\nIn your notebook:")
    log(f"  import duckdb")
    log(f"  conn = duckdb.connect('{db_path}', read_only=True)")
    log(f"  conn.sql('SELECT * FROM org_grant_summary ORDER BY total_funding DESC LIMIT 20').df()")

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--zip', required=True)
    parser.add_argument('--db', default=os.path.join(PROCESSED_DIR, 'impact.duckdb'))
    parser.add_argument('--states', nargs='+', help='BMF states (default: all)')
    args = parser.parse_args()
    run(args.zip, args.db, [s.lower() for s in args.states] if args.states else None)
