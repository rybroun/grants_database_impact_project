"""
Load USASpending bulk CSV + IRS BMF into a local SQLite database.

Usage:
    python -m pipeline.sqlite_loader --zip experiments/data/usaspending_archive_FY2024.zip
    python -m pipeline.sqlite_loader --zip experiments/data/usaspending_archive_FY2024.zip --db experiments/data/impact.db
"""
import argparse
import csv
import io
import math
import os
import sqlite3
import time
import uuid
import zipfile

from pipeline.config import DATA_DIR
from pipeline.bmf_loader import download_bmf, load_bmf, build_bmf_index
from pipeline.er_matcher import match_recipient, is_govt_entity
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _parse_float(val):
    try:
        f = float(val) if val else None
        if f is not None and (math.isnan(f) or math.isinf(f)):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _parse_business_types(val):
    if not val:
        return ""
    return val


def create_schema(conn):
    """Create all tables in SQLite."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS organizations (
            organization_id TEXT PRIMARY KEY,
            canonical_org_id TEXT,
            parent_org_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            confidence_score REAL,
            name TEXT NOT NULL,
            name_aliases TEXT,
            org_type TEXT,
            ein TEXT,
            sam_uei TEXT,
            duns_number TEXT,
            street_address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            ntee_code TEXT,
            business_types_description TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS sectors (
            sector_id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            color TEXT
        );

        CREATE TABLE IF NOT EXISTS action_types (
            action_type_id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS grants (
            grant_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'active',
            title TEXT,
            granter_org_id TEXT,
            department TEXT,
            program TEXT,
            cfda_number TEXT,
            award_number TEXT,
            geographic_scope TEXT,
            geo_state TEXT,
            geo_city TEXT,
            original_funding_amount REAL,
            funding_year INTEGER,
            source_database TEXT DEFAULT 'usaspending',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS grant_grantees (
            grant_id TEXT,
            organization_id TEXT,
            allocation REAL,
            PRIMARY KEY (grant_id, organization_id)
        );

        CREATE TABLE IF NOT EXISTS reports (
            report_id TEXT PRIMARY KEY,
            grant_id TEXT,
            action_type_id TEXT,
            change_amount REAL,
            new_total REAL,
            effective_date TEXT,
            date_reported TEXT,
            summary TEXT,
            scope_of_impact TEXT,
            latitude REAL,
            longitude REAL,
            is_testimonial INTEGER DEFAULT 0,
            is_doge_data INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            source_type TEXT,
            title TEXT,
            url TEXT,
            publisher TEXT,
            published_date TEXT,
            summary TEXT,
            gdelt_id TEXT
        );

        CREATE TABLE IF NOT EXISTS report_sources (
            report_id TEXT,
            source_id TEXT,
            PRIMARY KEY (report_id, source_id)
        );

        -- Raw awards (all 112 columns from USASpending bulk CSV, no filtering)
        -- This table is created dynamically from CSV headers

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_org_ein ON organizations(ein);
        CREATE INDEX IF NOT EXISTS idx_org_uei ON organizations(sam_uei);
        CREATE INDEX IF NOT EXISTS idx_org_state ON organizations(state);
        CREATE INDEX IF NOT EXISTS idx_org_status ON organizations(status);
        CREATE INDEX IF NOT EXISTS idx_grants_award ON grants(award_number);
        CREATE INDEX IF NOT EXISTS idx_grants_cfda ON grants(cfda_number);
        CREATE INDEX IF NOT EXISTS idx_grants_dept ON grants(department);
        CREATE INDEX IF NOT EXISTS idx_grants_state ON grants(geo_state);
    """)


def seed_reference_data(conn):
    """Insert sectors and action types."""
    from pipeline.seed_data import SECTORS, ACTION_TYPES
    for name, color in SECTORS:
        conn.execute(
            "INSERT OR IGNORE INTO sectors (sector_id, name, color) VALUES (?, ?, ?)",
            (str(uuid.uuid4()), name, color)
        )
    for name, category in ACTION_TYPES:
        conn.execute(
            "INSERT OR IGNORE INTO action_types (action_type_id, name, category) VALUES (?, ?, ?)",
            (str(uuid.uuid4()), name, category)
        )
    conn.commit()


def load_bulk_csv(zip_path):
    """Parse USASpending bulk ZIP."""
    log(f"Parsing {zip_path}...")
    awards = []
    recipients_by_uei = {}

    with zipfile.ZipFile(zip_path) as zf:
        for csv_name in sorted(zf.namelist()):
            if not csv_name.endswith('.csv'):
                continue
            log(f"  Reading {csv_name}...")
            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8', errors='replace'))
                row_count = 0
                for row in reader:
                    row_count += 1
                    uei = (row.get('recipient_uei') or '').strip()

                    # Skip redacted/individual records (no org name or UEI)
                    rname = row.get('recipient_name', '')
                    if not uei or 'REDACTED' in rname.upper():
                        continue

                    award_id = row.get('award_id_fain') or row.get('award_id_uri') or row.get('assistance_award_unique_key') or ''
                    awards.append({
                        'award_id': award_id,
                        'recipient_name': rname,
                        'recipient_uei': uei,
                        'award_amount': _parse_float(row.get('total_funding_amount') or row.get('federal_action_obligation', '0')),
                        'awarding_agency': row.get('awarding_agency_name', ''),
                        'awarding_sub_agency': row.get('awarding_sub_agency_name', ''),
                        'cfda_number': row.get('cfda_number', ''),
                        'award_type': row.get('award_type', ''),
                        'start_date': row.get('period_of_performance_start_date', ''),
                        'end_date': row.get('period_of_performance_current_end_date', ''),
                        'description': (row.get('prime_award_base_transaction_description') or row.get('award_description') or '')[:500],
                        'pop_state': row.get('primary_place_of_performance_state_code') or row.get('prime_award_transaction_place_of_performance_state_fips_code', ''),
                        'pop_city': row.get('primary_place_of_performance_city_name', ''),
                        'business_types': row.get('business_types', ''),
                    })

                    if uei not in recipients_by_uei:
                        recipients_by_uei[uei] = {
                            'name': rname,
                            'uei': uei,
                            'duns': row.get('recipient_duns') or row.get('recipient_parent_duns', ''),
                            'address': row.get('recipient_address_line_1', ''),
                            'city': row.get('recipient_city_name', ''),
                            'state': row.get('recipient_state_code', ''),
                            'zip': row.get('recipient_zip_4_code', ''),
                            'business_types': _parse_business_types(row.get('business_types', '')),
                            'business_types_description': row.get('business_types_description', ''),
                            'alt_names': [],
                        }

                    if row_count % 500000 == 0:
                        log(f"    {row_count} rows, {len(recipients_by_uei)} unique recipients...")

                log(f"  {csv_name}: {row_count} rows")

    recipients = list(recipients_by_uei.values())
    log(f"Parsed: {len(awards)} awards, {len(recipients)} unique recipients")
    return awards, recipients


def load_raw_to_sqlite(zip_path, conn):
    """Load all CSV rows into raw_awards table with all original columns."""
    log("=== Loading raw awards (all columns) ===")
    import pandas as pd
    total = 0
    with zipfile.ZipFile(zip_path) as zf:
        for csv_name in sorted(zf.namelist()):
            if not csv_name.endswith('.csv'):
                continue
            log(f"  Reading {csv_name}...")
            with zf.open(csv_name) as f:
                # Read in chunks to manage memory
                for chunk in pd.read_csv(f, dtype=str, chunksize=100000):
                    # Sanitize column names for SQLite (replace special chars)
                    chunk.columns = [c.replace('-', '_').replace(' ', '_') for c in chunk.columns]
                    chunk.to_sql('raw_awards', conn, if_exists='append', index=False)
                    total += len(chunk)
            log(f"    {total} rows loaded so far")
    log(f"  Total raw rows: {total}")
    # Add indexes on key columns
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_uei ON raw_awards(recipient_uei)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_fain ON raw_awards(award_id_fain)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_agency ON raw_awards(awarding_agency_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_biztype ON raw_awards(business_types_description)")
        conn.commit()
    except Exception:
        pass
    return total


def run(zip_path, db_path, bmf_states=None):
    start = time.time()

    # Create DB
    log(f"Creating database at {db_path}")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    create_schema(conn)
    seed_reference_data(conn)

    # Load raw data first (all 112 columns, unfiltered)
    load_raw_to_sqlite(zip_path, conn)

    # Parse CSV for pipeline (filtered)
    awards, recipients = load_bulk_csv(zip_path)

    # Load BMF
    log("=== Loading IRS BMF ===")
    if bmf_states:
        download_bmf(bmf_states)
    bmf_records = load_bmf(bmf_states)
    bmf_index = build_bmf_index(bmf_records)
    log(f"BMF: {len(bmf_records)} records, {len(bmf_index['by_exact'])} unique names")

    # ER Matching
    log("=== ER Matching ===")
    bmf_matches = {}
    matched = er_created = govt_filtered = 0

    for i, rec in enumerate(recipients, 1):
        uei = rec.get('uei', '')
        if is_govt_entity(rec):
            govt_filtered += 1
            bmf_matches[uei] = (None, 0, 'govt_filtered')
            continue
        m, score, method = match_recipient(rec, bmf_index)
        bmf_matches[uei] = (m, score, method)
        if m:
            matched += 1
        else:
            er_created += 1
        if i % 10000 == 0:
            log(f"  ER: {i}/{len(recipients)} ({matched} matched, {er_created} er_created)")

    nonprofit_total = matched + er_created
    if nonprofit_total > 0:
        log(f"ER: {matched}/{nonprofit_total} matched ({matched/nonprofit_total*100:.1f}%), {er_created} er_created, {govt_filtered} govt")

    # Load organizations
    log("=== Loading organizations ===")
    uei_to_org_id = {}
    org_rows = []
    for rec in recipients:
        uei = rec.get('uei', '')
        if not uei:
            continue
        bmf_match, score, method = bmf_matches.get(uei, (None, 0, None))
        is_govt = is_govt_entity(rec)

        org_type = 'nonprofit'
        if is_govt:
            org_type = 'local_government'

        status = 'active' if bmf_match else 'er_created'
        if is_govt:
            status = 'er_created'

        org_id = str(uuid.uuid4())
        uei_to_org_id[uei] = org_id
        ntee = bmf_match.get('NTEE_CD') if bmf_match else None
        if ntee and not isinstance(ntee, str):
            ntee = None

        org_rows.append((
            org_id, status, round(score, 3) if score else None,
            rec['name'], org_type,
            bmf_match['EIN'] if bmf_match else None,
            uei, rec.get('duns') or None,
            rec.get('address') or None, rec.get('city') or None,
            rec.get('state') or None, rec.get('zip') or None,
            ntee, rec.get('business_types_description') or None,
        ))

    conn.executemany(
        """INSERT INTO organizations
           (organization_id, status, confidence_score, name, org_type, ein, sam_uei,
            duns_number, street_address, city, state, zip, ntee_code, business_types_description)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        org_rows
    )
    conn.commit()
    log(f"  {len(org_rows)} organizations inserted")

    # Load agencies
    log("=== Loading agencies + grants + grantees ===")
    agency_cache = {}
    def get_agency(name):
        if name in agency_cache:
            return agency_cache[name]
        oid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO organizations (organization_id, status, name, org_type) VALUES (?,?,?,?)",
            (oid, 'active', name, 'federal_agency')
        )
        agency_cache[name] = oid
        return oid

    # Load grants + grantees (deduplicated)
    seen = set()
    grant_rows = []
    grantee_rows = []
    for a in awards:
        aid = a['award_id']
        if not aid or aid in seen:
            continue
        seen.add(aid)
        gid = str(uuid.uuid4())
        agency_name = a.get('awarding_agency') or 'Unknown Agency'
        granter_id = get_agency(agency_name)

        grant_rows.append((
            gid, 'active', (a['description'][:200] if a['description'] else a.get('award_type')),
            granter_id, agency_name, a.get('awarding_sub_agency'),
            a.get('cfda_number'), aid,
            a.get('pop_state'), a.get('pop_city'),
            a.get('award_amount'),
        ))

        uei = a.get('recipient_uei', '')
        org_id = uei_to_org_id.get(uei)
        if org_id:
            grantee_rows.append((gid, org_id))

    log(f"  Inserting {len(grant_rows)} grants...")
    conn.executemany(
        """INSERT INTO grants
           (grant_id, status, title, granter_org_id, department, program,
            cfda_number, award_number, geo_state, geo_city, original_funding_amount)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        grant_rows
    )
    conn.commit()
    log(f"  {len(grant_rows)} grants inserted")

    log(f"  Inserting {len(grantee_rows)} grant_grantees...")
    conn.executemany(
        "INSERT INTO grant_grantees (grant_id, organization_id) VALUES (?,?)",
        grantee_rows
    )
    conn.commit()
    log(f"  {len(grantee_rows)} grant_grantees inserted")

    # Summary
    elapsed = time.time() - start
    db_size = os.path.getsize(db_path) / (1024 * 1024)

    log(f"\n{'='*60}")
    log(f"COMPLETE in {elapsed/60:.1f} minutes")
    log(f"  Database: {db_path} ({db_size:.0f} MB)")
    log(f"  Organizations: {len(org_rows)} ({matched} with EIN)")
    log(f"  Grants: {len(grant_rows)} unique awards")
    log(f"  Grant-grantees: {len(grantee_rows)}")
    log(f"  Agencies: {len(agency_cache)}")
    log(f"{'='*60}")
    log(f"\nQuery with: sqlite3 {db_path}")
    log(f"Or in Python: pd.read_sql('SELECT * FROM organizations LIMIT 10', sqlite3.connect('{db_path}'))")

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--zip', required=True)
    parser.add_argument('--db', default=os.path.join(DATA_DIR, 'impact.db'))
    parser.add_argument('--states', nargs='+', help='BMF states (default: all)')
    args = parser.parse_args()
    run(args.zip, args.db, [s.lower() for s in args.states] if args.states else None)
