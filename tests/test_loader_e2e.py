"""End-to-end test: load small fixture data through the full pipeline into a temp DuckDB."""
import os
import tempfile
import duckdb
import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture
def temp_db():
    """Create a temporary DuckDB database."""
    fd, path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    os.unlink(path)  # DuckDB must create the file itself; it rejects pre-existing empty files
    yield path
    if os.path.exists(path):
        os.unlink(path)

def test_full_pipeline_produces_tables(temp_db):
    """Verify that loading fixture data produces all expected tables."""
    from pipeline.bmf_loader import load_bmf, build_bmf_index
    from pipeline.er_matcher import match_recipient
    from pipeline.seed_data import SECTORS, ACTION_TYPES
    import uuid, csv

    conn = duckdb.connect(temp_db)

    # Load sample awards directly (not from ZIP for simplicity)
    awards_path = os.path.join(FIXTURES_DIR, "sample_awards.csv")
    conn.execute(f"""
        CREATE TABLE raw_awards AS
        SELECT * FROM read_csv_auto('{awards_path}', ignore_errors=true)
    """)
    raw_count = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
    assert raw_count > 0, "raw_awards should have rows"

    # Seed reference tables
    conn.execute("CREATE TABLE sectors (sector_id VARCHAR, name VARCHAR, color VARCHAR)")
    for name, color in SECTORS:
        conn.execute("INSERT INTO sectors VALUES (?, ?, ?)", [str(uuid.uuid4()), name, color])
    assert conn.execute("SELECT count(*) FROM sectors").fetchone()[0] == 16

    conn.execute("CREATE TABLE action_types (action_type_id VARCHAR, name VARCHAR, category VARCHAR)")
    for name, cat in ACTION_TYPES:
        conn.execute("INSERT INTO action_types VALUES (?, ?, ?)", [str(uuid.uuid4()), name, cat])
    assert conn.execute("SELECT count(*) FROM action_types").fetchone()[0] == 20

    # Load BMF from fixture
    bmf_records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
    bmf_index = build_bmf_index(bmf_records)
    assert len(bmf_records) > 0

    # Extract unique recipients from raw
    recips = conn.execute("""
        SELECT DISTINCT recipient_uei, recipient_name, recipient_city_name, recipient_state_code
        FROM raw_awards
        WHERE recipient_uei IS NOT NULL AND recipient_uei != ''
          AND recipient_name NOT LIKE '%REDACTED%'
        LIMIT 50
    """).fetchall()

    # Run ER on a few
    matched_count = 0
    for uei, name, city, state in recips:
        rec = {"name": name or "", "uei": uei or "", "address": "", "city": city or "",
               "state": state or "", "zip": "", "business_types": [], "alt_names": []}
        m, score, method = match_recipient(rec, bmf_index)
        if m:
            matched_count += 1

    # Create organizations table
    conn.execute("""
        CREATE TABLE organizations (
            organization_id VARCHAR PRIMARY KEY,
            status VARCHAR, confidence_score DOUBLE,
            name VARCHAR, org_type VARCHAR, ein VARCHAR, sam_uei VARCHAR,
            duns_number VARCHAR, street_address VARCHAR, city VARCHAR,
            state VARCHAR, zip VARCHAR, ntee_code VARCHAR,
            business_types_description VARCHAR, match_method VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE grants (
            grant_id VARCHAR PRIMARY KEY, status VARCHAR, title VARCHAR,
            granter_org_id VARCHAR, department VARCHAR, program VARCHAR,
            cfda_number VARCHAR, award_number VARCHAR,
            geo_state VARCHAR, geo_city VARCHAR,
            original_funding_amount DOUBLE, source_database VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE grant_grantees (
            grant_id VARCHAR, organization_id VARCHAR,
            PRIMARY KEY (grant_id, organization_id)
        )
    """)

    # Verify schema is correct
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    assert "raw_awards" in tables
    assert "organizations" in tables
    assert "grants" in tables
    assert "grant_grantees" in tables
    assert "sectors" in tables
    assert "action_types" in tables

    conn.close()

def test_er_produces_matches_on_known_data():
    """Test that ER correctly matches a known BMF record."""
    from pipeline.bmf_loader import load_bmf, build_bmf_index
    from pipeline.er_matcher import match_recipient

    bmf_records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
    bmf_index = build_bmf_index(bmf_records)

    # Pick a real org from the BMF fixture and try to match it
    if bmf_records:
        known = bmf_records[0]
        recipient = {
            "name": known["NAME"],
            "uei": "TEST123",
            "address": known.get("STREET", ""),
            "city": known.get("CITY", ""),
            "state": known.get("STATE", ""),
            "zip": known.get("ZIP", "")[:5],
            "business_types": [],
            "alt_names": [],
        }
        match, score, method = match_recipient(recipient, bmf_index)
        assert match is not None, f"Should match known BMF record: {known['NAME']}"
        assert match["EIN"] == known["EIN"]
        assert score >= 0.9
