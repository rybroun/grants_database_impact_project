"""Tests for Prefect pipeline flow tasks."""
import logging
import os
import csv
import tempfile
import uuid
from unittest.mock import patch, MagicMock
import pytest
import duckdb

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

_test_logger = logging.getLogger("test_flow")


@pytest.fixture(autouse=True)
def patch_prefect_logger():
    """Patch get_run_logger so tasks can be called outside Prefect context."""
    with patch("pipeline.flow.get_run_logger", return_value=_test_logger):
        yield


@pytest.fixture
def temp_db():
    """Create a temporary DuckDB database path."""
    fd, path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    os.unlink(path)  # DuckDB creates it fresh
    yield path
    if os.path.exists(path):
        os.unlink(path)
    wal = path + ".wal"
    if os.path.exists(wal):
        os.unlink(wal)


class TestBronzeLayer:
    """Tests for Bronze (raw data loading) tasks."""

    def test_load_raw_awards_creates_table(self, temp_db):
        """Loading a CSV creates raw_awards table with correct row count."""
        from pipeline.flow import load_raw_awards
        import zipfile
        zip_path = temp_db + ".zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.write(os.path.join(FIXTURES_DIR, "sample_awards.csv"), "test.csv")

        count = load_raw_awards.fn(zip_path, temp_db)
        assert count == 100

        conn = duckdb.connect(temp_db, read_only=True)
        actual = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
        assert actual == 100
        conn.close()
        os.unlink(zip_path)

    def test_load_raw_awards_appends(self, temp_db):
        """Loading twice appends, doesn't overwrite."""
        from pipeline.flow import load_raw_awards
        import zipfile
        zip_path = temp_db + ".zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.write(os.path.join(FIXTURES_DIR, "sample_awards.csv"), "test.csv")

        load_raw_awards.fn(zip_path, temp_db)
        count = load_raw_awards.fn(zip_path, temp_db)
        assert count == 200  # appended
        os.unlink(zip_path)

    def test_load_bmf_to_duckdb(self, temp_db):
        """BMF records load into DuckDB with index."""
        from pipeline.flow import load_bmf_to_duckdb
        import pipeline.flow as flow_module

        bmf_path = os.path.join(FIXTURES_DIR, "eo_wy.csv")
        if not os.path.exists(bmf_path):
            pytest.skip("BMF fixture not available")

        # Patch BMF_DIR in the flow module (it's imported directly there)
        original = flow_module.BMF_DIR
        flow_module.BMF_DIR = FIXTURES_DIR
        try:
            count = load_bmf_to_duckdb.fn(temp_db)
            assert count > 0

            conn = duckdb.connect(temp_db, read_only=True)
            tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            assert "bmf_records" in tables
            conn.close()
        finally:
            flow_module.BMF_DIR = original


class TestSilverLayer:
    """Tests for Silver (normalization + ER) tasks."""

    def test_extract_recipients_deduplicates(self, temp_db):
        """Extract recipients returns unique UEIs only."""
        from pipeline.flow import extract_recipients

        conn = duckdb.connect(temp_db)
        # Create raw_awards with duplicate UEIs
        conn.execute("""
            CREATE TABLE raw_awards (
                recipient_uei VARCHAR,
                recipient_name VARCHAR,
                recipient_duns VARCHAR,
                recipient_address_line_1 VARCHAR,
                recipient_city_name VARCHAR,
                recipient_state_code VARCHAR,
                recipient_zip_code VARCHAR,
                business_types_description VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO raw_awards VALUES
            ('UEI1', 'ORG A', NULL, '123 MAIN ST', 'DENVER', 'CO', '80202', 'NONPROFIT WITH 501C3 IRS STATUS'),
            ('UEI1', 'ORG A', NULL, '123 MAIN ST', 'DENVER', 'CO', '80202', 'NONPROFIT WITH 501C3 IRS STATUS'),
            ('UEI2', 'ORG B', NULL, '456 ELM ST', 'BOULDER', 'CO', '80301', 'OTHER'),
            (NULL, 'REDACTED DUE TO PII', NULL, NULL, NULL, NULL, NULL, NULL)
        """)
        conn.close()

        recipients = extract_recipients.fn(temp_db)
        assert len(recipients) == 2  # UEI1 + UEI2, not NULL/REDACTED
        ueis = {r["uei"] for r in recipients}
        assert ueis == {"UEI1", "UEI2"}

    def test_er_matching_produces_results(self):
        """ER matching returns results for each recipient."""
        from pipeline.flow import run_er_matching
        from pipeline.bmf_loader import load_bmf, build_bmf_index

        bmf_records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
        bmf_index = build_bmf_index(bmf_records)

        # Use a known BMF record as a recipient
        if bmf_records:
            known = bmf_records[0]
            recipients = [{
                "name": known["NAME"],
                "uei": "TESTUEI123",
                "duns": "",
                "address": known.get("STREET", ""),
                "city": known.get("CITY", ""),
                "state": known.get("STATE", ""),
                "zip": known.get("ZIP", "")[:5],
                "business_types": [],
                "business_types_description": "NONPROFIT WITH 501C3 IRS STATUS",
                "alt_names": [],
            }]
            results = run_er_matching.fn(recipients, bmf_index)
            assert len(results) == 1
            assert results[0][1] == "active"  # status
            assert results[0][5] == known["EIN"]  # ein


class TestGoldLayer:
    """Tests for Gold (final tables) tasks."""

    def test_seed_reference_data(self, temp_db):
        """Seed data creates sectors and action_types."""
        from pipeline.flow import seed_reference_data
        seed_reference_data.fn(temp_db)

        conn = duckdb.connect(temp_db, read_only=True)
        sectors = conn.execute("SELECT count(*) FROM sectors").fetchone()[0]
        actions = conn.execute("SELECT count(*) FROM action_types").fetchone()[0]
        assert sectors == 16
        assert actions == 20
        conn.close()

    def test_seed_reference_data_idempotent(self, temp_db):
        """Running seed twice doesn't duplicate."""
        from pipeline.flow import seed_reference_data
        seed_reference_data.fn(temp_db)
        seed_reference_data.fn(temp_db)

        conn = duckdb.connect(temp_db, read_only=True)
        sectors = conn.execute("SELECT count(*) FROM sectors").fetchone()[0]
        assert sectors == 16  # not 32
        conn.close()

    def test_load_organizations_creates_records(self, temp_db):
        """Organizations are inserted with correct fields."""
        from pipeline.flow import load_organizations

        er_results = [
            (str(uuid.uuid4()), "active", 0.95, "TEST ORG", "nonprofit",
             "123456789", "TESTUEI", None, "123 MAIN ST", "DENVER", "CO",
             "80202", "B20", "NONPROFIT WITH 501C3", "exact+zip"),
        ]
        uei_map = load_organizations.fn(temp_db, er_results)

        assert "TESTUEI" in uei_map
        conn = duckdb.connect(temp_db, read_only=True)
        row = conn.execute("SELECT * FROM organizations WHERE sam_uei = 'TESTUEI'").fetchone()
        assert row is not None
        conn.close()


class TestEndToEnd:
    """Full pipeline end-to-end test with fixture data."""

    def test_full_pipeline_with_fixtures(self, temp_db):
        """Run the complete pipeline with test fixtures and verify all tables."""
        import zipfile
        from pipeline.flow import (
            load_raw_awards, load_bmf_task, build_index_task,
            extract_recipients, run_er_matching, seed_reference_data,
            load_organizations, load_agencies, create_views_and_indexes,
        )
        from pipeline.bmf_loader import load_bmf, build_bmf_index

        # Create test ZIP
        zip_path = temp_db + ".zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.write(os.path.join(FIXTURES_DIR, "sample_awards.csv"), "test.csv")

        # Bronze
        load_raw_awards.fn(zip_path, temp_db)

        # Silver — load BMF directly using fixture data_dir to avoid network calls
        bmf_records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
        bmf_index = build_bmf_index(bmf_records)
        recipients = extract_recipients.fn(temp_db)
        er_results = run_er_matching.fn(recipients, bmf_index)

        # Gold
        seed_reference_data.fn(temp_db)
        uei_map = load_organizations.fn(temp_db, er_results)

        # Agencies require organizations table to exist (already created above)
        agency_map = load_agencies.fn(temp_db)

        # Verify
        conn = duckdb.connect(temp_db, read_only=True)
        raw_count = conn.execute("SELECT count(*) FROM raw_awards").fetchone()[0]
        org_count = conn.execute("SELECT count(*) FROM organizations").fetchone()[0]
        sector_count = conn.execute("SELECT count(*) FROM sectors").fetchone()[0]

        assert raw_count == 100
        assert org_count == len(er_results) + len(agency_map)
        assert sector_count == 16
        assert len(uei_map) > 0

        conn.close()
        os.unlink(zip_path)
