import os
import pytest
from pipeline.bmf_loader import load_bmf, build_bmf_index

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

def test_load_bmf_from_fixture():
    records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
    assert len(records) == 100
    assert all("_norm_name" in r for r in records)
    assert all("_tokens" in r for r in records)
    assert all("_zip5" in r for r in records)

def test_build_bmf_index():
    records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
    index = build_bmf_index(records)
    assert "by_exact" in index
    assert "by_city" in index
    assert "all" in index
    assert len(index["all"]) == 100
    assert len(index["by_exact"]) > 0
    assert len(index["by_city"]) > 0

def test_bmf_records_have_required_fields():
    records = load_bmf(["wy"], data_dir=FIXTURES_DIR)
    for r in records:
        assert "EIN" in r
        assert "NAME" in r
        assert "STATE" in r
        assert "_norm_name" in r
        assert "_norm_city" in r
        assert "_norm_addr" in r
        assert isinstance(r["_tokens"], set)
