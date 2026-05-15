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


def test_address_only_match_for_renamed_nonprofit():
    """Pass 2.5: nonprofit at same address but completely different name (renamed org)."""
    bmf_index = {
        "by_exact": {},
        "by_city": {
            "SOUTH PASADENA": [
                {"EIN": "262619591", "NAME": "INNOVATION CENTER FOR ENERGY AND SUSTAINABILITY",
                 "STREET": "592 GARFIELD AVE", "CITY": "SOUTH PASADENA",
                 "STATE": "CA", "ZIP": "91030",
                 "_norm_name": "INNOVATION CENTER FOR ENERGY AND SUSTAINABILITY",
                 "_norm_addr": "592 GARFIELD AVE",
                 "_norm_city": "SOUTH PASADENA", "_zip5": "91030",
                 "_tokens": {"INNOVATION", "CENTER", "ENERGY", "SUSTAINABILITY"}}
            ]
        },
        "all": [],
    }
    recipient = {
        "name": "INNOVATION CENTER FOR ENERGY & TRANSPORTATION",
        "address": "592 GARFIELD AVE",
        "city": "SOUTH PASADENA",
        "zip": "91030",
        "alt_names": [],
        "business_types": [],
        "business_types_description": "NONPROFIT WITH 501C3 IRS STATUS (OTHER THAN AN INSTITUTION OF HIGHER EDUCATION)",
    }
    match, score, method = match_recipient(recipient, bmf_index)
    assert match is not None
    assert match["EIN"] == "262619591"
    assert "addr_only_nonprofit" in method
    assert score >= 0.7


def test_address_only_skipped_for_non_nonprofit():
    """Pass 2.5 should NOT fire for for-profit orgs — avoids false positives at shared addresses."""
    bmf_index = {
        "by_exact": {},
        "by_city": {
            "CHICAGO": [
                {"EIN": "123456789", "NAME": "SOME NONPROFIT",
                 "STREET": "100 MAIN ST", "CITY": "CHICAGO",
                 "STATE": "IL", "ZIP": "60601",
                 "_norm_name": "SOME NONPROFIT", "_norm_addr": "100 MAIN ST",
                 "_norm_city": "CHICAGO", "_zip5": "60601",
                 "_tokens": {"SOME", "NONPROFIT"}}
            ]
        },
        "all": [],
    }
    recipient = {
        "name": "TOTALLY DIFFERENT FOR-PROFIT LLC",
        "address": "100 MAIN ST",
        "city": "CHICAGO",
        "zip": "60601",
        "alt_names": [],
        "business_types": [],
        "business_types_description": "FOR-PROFIT ORGANIZATION (OTHER THAN SMALL BUSINESS)",
    }
    match, score, method = match_recipient(recipient, bmf_index)
    assert match is None
    assert score == 0
