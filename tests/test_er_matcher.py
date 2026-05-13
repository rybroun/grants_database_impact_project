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
