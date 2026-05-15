from pipeline.normalize import normalize_name, normalize_address, normalize_city

def test_normalize_name_strips_suffixes():
    assert normalize_name("PARTNERS IN HOUSING INC") == "PARTNERS IN HOUSING"
    assert normalize_name("HOLY CROSS ELECTRIC ASSOCIATION, INC.") == "HOLY CROSS ELECTRIC"
    assert normalize_name("DIVERSUS HEALTH SERVICES") == "DIVERSUS HEALTH"

def test_normalize_name_corporation_before_corp():
    """Regression: CORPORATION must be stripped before CORP to avoid 'HEALTHORATION'."""
    assert normalize_name("DIMENSIONS HEALTH CORPORATION") == "DIMENSIONS HEALTH"
    assert normalize_name("ABC CORPORATION") == "ABC"
    assert normalize_name("ABC CORP") == "ABC"
    assert normalize_name("ABC CORP.") == "ABC"

def test_normalize_name_incorporated_before_inc():
    """Regression: INCORPORATED must be stripped before INC."""
    assert normalize_name("HELPERS INCORPORATED") == "HELPERS"
    assert normalize_name("HELPERS INC") == "HELPERS"

def test_normalize_name_hyphens_become_spaces():
    """Regression: hyphens should become spaces, not merge words."""
    assert normalize_name("CHILDRENS-BOOKS ON WHEELS") == "CHILDRENS BOOKS ON WHEELS"
    assert normalize_name("MID-SOUTH COMMUNITY CENTER") == "MID SOUTH COMMUNITY CENTER"

def test_normalize_name_strips_association():
    assert normalize_name("WYOMING WATER USERS ASSOCIATION") == "WYOMING WATER USERS"
    assert normalize_name("GRAND VALLEY ASSN") == "GRAND VALLEY"

def test_normalize_name_handles_ampersand():
    assert normalize_name("WYOMING CHILD & FAMILY DEVELOPMENT") == "WYOMING CHILD AND FAMILY DEVELOPMENT"

def test_normalize_name_strips_the():
    assert normalize_name("THE SALVATION ARMY") == "SALVATION ARMY"

def test_normalize_name_removes_parenthetical():
    assert normalize_name("HERITAGE TOWERS OF THE CHRISTIAN CHURCH (DISCIPLES OF CHRIST)") == "HERITAGE TOWERS OF CHRISTIAN CHURCH"

def test_normalize_address_normalizes_suffixes():
    assert normalize_address("3 ETHETE ROAD PO BOX 661") == "3 ETHETE RD"
    assert normalize_address("455 GOLD PASS HTS") == "455 GOLD PASS HTS"

def test_normalize_address_strips_suite():
    assert normalize_address("675 SOUTHPOINTE CT STE 100") == "675 SOUTHPOINTE CT"
    assert normalize_address("1700 BROADWAY STE 500") == "1700 BROADWAY"

def test_normalize_city_maps_abbreviations():
    assert normalize_city("COLORADO SPGS") == "COLORADO SPRINGS"
    assert normalize_city("GRAND JCT") == "GRAND JUNCTION"
    assert normalize_city("DENVER") == "DENVER"
