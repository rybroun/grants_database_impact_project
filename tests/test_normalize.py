from pipeline.normalize import normalize_name, normalize_address, normalize_city

def test_normalize_name_strips_suffixes():
    assert normalize_name("PARTNERS IN HOUSING INC") == "PARTNERS IN HOUSING"
    assert normalize_name("HOLY CROSS ELECTRIC ASSOCIATION, INC.") == "HOLY CROSS ELECTRIC ASSOCIATION"
    assert normalize_name("DIVERSUS HEALTH SERVICES") == "DIVERSUS HEALTH"

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
