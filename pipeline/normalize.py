import re

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.upper().strip()
    for suffix in [
        ", INC.", ", INC", " INC.", " INC", " INCORPORATED",
        ", LLC", " LLC", ", L.L.C.",
        ", CORP.", ", CORP", " CORP.", " CORP", " CORPORATION",
        ", CO.", ", CO", " COMPANY",
        ", LTD", " LTD", " LIMITED",
        ", P.C.", ", PC",
        " SERVICES", " ASSOCIATES", " GROUP", " AGENCY",
    ]:
        name = name.replace(suffix, "")
    name = name.replace(" & ", " AND ").replace("&", " AND ")
    name = name.replace("'S", "S").replace("'", "")
    name = re.sub(r"\([^)]*\)", "", name)
    name = re.sub(r"\bTHE\s+", "", name)
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    addr = addr.upper().strip()
    addr = re.sub(r"\bP\.?O\.?\s*BOX\s*\d*", "", addr)
    addr = re.sub(
        r"\b(STE|SUITE|UNIT|APT|RM|ROOM|FL|FLOOR|BLDG|BUILDING)\s*[#]?\s*\w*",
        "", addr,
    )
    replacements = {
        r"\bROAD\b": "RD", r"\bSTREET\b": "ST", r"\bAVENUE\b": "AVE",
        r"\bBOULEVARD\b": "BLVD", r"\bDRIVE\b": "DR", r"\bLANE\b": "LN",
        r"\bCOURT\b": "CT", r"\bCIRCLE\b": "CIR", r"\bPLACE\b": "PL",
        r"\bTERRACE\b": "TER", r"\bHIGHWAY\b": "HWY", r"\bPARKWAY\b": "PKWY",
        r"\bNORTH\b": "N", r"\bSOUTH\b": "S", r"\bEAST\b": "E", r"\bWEST\b": "W",
    }
    for pattern, repl in replacements.items():
        addr = re.sub(pattern, repl, addr)
    addr = re.sub(r"[^A-Z0-9\s]", "", addr)
    return re.sub(r"\s+", " ", addr).strip()


_CITY_ABBREVS = {
    "COLORADO SPGS": "COLORADO SPRINGS", "COLO SPGS": "COLORADO SPRINGS",
    "GRAND JCT": "GRAND JUNCTION", "FT COLLINS": "FORT COLLINS",
    "FT WASHAKIE": "FORT WASHAKIE", "GLENWOOD SPGS": "GLENWOOD SPRINGS",
    "STEAMBT SPGS": "STEAMBOAT SPRINGS", "PAGOSA SPGS": "PAGOSA SPRINGS",
    "IDAHO SPGS": "IDAHO SPRINGS", "MANITOU SPGS": "MANITOU SPRINGS",
    "FT WORTH": "FORT WORTH", "FT LAUDERDALE": "FORT LAUDERDALE",
    "ST LOUIS": "SAINT LOUIS", "ST PAUL": "SAINT PAUL",
}


def normalize_city(city: str) -> str:
    if not city:
        return ""
    city = city.upper().strip()
    return _CITY_ABBREVS.get(city, city)
