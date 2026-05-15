import os

# Base directories
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
BMF_DIR = os.path.join(RAW_DIR, "bmf")
USASPENDING_DIR = os.path.join(RAW_DIR, "usaspending")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# Default database path
DUCKDB_PATH = os.path.join(PROCESSED_DIR, "impact.duckdb")

# IRS BMF
US_STATES = [
    "al","ak","az","ar","ca","co","ct","de","dc","fl","ga","hi","id","il","in",
    "ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh",
    "nj","nm","ny","nc","nd","oh","ok","or","pa","pr","ri","sc","sd","tn","tx",
    "ut","vt","va","wa","wv","wi","wy"
]
BMF_URL_TEMPLATE = "https://www.irs.gov/pub/irs-soi/eo_{state}.csv"

# USASpending
USASPENDING_API_BASE = "https://api.usaspending.gov/api/v2"
USASPENDING_ARCHIVE_URL = "https://files.usaspending.gov/award_data_archive"
