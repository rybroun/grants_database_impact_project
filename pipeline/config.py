import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "experiments", "data")

US_STATES = [
    "al","ak","az","ar","ca","co","ct","de","dc","fl","ga","hi","id","il","in",
    "ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh",
    "nj","nm","ny","nc","nd","oh","ok","or","pa","pr","ri","sc","sd","tn","tx",
    "ut","vt","va","wa","wv","wi","wy"
]

BMF_URL_TEMPLATE = "https://www.irs.gov/pub/irs-soi/eo_{state}.csv"
USASPENDING_API_BASE = "https://api.usaspending.gov/api/v2"
