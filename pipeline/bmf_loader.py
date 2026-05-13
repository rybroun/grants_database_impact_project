import os
import pandas as pd
import requests
from pipeline.config import BMF_URL_TEMPLATE, US_STATES, DATA_DIR
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def download_bmf(states: list[str] | None = None, data_dir: str = DATA_DIR) -> None:
    """Download IRS BMF CSV files for the given states."""
    os.makedirs(data_dir, exist_ok=True)
    states = states or US_STATES
    for state in states:
        path = os.path.join(data_dir, f"eo_{state}.csv")
        if os.path.exists(path):
            print(f"  {state}: already downloaded")
            continue
        url = BMF_URL_TEMPLATE.format(state=state)
        print(f"  {state}: downloading from {url}")
        resp = requests.get(url)
        resp.raise_for_status()
        with open(path, "wb") as f:
            f.write(resp.content)


def load_bmf(states: list[str] | None = None, data_dir: str = DATA_DIR) -> list[dict]:
    """Load all BMF CSVs into a list of normalized dicts."""
    states = states or US_STATES
    records = []
    for state in states:
        path = os.path.join(data_dir, f"eo_{state}.csv")
        if not os.path.exists(path):
            print(f"  WARNING: {path} not found, skipping")
            continue
        df = pd.read_csv(path, dtype=str)
        for _, row in df.iterrows():
            r = row.to_dict()
            r["_norm_name"] = normalize_name(r.get("NAME", ""))
            r["_norm_addr"] = normalize_address(r.get("STREET", ""))
            r["_norm_city"] = normalize_city(r.get("CITY", ""))
            r["_zip5"] = str(r.get("ZIP", ""))[:5]
            r["_tokens"] = set(
                w for w in r["_norm_name"].split() if len(w) > 2
            )
            r["_source_file"] = f"eo_{state}.csv"
            records.append(r)
    print(f"Loaded {len(records)} BMF records from {len(states)} states")
    return records


def build_bmf_index(records: list[dict]) -> dict:
    """Build lookup indices for fast matching."""
    by_exact = {}
    by_city = {}
    for r in records:
        by_exact.setdefault(r["_norm_name"], []).append(r)
        by_city.setdefault(r["_norm_city"], []).append(r)
    return {"by_exact": by_exact, "by_city": by_city, "all": records}
