"""
First-pass national data load.

Usage:
    # Full national run (all 50 states):
    python -m pipeline.run_pipeline

    # Single state test:
    python -m pipeline.run_pipeline --states WY

    # Multiple states:
    python -m pipeline.run_pipeline --states WY CO CA
"""
import argparse
import json
import os
import time

from pipeline.config import DATA_DIR, US_STATES
from pipeline.bmf_loader import download_bmf, load_bmf, build_bmf_index
from pipeline.usaspending_client import fetch_nonprofit_recipients, fetch_awards_for_state
from pipeline.er_matcher import match_recipient, is_govt_entity
from pipeline.db_loader import (
    load_raw_bmf, load_raw_recipients, load_raw_awards,
    load_staging_orgs, load_staging_er_candidates,
    load_organizations, load_grants,
)


def run(states: list[str] | None = None):
    states = [s.lower() for s in (states or US_STATES)]
    state_codes_upper = [s.upper() for s in states]
    start = time.time()

    # Step 1: Download BMF
    print("\n=== Step 1: Download IRS BMF ===")
    download_bmf(states)

    # Step 2: Load and index BMF
    print("\n=== Step 2: Load and index BMF ===")
    bmf_records = load_bmf(states)
    bmf_index = build_bmf_index(bmf_records)

    # Step 3: Fetch USASpending recipients per state
    print("\n=== Step 3: Fetch USASpending recipients ===")
    all_recipients = []
    all_awards = []
    recipient_id_to_uei = {}

    for state in state_codes_upper:
        print(f"\n--- {state} ---")
        cache_path = os.path.join(DATA_DIR, f"{state.lower()}_nonprofit_recipients.json")

        if os.path.exists(cache_path):
            print(f"  Loading cached recipients from {cache_path}")
            with open(cache_path) as f:
                recipients = json.load(f)
        else:
            recipients = fetch_nonprofit_recipients(state)
            with open(cache_path, "w") as f:
                json.dump(recipients, f)

        all_recipients.extend(recipients)

        awards = fetch_awards_for_state(state)
        all_awards.extend(awards)

        # Build recipient_id -> UEI lookup from awards
        for a in awards:
            rid = a.get("recipient_id", "")
            for r in recipients:
                if r["name"] == a.get("Recipient Name"):
                    recipient_id_to_uei[rid] = r.get("uei", "")
                    break

    # Deduplicate recipients by UEI
    seen_uei = set()
    unique_recipients = []
    for r in all_recipients:
        uei = r.get("uei", "")
        if uei and uei not in seen_uei:
            seen_uei.add(uei)
            unique_recipients.append(r)

    print(f"\nTotal unique recipients across {len(states)} states: {len(unique_recipients)}")
    print(f"Total awards: {len(all_awards)}")

    # Step 4: Run ER matching
    print("\n=== Step 4: ER Matching ===")
    bmf_matches = {}
    matched = 0
    er_created = 0
    govt_filtered = 0

    for rec in unique_recipients:
        uei = rec.get("uei", "")
        if is_govt_entity(rec):
            govt_filtered += 1
            bmf_matches[uei] = (None, 0, "govt_filtered")
            continue
        m, score, method = match_recipient(rec, bmf_index)
        bmf_matches[uei] = (m, score, method)
        if m:
            matched += 1
        else:
            er_created += 1

    nonprofit_total = matched + er_created
    if nonprofit_total > 0:
        print(f"Govt filtered: {govt_filtered}")
        print(f"Matched: {matched}/{nonprofit_total} ({matched/nonprofit_total*100:.1f}%)")
        print(f"ER-created: {er_created}")

    # Step 5a: Load raw layer (Bronze)
    print("\n=== Step 5a: Load raw layer ===")
    load_raw_bmf(bmf_records)
    load_raw_recipients(unique_recipients)
    load_raw_awards(all_awards)

    # Step 5b: Load staging layer (Silver)
    print("\n=== Step 5b: Load staging layer ===")
    load_staging_orgs(unique_recipients)
    load_staging_er_candidates(bmf_matches)

    # Step 5c: Load public layer (Gold)
    print("\n=== Step 5c: Load public layer ===")
    uei_to_org_id = load_organizations(unique_recipients, bmf_matches)
    load_grants(all_awards, uei_to_org_id, recipient_id_to_uei)

    elapsed = time.time() - start
    print(f"\n=== Done in {elapsed/60:.1f} minutes ===")
    print(f"Organizations: {len(uei_to_org_id)}")
    print(f"Grants: {len(all_awards)}")
    print(f"Raw BMF records: {len(bmf_records)}")
    print(f"Raw awards: {len(all_awards)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", nargs="+", help="State codes (e.g., WY CO)")
    args = parser.parse_args()
    run(args.states)
