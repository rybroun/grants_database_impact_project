"""
Load data from USASpending bulk CSV archive + IRS BMF into Supabase.

Usage:
    python -m pipeline.bulk_loader --zip experiments/data/usaspending_archive_FY2024.zip
    python -m pipeline.bulk_loader --zip experiments/data/usaspending_archive_FY2024.zip --states WY CO
"""
import argparse
import csv
import io
import os
import time
import uuid
import zipfile

import pandas as pd
from supabase import create_client

from pipeline.config import SUPABASE_URL, SUPABASE_KEY, DATA_DIR
from pipeline.bmf_loader import download_bmf, load_bmf, build_bmf_index
from pipeline.er_matcher import match_recipient, is_govt_entity
from pipeline.normalize import normalize_name, normalize_address, normalize_city


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def get_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _sanitize(val):
    """Replace NaN/Inf with None for JSON serialization."""
    if isinstance(val, float):
        import math
        if math.isnan(val) or math.isinf(val):
            return None
    return val


def _sanitize_row(row: dict) -> dict:
    return {k: _sanitize(v) for k, v in row.items()}


def _batch_insert(client, table: str, rows: list[dict], batch_size: int = 500, schema: str = "public") -> int:
    """Insert rows in batches. Returns count inserted."""
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = [_sanitize_row(r) for r in rows[i : i + batch_size]]
        try:
            if schema != "public":
                client.schema(schema).table(table).insert(batch).execute()
            else:
                client.table(table).insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            log(f"    ERROR inserting batch {i//batch_size} into {schema}.{table}: {e}")
            # Try one-by-one for the failed batch
            for row in batch:
                try:
                    if schema != "public":
                        client.schema(schema).table(table).insert(row).execute()
                    else:
                        client.table(table).insert(row).execute()
                    inserted += 1
                except Exception:
                    pass
    return inserted


def load_bulk_csv(zip_path: str) -> tuple[list[dict], list[dict]]:
    """Parse USASpending bulk ZIP into awards and unique recipients."""
    log(f"Parsing {zip_path}...")
    awards = []
    recipients_by_uei = {}

    with zipfile.ZipFile(zip_path) as zf:
        for csv_name in zf.namelist():
            if not csv_name.endswith('.csv'):
                continue
            log(f"  Reading {csv_name}...")
            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8', errors='replace'))
                row_count = 0
                for row in reader:
                    row_count += 1
                    uei = (row.get('recipient_uei') or '').strip()

                    # Build award record
                    awards.append({
                        'award_id': row.get('fain') or row.get('uri') or row.get('award_id_fain', ''),
                        'recipient_name': row.get('recipient_name', ''),
                        'recipient_uei': uei,
                        'award_amount': _parse_float(row.get('total_funding_amount') or row.get('federal_action_obligation', '0')),
                        'awarding_agency': row.get('awarding_agency_name', ''),
                        'awarding_sub_agency': row.get('awarding_sub_agency_name', ''),
                        'cfda_number': row.get('cfda_number', ''),
                        'award_type': row.get('award_type', ''),
                        'start_date': row.get('period_of_performance_start_date', ''),
                        'end_date': row.get('period_of_performance_current_end_date', ''),
                        'description': (row.get('award_description') or '')[:2000],
                        'place_of_performance_state': row.get('primary_place_of_performance_state_code', ''),
                        'place_of_performance_city': row.get('primary_place_of_performance_city_name', ''),
                        'place_of_performance_zip': row.get('primary_place_of_performance_zip_4', ''),
                        'recipient_city': row.get('recipient_city_name', ''),
                        'recipient_state': row.get('recipient_state_code', ''),
                        'recipient_zip': row.get('recipient_zip_4_code', ''),
                        'recipient_address': row.get('recipient_address_line_1', ''),
                        'business_types': row.get('business_types', ''),
                    })

                    # Build unique recipient
                    if uei and uei not in recipients_by_uei:
                        recipients_by_uei[uei] = {
                            'name': row.get('recipient_name', ''),
                            'uei': uei,
                            'duns': row.get('recipient_duns', ''),
                            'address': row.get('recipient_address_line_1', ''),
                            'city': row.get('recipient_city_name', ''),
                            'state': row.get('recipient_state_code', ''),
                            'zip': row.get('recipient_zip_4_code', ''),
                            'business_types': _parse_business_types(row.get('business_types', '')),
                            'alt_names': [],
                        }

                    if row_count % 100000 == 0:
                        log(f"    {row_count} rows, {len(recipients_by_uei)} unique recipients...")

                log(f"  {csv_name}: {row_count} rows")

    recipients = list(recipients_by_uei.values())
    log(f"Parsed: {len(awards)} awards, {len(recipients)} unique recipients")
    return awards, recipients


def _parse_float(val: str) -> float | None:
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def _parse_business_types(val: str) -> list[str]:
    """Parse business_types string from CSV into list."""
    if not val:
        return []
    # USASpending bulk CSV has business types as semicolon or comma-separated codes
    return [t.strip() for t in val.replace(';', ',').split(',') if t.strip()]


def run(zip_path: str, bmf_states: list[str] | None = None, skip_raw: bool = False):
    start = time.time()
    client = get_client()

    # ─── Step 1: Parse bulk CSV ───
    awards, recipients = load_bulk_csv(zip_path)

    # ─── Step 2: Load + index BMF ───
    log("=== Loading IRS BMF ===")
    if bmf_states:
        download_bmf(bmf_states)
    bmf_records = load_bmf(bmf_states)
    bmf_index = build_bmf_index(bmf_records)
    log(f"BMF: {len(bmf_records)} records, {len(bmf_index['by_exact'])} unique names")

    # ─── Step 3: ER Matching ───
    log("=== ER Matching ===")
    bmf_matches = {}
    matched = er_created = govt_filtered = 0

    for i, rec in enumerate(recipients, 1):
        uei = rec.get('uei', '')
        if is_govt_entity(rec):
            govt_filtered += 1
            bmf_matches[uei] = (None, 0, 'govt_filtered')
            continue
        m, score, method = match_recipient(rec, bmf_index)
        bmf_matches[uei] = (m, score, method)
        if m:
            matched += 1
        else:
            er_created += 1
        if i % 5000 == 0:
            log(f"  ER: {i}/{len(recipients)} ({matched} matched, {er_created} er_created)")

    nonprofit_total = matched + er_created
    if nonprofit_total > 0:
        log(f"ER: {matched}/{nonprofit_total} matched ({matched/nonprofit_total*100:.1f}%), {er_created} er_created, {govt_filtered} govt")

    # ─── Step 4a: Raw layer ───
    if not skip_raw:
        log("=== Loading raw layer ===")
        log(f"  raw.bmf_records ({len(bmf_records)})...")
        raw_bmf = [{
            'ein': r.get('EIN'), 'name': r.get('NAME'), 'street': r.get('STREET'),
            'city': r.get('CITY'), 'state': r.get('STATE'), 'zip': r.get('ZIP'),
            'subsection': r.get('SUBSECTION'), 'ntee_cd': r.get('NTEE_CD'),
            'asset_amt': r.get('ASSET_AMT'), 'income_amt': r.get('INCOME_AMT'),
            'source_file': r.get('_source_file', ''),
        } for r in bmf_records]
        n = _batch_insert(client, 'bmf_records', raw_bmf, schema='raw')
        log(f"    {n} inserted")

        log(f"  raw.usaspending_recipients ({len(recipients)})...")
        raw_recip = [{
            'name': r['name'], 'uei': r['uei'], 'duns': r.get('duns'),
            'address': r.get('address'), 'city': r.get('city'),
            'state': r.get('state'), 'zip': r.get('zip'),
            'business_types': r.get('business_types'),
        } for r in recipients]
        n = _batch_insert(client, 'usaspending_recipients', raw_recip, schema='raw')
        log(f"    {n} inserted")

        log(f"  raw.usaspending_awards ({len(awards)})...")
        raw_awards = [{
            'award_id': a['award_id'], 'recipient_name': a['recipient_name'],
            'recipient_uei': a['recipient_uei'], 'award_amount': a['award_amount'],
            'awarding_agency': a['awarding_agency'], 'awarding_sub_agency': a['awarding_sub_agency'],
            'cfda_number': a['cfda_number'], 'award_type': a['award_type'],
            'start_date': a['start_date'], 'end_date': a['end_date'],
            'description': a['description'],
            'place_of_performance_state': a.get('place_of_performance_state'),
            'place_of_performance_city': a.get('place_of_performance_city'),
        } for a in awards]
        n = _batch_insert(client, 'usaspending_awards', raw_awards, schema='raw')
        log(f"    {n} inserted")

    # ─── Step 4b: Staging layer ───
    log("=== Loading staging layer ===")
    staging_orgs = [{
        'source': 'usaspending', 'source_id': r['uei'], 'name': r['name'],
        'normalized_name': normalize_name(r.get('name', '')),
        'normalized_city': normalize_city(r.get('city', '')),
        'state': r.get('state'), 'zip5': str(r.get('zip', ''))[:5],
        'uei': r['uei'], 'business_types': r.get('business_types'),
    } for r in recipients]
    n = _batch_insert(client, 'normalized_orgs', staging_orgs, schema='staging')
    log(f"  staging.normalized_orgs: {n} inserted")

    er_cands = [{
        'usaspending_uei': uei, 'bmf_ein': m.get('EIN') if m else None,
        'bmf_name': m.get('NAME') if m else None,
        'confidence_score': round(score, 3), 'match_method': method,
        'accepted': score >= 0.55,
    } for uei, (m, score, method) in bmf_matches.items() if m is not None and method != 'govt_filtered']
    n = _batch_insert(client, 'er_candidates', er_cands, schema='staging')
    log(f"  staging.er_candidates: {n} inserted")

    # ─── Step 4c: Public layer ───
    log("=== Loading public layer ===")

    # Organizations
    uei_to_org_id = {}
    org_rows = []
    for rec in recipients:
        uei = rec.get('uei', '')
        if not uei:
            continue
        bmf_match, score, method = bmf_matches.get(uei, (None, 0, None))
        is_govt = is_govt_entity(rec)

        org_type = 'nonprofit'
        if is_govt:
            org_type = 'local_government'
        elif not rec.get('business_types'):
            org_type = 'private'

        status = 'active' if bmf_match else 'er_created'
        if is_govt:
            status = 'er_created'

        org_id = str(uuid.uuid4())
        uei_to_org_id[uei] = org_id
        org_rows.append({
            'organization_id': org_id, 'status': status,
            'confidence_score': round(score, 3) if score else None,
            'name': rec['name'], 'org_type': org_type,
            'ein': bmf_match['EIN'] if bmf_match else None,
            'sam_uei': uei, 'duns_number': rec.get('duns') or None,
            'street_address': rec.get('address') or None,
            'city': rec.get('city') or None,
            'state': rec.get('state') or None,
            'zip': rec.get('zip') or None,
        })

    n = _batch_insert(client, 'organizations', org_rows)
    log(f"  organizations: {n} inserted")

    # Agencies
    agency_cache = {}
    def get_agency(name):
        if name in agency_cache:
            return agency_cache[name]
        oid = str(uuid.uuid4())
        try:
            client.table('organizations').insert({
                'organization_id': oid, 'status': 'active',
                'name': name, 'org_type': 'federal_agency',
            }).execute()
        except Exception:
            pass
        agency_cache[name] = oid
        return oid

    # Grants + grantees
    log(f"  grants ({len(awards)} awards, deduplicating)...")
    grant_rows = []
    grantee_rows = []
    seen = set()
    for a in awards:
        aid = a['award_id']
        if not aid or aid in seen:
            continue
        seen.add(aid)
        gid = str(uuid.uuid4())
        agency_name = a.get('awarding_agency', 'Unknown Agency')
        granter_id = get_agency(agency_name)

        grant_rows.append({
            'grant_id': gid, 'status': 'active',
            'title': a['description'][:200] if a['description'] else a.get('award_type'),
            'granter_org_id': granter_id, 'department': agency_name,
            'program': a.get('awarding_sub_agency'),
            'cfda_number': a.get('cfda_number'), 'award_number': aid,
            'original_funding_amount': a.get('award_amount'),
            'geo_state': a.get('place_of_performance_state'),
            'geo_city': a.get('place_of_performance_city'),
            'source_database': 'usaspending',
        })

        uei = a.get('recipient_uei', '')
        org_id = uei_to_org_id.get(uei)
        if org_id:
            grantee_rows.append({'grant_id': gid, 'organization_id': org_id})

    n = _batch_insert(client, 'grants', grant_rows)
    log(f"  grants: {n} inserted ({len(seen)} unique awards)")
    n = _batch_insert(client, 'grant_grantees', grantee_rows)
    log(f"  grant_grantees: {n} inserted")

    # ─── Summary ───
    elapsed = time.time() - start
    log(f"\n{'='*60}")
    log(f"PIPELINE COMPLETE in {elapsed/60:.1f} minutes")
    log(f"  Organizations: {len(uei_to_org_id)}")
    log(f"  Grants: {len(seen)}")
    log(f"  Agencies: {len(agency_cache)}")
    log(f"  ER: {matched} matched, {er_created} er_created, {govt_filtered} govt")
    log(f"{'='*60}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--zip', required=True, help='Path to USASpending bulk ZIP')
    parser.add_argument('--states', nargs='+', help='BMF states to load (default: all)')
    parser.add_argument('--skip-raw', action='store_true')
    args = parser.parse_args()
    run(args.zip, bmf_states=[s.lower() for s in args.states] if args.states else None, skip_raw=args.skip_raw)
