#!/usr/bin/env python3
"""
scraper_code_violations.py
Pulls open housing/rental code violations from Minneapolis Open Data (ArcGIS).
Outputs: docs/data/code_violations.json

Source: opendata.minneapolismn.gov — ArcGIS Feature Service
API returns paginated JSON — no auth required.
"""
import requests
import json
import time
from pathlib import Path
from datetime import datetime

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'code_violations.json'

# ArcGIS Feature Service endpoints for Minneapolis violations
# These are the stable REST endpoints from the Minneapolis ArcGIS hub
ENDPOINTS = [
    {
        'name': 'Rental Housing Violations',
        'url': 'https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Rental_Housing_Violations/FeatureServer/0/query',
        'params': {
            'where': "STATUS='Open'",
            'outFields': 'ADDRESS,VIOLATION_DATE,VIOLATION_TYPE,VIOLATION_DESC,STATUS,CASE_NUMBER',
            'f': 'json',
            'resultOffset': 0,
            'resultRecordCount': 1000,
        }
    },
    {
        'name': 'Housing Inspection Cases',
        'url': 'https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/HousingInspectionCases/FeatureServer/0/query',
        'params': {
            'where': "1=1",
            'outFields': 'ADDRESS,CASE_OPENED,CASE_STATUS,VIOLATION_CODE,DESCRIPTION',
            'f': 'json',
            'resultOffset': 0,
            'resultRecordCount': 1000,
        }
    },
]

# Fallback: Minneapolis 311 open data (CKAN-style endpoint)
FALLBACK_311 = 'https://opendata.minneapolismn.gov/api/3/action/datastore_search'
FALLBACK_RESOURCE = 'housing-code-violations'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (MotivatedSeller-Intel/1.0; real estate research)',
    'Accept': 'application/json',
}


def fetch_arcgis_endpoint(ep: dict) -> list:
    """Pull all records from a paginated ArcGIS endpoint with retry logic."""
    records = []
    offset = 0
    page_size = 1000
    max_retries = 3

    print(f"  [{ep['name']}] Fetching...", flush=True)

    while True:
        params = {**ep['params'], 'resultOffset': offset}
        success = False

        for attempt in range(max_retries):
            try:
                r = requests.get(ep['url'], params=params, headers=HEADERS, timeout=30)
                r.raise_for_status()
                data = r.json()

                if 'error' in data:
                    print(f"  [{ep['name']}] API error: {data['error']}", flush=True)
                    return records

                features = data.get('features', [])
                if not features:
                    return records

                for f in features:
                    attrs = f.get('attributes', {})
                    records.append(attrs)

                print(f"  [{ep['name']}] Page {offset//page_size + 1}: {len(features)} records", flush=True)

                if len(features) < page_size:
                    return records

                offset += page_size
                success = True
                time.sleep(0.5)
                break

            except requests.exceptions.RequestException as e:
                print(f"  [{ep['name']}] Attempt {attempt+1} failed: {e}", flush=True)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  [{ep['name']}] All retries failed.", flush=True)
                    return records

    return records


def normalize_record(raw: dict, source: str) -> dict:
    """Normalize to a common schema regardless of source."""
    # Try multiple possible field names for address
    address = (
        raw.get('ADDRESS') or
        raw.get('SITE_ADDRESS') or
        raw.get('PROPERTY_ADDRESS') or
        raw.get('address') or
        ''
    ).strip().upper()

    date_str = (
        raw.get('VIOLATION_DATE') or
        raw.get('CASE_OPENED') or
        raw.get('date') or
        ''
    )

    # Convert epoch milliseconds if numeric
    if isinstance(date_str, (int, float)) and date_str > 0:
        try:
            date_str = datetime.fromtimestamp(date_str / 1000).strftime('%Y-%m-%d')
        except Exception:
            date_str = ''

    return {
        'address': address,
        'date': str(date_str)[:10] if date_str else '',
        'type': str(raw.get('VIOLATION_TYPE') or raw.get('VIOLATION_CODE') or 'Code Violation'),
        'description': str(raw.get('VIOLATION_DESC') or raw.get('DESCRIPTION') or ''),
        'status': str(raw.get('STATUS') or raw.get('CASE_STATUS') or 'Open'),
        'case_number': str(raw.get('CASE_NUMBER') or raw.get('CASE_ID') or ''),
        'source': source,
    }


def main():
    print("\n=== SCRAPER: Minneapolis Code Violations ===", flush=True)
    all_records = []

    for ep in ENDPOINTS:
        raw_records = fetch_arcgis_endpoint(ep)
        for r in raw_records:
            norm = normalize_record(r, ep['name'])
            if norm['address']:
                all_records.append(norm)

    # Deduplicate by address + date
    seen = set()
    deduped = []
    for r in all_records:
        key = f"{r['address']}|{r['date']}"
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    print(f"\n  Total unique violations: {len(deduped)}", flush=True)

    # Build address lookup for cross-referencing with leads.json
    by_address = {}
    for r in deduped:
        addr = r['address']
        if addr:
            if addr not in by_address:
                by_address[addr] = []
            by_address[addr].append(r)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'Minneapolis Open Data — Code Violations',
        'total_violations': len(deduped),
        'unique_addresses': len(by_address),
        'violations': deduped,
        'by_address': by_address,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    print(f"  Written: {OUTPUT} ({OUTPUT.stat().st_size / 1024:.0f} KB)", flush=True)
    return len(deduped)


if __name__ == '__main__':
    count = main()
    print(f"\n  Code violations scraper complete: {count} records", flush=True)
    if count == 0:
        print("  WARNING: Zero records. Endpoint may have changed. Check URL in scraper_code_violations.py", flush=True)
        exit(1)
