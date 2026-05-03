#!/usr/bin/env python3
"""
scraper_code_violations.py
Pulls Minneapolis housing/rental code violations from the City's open data.

SOURCE: Minneapolis Open Data Portal (ArcGIS Hub)
  - 311 Complaints dataset (open, updated daily)
  - Active Rental Licenses (cross-reference for problem properties)

The Minneapolis Regulatory Services violations dashboard (Tableau) does NOT
have a public API — it is browser-only. We use the 311 complaints dataset
instead, which captures complaint-triggered violations and IS publicly accessible.

OUTPUT: docs/data/code_violations.json
"""
import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'code_violations.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (MotivatedSeller-Intel/1.0; property research)',
    'Accept': 'application/json',
}

# Minneapolis Open Data — ArcGIS Hub direct query endpoints
# These are the verified public dataset IDs from opendata.minneapolismn.gov
SOURCES = [
    {
        'name': '311 Housing Complaints',
        # 311 service requests — filter to housing/property complaint types
        'url': 'https://opendata.arcgis.com/api/v3/datasets/2f0b741bedd14d9887aefb1c6c08af8e_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1',
        'type': 'csv',
    },
    {
        'name': 'Minneapolis Problem Properties',
        # Problem properties list — directly maintained by city
        'url': 'https://opendata.arcgis.com/api/v3/datasets/cityoflakes::problem-properties/downloads/data?format=csv&spatialRefId=4326',
        'type': 'csv',
    },
]

# Fallback — direct ArcGIS feature query using known Minneapolis org ID
ARCGIS_ORG = 'afSMGVsC7QlRK1kZ'
ARCGIS_QUERY = f'https://services.arcgis.com/{ARCGIS_ORG}/arcgis/rest/services'

MAX_RETRIES = 3


def fetch_with_retry(url, params=None, timeout=45):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            print(f"  HTTP {r.status_code} (attempt {attempt+1})", flush=True)
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 ** attempt)
    return None


def try_arcgis_services_list():
    """Try to discover active ArcGIS services for Minneapolis violations."""
    print("  Discovering Minneapolis ArcGIS services...", flush=True)
    r = fetch_with_retry(f'{ARCGIS_QUERY}?f=json')
    if not r:
        return []
    try:
        data = r.json()
        services = data.get('services', [])
        violation_services = [s for s in services
                              if any(k in s.get('name', '').lower()
                                     for k in ['violation', 'inspection', 'housing', '311'])]
        print(f"  Found {len(violation_services)} candidate services", flush=True)
        return violation_services
    except Exception as e:
        print(f"  Could not parse services: {e}", flush=True)
        return []


def fetch_csv_source(source):
    """Fetch and parse a CSV source."""
    print(f"  [{source['name']}] Fetching CSV...", flush=True)
    r = fetch_with_retry(source['url'])
    if not r:
        print(f"  [{source['name']}] Failed", flush=True)
        return []

    lines = r.text.strip().split('\n')
    if len(lines) < 2:
        return []

    headers = [h.strip().strip('"').upper() for h in lines[0].split(',')]
    records = []

    for line in lines[1:]:
        vals = line.split(',')
        if len(vals) < len(headers):
            continue
        row = {headers[i]: vals[i].strip().strip('"') for i in range(len(headers))}

        # Extract address — try multiple field names
        address = (row.get('ADDRESS') or row.get('SITE_ADDRESS') or
                   row.get('PROPERTY_ADDRESS') or row.get('LOCATION') or '')

        # Filter to housing/property violation types
        category = (row.get('CATEGORY') or row.get('TYPE') or
                    row.get('COMPLAINT_TYPE') or row.get('CASE_TYPE') or '').upper()

        housing_keywords = ['HOUSING', 'RENTAL', 'PROPERTY', 'BUILDING',
                            'CODE', 'VIOLATION', 'INSPECTION', 'UNSAFE']
        if address and any(k in category for k in housing_keywords):
            records.append({
                'address': address.upper().strip(),
                'date': row.get('DATE') or row.get('OPEN_DATE') or row.get('SUBMITTED') or '',
                'type': category,
                'status': row.get('STATUS') or row.get('CASE_STATUS') or 'Unknown',
                'case_number': row.get('CASE_NUMBER') or row.get('SERVICE_REQUEST_ID') or '',
                'source': source['name'],
            })

    print(f"  [{source['name']}] {len(records)} housing records", flush=True)
    return records


def main():
    print("\n=== SCRAPER: Minneapolis Code Violations ===", flush=True)
    all_records = []

    # Try each source
    for source in SOURCES:
        records = fetch_csv_source(source)
        all_records.extend(records)
        time.sleep(1)

    # If CSV sources failed, try discovering ArcGIS services
    if len(all_records) == 0:
        print("\n  CSV sources returned 0. Trying ArcGIS service discovery...", flush=True)
        services = try_arcgis_services_list()
        for svc in services[:3]:
            svc_url = f"{ARCGIS_QUERY}/{svc['name']}/FeatureServer/0/query"
            params = {
                'where': "1=1",
                'outFields': '*',
                'f': 'json',
                'resultRecordCount': 1000,
            }
            r = fetch_with_retry(svc_url, params=params)
            if r:
                try:
                    data = r.json()
                    features = data.get('features', [])
                    for feat in features:
                        attrs = feat.get('attributes', {})
                        addr = str(attrs.get('ADDRESS') or attrs.get('SITE_ADDRESS') or '')
                        if addr:
                            all_records.append({
                                'address': addr.upper(),
                                'date': '',
                                'type': svc['name'],
                                'status': 'Open',
                                'case_number': '',
                                'source': svc['name'],
                            })
                    print(f"  {svc['name']}: {len(features)} records", flush=True)
                except Exception:
                    pass
            time.sleep(1)

    # Deduplicate
    seen = set()
    deduped = []
    for r in all_records:
        key = f"{r['address']}|{r.get('case_number', '')}"
        if key not in seen and r['address']:
            seen.add(key)
            deduped.append(r)

    print(f"\n  Total unique violations: {len(deduped)}", flush=True)

    by_address = {}
    for r in deduped:
        by_address.setdefault(r['address'], []).append(r)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'Minneapolis Open Data — 311 Housing Complaints',
        'total_violations': len(deduped),
        'unique_addresses': len(by_address),
        'violations': deduped,
        'by_address': by_address,
        'note': 'Full violations dataset requires Tableau access — using 311 complaints as proxy',
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    size = OUTPUT.stat().st_size / 1024
    print(f"  Written: {OUTPUT} ({size:.0f} KB)", flush=True)
    return len(deduped)


if __name__ == '__main__':
    count = main()
    print(f"\n  Code violations scraper: {count} records", flush=True)
    if count == 0:
        print("  NOTE: Minneapolis violations data is in Tableau (no public API).", flush=True)
        print("  This is a known limitation. Output file written with 0 records.", flush=True)
        # Exit 0 — not a hard failure, just no data available via API
