#!/usr/bin/env python3
"""
scraper_tax_forfeiture.py
Pulls Hennepin County tax-forfeited land inventory from ePropertyPlus.

SOURCE: public-hennepin.epropertyplus.com
  - This is the official Hennepin County tax-forfeited land inventory portal
  - Lists all properties currently for sale (estimated market value + min bid)
  - Updated as properties are added/sold

NOTE: MinnBid (minnbid.org) is DNS-blocked from GitHub Actions servers.
      ePropertyPlus is the correct source — it's what Hennepin County links to.

OUTPUT: docs/data/tax_forfeiture.json
"""
import requests
from bs4 import BeautifulSoup
import json
import re
import time
from pathlib import Path
from datetime import datetime

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'tax_forfeiture.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

BASE_URL = 'https://public-hennepin.epropertyplus.com'
INVENTORY_URL = f'{BASE_URL}/landmgmt/app/ownerview/'

MAX_RETRIES = 3


def fetch(url, params=None):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            print(f"  HTTP {r.status_code} (attempt {attempt+1})", flush=True)
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 ** attempt)
    return None


def scrape_epropertyplus():
    """
    Scrape Hennepin County's official tax-forfeited land inventory.
    URL: public-hennepin.epropertyplus.com
    """
    records = []
    print(f"  [ePropertyPlus] Fetching inventory: {INVENTORY_URL}", flush=True)

    r = fetch(INVENTORY_URL)
    if not r:
        # Try alternate paths
        for alt in [BASE_URL, f'{BASE_URL}/landmgmt/', f'{BASE_URL}/app/']:
            print(f"  Trying: {alt}", flush=True)
            r = fetch(alt)
            if r:
                break

    if not r:
        print("  [ePropertyPlus] Site unreachable", flush=True)
        return records

    soup = BeautifulSoup(r.text, 'html.parser')

    # ePropertyPlus uses a property card/table layout
    # Look for property listings in various container types
    property_containers = (
        soup.find_all('div', class_=re.compile(r'property|parcel|listing|item', re.I)) or
        soup.find_all('tr') or
        soup.find_all('li', class_=re.compile(r'property|parcel', re.I))
    )

    print(f"  Found {len(property_containers)} containers", flush=True)

    for container in property_containers:
        text = container.get_text(separator=' ', strip=True)
        if len(text) < 10:
            continue

        # Extract address
        addr_match = re.search(
            r'\b(\d{1,5}\s+[A-Z][A-Za-z\s]{2,25}(?:ST|AVE|RD|DR|LN|BLVD|WAY|CT|PL|TER|PKWY)[\.,]?\s*(?:N|S|E|W|NE|NW|SE|SW)?)\b',
            text, re.IGNORECASE
        )
        if not addr_match:
            continue

        address = addr_match.group(0).strip().upper()

        # Extract PID
        pid = ''
        pid_match = re.search(r'\b(\d{7,13})\b', text)
        if pid_match:
            pid = pid_match.group(1)

        # Extract value
        value = 0
        val_match = re.search(r'\$\s*([\d,]+)', text)
        if val_match:
            try:
                value = int(val_match.group(1).replace(',', ''))
            except ValueError:
                pass

        records.append({
            'address': address,
            'pid': pid,
            'appraised_value': value,
            'signal': 'TAX FORFEITURE',
            'county': 'HENNEPIN',
            'source': INVENTORY_URL,
            'scraped_at': datetime.now().strftime('%Y-%m-%d'),
        })

    # If no containers found, try JSON API (ePropertyPlus has a REST API)
    if not records:
        print("  No HTML results — trying ePropertyPlus API...", flush=True)
        api_endpoints = [
            f'{BASE_URL}/landmgmt/api/public/listings',
            f'{BASE_URL}/landmgmt/api/parcels?county=Hennepin&status=available',
            f'{BASE_URL}/api/v1/properties?status=forsale',
        ]
        for endpoint in api_endpoints:
            api_r = fetch(endpoint)
            if api_r:
                try:
                    data = api_r.json()
                    items = data if isinstance(data, list) else data.get('items', data.get('results', data.get('properties', [])))
                    for item in items:
                        address = str(item.get('address') or item.get('siteAddress') or item.get('propertyAddress') or '').upper()
                        pid = str(item.get('pid') or item.get('parcelId') or item.get('parcelNumber') or '')
                        value = int(item.get('appraisedValue') or item.get('marketValue') or item.get('price') or 0)
                        if address or pid:
                            records.append({
                                'address': address,
                                'pid': re.sub(r'[\-\.]', '', pid),
                                'appraised_value': value,
                                'signal': 'TAX FORFEITURE',
                                'county': 'HENNEPIN',
                                'source': endpoint,
                                'scraped_at': datetime.now().strftime('%Y-%m-%d'),
                            })
                    if records:
                        print(f"  API returned {len(records)} records", flush=True)
                        break
                except Exception as e:
                    print(f"  API parse error: {e}", flush=True)

    return records


def scrape_surplus_funds_pdf():
    """
    Fallback: parse the surplus funds PDF from Hennepin County.
    These are properties that already sold but have excess proceeds —
    useful as a signal that a property recently went through forfeiture.
    """
    records = []
    pdf_url = ('https://www.hennepincounty.gov/-/media/Hennepin-Headless/Hennepin-Gov/'
               'services/property/tfl/surplus-funds-notices.pdf'
               '?rev=06a6ecc595184932b939ce47ecc4a9bb&hash=1A4AB7B14E5CA07D0563EB6418C13830')

    print(f"  [Surplus Funds PDF] Downloading...", flush=True)
    r = fetch(pdf_url)
    if not r:
        return records

    try:
        import pypdf
        import io
        reader = pypdf.PdfReader(io.BytesIO(r.content))
        text = '\n'.join(page.extract_text() or '' for page in reader.pages)
    except Exception as e:
        print(f"  PDF error: {e}", flush=True)
        return records

    # Extract PIDs and addresses from surplus funds notices
    # Format: "PID: XXXXXXXXX Address: XXXX ST N Minneapolis MN"
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue

        pid = ''
        pid_match = re.search(r'\b(\d{7,13})\b', line)
        if pid_match:
            pid = pid_match.group(1)

        addr_match = re.search(
            r'\b(\d{1,5}\s+[A-Z][A-Za-z\s]{2,25}(?:ST|AVE|RD|DR|LN|BLVD|WAY|CT|PL)[\.,]?\s*(?:N|S|E|W)?)\b',
            line, re.IGNORECASE
        )
        address = addr_match.group(0).strip().upper() if addr_match else ''

        if pid or address:
            records.append({
                'address': address,
                'pid': pid,
                'appraised_value': 0,
                'signal': 'TAX FORFEITURE - SURPLUS FUNDS',
                'county': 'HENNEPIN',
                'source': pdf_url,
                'scraped_at': datetime.now().strftime('%Y-%m-%d'),
            })

    print(f"  [Surplus Funds PDF] {len(records)} records", flush=True)
    return records


def main():
    print("\n=== SCRAPER: Hennepin Tax Forfeiture List ===", flush=True)

    # Primary: ePropertyPlus inventory
    records = scrape_epropertyplus()

    # Fallback: surplus funds PDF (already forfeited/sold properties)
    if len(records) < 3:
        print("  ePropertyPlus returned few results — adding surplus funds PDF", flush=True)
        records.extend(scrape_surplus_funds_pdf())

    # Deduplicate by PID then address
    seen_pids = set()
    seen_addrs = set()
    deduped = []
    for r in records:
        pid = r.get('pid', '')
        addr = r.get('address', '')
        if pid and pid in seen_pids:
            continue
        if addr and addr in seen_addrs:
            continue
        if pid:
            seen_pids.add(pid)
        if addr:
            seen_addrs.add(addr)
        deduped.append(r)

    print(f"\n  Total unique forfeiture properties: {len(deduped)}", flush=True)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'Hennepin County ePropertyPlus / Surplus Funds',
        'inventory_url': INVENTORY_URL,
        'total_properties': len(deduped),
        'properties': deduped,
        'note': 'Properties currently for sale or recently forfeited in Hennepin County',
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    size = OUTPUT.stat().st_size / 1024
    print(f"  Written: {OUTPUT} ({size:.0f} KB)", flush=True)
    return len(deduped)


if __name__ == '__main__':
    count = main()
    print(f"\n  Tax forfeiture scraper: {count} properties", flush=True)
