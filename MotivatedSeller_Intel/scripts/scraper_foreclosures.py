#!/usr/bin/env python3
"""
scraper_foreclosures.py
Pulls mortgage foreclosure notices for Hennepin County properties.

SOURCE: Minnesota Notice Connect (noticeconnect.com)
  - Minnesota's public legal notice database
  - Fully open, no login required, no 403 blocks
  - Covers all MN counties including Hennepin
  - Updated daily by newspaper publishers

Finance & Commerce and Star Tribune block automated access (403).
MN Notice Connect is the correct open alternative.

OUTPUT: docs/data/foreclosures.json
"""
import requests
from bs4 import BeautifulSoup
import json
import re
import time
from pathlib import Path
from datetime import datetime

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'foreclosures.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.noticeconnect.com/',
}

MAX_RETRIES = 3
BASE_URL = 'https://www.noticeconnect.com'


def fetch_with_retry(url, params=None):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                print(f"  Rate limited. Waiting {wait}s...", flush=True)
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code} (attempt {attempt+1})", flush=True)
                time.sleep(2 * (attempt + 1))
        except Exception as e:
            print(f"  Error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(3)
    return None


def scrape_notice_connect():
    """
    MN Notice Connect — the official public legal notice database for MN.
    Search for Hennepin County mortgage foreclosure notices.
    """
    records = []
    print("  [MN Notice Connect] Searching foreclosure notices...", flush=True)

    # Search endpoint
    search_url = f'{BASE_URL}/notices'
    params = {
        'state': 'MN',
        'county': 'Hennepin',
        'category': 'Foreclosure',
        'q': 'mortgage foreclosure sale',
    }

    r = fetch_with_retry(search_url, params=params)
    if not r:
        # Try alternate URL format
        alt_url = f'{BASE_URL}/mn/hennepin/foreclosure'
        print(f"  Trying alternate URL: {alt_url}", flush=True)
        r = fetch_with_retry(alt_url)

    if not r:
        print("  [MN Notice Connect] Failed to reach site", flush=True)
        return records

    soup = BeautifulSoup(r.text, 'html.parser')

    # Find notice listings — Notice Connect uses article/div cards
    notice_containers = (
        soup.find_all('article', class_=re.compile(r'notice', re.I)) or
        soup.find_all('div', class_=re.compile(r'notice-item|legal-notice|result', re.I)) or
        soup.find_all('li', class_=re.compile(r'notice|result', re.I))
    )

    print(f"  Found {len(notice_containers)} notice containers", flush=True)

    for container in notice_containers[:50]:
        text = container.get_text(separator=' ', strip=True)
        if not any(k in text.upper() for k in ['FORECLOSURE', 'MORTGAGE', 'HENNEPIN']):
            continue

        # Get link to full notice
        link = container.find('a', href=True)
        full_text = text

        if link:
            href = link['href']
            if not href.startswith('http'):
                href = BASE_URL + href
            notice_r = fetch_with_retry(href)
            if notice_r:
                full_text = BeautifulSoup(notice_r.text, 'html.parser').get_text(' ', strip=True)
            time.sleep(0.5)

        record = parse_mn_foreclosure(full_text)
        if record:
            records.append(record)

    # If no containers found, try parsing the page text directly
    if not notice_containers:
        print("  No containers found — parsing page text directly", flush=True)
        page_text = soup.get_text(' ', strip=True)
        # Split on notice boundaries (MN notices start with "NOTICE OF MORTGAGE")
        notices = re.split(r'NOTICE OF MORTGAGE FORECLOSURE SALE', page_text, flags=re.I)
        for notice_text in notices[1:]:  # skip first (before first notice)
            record = parse_mn_foreclosure('NOTICE OF MORTGAGE FORECLOSURE SALE ' + notice_text[:2000])
            if record:
                records.append(record)

    print(f"  [MN Notice Connect] {len(records)} notices parsed", flush=True)
    return records


def scrape_mn_court_public():
    """
    MN Judicial Branch public case search — lis pendens / foreclosure filings.
    MCRO (MN Court Records Online) — no login required for basic search.
    """
    records = []
    print("  [MN Courts] Checking public foreclosure filings...", flush=True)

    # MCRO public search
    url = 'https://publicaccess.courts.state.mn.us/CaseSearch'
    params = {
        'county': '27',  # Hennepin County code
        'caseType': 'FC',  # Foreclosure
        'dateFiledStart': (datetime.now().replace(month=1, day=1)).strftime('%m/%d/%Y'),
        'dateFiledEnd': datetime.now().strftime('%m/%d/%Y'),
    }

    r = fetch_with_retry(url, params=params)
    if not r:
        print("  [MN Courts] Could not reach MCRO", flush=True)
        return records

    soup = BeautifulSoup(r.text, 'html.parser')

    # Parse case results table
    rows = soup.find_all('tr')
    for row in rows[1:]:  # skip header
        cells = row.find_all('td')
        if len(cells) < 3:
            continue

        case_text = ' '.join(c.get_text(strip=True) for c in cells)
        record = parse_mn_foreclosure(case_text)
        if record:
            records.append(record)

    print(f"  [MN Courts] {len(records)} records", flush=True)
    return records


def parse_mn_foreclosure(text):
    """Extract key fields from MN mortgage foreclosure notice text."""
    text_up = text.upper()

    if 'HENNEPIN' not in text_up:
        return None
    if not any(k in text_up for k in ['FORECLOSURE', 'MORTGAGE', 'FORFEITURE']):
        return None

    # Address extraction
    address = ''
    for pattern in [
        r'\b(\d{2,5}\s+[A-Z][A-Za-z\s]{2,25}(?:STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|LANE|LN|BLVD|BOULEVARD|WAY|COURT|CT|PLACE|PL)[\s,.]?\s*(?:N|S|E|W|NE|NW|SE|SW)?)',
        r'(?:located at|property at|premises at|situated at)[:\s]+([^\n,]{10,60})',
        r'(?:property address)[:\s]+([^\n,]{10,60})',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            address = m.group(1).strip().upper()
            break

    # Sale date
    sale_date = ''
    for pattern in [
        r'(?:sale date|date of sale)[:\s]+(\w+ \d{1,2},? \d{4})',
        r'(?:sold on|foreclosure sale on)[:\s]+(\w+ \d{1,2},? \d{4})',
        r'(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            sale_date = m.group(1).strip()
            break

    # Mortgagor (owner)
    owner = ''
    for pattern in [
        r'(?:mortgagor[s]?|grantor[s]?)[:\s]+([A-Z][A-Za-z\s,&]+?)(?:,|\.|;|\n)',
        r'(?:owner[s]?)[:\s]+([A-Z][A-Za-z\s,&]+?)(?:,|\.|;|\n)',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            owner = m.group(1).strip().upper()
            break

    # Original amount
    amount = ''
    m = re.search(r'\$([\d,]+(?:\.\d{2})?)', text)
    if m:
        amount = '$' + m.group(1)

    if not address and not owner:
        return None

    return {
        'address': address,
        'owner': owner,
        'sale_date': sale_date,
        'original_amount': amount,
        'county': 'HENNEPIN',
        'signal': 'FORECLOSURE NOTICE',
        'source': 'MN Notice Connect / MN Courts',
        'scraped_at': datetime.now().strftime('%Y-%m-%d'),
    }


def main():
    print("\n=== SCRAPER: Foreclosure Notices ===", flush=True)
    all_records = []

    # Primary: MN Notice Connect
    nc_records = scrape_notice_connect()
    all_records.extend(nc_records)

    # Backup: MN Court public search
    if len(all_records) < 3:
        time.sleep(2)
        court_records = scrape_mn_court_public()
        all_records.extend(court_records)

    # Deduplicate
    seen = set()
    deduped = []
    for r in all_records:
        key = r.get('address', '') + r.get('owner', '')
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)

    print(f"\n  Total unique foreclosure notices: {len(deduped)}", flush=True)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'MN Notice Connect / MN Court Records',
        'total_notices': len(deduped),
        'notices': deduped,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    print(f"  Written: {OUTPUT}", flush=True)
    return len(deduped)


if __name__ == '__main__':
    count = main()
    print(f"\n  Foreclosure scraper: {count} records", flush=True)
