#!/usr/bin/env python3
"""
scraper_tax_forfeiture.py
Downloads and parses the Hennepin County tax-forfeited land list from MinnBid.

Source: https://www.hennepincounty.gov/services/property/tax-forfeited-land
        + MinnBid auction platform PDFs

Outputs: docs/data/tax_forfeiture.json

GREEN/YELLOW — the PDF URL is predictable but changes when a new list is published.
The scraper first finds the current list URL from the Hennepin page, then downloads
and parses it with pypdf.
"""
import requests
from bs4 import BeautifulSoup
import json
import re
import time
import io
from pathlib import Path
from datetime import datetime

try:
    import pypdf
    PYPDF_OK = True
except ImportError:
    try:
        import PyPDF2 as pypdf
        PYPDF_OK = True
    except ImportError:
        PYPDF_OK = False

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'tax_forfeiture.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

HENNEPIN_TFL_URL = 'https://www.hennepincounty.gov/services/property/tax-forfeited-land'
MINNBID_BASE = 'https://www.minnbid.org'

MAX_RETRIES = 3


def fetch_with_retry(url: str) -> requests.Response | None:
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code == 200:
                return r
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}", flush=True)
            time.sleep(2 ** attempt)
    return None


def find_pdf_url() -> str | None:
    """
    Scrape the Hennepin County TFL page to find the current auction PDF link.
    Also checks MinnBid directly.
    """
    print("  Searching for current Hennepin TFL PDF...", flush=True)

    # Try Hennepin County page
    r = fetch_with_retry(HENNEPIN_TFL_URL)
    if r:
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            text = link.get_text(strip=True).lower()
            if (('.pdf' in href.lower() or 'pdf' in text) and
                    ('forfeiture' in href.lower() or 'forfeiture' in text or
                     'auction' in href.lower() or 'list' in text)):
                if not href.startswith('http'):
                    href = 'https://www.hennepincounty.gov' + href
                print(f"  Found PDF link: {href}", flush=True)
                return href

    # Try MinnBid directly
    minnbid_url = 'https://www.minnbid.org/auctions/?county=Hennepin'
    r2 = fetch_with_retry(minnbid_url)
    if r2:
        soup2 = BeautifulSoup(r2.text, 'html.parser')
        for link in soup2.find_all('a', href=True):
            href = link['href']
            if '.pdf' in href.lower() and 'hennepin' in href.lower():
                if not href.startswith('http'):
                    href = MINNBID_BASE + href
                print(f"  Found MinnBid PDF: {href}", flush=True)
                return href

    # Last resort — try the known MinnBid upload path pattern
    # These URLs are predictable: minnbidapi-prod.ecommerce.auction/uploads/...
    print("  Could not find PDF URL automatically.", flush=True)
    return None


def parse_pdf_for_properties(pdf_bytes: bytes) -> list:
    """Extract property addresses and PIDs from the TFL PDF."""
    records = []

    if not PYPDF_OK:
        print("  pypdf not installed. Run: pip install pypdf", flush=True)
        return records

    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        full_text = ''
        for page in reader.pages:
            full_text += page.extract_text() + '\n'
    except Exception as e:
        print(f"  PDF parse error: {e}", flush=True)
        return records

    lines = full_text.split('\n')

    # Hennepin TFL lists follow a pattern:
    # PID | Address | Legal Description | Appraised Value | Min Bid
    # We'll extract address-like lines and appraised values
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Look for lines that look like property addresses
        # MN addresses: number + street name
        addr_match = re.search(
            r'\b(\d{1,5})\s+([A-Z][A-Za-z\s]{2,30}(?:ST|AVE|RD|DR|LN|BLVD|WAY|CT|PL|TER|CIR)[\.,]?\s*(?:N|S|E|W|NE|NW|SE|SW)?)',
            line, re.IGNORECASE
        )

        if addr_match:
            address = addr_match.group(0).strip().upper()

            # Look for appraised value in same or adjacent line
            value = 0
            val_text = line + (lines[i+1] if i+1 < len(lines) else '')
            val_match = re.search(r'\$?([\d,]+(?:\.\d{2})?)', val_text)
            if val_match:
                try:
                    value = int(val_match.group(1).replace(',', '').split('.')[0])
                except ValueError:
                    pass

            # Extract PID if present (Hennepin PIDs are numeric, up to 13 digits)
            pid = ''
            pid_match = re.search(r'\b(\d{7,13})\b', line)
            if pid_match:
                pid = pid_match.group(1)

            if address:
                records.append({
                    'address': address,
                    'pid': pid,
                    'appraised_value': value,
                    'source': 'Hennepin County Tax Forfeiture List',
                    'signal': 'TAX FORFEITURE',
                    'county': 'HENNEPIN',
                    'scraped_at': datetime.now().strftime('%Y-%m-%d'),
                })

    # Deduplicate
    seen = set()
    deduped = []
    for r in records:
        if r['address'] not in seen:
            seen.add(r['address'])
            deduped.append(r)

    return deduped


def main():
    print("\n=== SCRAPER: Hennepin Tax Forfeiture List ===", flush=True)

    pdf_url = find_pdf_url()

    if not pdf_url:
        print("  No PDF found. Outputting empty dataset.", flush=True)
        records = []
    else:
        print(f"  Downloading PDF: {pdf_url}", flush=True)
        r = fetch_with_retry(pdf_url)
        if not r:
            print("  Failed to download PDF.", flush=True)
            records = []
        else:
            print(f"  PDF downloaded ({len(r.content) / 1024:.0f} KB). Parsing...", flush=True)
            records = parse_pdf_for_properties(r.content)

    print(f"\n  Total forfeiture properties found: {len(records)}", flush=True)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'Hennepin County Tax Forfeited Land / MinnBid',
        'pdf_url': pdf_url or '',
        'total_properties': len(records),
        'properties': records,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    print(f"  Written: {OUTPUT}", flush=True)
    return len(records)


if __name__ == '__main__':
    count = main()
    print(f"\n  Tax forfeiture scraper complete: {count} properties", flush=True)
