#!/usr/bin/env python3
"""
scraper_tax_forfeiture.py
Downloads Hennepin County tax-forfeited land auction lists from MinnBid.

SOURCE: MinnBid (minnbid.org) — Minnesota's official tax-forfeited land auction platform
  - Lists all Hennepin County properties up for tax forfeiture auction
  - PDFs published per auction cycle (typically 2-3x per year)
  - The scraper finds the current auction list, NOT the checklist or other docs

The previous version grabbed "before-you-bid-checklist.pdf" — wrong file.
This version specifically targets the property LIST (numbered parcel inventory).

OUTPUT: docs/data/tax_forfeiture.json
"""
import requests
from bs4 import BeautifulSoup
import json
import re
import io
from pathlib import Path
from datetime import datetime

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'tax_forfeiture.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

MINNBID_HENNEPIN = 'https://www.minnbid.org/auctions/list/?county=Hennepin&type=land'
HENNEPIN_TFL_PAGE = 'https://www.hennepincounty.gov/services/property/tax-forfeited-land'

MAX_RETRIES = 3

# Keywords that identify the PROPERTY LIST (not checklists, notices, maps etc.)
LIST_KEYWORDS = ['list', 'parcel', 'property', 'auction', 'inventory', 'sale list']
EXCLUDE_KEYWORDS = ['checklist', 'check-list', 'before you bid', 'map', 'instructions',
                    'terms', 'notice of sale', 'how to']


def fetch_with_retry(url):
    import time
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code == 200:
                return r
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 ** attempt)
    return None


def is_property_list(url, link_text):
    """Return True if the PDF link looks like a property auction list, not a checklist."""
    url_lower = url.lower()
    text_lower = link_text.lower()
    combined = url_lower + ' ' + text_lower

    # Exclude obvious non-list documents
    if any(k in combined for k in EXCLUDE_KEYWORDS):
        return False

    # Must contain at least one list-like keyword
    return any(k in combined for k in LIST_KEYWORDS)


def find_auction_pdf_urls():
    """Search MinnBid and Hennepin County page for current property auction list PDFs."""
    pdf_urls = []

    # 1. Try MinnBid
    print("  Checking MinnBid...", flush=True)
    r = fetch_with_retry(MINNBID_HENNEPIN)
    if r:
        soup = BeautifulSoup(r.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True)
            if '.pdf' in href.lower() and is_property_list(href, text):
                if not href.startswith('http'):
                    href = 'https://www.minnbid.org' + href
                print(f"  MinnBid PDF found: {text[:60]} → {href[:80]}", flush=True)
                pdf_urls.append(href)

    # 2. Try Hennepin County TFL page
    print("  Checking Hennepin County TFL page...", flush=True)
    r2 = fetch_with_retry(HENNEPIN_TFL_PAGE)
    if r2:
        soup2 = BeautifulSoup(r2.text, 'html.parser')
        for link in soup2.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True)
            if '.pdf' in href.lower() and is_property_list(href, text):
                if not href.startswith('http'):
                    href = 'https://www.hennepincounty.gov' + href
                print(f"  Hennepin PDF found: {text[:60]} → {href[:80]}", flush=True)
                pdf_urls.append(href)

    # 3. Try MinnBid main auction page with broader search
    if not pdf_urls:
        print("  Trying MinnBid main page...", flush=True)
        r3 = fetch_with_retry('https://www.minnbid.org')
        if r3:
            soup3 = BeautifulSoup(r3.text, 'html.parser')
            for link in soup3.find_all('a', href=True):
                href = link['href']
                text = link.get_text(strip=True)
                if ('.pdf' in href.lower() and
                        'hennepin' in (href + text).lower() and
                        is_property_list(href, text)):
                    if not href.startswith('http'):
                        href = 'https://www.minnbid.org' + href
                    pdf_urls.append(href)

    return list(dict.fromkeys(pdf_urls))  # deduplicate preserving order


def parse_pdf(pdf_bytes):
    """Extract property addresses and PIDs from auction list PDF."""
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        text = '\n'.join(page.extract_text() or '' for page in reader.pages)
    except Exception as e:
        print(f"  pypdf error: {e}", flush=True)
        return []

    records = []
    lines = text.split('\n')

    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Look for property addresses
        addr_match = re.search(
            r'\b(\d{1,5})\s+([A-Z][A-Za-z\s]{2,25}(?:ST|AVE|RD|DR|LN|BLVD|WAY|CT|PL|TER|CIR|PKWY)[\.,]?\s*(?:N|S|E|W|NE|NW|SE|SW)?)\b',
            line, re.IGNORECASE
        )
        if not addr_match:
            continue

        address = addr_match.group(0).strip().upper()

        # Look for appraised value
        value = 0
        val_match = re.search(r'\$\s*([\d,]+)', line + ' ' + (lines[i+1] if i+1 < len(lines) else ''))
        if val_match:
            try:
                value = int(val_match.group(1).replace(',', ''))
            except ValueError:
                pass

        # Look for PID (Hennepin PIDs: numeric, 7-13 digits)
        pid = ''
        pid_match = re.search(r'\b(\d{7,13})\b', line)
        if pid_match:
            pid = pid_match.group(1)

        records.append({
            'address': address,
            'pid': pid,
            'appraised_value': value,
            'signal': 'TAX FORFEITURE',
            'county': 'HENNEPIN',
            'source': 'Hennepin County / MinnBid Auction List',
            'scraped_at': datetime.now().strftime('%Y-%m-%d'),
        })

    # Deduplicate by address
    seen = set()
    deduped = []
    for r in records:
        if r['address'] not in seen:
            seen.add(r['address'])
            deduped.append(r)

    return deduped


def main():
    print("\n=== SCRAPER: Hennepin Tax Forfeiture List ===", flush=True)

    pdf_urls = find_auction_pdf_urls()

    if not pdf_urls:
        print("  No auction list PDFs found. Hennepin may not have an active auction.", flush=True)
        print("  This is normal between auction cycles (Hennepin runs 2-3 auctions/year).", flush=True)
        records = []
        pdf_url_used = ''
    else:
        # Use first (most relevant) PDF found
        pdf_url_used = pdf_urls[0]
        print(f"  Downloading: {pdf_url_used}", flush=True)
        r = fetch_with_retry(pdf_url_used)
        if not r:
            print("  Download failed.", flush=True)
            records = []
        else:
            print(f"  Downloaded {len(r.content)/1024:.0f} KB. Parsing...", flush=True)
            records = parse_pdf(r.content)
            print(f"  Parsed {len(records)} properties from PDF", flush=True)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'Hennepin County Tax Forfeited Land / MinnBid',
        'pdf_url': pdf_url_used,
        'all_pdfs_found': pdf_urls,
        'total_properties': len(records),
        'properties': records,
        'note': 'Hennepin runs 2-3 auctions/year. Zero records between cycles is normal.',
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    size = OUTPUT.stat().st_size / 1024
    print(f"  Written: {OUTPUT} ({size:.0f} KB)", flush=True)
    return len(records)


if __name__ == '__main__':
    count = main()
    print(f"\n  Tax forfeiture scraper: {count} properties", flush=True)
