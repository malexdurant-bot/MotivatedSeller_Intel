#!/usr/bin/env python3
"""
scraper_foreclosures.py
Pulls Hennepin County mortgage foreclosure notices from public legal notice databases.

SOURCES (in priority order):
  1. mnpublicnotice.com — official MN Newspaper Association public notice DB (free, open)
  2. classifieds.startribune.com/mn/foreclosures — Star Tribune legal classifieds
     (notices include TAX PARCEL NO. — perfect for cross-referencing against tax roll)
  3. Hennepin County Sheriff foreclosure database (backup)

Each notice contains: owner name, property address, TAX PARCEL NO., sale date, amount due.
We extract the parcel ID and cross-reference against the 448K tax roll.

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
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

MAX_RETRIES = 3


def fetch(url, params=None):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            print(f"  HTTP {r.status_code} (attempt {attempt+1}): {url[:80]}", flush=True)
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  Error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 ** attempt)
    return None


def parse_notice_text(text, source_url=''):
    """
    Extract key fields from a MN mortgage foreclosure notice.
    MN notices follow a standard legal format — TAX PARCEL NO. is always present.
    """
    text_up = text.upper()

    # Must be Hennepin County
    if 'HENNEPIN' not in text_up:
        return None

    # Must be a foreclosure notice
    if not any(k in text_up for k in ['FORECLOSURE', 'SHERIFF', 'MORTGAGE']):
        return None

    # --- TAX PARCEL NUMBER (most reliable identifier for tax roll lookup) ---
    pid = ''
    for pattern in [
        r'TAX PARCEL(?:\s+NO\.?|ID|NUMBER)[:\s]+([0-9\-\.]+)',
        r'PARCEL(?:\s+ID|NUMBER|NO\.?)[:\s]+([0-9\-\.]+)',
        r'PROPERTY IDENTIFICATION NUMBER[:\s]+([0-9\-\.]+)',
        r'PID[:\s#]+([0-9\-\.]+)',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            pid = re.sub(r'[\-\.]', '', m.group(1).strip())
            break

    # --- PROPERTY ADDRESS ---
    address = ''
    for pattern in [
        r'(?:ADDRESS OF PROPERTY|PROPERTY ADDRESS|STREET ADDRESS)[:\s]+([^\n]{10,80})',
        r'(?:located at|situated at|premises at)[:\s]+([^\n,]{10,70})',
        r'\b(\d{2,5}\s+[A-Z][A-Za-z\s]{2,25}(?:ST|AVE|RD|DR|LN|BLVD|WAY|CT|PL|TER)[\.,]?\s*(?:N|S|E|W|NE|NW|SE|SW)?)',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            address = m.group(1).strip().upper()
            # Clean up trailing legal description text
            address = re.split(r'\s{2,}|COUNTY|MINNESOTA|MN\s+\d{5}', address)[0].strip()
            break

    # --- OWNER NAME (mortgagor) ---
    owner = ''
    for pattern in [
        r'MORTGAGOR[S]?[:\s]+([A-Z][A-Za-z\s,&\.]+?)(?:MORTGAGEE|,\s+[Aa] |;\s|\n|$)',
        r'Mortgagor[:\s]+([A-Z][A-Za-z\s,&\.]+?)(?:Mortgagee|,\s+a |\n|$)',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            owner = m.group(1).strip().upper()
            owner = re.sub(r'\s+', ' ', owner)[:60]
            break

    # --- SALE DATE ---
    sale_date = ''
    for pattern in [
        r'DATE AND TIME OF SALE[:\s]+([A-Za-z]+ \d{1,2},? \d{4})',
        r'(?:sold|sale) on[:\s]+([A-Za-z]+ \d{1,2},? \d{4})',
        r'(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+202[4-9])',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            sale_date = m.group(1).strip()
            break

    # --- AMOUNT DUE ---
    amount = ''
    for pattern in [
        r'AMOUNT DUE[^:]*:[:\s]+\$?([\d,]+(?:\.\d{2})?)',
        r'AMOUNT CLAIMED[^:]*:[:\s]+\$?([\d,]+(?:\.\d{2})?)',
        r'THE AMOUNT[^:]*:[:\s]+\$?([\d,]+(?:\.\d{2})?)',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            amount = '$' + m.group(1).strip()
            break

    # Need at least address or PID to be useful
    if not address and not pid:
        return None

    return {
        'pid': pid,
        'address': address,
        'owner': owner,
        'sale_date': sale_date,
        'amount_due': amount,
        'county': 'HENNEPIN',
        'signal': 'FORECLOSURE NOTICE',
        'source': source_url,
        'scraped_at': datetime.now().strftime('%Y-%m-%d'),
    }


def scrape_mn_public_notice():
    """
    mnpublicnotice.com — official MN Newspaper Association public notice database.
    Free, open, updated daily. Search for Hennepin County foreclosures.
    """
    records = []
    print("  [mnpublicnotice.com] Searching...", flush=True)

    # Search URL for MN public notices - foreclosure category, Hennepin County
    search_urls = [
        'https://www.mnpublicnotice.com/search/?q=mortgage+foreclosure+sale&county=Hennepin',
        'https://www.mnpublicnotice.com/mn/hennepin/foreclosure/',
        'https://www.mnpublicnotice.com/?q=notice+of+mortgage+foreclosure&county=27',
    ]

    for url in search_urls:
        r = fetch(url)
        if not r:
            continue

        soup = BeautifulSoup(r.text, 'html.parser')

        # Find notice links
        notice_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True)
            if any(k in (href + text).lower() for k in ['foreclosure', 'notice', 'mortgage']):
                if not href.startswith('http'):
                    href = 'https://www.mnpublicnotice.com' + href
                notice_links.append(href)

        # Also try to parse notices directly from the page
        page_text = soup.get_text(' ', strip=True)
        # Split on notice boundaries
        splits = re.split(r'NOTICE OF MORTGAGE FORECLOSURE SALE', page_text, flags=re.I)
        for chunk in splits[1:]:
            record = parse_notice_text('NOTICE OF MORTGAGE FORECLOSURE SALE ' + chunk[:3000])
            if record:
                record['source'] = url
                records.append(record)

        # Fetch individual notice pages
        for link in notice_links[:20]:
            time.sleep(0.5)
            nr = fetch(link)
            if nr:
                notice_soup = BeautifulSoup(nr.text, 'html.parser')
                notice_text = notice_soup.get_text(' ', strip=True)
                record = parse_notice_text(notice_text, source_url=link)
                if record:
                    records.append(record)

        if records:
            break
        time.sleep(1)

    print(f"  [mnpublicnotice.com] {len(records)} notices", flush=True)
    return records


def scrape_star_tribune_classifieds():
    """
    Star Tribune legal classifieds — Hennepin County foreclosure notices.
    URL: classifieds.startribune.com/mn/foreclosures/
    Notices include TAX PARCEL NO. which maps directly to Hennepin tax roll PIDs.
    """
    records = []
    print("  [Star Tribune Classifieds] Searching...", flush=True)

    # The classifieds section (not the main news site) is accessible
    search_urls = [
        'https://classifieds.startribune.com/mn/foreclosures/',
        'https://classifieds.startribune.com/mn/legal-notices/notice-of-mortgage-foreclosure-sale/',
        'https://classifieds.startribune.com/mn/public-notices/notice-of-mortgage-foreclosure-sale/',
    ]

    for url in search_urls:
        r = fetch(url)
        if not r:
            continue

        soup = BeautifulSoup(r.text, 'html.parser')
        page_text = soup.get_text(' ', strip=True)

        # Split on notice boundaries
        splits = re.split(r'NOTICE OF MORTGAGE FORECLOSURE SALE', page_text, flags=re.I)
        for chunk in splits[1:]:
            record = parse_notice_text('NOTICE OF MORTGAGE FORECLOSURE SALE ' + chunk[:4000])
            if record:
                record['source'] = url
                records.append(record)

        # Also find links to individual notices
        notice_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'foreclosure' in href.lower() or 'mortgage' in href.lower():
                if not href.startswith('http'):
                    href = 'https://classifieds.startribune.com' + href
                if href not in notice_links:
                    notice_links.append(href)

        for link in notice_links[:15]:
            time.sleep(0.5)
            nr = fetch(link)
            if nr:
                notice_soup = BeautifulSoup(nr.text, 'html.parser')
                notice_text = notice_soup.get_text(' ', strip=True)
                record = parse_notice_text(notice_text, source_url=link)
                if record:
                    records.append(record)

        if records:
            break
        time.sleep(1)

    print(f"  [Star Tribune Classifieds] {len(records)} notices", flush=True)
    return records


def scrape_hennepin_sheriff():
    """
    Hennepin County Sheriff foreclosure sales database.
    https://foreclosure.hennepin.us — 12 months of sales records.
    """
    records = []
    print("  [Hennepin Sheriff] Checking foreclosure database...", flush=True)

    url = 'https://foreclosure.hennepin.us'
    r = fetch(url)
    if not r:
        print("  [Hennepin Sheriff] Site unavailable", flush=True)
        return records

    soup = BeautifulSoup(r.text, 'html.parser')
    page_text = soup.get_text(' ', strip=True)

    # Parse the sales table if available
    rows = soup.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 3:
            row_text = ' '.join(c.get_text(strip=True) for c in cells)
            record = parse_notice_text(row_text, source_url=url)
            if record:
                records.append(record)

    # Also try to parse the full page text
    if not records:
        splits = re.split(r'NOTICE OF|FORECLOSURE SALE', page_text, flags=re.I)
        for chunk in splits[1:]:
            record = parse_notice_text(chunk[:3000], source_url=url)
            if record:
                records.append(record)

    print(f"  [Hennepin Sheriff] {len(records)} records", flush=True)
    return records


def main():
    print("\n=== SCRAPER: Foreclosure Notices ===", flush=True)
    all_records = []

    # Try all three sources
    all_records.extend(scrape_mn_public_notice())
    time.sleep(2)
    all_records.extend(scrape_star_tribune_classifieds())
    time.sleep(2)
    all_records.extend(scrape_hennepin_sheriff())

    # Deduplicate — by PID first, then address
    seen_pids = set()
    seen_addrs = set()
    deduped = []
    for r in all_records:
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

    print(f"\n  Total unique foreclosure notices: {len(deduped)}", flush=True)
    if deduped:
        print(f"  Sample: {deduped[0].get('address')} | PID: {deduped[0].get('pid')}", flush=True)

    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'mnpublicnotice.com / Star Tribune Classifieds / Hennepin Sheriff',
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
