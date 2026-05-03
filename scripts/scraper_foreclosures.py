#!/usr/bin/env python3
"""
scraper_foreclosures.py
Scrapes mortgage foreclosure sale notices published in Minnesota legal newspapers.

Sources (in order of reliability):
  1. Finance & Commerce — MN's official legal newspaper for Hennepin County
  2. Star Tribune legal notices (backup)
  3. MN Court Records public notice search (backup)

Outputs: docs/data/foreclosures.json

YELLOW tier — retry logic handles rate limits and layout changes.
If zero records returned, check the URL and CSS selectors below.
"""
import requests
from bs4 import BeautifulSoup
import json
import re
import time
from pathlib import Path
from datetime import datetime, timedelta

OUTPUT = Path(__file__).parent.parent / 'docs' / 'data' / 'foreclosures.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds


def fetch_with_retry(url: str, params: dict = None) -> requests.Response | None:
    """Fetch a URL with exponential backoff retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = RETRY_DELAY * (2 ** attempt)
                print(f"  Rate limited. Waiting {wait}s...", flush=True)
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code} on attempt {attempt+1}", flush=True)
                time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            print(f"  Request failed (attempt {attempt+1}): {e}", flush=True)
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


def scrape_finance_commerce() -> list:
    """
    Finance & Commerce publishes Hennepin County mortgage foreclosure notices.
    URL: https://finance-commerce.com/public-notices/
    Search for 'Notice of Mortgage Foreclosure Sale' in Hennepin County.
    """
    records = []
    print("  [Finance & Commerce] Fetching legal notices...", flush=True)

    base_url = 'https://finance-commerce.com/public-notices/'
    search_url = 'https://finance-commerce.com/?s=Notice+of+Mortgage+Foreclosure+Sale+Hennepin'

    r = fetch_with_retry(search_url)
    if not r:
        print("  [Finance & Commerce] Failed to fetch. Skipping.", flush=True)
        return records

    soup = BeautifulSoup(r.text, 'html.parser')

    # Finance & Commerce search results — article links
    articles = soup.find_all('article') or soup.find_all('div', class_='search-result')

    for article in articles[:20]:
        link_tag = article.find('a', href=True)
        if not link_tag:
            continue
        title = link_tag.get_text(strip=True)
        href = link_tag['href']

        if 'foreclosure' not in title.lower() and 'mortgage' not in title.lower():
            continue

        # Fetch the notice page
        notice_r = fetch_with_retry(href)
        if not notice_r:
            continue

        notice_soup = BeautifulSoup(notice_r.text, 'html.parser')
        body = notice_soup.get_text(separator=' ', strip=True)

        record = parse_foreclosure_text(body, source='Finance & Commerce', url=href)
        if record:
            records.append(record)

        time.sleep(1)  # Be polite

    print(f"  [Finance & Commerce] Parsed {len(records)} notices", flush=True)
    return records


def scrape_star_tribune() -> list:
    """
    Star Tribune legal notices — backup source.
    URL: https://www.startribune.com/legal-notices
    """
    records = []
    print("  [Star Tribune] Fetching legal notices...", flush=True)

    url = 'https://www.startribune.com/legal-notices'
    search_url = f'{url}?query=Notice+of+Mortgage+Foreclosure+Sale&county=Hennepin'

    r = fetch_with_retry(search_url)
    if not r:
        print("  [Star Tribune] Failed to fetch. Skipping.", flush=True)
        return records

    soup = BeautifulSoup(r.text, 'html.parser')

    # Star Tribune legal notices are in notice cards / list items
    notice_items = (
        soup.find_all('div', class_='notice-item') or
        soup.find_all('li', class_='legal-notice') or
        soup.find_all('article')
    )

    for item in notice_items[:30]:
        text = item.get_text(separator=' ', strip=True)
        if 'foreclosure' not in text.lower():
            continue

        record = parse_foreclosure_text(text, source='Star Tribune', url=url)
        if record:
            records.append(record)

    print(f"  [Star Tribune] Parsed {len(records)} notices", flush=True)
    return records


def parse_foreclosure_text(text: str, source: str, url: str) -> dict | None:
    """
    Extract key fields from a mortgage foreclosure notice text.
    MN notices follow a standard legal format.
    """
    text_upper = text.upper()

    # Must be Hennepin County
    if 'HENNEPIN' not in text_upper:
        return None

    # Must be a foreclosure sale notice
    if 'FORECLOSURE' not in text_upper and 'MORTGAGE' not in text_upper:
        return None

    # Extract property address — look for common patterns
    address = ''
    addr_patterns = [
        r'\b(\d{2,5}\s+[A-Z][A-Za-z\s]+(Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Blvd|Boulevard|Way|Court|Ct|Place|Pl)[\.,]?\s*(?:N|S|E|W|NE|NW|SE|SW)?)',
        r'property(?:\s+address)?[:\s]+([^,\n]{10,60})',
        r'located at[:\s]+([^,\n]{10,60})',
        r'premises at[:\s]+([^,\n]{10,60})',
    ]
    for pattern in addr_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            address = match.group(1).strip()
            break

    # Extract sale date
    sale_date = ''
    date_patterns = [
        r'sale(?:\s+date)?[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
        r'(\w+ \d{1,2}, \d{4})',
        r'(\d{1,2}/\d{1,2}/\d{4})',
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            sale_date = match.group(1).strip()
            break

    # Extract mortgagor (owner) name
    owner = ''
    owner_patterns = [
        r'mortgagor[:\s]+([A-Z][A-Za-z\s,]+?)(?:,|\.|and)',
        r'owner[:\s]+([A-Z][A-Za-z\s,]+?)(?:,|\.|and)',
    ]
    for pattern in owner_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            owner = match.group(1).strip()
            break

    # Extract original loan amount
    amount = ''
    amt_match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
    if amt_match:
        amount = amt_match.group(0)

    # Skip if we couldn't extract an address
    if not address and not owner:
        return None

    return {
        'address': address.upper(),
        'owner': owner.upper(),
        'sale_date': sale_date,
        'original_amount': amount,
        'source': source,
        'url': url,
        'scraped_at': datetime.now().strftime('%Y-%m-%d'),
        'county': 'HENNEPIN',
        'signal': 'FORECLOSURE NOTICE',
    }


def main():
    print("\n=== SCRAPER: Foreclosure Notices ===", flush=True)
    all_records = []

    # Try Finance & Commerce first
    fc_records = scrape_finance_commerce()
    all_records.extend(fc_records)

    # Backup: Star Tribune
    if len(all_records) < 5:
        print("  Finance & Commerce returned few results, trying Star Tribune...", flush=True)
        st_records = scrape_star_tribune()
        all_records.extend(st_records)

    # Deduplicate by address
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
        'source': 'Finance & Commerce / Star Tribune Legal Notices',
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
    print(f"\n  Foreclosure scraper complete: {count} records", flush=True)
    if count == 0:
        print("  WARNING: Zero records. Legal notice sites may have changed layout.", flush=True)
        print("  To debug: open scraper_foreclosures.py and check the CSS selectors.", flush=True)
        # Don't exit 1 — foreclosure scraper is fragile, zero is non-fatal
