#!/usr/bin/env python3
"""
push_to_ghl.py
Pushes HOT + WARM leads and any scraper-boosted leads to GoHighLevel (Jarvis).

- Reads from docs/data/leads_enhanced.json (or leads.json as fallback)
- Pushes contacts via GHL API v2
- Skips contacts that already exist (dedupes by address + owner name)
- Tags contacts with: MS_INTEL, score tier, lead type, signal flags
- Runs after run_all.py in the daily GitHub Actions workflow

REQUIRED ENVIRONMENT VARIABLES (set as GitHub Secrets):
  GHL_API_KEY       — Private integration access token (pit-xxxx...)
  GHL_LOCATION_ID   — GHL sub-account / location ID

OUTPUT: docs/data/ghl_push_log.json — log of what was pushed each run
"""
import os
import json
import time
import requests
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
LEADS_FILE  = ROOT / 'docs' / 'data' / 'leads_enhanced.json'
FALLBACK    = ROOT / 'docs' / 'data' / 'leads.json'
PUSH_LOG    = ROOT / 'docs' / 'data' / 'ghl_push_log.json'
PUSHED_IDS  = ROOT / 'docs' / 'data' / 'ghl_pushed_pids.json'

GHL_API_KEY     = os.environ.get('GHL_API_KEY', '')
GHL_LOCATION_ID = os.environ.get('GHL_LOCATION_ID', '')

GHL_BASE    = 'https://services.leadconnectorhq.com'
HEADERS     = {
    'Authorization': f'Bearer {GHL_API_KEY}',
    'Content-Type':  'application/json',
    'Version':       '2021-07-28',
}

# Push leads at or above this score — HOT (80+) + WARM (60+) + boosted
MIN_PUSH_SCORE = 60
MAX_PER_RUN    = 500   # rate limit safety — GHL allows ~100 req/10s
RATE_DELAY     = 0.15  # seconds between API calls


# ── Helpers ──────────────────────────────────────────────────────────────────
def fmt_dollar(n):
    try:
        return f"${int(float(n)):,}"
    except:
        return str(n)


def load_pushed_pids() -> set:
    """Load set of PIDs already pushed to GHL so we don't duplicate."""
    if PUSHED_IDS.exists():
        with open(PUSHED_IDS) as f:
            data = json.load(f)
        return set(data.get('pids', []))
    return set()


def save_pushed_pids(pids: set):
    with open(PUSHED_IDS, 'w') as f:
        json.dump({'pids': list(pids), 'updated': datetime.now().isoformat()}, f)


def build_contact(lead: dict) -> dict:
    """Map a lead record to a GHL contact payload."""
    score     = lead.get('score', 0)
    tier      = 'HOT' if score >= 80 else 'WARM' if score >= 60 else 'WATCH'
    signals   = lead.get('signals', [])
    lead_type = lead.get('lead_type', '')

    # Tags — searchable in GHL
    tags = [
        'MS_INTEL',
        f'Score_{tier}',
        f'Score_{score}',
        lead_type.replace(' ', '_') if lead_type else 'UNCLASSIFIED',
        'Hennepin_MN',
    ]

    # Add signal-based tags
    sig_text = ' '.join(signals).upper()
    if 'DELINQUENT' in sig_text:
        tags.append('Tax_Delinquent')
    if 'ABSENTEE' in sig_text:
        tags.append('Absentee_Owner')
    if 'FORECLOSURE' in sig_text:
        tags.append('Foreclosure_Notice')
    if 'FORFEITURE' in sig_text:
        tags.append('Tax_Forfeiture')
    if 'VIOLATION' in sig_text:
        tags.append('Code_Violations')
    if 'EQUITY' in sig_text:
        tags.append('High_Equity')

    # Parse name — tax roll format is usually LASTNAME FIRSTNAME
    owner = lead.get('owner', '')
    first = lead.get('first_name', '')
    last  = lead.get('last_name', owner)

    # Address
    address  = lead.get('address', '')
    city     = lead.get('city', '')
    state    = lead.get('state', 'MN')
    zip_code = lead.get('zip', '')

    # Mailing address (where owner actually lives — use for skip trace)
    mail_addr = lead.get('mail_address', '')
    mail_city = lead.get('mail_city', '')

    # Build notes field — full signal profile
    notes_lines = [
        f"=== MS INTEL — Motivated Seller Lead ===",
        f"Score: {score}/100 ({tier})",
        f"PID: {lead.get('pid', '')}",
        f"Property: {address}, {city}, {state} {zip_code}",
        f"Mailing: {mail_addr}, {mail_city}",
        f"Market Value{fmt_dollar(lead.get('market_value', 0))}",
        f"Last Sale{fmt_dollar(lead.get('last_sale_price', 0))} ({lead.get('last_sale_year', 'N/A')})",
        f"Est. Equity{fmt_dollar(lead.get('market_value', 0) - lead.get('last_sale_price', 0))}",
        f"Built: {lead.get('build_year', 'N/A')}",
        f"Type: {lead.get('property_type', 'N/A')}",
        f"Homestead: {'Yes' if lead.get('is_homestead') else 'No'}",
        f"",
        f"DISTRESS SIGNALS:",
    ]
    for sig in signals:
        notes_lines.append(f"  • {sig}")

    if lead.get('foreclosure_sale_date'):
        notes_lines.append(f"  Foreclosure Sale Date: {lead['foreclosure_sale_date']}")
    if lead.get('foreclosure_amount'):
        notes_lines.append(f"  Amount Due: {lead['foreclosure_amount']}")

    notes_lines.append(f"\nScraped: {datetime.now().strftime('%Y-%m-%d')}")

    return {
        'locationId':  GHL_LOCATION_ID,
        'firstName':   first or owner.split()[0] if owner else '',
        'lastName':    last,
        'name':        owner,
        'address1':    mail_addr or address,
        'city':        mail_city or city,
        'state':       state,
        'postalCode':  zip_code,
        'country':     'US',
        'tags':        tags,
        'source':      'MS INTEL',
        'customFields': [
            {'key': 'ms_intel_score',        'field_value': str(score)},
            {'key': 'ms_intel_tier',         'field_value': tier},
            {'key': 'ms_intel_pid',          'field_value': lead.get('pid', '')},
            {'key': 'ms_intel_property_addr','field_value': f"{address}, {city}, {state} {zip_code}"},
            {'key': 'ms_intel_market_value', 'field_value': str(lead.get('market_value', 0))},
            {'key': 'ms_intel_equity',       'field_value': str(lead.get('market_value', 0) - lead.get('last_sale_price', 0))},
            {'key': 'ms_intel_signals',      'field_value': ' | '.join(signals)},
            {'key': 'ms_intel_lead_type',    'field_value': lead_type},
        ],
        'notes': '\n'.join(notes_lines),
    }


def contact_exists(address: str, owner: str) -> str | None:
    """
    Search GHL for an existing contact by owner name.
    Returns contact ID if found, None if not.
    """
    if not owner:
        return None

    url = f'{GHL_BASE}/contacts/search/duplicate'
    params = {
        'locationId': GHL_LOCATION_ID,
        'name': owner[:50],
    }
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            contacts = data.get('contacts', [])
            if contacts:
                return contacts[0].get('id')
    except Exception:
        pass
    return None


def push_contact(contact: dict) -> tuple[bool, str]:
    """
    Create or update a contact in GHL.
    Returns (success, contact_id or error message).
    """
    url = f'{GHL_BASE}/contacts/'
    try:
        r = requests.post(url, headers=HEADERS, json=contact, timeout=15)
        if r.status_code in (200, 201):
            data = r.json()
            contact_id = data.get('contact', {}).get('id', '')
            return True, contact_id
        else:
            return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def main():
    print("\n=== GHL PUSH: MS INTEL → Jarvis ===", flush=True)

    if not GHL_API_KEY:
        print("  ERROR: GHL_API_KEY not set. Add it to GitHub Secrets.", flush=True)
        return
    if not GHL_LOCATION_ID:
        print("  ERROR: GHL_LOCATION_ID not set. Add it to GitHub Secrets.", flush=True)
        return

    # Load leads
    leads_path = LEADS_FILE if LEADS_FILE.exists() else FALLBACK
    with open(leads_path) as f:
        data = json.load(f)
    all_leads = data.get('leads', [])
    print(f"  Loaded {len(all_leads):,} leads from {leads_path.name}", flush=True)

    # Load already-pushed PIDs
    pushed_pids = load_pushed_pids()
    print(f"  Already pushed: {len(pushed_pids):,} PIDs", flush=True)

    # Filter: HOT + WARM + boosted by scrapers, not already pushed
    to_push = []
    for lead in all_leads:
        score = lead.get('score', 0)
        pid   = lead.get('pid', '')
        signals = ' '.join(lead.get('signals', [])).upper()

        # Include if HOT/WARM score OR has yellow tier signal
        yellow_boosted = any(k in signals for k in [
            'FORECLOSURE', 'FORFEITURE', 'VIOLATION'
        ])

        if (score >= MIN_PUSH_SCORE or yellow_boosted) and pid not in pushed_pids:
            to_push.append(lead)

    print(f"  Eligible to push: {len(to_push):,}", flush=True)

    # Cap per run to avoid rate limits
    if len(to_push) > MAX_PER_RUN:
        # Prioritize by score descending
        to_push = sorted(to_push, key=lambda x: x.get('score', 0), reverse=True)[:MAX_PER_RUN]
        print(f"  Capped to top {MAX_PER_RUN} for this run", flush=True)

    # Push to GHL
    pushed_ok  = []
    pushed_err = []

    for i, lead in enumerate(to_push):
        contact = build_contact(lead)
        success, result = push_contact(contact)

        if success:
            pushed_ok.append({'pid': lead.get('pid'), 'owner': lead.get('owner'), 'contact_id': result})
            pushed_pids.add(lead.get('pid', ''))
            if (i + 1) % 50 == 0:
                print(f"  Pushed {i+1}/{len(to_push)}...", flush=True)
        else:
            pushed_err.append({'pid': lead.get('pid'), 'owner': lead.get('owner'), 'error': result})

        time.sleep(RATE_DELAY)

    # Save updated pushed PIDs
    save_pushed_pids(pushed_pids)

    print(f"\n  ✅ Pushed: {len(pushed_ok):,}", flush=True)
    print(f"  ❌ Errors: {len(pushed_err):,}", flush=True)
    if pushed_err:
        for e in pushed_err[:5]:
            print(f"     {e['owner']}: {e['error']}", flush=True)

    # Write push log
    log = {
        'run_at':      datetime.now().isoformat(),
        'pushed_count': len(pushed_ok),
        'error_count':  len(pushed_err),
        'total_pushed_all_time': len(pushed_pids),
        'pushed': pushed_ok,
        'errors': pushed_err[:20],
    }
    with open(PUSH_LOG, 'w') as f:
        json.dump(log, f, separators=(',', ':'))
    print(f"  Log written: {PUSH_LOG}", flush=True)


if __name__ == '__main__':
    main()
