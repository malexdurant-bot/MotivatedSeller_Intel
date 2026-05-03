#!/usr/bin/env python3
"""
run_all.py
Runs all YELLOW tier scrapers, then cross-references results against the
FULL 448K tax roll to get complete owner data.
Only leads already in the scored 15K get boosted — the output set stays the same.

Usage:
  python scripts/run_all.py

GitHub Actions runs this every morning at 7am UTC.
"""
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
LEADS_JSON       = ROOT / 'docs' / 'data' / 'leads.json'
VIOLATIONS_JSON  = ROOT / 'docs' / 'data' / 'code_violations.json'
FORECLOSURES_JSON= ROOT / 'docs' / 'data' / 'foreclosures.json'
FORFEITURE_JSON  = ROOT / 'docs' / 'data' / 'tax_forfeiture.json'
ENHANCED_JSON    = ROOT / 'docs' / 'data' / 'leads_enhanced.json'

# Full tax roll — owner lookup when a scraper finds a signal address.
# Only the 15K scored leads appear in output — this just enriches their data.
TAX_ROLL_CSV  = ROOT / 'data_raw' / 'County_Parcels.csv'
TAX_ROLL_COLS = [
    'PID_TEXT', 'HOUSE_NO', 'STREET_NM', 'MUNIC_NM', 'ZIP_CD',
    'OWNER_NM', 'TAXPAYER_NM', 'TAXPAYER_NM_1', 'MAILING_MUNIC_NM',
    'MKT_VAL_TOT', 'SALE_PRICE', 'SALE_DATE', 'BUILD_YR',
    'HMSTD_CD1', 'EARLIEST_DELQ_YR', 'LAT', 'LON',
]

SCRAPERS = [
    ('Code Violations', 'scripts/scraper_code_violations.py'),
    ('Foreclosures',    'scripts/scraper_foreclosures.py'),
    ('Tax Forfeiture',  'scripts/scraper_tax_forfeiture.py'),
]


def run_scraper(name: str, script: str) -> bool:
    print(f"\n{'='*50}", flush=True)
    print(f"Running: {name}", flush=True)
    print(f"{'='*50}", flush=True)
    result = subprocess.run(
        [sys.executable, str(ROOT / script)],
        capture_output=False,
        text=True,
    )
    if result.returncode == 0:
        print(f"\n✅ {name} completed", flush=True)
        return True
    else:
        print(f"\n⚠️  {name} exit code {result.returncode}", flush=True)
        return False


def normalize_address(addr: str) -> str:
    """Normalize address for fuzzy matching."""
    if not addr:
        return ''
    addr = str(addr).upper().strip()
    addr = addr.replace('.', '').replace(',', '')
    for long, short in [
        (' STREET',' ST'), (' AVENUE',' AVE'), (' ROAD',' RD'),
        (' DRIVE',' DR'), (' BOULEVARD',' BLVD'), (' LANE',' LN'),
        (' COURT',' CT'), (' PLACE',' PL'), (' NORTH',' N'),
        (' SOUTH',' S'), (' EAST',' E'), (' WEST',' W'),
    ]:
        addr = addr.replace(long, short)
    while '  ' in addr:
        addr = addr.replace('  ', ' ')
    return addr.strip()


def build_tax_roll_index() -> dict:
    """
    Load the full 448K tax roll and build two indexes:
      address_index: normalized_address -> list of parcel dicts
      pid_index:     pid_text -> parcel dict

    This lets scrapers look up complete owner info from just an address or PID.
    Only runs if the CSV is present (skipped in GitHub Actions — no CSV there).
    """
    if not TAX_ROLL_CSV.exists():
        print(f"  Tax roll CSV not found at {TAX_ROLL_CSV}", flush=True)
        print("  Skipping full-roll lookup (GitHub Actions mode)", flush=True)
        return {'address': {}, 'pid': {}}

    try:
        import pandas as pd
    except ImportError:
        print("  pandas not installed — skipping full-roll index", flush=True)
        return {'address': {}, 'pid': {}}

    print(f"\n  Building full tax roll index ({TAX_ROLL_CSV.name})...", flush=True)
    df = pd.read_csv(
        TAX_ROLL_CSV,
        usecols=TAX_ROLL_COLS,
        low_memory=False,
        encoding='utf-8-sig',
        dtype=str,
    )
    print(f"  Loaded {len(df):,} parcels", flush=True)

    address_index = {}
    pid_index = {}

    for _, row in df.iterrows():
        house = str(row.get('HOUSE_NO', '') or '').strip().split('.')[0]
        street = str(row.get('STREET_NM', '') or '').strip()
        raw_addr = f"{house} {street}".strip()
        norm_addr = normalize_address(raw_addr)
        pid = str(row.get('PID_TEXT', '') or '').strip()

        record = {
            'pid':          pid,
            'owner':        str(row.get('OWNER_NM', '') or '').strip(),
            'taxpayer':     str(row.get('TAXPAYER_NM', '') or '').strip(),
            'mail_address': str(row.get('TAXPAYER_NM_1', '') or '').strip(),
            'mail_city':    str(row.get('MAILING_MUNIC_NM', '') or '').strip(),
            'address':      raw_addr.upper(),
            'city':         str(row.get('MUNIC_NM', '') or '').strip(),
            'zip':          str(row.get('ZIP_CD', '') or '').strip().split('.')[0],
            'market_value': str(row.get('MKT_VAL_TOT', '') or '').strip(),
            'build_year':   str(row.get('BUILD_YR', '') or '').strip().split('.')[0],
            'is_homestead': str(row.get('HMSTD_CD1', '') or '').strip().upper() == 'Y',
            'lat':          str(row.get('LAT', '') or '').strip(),
            'lon':          str(row.get('LON', '') or '').strip(),
        }

        if norm_addr:
            if norm_addr not in address_index:
                address_index[norm_addr] = []
            address_index[norm_addr].append(record)

        if pid:
            pid_index[pid] = record

    print(f"  Address index: {len(address_index):,} unique addresses", flush=True)
    print(f"  PID index:     {len(pid_index):,} unique PIDs", flush=True)
    return {'address': address_index, 'pid': pid_index}


def cross_reference_leads(tax_roll_index: dict):
    """
    For each of the 15K scored leads:
      1. Check if the address appears in any YELLOW tier scraper output
      2. If yes, boost score + add signal
      3. Enrich with full owner data from the tax roll index if available

    Output set stays exactly the same 15K leads.
    """
    print(f"\n{'='*50}", flush=True)
    print("Cross-referencing leads...", flush=True)
    print(f"{'='*50}", flush=True)

    if not LEADS_JSON.exists():
        print("  ERROR: leads.json not found. Run process_parcels.py first.", flush=True)
        return

    with open(LEADS_JSON) as f:
        leads_data = json.load(f)

    leads = leads_data.get('leads', [])
    print(f"  Base leads loaded: {len(leads):,}", flush=True)

    # Build PID set from the 15K for fast lookup
    scored_pids = {L.get('pid', '') for L in leads}

    # Load YELLOW tier signal data — index by both PID and address.
    # PID match is exact and preferred. Address match is fallback.
    violations_by_addr = {}
    violations_by_pid = {}
    foreclosure_addrs = set()
    foreclosure_pids = set()
    foreclosure_by_pid = {}
    forfeiture_addrs = set()
    forfeiture_pids = set()
    forfeiture_by_addr = {}
    forfeiture_by_pid = {}

    def normalize_pid(p):
        """Strip non-digits — Hennepin PIDs are pure numeric."""
        if not p:
            return ''
        return ''.join(ch for ch in str(p) if ch.isdigit())

    if VIOLATIONS_JSON.exists():
        with open(VIOLATIONS_JSON) as f:
            v = json.load(f)
        for item in v.get('violations', []):
            norm = normalize_address(item.get('address', ''))
            pid = normalize_pid(item.get('pid', ''))
            if norm:
                violations_by_addr.setdefault(norm, []).append(item)
            if pid:
                violations_by_pid.setdefault(pid, []).append(item)
        print(f"  Violations index: {len(violations_by_addr):,} addresses, {len(violations_by_pid):,} PIDs", flush=True)

    if FORECLOSURES_JSON.exists():
        with open(FORECLOSURES_JSON) as f:
            fc = json.load(f)
        for notice in fc.get('notices', []):
            # Skip non-Hennepin notices that snuck through search
            if notice.get('county', '').upper() != 'HENNEPIN':
                continue
            norm = normalize_address(notice.get('address', ''))
            pid = normalize_pid(notice.get('pid', ''))
            if norm:
                foreclosure_addrs.add(norm)
            if pid:
                foreclosure_pids.add(pid)
                foreclosure_by_pid[pid] = notice
        print(f"  Foreclosure notices: {len(foreclosure_addrs):,} addresses, {len(foreclosure_pids):,} PIDs", flush=True)

    if FORFEITURE_JSON.exists():
        with open(FORFEITURE_JSON) as f:
            tf = json.load(f)
        for prop in tf.get('properties', []):
            norm = normalize_address(prop.get('address', ''))
            pid = normalize_pid(prop.get('pid', ''))
            if norm:
                forfeiture_addrs.add(norm)
                forfeiture_by_addr[norm] = prop
            if pid:
                forfeiture_pids.add(pid)
                forfeiture_by_pid[pid] = prop
        print(f"  Forfeiture list: {len(forfeiture_addrs):,} addresses, {len(forfeiture_pids):,} PIDs", flush=True)

    # -------------------------------------------------------
    # FULL TAX ROLL LOOKUP
    # For any scraper signal address NOT already in the 15K,
    # check if it's in the full roll — log it for future scoring runs.
    # (These don't appear in output but help us know what we're missing.)
    # -------------------------------------------------------
    addr_idx = tax_roll_index.get('address', {})
    pid_idx  = tax_roll_index.get('pid', {})
    new_candidates = []  # signals on parcels outside the 15K

    if addr_idx:
        all_signal_addrs = (
            set(violations_by_addr.keys()) |
            foreclosure_addrs |
            forfeiture_addrs
        )
        for norm_addr in all_signal_addrs:
            matches = addr_idx.get(norm_addr, [])
            for parcel in matches:
                pid = parcel.get('pid', '')
                if pid and pid not in scored_pids:
                    new_candidates.append({
                        'pid':     pid,
                        'address': parcel.get('address', ''),
                        'owner':   parcel.get('owner', ''),
                        'city':    parcel.get('city', ''),
                        'zip':     parcel.get('zip', ''),
                        'mail_address': parcel.get('mail_address', ''),
                        'mail_city':    parcel.get('mail_city', ''),
                        'market_value': parcel.get('market_value', ''),
                        'signal_source': (
                            'CODE_VIOLATION' if norm_addr in violations_by_addr else
                            'FORECLOSURE' if norm_addr in foreclosure_addrs else
                            'TAX_FORFEITURE'
                        ),
                    })

        if new_candidates:
            print(f"\n  🔍 Found {len(new_candidates)} NEW signal hits outside the 15K", flush=True)
            print("     These will be included in next process_parcels.py run", flush=True)
            # Write them out so you can review
            new_cands_path = ROOT / 'docs' / 'data' / 'new_signal_candidates.json'
            with open(new_cands_path, 'w') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'note': 'Parcels with YELLOW tier signals not in current 15K leads. Re-run process_parcels.py to incorporate.',
                    'count': len(new_candidates),
                    'candidates': new_candidates,
                }, f, separators=(',', ':'))
            print(f"     Written: {new_cands_path}", flush=True)

    # -------------------------------------------------------
    # BOOST SCORES for leads already in the 15K
    # -------------------------------------------------------
    enhanced_count = 0
    pid_match_count = 0
    addr_match_count = 0

    for lead in leads:
        lead_addr = normalize_address(lead.get('address', ''))
        lead_pid = normalize_pid(lead.get('pid', ''))
        new_signals = list(lead.get('signals', []))
        bonus = 0
        matched_by = []

        # Enrich mailing data from full roll if available
        if pid_idx:
            parcel = pid_idx.get(lead.get('pid', ''))
            if parcel and not lead.get('mail_address'):
                lead['mail_address'] = parcel.get('mail_address', '')
                lead['mail_city']    = parcel.get('mail_city', '')

        # --- Code violations: PID match first, then address ---
        v_match = None
        if lead_pid and lead_pid in violations_by_pid:
            v_match = violations_by_pid[lead_pid]
            matched_by.append('PID')
        elif lead_addr in violations_by_addr:
            v_match = violations_by_addr[lead_addr]
            matched_by.append('addr')
        if v_match:
            count = len(v_match)
            if count >= 3:
                bonus += 25
                new_signals.append(f"🔴 {count} open code violations")
            else:
                bonus += 15
                new_signals.append(f"🟡 {count} code violation(s)")

        # --- Foreclosure: PID match first, then address ---
        fc_matched = False
        if lead_pid and lead_pid in foreclosure_pids:
            fc_matched = True
            pid_match_count += 1
        elif lead_addr in foreclosure_addrs:
            fc_matched = True
            addr_match_count += 1
        if fc_matched:
            bonus += 30
            new_signals.append("⚡ FORECLOSURE NOTICE FILED")
            # Pull notice details
            fc_rec = foreclosure_by_pid.get(lead_pid, {})
            if fc_rec.get('sale_date'):
                lead['foreclosure_sale_date'] = fc_rec['sale_date']
            if fc_rec.get('amount_due'):
                lead['foreclosure_amount'] = fc_rec['amount_due']

        # --- Tax forfeiture: PID match first, then address ---
        tf_matched = False
        tf_rec = {}
        if lead_pid and lead_pid in forfeiture_pids:
            tf_matched = True
            tf_rec = forfeiture_by_pid.get(lead_pid, {})
        elif lead_addr in forfeiture_addrs:
            tf_matched = True
            tf_rec = forfeiture_by_addr.get(lead_addr, {})
        if tf_matched:
            bonus += 35
            new_signals.append("🔥 ON TAX FORFEITURE LIST")
            if tf_rec.get('appraised_value'):
                lead['forfeiture_appraised'] = tf_rec['appraised_value']

        if bonus > 0:
            lead['score'] = min(100, lead['score'] + bonus)
            lead['signals'] = new_signals
            enhanced_count += 1

    # Re-sort by new scores
    leads.sort(key=lambda x: x['score'], reverse=True)

    hot   = sum(1 for L in leads if L['score'] >= 80)
    warm  = sum(1 for L in leads if 60 <= L['score'] < 80)
    watch = sum(1 for L in leads if L['score'] < 60)

    print(f"\n  Leads boosted by YELLOW signals: {enhanced_count:,}", flush=True)
    print(f"    PID matches:     {pid_match_count:,}  (exact)", flush=True)
    print(f"    Address matches: {addr_match_count:,}  (fuzzy)", flush=True)
    print(f"  HOT: {hot:,} | WARM: {warm:,} | WATCH: {watch:,}", flush=True)

    enhanced_data = {
        **leads_data,
        'generated_at':   datetime.now().isoformat(),
        'enhanced_at':    datetime.now().isoformat(),
        'enhanced_count': enhanced_count,
        'new_signal_candidates': len(new_candidates),
        'yellow_tier_active': {
            'code_violations': VIOLATIONS_JSON.exists(),
            'foreclosures':    FORECLOSURES_JSON.exists(),
            'tax_forfeiture':  FORFEITURE_JSON.exists(),
        },
        'hot_leads':   hot,
        'warm_leads':  warm,
        'watch_leads': watch,
        'leads':       leads,
    }

    with open(ENHANCED_JSON, 'w') as f:
        json.dump(enhanced_data, f, separators=(',', ':'))

    size_mb = ENHANCED_JSON.stat().st_size / 1024 / 1024
    print(f"  Written: {ENHANCED_JSON} ({size_mb:.1f} MB)", flush=True)


def main():
    print(f"\n{'#'*50}", flush=True)
    print("MS INTEL — YELLOW TIER SCRAPER RUN", flush=True)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}", flush=True)
    print(f"{'#'*50}", flush=True)

    # Build full tax roll index first (local only — skipped in Actions)
    tax_roll_index = build_tax_roll_index()

    # Run all scrapers
    results = {}
    for name, script in SCRAPERS:
        results[name] = run_scraper(name, script)

    # Cross-reference against full roll + boost 15K leads
    cross_reference_leads(tax_roll_index)

    # Summary
    print(f"\n{'#'*50}", flush=True)
    print("SUMMARY", flush=True)
    for name, ok in results.items():
        print(f"  {name}: {'✅' if ok else '⚠️ '}", flush=True)
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}", flush=True)


if __name__ == '__main__':
    main()
