#!/usr/bin/env python3
"""
process_parcels.py - Score Hennepin County parcels for motivated seller signals.

INPUT:  ../data/County_Parcels.csv (Hennepin County Open Data tax roll, ~448K rows)
OUTPUT: ../data/leads.json  (top 15K scored leads, ~10-15MB)

SCORING (0-100):
  +30  Tax delinquent (EARLIEST_DELQ_YR populated)
  +15  Confession of judgment / payment plan (COMP_JUDG_IND = Y)
  +20  Absentee owner (mailing address ≠ property address)
  +15  Non-homestead (HMSTD_CD1 = N) — investor / rental
  +15  Long-term hold (sale > 20 years ago OR no sale on record)
  +15  High equity (market value > 2x last sale price, sale within 30y)
  +10  Aged property (built before 1960)
  +10  Penalty paid recently (TOT_PENALTY_PD > 0)
  +5   Vacant land or low improvement ratio (BLDG_MV / TOTAL_MV < 0.2)

Capped at 100. Filters to RESIDENTIAL property types only.
"""
import pandas as pd
import numpy as np
import json
import sys
from pathlib import Path

INPUT_CSV  = Path(__file__).parent.parent / 'data_raw' / 'County_Parcels.csv'
OUTPUT_JSON = Path(__file__).parent.parent / 'docs' / 'data' / 'leads.json'

# No cap — all parcels scoring at or above this threshold are included.
# Raise to tighten the list, lower to widen it.
MIN_SCORE = 20


# Residential property types we care about
RESIDENTIAL_TYPES = {
    'RESIDENTIAL',
    'RESIDENTIAL-NON HMSTD',
    'RESIDENTIAL HOMESTEAD',
    'RES NON-HOMESTEAD',
    'APARTMENT',
    'APARTMENT-LOW INCOME',
    'RESIDENTIAL LAKESHORE',
    'DOUBLE BUNGALOW',
    'TRIPLEX',
    'FOURPLEX',
    'CONDOMINIUM',
    'TOWNHOUSE',
}

# Columns we actually need (saves memory loading 121-col CSV)
USE_COLS = [
    'PID_TEXT', 'HOUSE_NO', 'STREET_NM', 'MAILING_MUNIC_NM', 'ZIP_CD',
    'OWNER_NM', 'TAXPAYER_NM', 'TAXPAYER_NM_1', 'MUNIC_NM', 'BUILD_YR',
    'SALE_DATE', 'SALE_PRICE', 'PARCEL_AREA', 'MKT_VAL_TOT',
    'TOT_NET_TAX', 'NET_TAX_PD', 'TOT_PENALTY_PD', 'EARLIEST_DELQ_YR',
    'COMP_JUDG_IND', 'PR_TYP_NM1', 'HMSTD_CD1', 'LAND_MV1', 'BLDG_MV1',
    'TOTAL_MV1', 'LAT', 'LON',
]


def clean_str(x):
    if pd.isna(x):
        return ''
    return str(x).strip()


def is_absentee(taxpayer_street, prop_house_no, prop_street):
    """True if taxpayer mailing address differs from property address."""
    tp = clean_str(taxpayer_street).upper()
    if not tp:
        return False
    # Property address as it would appear on a mailing label
    prop_house = clean_str(prop_house_no).split('.')[0]  # "2901.0" -> "2901"
    prop_st = clean_str(prop_street).upper()
    if prop_house and prop_house in tp and prop_st and prop_st.split()[0] in tp:
        return False
    return True


def score_row(r):
    score = 0
    signals = []

    # Tax delinquency (strongest signal)
    delq_yr = clean_str(r['EARLIEST_DELQ_YR'])
    if delq_yr and delq_yr != '0':
        score += 30
        # Hennepin stores 2-digit years; convert to 4-digit
        try:
            yy = int(delq_yr)
            full_yr = 2000 + yy if yy < 50 else 1900 + yy
            signals.append(f"Tax delinquent since {full_yr}")
        except ValueError:
            signals.append("Tax delinquent")

    # Confession of judgment (on payment plan = financial stress)
    if clean_str(r['COMP_JUDG_IND']).upper() == 'Y':
        score += 15
        signals.append("Confession of judgment filed")

    # Absentee owner
    if is_absentee(r['TAXPAYER_NM_1'], r['HOUSE_NO'], r['STREET_NM']):
        score += 20
        signals.append("Absentee owner")

    # Non-homestead (investor / rental / second home)
    if clean_str(r['HMSTD_CD1']).upper() == 'N':
        score += 15
        signals.append("Non-homestead")

    # Long-term hold
    sale_date = clean_str(r['SALE_DATE'])
    sale_year = None
    if sale_date and sale_date.isdigit() and len(sale_date) >= 4:
        sale_year = int(sale_date[:4])
    if sale_year is None:
        score += 15
        signals.append("No recent sale on record")
    elif sale_year < 2005:
        score += 15
        signals.append(f"Long-term hold (since {sale_year})")

    # High equity (current market value much higher than purchase price)
    try:
        mv = float(r['MKT_VAL_TOT']) if not pd.isna(r['MKT_VAL_TOT']) else 0
        sp = float(r['SALE_PRICE']) if not pd.isna(r['SALE_PRICE']) else 0
        if sale_year and sale_year >= 1995 and sp > 10000 and mv > sp * 2:
            score += 15
            equity_pct = int(((mv - sp) / mv) * 100)
            signals.append(f"~{equity_pct}% equity gain")
    except (ValueError, TypeError):
        pass

    # Aged property
    try:
        by = int(r['BUILD_YR']) if not pd.isna(r['BUILD_YR']) else 0
        if 1800 < by < 1960:
            score += 10
            signals.append(f"Built {by}")
    except (ValueError, TypeError):
        pass

    # Recent penalty paid (financial distress in last cycle)
    try:
        pen = float(r['TOT_PENALTY_PD']) if not pd.isna(r['TOT_PENALTY_PD']) else 0
        if pen > 0:
            score += 10
            signals.append(f"Penalty paid: ${pen:,.0f}")
    except (ValueError, TypeError):
        pass

    # Low improvement ratio (vacant land, tear-down, or improvement opportunity)
    try:
        bldg = float(r['BLDG_MV1']) if not pd.isna(r['BLDG_MV1']) else 0
        total = float(r['TOTAL_MV1']) if not pd.isna(r['TOTAL_MV1']) else 0
        if total > 0 and bldg / total < 0.2:
            score += 5
            signals.append("Low improvement ratio")
    except (ValueError, TypeError, ZeroDivisionError):
        pass

    return min(score, 100), signals


def classify_lead_type(signals):
    """Pick a primary lead type for display."""
    sig_text = ' '.join(signals).lower()
    if 'delinquent' in sig_text or 'judgment' in sig_text:
        return 'TAX DELINQUENT'
    if 'absentee' in sig_text:
        return 'ABSENTEE OWNER'
    if 'long-term hold' in sig_text or 'no recent sale' in sig_text:
        return 'LONG-TERM HOLD'
    if 'equity' in sig_text:
        return 'HIGH EQUITY'
    if 'built' in sig_text:
        return 'AGED PROPERTY'
    return 'WATCH LIST'


def main():
    print(f"Reading {INPUT_CSV} ...")
    df = pd.read_csv(
        INPUT_CSV,
        usecols=USE_COLS,
        low_memory=False,
        encoding='utf-8-sig',
        dtype=str,  # read everything as string, convert as needed
    )
    print(f"  Loaded {len(df):,} parcels with {len(df.columns)} columns")

    # Filter to residential
    df['_pr_type'] = df['PR_TYP_NM1'].apply(clean_str).str.upper()
    df = df[df['_pr_type'].isin(RESIDENTIAL_TYPES)].copy()
    print(f"  After residential filter: {len(df):,} parcels")

    # Drop parcels with no street address (no point sending mail)
    df = df[df['STREET_NM'].apply(clean_str) != ''].copy()
    df = df[df['HOUSE_NO'].apply(clean_str) != ''].copy()
    print(f"  After address filter: {len(df):,} parcels")

    # Convert numeric columns
    for col in ['MKT_VAL_TOT', 'SALE_PRICE', 'TOT_PENALTY_PD',
                'BLDG_MV1', 'TOTAL_MV1', 'LAND_MV1', 'BUILD_YR',
                'TOT_NET_TAX', 'NET_TAX_PD', 'PARCEL_AREA',
                'LAT', 'LON']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Score every row
    print("Scoring parcels...")
    scores = []
    signals_list = []
    for i, r in enumerate(df.itertuples(index=False)):
        if i % 50000 == 0 and i > 0:
            print(f"  ... scored {i:,}")
        row_dict = {col: getattr(r, col) for col in df.columns if col != '_pr_type'}
        s, sig = score_row(row_dict)
        scores.append(s)
        signals_list.append(sig)
    df['_score'] = scores
    df['_signals'] = signals_list
    print(f"  Done. Score distribution:")
    print(df['_score'].describe())

    # Filter by minimum score — no cap, all qualifying parcels included
    df = df[df['_score'] >= MIN_SCORE].sort_values('_score', ascending=False).reset_index(drop=True)
    print(f"  Leads at score >= {MIN_SCORE}: {len(df):,} (min score in set: {df['_score'].min() if len(df) else 'N/A'})")

    # Deduplicate by PID — the county's unique parcel identifier
    # Drop any parcel that appears more than once (data entry artefacts)
    before = len(df)
    df = df.drop_duplicates(subset=['PID_TEXT'], keep='first').reset_index(drop=True)
    dupes_removed = before - len(df)
    if dupes_removed:
        print(f"  Removed {dupes_removed:,} duplicate PIDs")
    else:
        print(f"  No duplicate PIDs found")

    # Build output records
    records = []
    for _, r in df.iterrows():
        house_no = clean_str(r['HOUSE_NO']).split('.')[0]
        street = clean_str(r['STREET_NM'])
        city = clean_str(r['MUNIC_NM'])
        zip_cd = clean_str(r['ZIP_CD']).split('.')[0]
        owner_full = clean_str(r['OWNER_NM'])

        # Split owner into first/last for GHL (best-effort)
        first_name, last_name = '', owner_full
        parts = owner_full.split()
        if len(parts) >= 2 and 'LLC' not in owner_full and 'INC' not in owner_full and 'TRUST' not in owner_full:
            # Format is usually LASTNAME FIRSTNAME or LASTNAME FIRSTNAME M
            last_name = parts[0]
            first_name = ' '.join(parts[1:])

        # Mailing address (where to send postcard if absentee)
        mail_street = clean_str(r['TAXPAYER_NM_1'])
        mail_city = clean_str(r['MAILING_MUNIC_NM'])

        sale_date_raw = clean_str(r['SALE_DATE'])
        sale_year = None
        if sale_date_raw.isdigit() and len(sale_date_raw) >= 4:
            sale_year = int(sale_date_raw[:4])

        rec = {
            'pid': clean_str(r['PID_TEXT']),
            'owner': owner_full,
            'first_name': first_name,
            'last_name': last_name,
            'address': f"{house_no} {street}".strip(),
            'city': city,
            'state': 'MN',
            'zip': zip_cd,
            'mail_address': mail_street,
            'mail_city': mail_city,
            'property_type': clean_str(r['PR_TYP_NM1']),
            'build_year': int(r['BUILD_YR']) if pd.notna(r['BUILD_YR']) else None,
            'parcel_sqft': int(r['PARCEL_AREA']) if pd.notna(r['PARCEL_AREA']) else None,
            'market_value': int(r['MKT_VAL_TOT']) if pd.notna(r['MKT_VAL_TOT']) else 0,
            'land_value': int(r['LAND_MV1']) if pd.notna(r['LAND_MV1']) else 0,
            'bldg_value': int(r['BLDG_MV1']) if pd.notna(r['BLDG_MV1']) else 0,
            'last_sale_price': int(r['SALE_PRICE']) if pd.notna(r['SALE_PRICE']) else 0,
            'last_sale_year': sale_year,
            'annual_tax': float(r['TOT_NET_TAX']) if pd.notna(r['TOT_NET_TAX']) else 0,
            'tax_paid': float(r['NET_TAX_PD']) if pd.notna(r['NET_TAX_PD']) else 0,
            'penalty_paid': float(r['TOT_PENALTY_PD']) if pd.notna(r['TOT_PENALTY_PD']) else 0,
            'delinquent_year': clean_str(r['EARLIEST_DELQ_YR']),
            'is_homestead': clean_str(r['HMSTD_CD1']).upper() == 'Y',
            'lat': float(r['LAT']) if pd.notna(r['LAT']) else None,
            'lon': float(r['LON']) if pd.notna(r['LON']) else None,
            'score': int(r['_score']),
            'signals': r['_signals'],
            'lead_type': classify_lead_type(r['_signals']),
        }
        records.append(rec)

    # Build summary stats for the dashboard cards
    total_leads = len(records)
    hot_leads = sum(1 for r in records if r['score'] >= 80)
    warm_leads = sum(1 for r in records if 60 <= r['score'] < 80)
    watch_leads = sum(1 for r in records if r['score'] < 60)
    with_address = sum(1 for r in records if r['address'].strip())

    # Group by city for the city-cards on the dashboard
    by_city = {}
    for r in records:
        c = r['city'] or 'UNKNOWN'
        by_city[c] = by_city.get(c, 0) + 1
    top_cities = sorted(by_city.items(), key=lambda x: -x[1])[:4]

    # Group by lead type
    by_type = {}
    for r in records:
        by_type[r['lead_type']] = by_type.get(r['lead_type'], 0) + 1

    output = {
        'generated_at': pd.Timestamp.now().isoformat(),
        'source': 'Hennepin County Open Data - Parcels',
        'total_leads': total_leads,
        'hot_leads': hot_leads,
        'warm_leads': warm_leads,
        'watch_leads': watch_leads,
        'with_address': with_address,
        'top_cities': top_cities,
        'by_lead_type': by_type,
        'leads': records,
    }

    print(f"Writing {OUTPUT_JSON} ...")
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    size_mb = OUTPUT_JSON.stat().st_size / 1024 / 1024
    print(f"  Wrote {len(records):,} leads ({size_mb:.1f} MB)")
    print(f"  Hot: {hot_leads:,} | Warm: {warm_leads:,} | Watch: {watch_leads:,}")
    print(f"  Top cities: {top_cities}")


if __name__ == '__main__':
    main()
