# MotivatedSeller_Intel — Hennepin County (MN)

A retrowave-styled motivated seller intelligence dashboard, scored against the **Hennepin County Open Data tax roll** (~448K parcels). Built using the AI Cheat Codes Tax Bot framework (GREEN / YELLOW / RED source mapping).

> **Deals not feelings.**

## Live URL

After GitHub Pages is enabled (see deploy steps below), this dashboard lives at:
**https://malexdurant-bot.github.io/MotivatedSeller_Intel/**

---

## What it does

- **Scores 367,000+ Hennepin County residential parcels** on 9 motivated-seller signals
- **Surfaces the top 15,000 leads** in a fast, filterable browser-based dashboard
- **Property comp tool** — find comparable parcels by zip + sqft + property type
- **Deal calculator** — 70% rule MAO, BRRRR, wholesale-fee modeling
- **GHL CSV export** — formatted for direct GoHighLevel (Jarvis) import
- **Skip-trace export** — formatted for BatchSkipTracing / TLO / IDI
- **Direct-mail export** — owner-to-mailing-address routing for absentee leads
- **Manual source workflows** — step-by-step instructions for the RED tier sources (probate, NOD, code violations, evictions, MLS comps)

---

## GREEN / YELLOW / RED — Source Mapping (Hennepin County)

Following the framework from the build module:

### 🟢 GREEN — Fully automated
- **Hennepin County Open Data parcels** (this dashboard, baked into `docs/data/leads.json`)
- Tax delinquency flags, absentee owner detection, equity estimation, long-term hold, non-homestead — all derived from the public tax roll

### 🟡 YELLOW — Scraped with retry logic (future module)
- City code violation portals (Minneapolis 311 + each Hennepin city)
- Public foreclosure notices via Star Tribune / Finance & Commerce legal sections
- Eviction filings via MN Court Records (rate-limited)

### 🔴 RED — Manual source cards (built into the dashboard)
- Pre-foreclosure / NOD lookup → Star Tribune legal notices walkthrough
- Probate filings → Hennepin County Probate Court walkthrough
- Code violations → Minneapolis 311 + city portals walkthrough
- Skip trace → BatchSkipTracing / TLO walkthrough
- Divorce filings → MCRO walkthrough
- MLS comps → Zillow / Redfin / PropStream walkthrough
- Eviction filings → Hennepin Housing Court walkthrough

---

## Scoring model (0-100)

| Signal | Points | Source field |
|--------|-------:|--------------|
| Tax delinquent | +30 | `EARLIEST_DELQ_YR` |
| Confession of judgment / payment plan | +15 | `COMP_JUDG_IND` |
| Absentee owner (mailing ≠ property) | +20 | `TAXPAYER_NM_1` vs `HOUSE_NO/STREET_NM` |
| Non-homestead (investor / rental) | +15 | `HMSTD_CD1 = N` |
| Long-term hold (>20 years or no sale) | +15 | `SALE_DATE` |
| High equity (MV > 2× last sale) | +15 | `MKT_VAL_TOT` vs `SALE_PRICE` |
| Aged property (built before 1960) | +10 | `BUILD_YR` |
| Recent penalty paid | +10 | `TOT_PENALTY_PD` |
| Low improvement ratio (<20%) | +5 | `BLDG_MV1 / TOTAL_MV1` |

Capped at 100. Tiers: **HOT** (80+), **WARM** (60-79), **WATCH** (<60).

---

## Repo structure

```
MotivatedSeller_Intel/
├── docs/                       ← GitHub Pages serves from here
│   ├── index.html              ← Dashboard markup
│   ├── styles.css              ← Retrowave neon styling
│   ├── app.js                  ← Filtering, scoring, comp, calc, export logic
│   ├── data/
│   │   └── leads.json          ← Pre-scored top 15K leads (committed)
│   └── assets/                 ← Dashboard screenshots
├── data_raw/                   ← (gitignored) Raw tax roll lives here
├── scripts/
│   └── process_parcels.py      ← Re-run when CSV is refreshed
├── README.md
├── LICENSE                     ← MIT
└── .gitignore
```

---

## Deploy to GitHub Pages

### 1. Push to GitHub

```bash
cd MotivatedSeller_Intel
git init
git add .
git commit -m "Initial commit — MS INTEL Hennepin dashboard"
git branch -M main
git remote add origin https://github.com/malexdurant-bot/MotivatedSeller_Intel.git
git push -u origin main
```

### 2. Enable GitHub Pages (in browser)

1. Go to `https://github.com/malexdurant-bot/MotivatedSeller_Intel`
2. Click **Settings** (top right of repo)
3. Click **Pages** in the left sidebar
4. Under **Build and Deployment**:
   - Source: **Deploy from a branch**
   - Branch: **main**
   - Folder: **/docs**
5. Click **Save**

### 3. Wait 60 seconds, then visit

`https://malexdurant-bot.github.io/MotivatedSeller_Intel/`

---

## Refreshing the data

Hennepin County publishes the tax roll regularly at:
**https://gis-hennepin.opendata.arcgis.com/datasets/county-parcels**

When you want fresh data:

```bash
# 1. Download the latest CSV from the Hennepin Open Data portal
#    Save as: data_raw/County_Parcels.csv

# 2. Install dependencies (first time only)
pip3 install pandas numpy

# 3. Re-run the scoring script
python3 scripts/process_parcels.py

# 4. Commit and push
git add docs/data/leads.json
git commit -m "Refresh leads $(date +%Y-%m-%d)"
git push origin main
```

GitHub Pages auto-redeploys within 30-60 seconds.

---

## Customizing the scoring

Open `scripts/process_parcels.py`. The `score_row()` function is where every signal lives. Adjust weights, add new ones, remove what doesn't fit your buy box.

Common changes:
- **Tighter geography**: Filter zip codes or municipalities at the top of `main()`
- **Higher-value buy box**: Filter `MKT_VAL_TOT` to your range (e.g. $200K-$500K)
- **More leads**: Bump `TOP_N` from 15000 → 25000

---

## Roadmap

This is the MN/Hennepin module. Next states (per repo description):
- 🔜 OH — Cuyahoga + Franklin County
- 🔜 GA — Fulton + DeKalb (the Quentin demo county)
- 🔜 IN — Marion County (Indianapolis)

Each county = same `docs/data/leads_{county}.json` pattern, swappable from a county selector dropdown.

---

## License

MIT — see LICENSE.

## Disclaimer

This dashboard scores public records. Nothing here is investment advice. Always verify ownership and lien status with title before closing. Pull current MLS comps before submitting offers. Comply with all applicable do-not-call, TCPA, and direct-mail regulations.
