/* ============================================
   MS INTEL — Application Logic
   ============================================ */

const STATE = {
  raw: [],
  filtered: [],
  meta: {},
  page: 1,
  perPage: 25,
  view: 'dashboard',
};

const PAGE_TITLES = {
  dashboard: 'MOTIVATED SELLER INTELLIGENCE',
  leads: 'LEAD DATABASE',
  comps: 'PROPERTY COMP TOOL',
  calculator: 'DEAL CALCULATOR',
  manual: 'MANUAL SOURCE WORKFLOWS',
  exports: 'EXPORT CENTER',
};

/* ------------- FORMATTERS ------------- */
const fmt$ = n => '$' + Math.round(n || 0).toLocaleString();
const fmtN = n => (n || 0).toLocaleString();
const fmtNum = n => Number(n || 0).toLocaleString();

/* ------------- BOOTSTRAP ------------- */
async function init() {
  try {
    // Try enhanced leads first (includes YELLOW tier signals)
    // Fall back to base leads.json if enhanced not yet generated
    let res, data;
    try {
      res = await fetch('data/leads_enhanced.json');
      if (!res.ok) throw new Error('enhanced not found');
      data = await res.json();
      console.log('Loaded enhanced leads (YELLOW tier active)');
    } catch {
      res = await fetch('data/leads.json');
      data = await res.json();
      console.log('Loaded base leads (run scrapers to activate YELLOW tier)');
    }
    STATE.raw = data.leads;
    STATE.meta = data;
    populateFilters();
    applyFilters();
    renderKPIs();
    renderCityCards();
    renderManualCards();
    bindEvents();
    setRefreshAge(data.generated_at);
  } catch (e) {
    document.getElementById('leadTable').innerHTML =
      `<div style="padding:30px; color:var(--neon-pink); font-family:var(--font-mono); font-size:16px;">
        ⚠ COULD NOT LOAD data/leads.json — make sure the file exists.<br/>
        Run: <code style="color:var(--neon-cyan)">python3 scripts/process_parcels.py</code>
      </div>`;
    console.error(e);
  }
}

/* ------------- FILTER POPULATION ------------- */
function populateFilters() {
  const types = [...new Set(STATE.raw.map(r => r.lead_type))].sort();
  const cities = [...new Set(STATE.raw.map(r => r.city).filter(Boolean))].sort();
  const fT = document.getElementById('f-type');
  const fC = document.getElementById('f-city');
  types.forEach(t => fT.insertAdjacentHTML('beforeend', `<option value="${t}">${t}</option>`));
  cities.forEach(c => fC.insertAdjacentHTML('beforeend', `<option value="${c}">${c}</option>`));
}

/* ------------- FILTER APPLICATION ------------- */
function applyFilters() {
  const search = document.getElementById('search').value.trim().toUpperCase();
  const type = document.getElementById('f-type').value;
  const city = document.getElementById('f-city').value;
  const minScore = parseInt(document.getElementById('f-score').value, 10);
  const sortBy = document.getElementById('f-sort').value;

  let f = STATE.raw.filter(r => {
    if (r.score < minScore) return false;
    if (type && r.lead_type !== type) return false;
    if (city && r.city !== city) return false;
    if (search) {
      const hay = `${r.owner} ${r.address} ${r.city} ${r.zip} ${r.pid}`.toUpperCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  if (sortBy === 'score')   f.sort((a,b) => b.score - a.score);
  if (sortBy === 'equity')  f.sort((a,b) => (b.market_value - b.last_sale_price) - (a.market_value - a.last_sale_price));
  if (sortBy === 'value')   f.sort((a,b) => b.market_value - a.market_value);
  if (sortBy === 'age')     f.sort((a,b) => (a.build_year || 9999) - (b.build_year || 9999));

  STATE.filtered = f;
  STATE.page = 1;
  renderTable();
  document.getElementById('exp-count').textContent = fmtN(f.length);
}

/* ------------- KPIs ------------- */
function renderKPIs() {
  const total = STATE.raw.length;
  const hot = STATE.raw.filter(r => r.score >= 80).length;
  const withAddr = STATE.raw.filter(r => r.address && r.zip).length;
  const avg = total > 0 ? Math.round(STATE.raw.reduce((s,r) => s + r.score, 0) / total) : 0;

  document.getElementById('kpi-total').textContent = fmtN(total);
  document.getElementById('kpi-hot').textContent = fmtN(hot);
  document.getElementById('kpi-address').textContent = fmtN(withAddr);
  document.getElementById('kpi-avg').textContent = avg;
}

function renderCityCards() {
  const grid = document.getElementById('cityGrid');
  const colors = ['kpi-cyan', 'kpi-pink', 'kpi-green', 'kpi-purple'];
  const top = STATE.meta.top_cities || [];
  grid.innerHTML = top.slice(0, 4).map((row, i) => {
    const [city, count] = row;
    const slug = city.toLowerCase().replace(/[^a-z]/g, '');
    return `
      <div class="kpi-card ${colors[i]}">
        <div class="kpi-label">${city.toUpperCase()} LEADS</div>
        <div class="kpi-value">${fmtN(count)}</div>
        <div class="kpi-delta">↑ HENNEPIN COUNTY</div>
        <svg class="kpi-icon" viewBox="0 0 40 40">
          <path d="M8 30 L8 12 L20 6 L32 12 L32 30 Z" fill="none" stroke="currentColor" stroke-width="1.5"/>
          <rect x="14" y="18" width="5" height="6" fill="currentColor" opacity="0.5"/>
          <rect x="22" y="18" width="5" height="6" fill="currentColor" opacity="0.5"/>
        </svg>
      </div>
    `;
  }).join('');
}

/* ------------- LEAD TABLE ------------- */
function renderTable() {
  const wrap = document.getElementById('leadTable');
  const start = (STATE.page - 1) * STATE.perPage;
  const slice = STATE.filtered.slice(start, start + STATE.perPage);

  if (slice.length === 0) {
    wrap.innerHTML = `<div style="padding:60px; text-align:center; font-family:var(--font-mono); color:var(--txt-dim); font-size:18px;">NO LEADS MATCH YOUR FILTERS</div>`;
  } else {
    wrap.innerHTML = slice.map((r, i) => {
      const tier = r.score >= 80 ? 'hot' : r.score >= 60 ? 'warm' : 'watch';
      const tierLabel = tier.toUpperCase();
      const equity = r.market_value - r.last_sale_price;
      const primarySignal = (r.signals[0] || 'Watch list').toUpperCase();
      const secondSignal = r.signals[1] ? r.signals[1] : '';
      return `
        <div class="lead-row" data-idx="${start + i}">
          <div>
            <div class="lead-seller">${r.owner}</div>
            <div class="lead-addr">${r.address}<br/>${r.city}, MN ${r.zip}</div>
          </div>
          <div>
            <div class="lead-loc">MN</div>
            <div class="lead-county">HENNEPIN</div>
          </div>
          <div class="lead-type">${r.lead_type}</div>
          <div class="lead-signal">
            <span class="lead-signal-tag">▸ ${primarySignal}</span>
            ${secondSignal ? `<span style="opacity:0.7">▸ ${secondSignal}</span>` : ''}
          </div>
          <div class="lead-score-box score-${tier}">
            ${r.score}
            <span class="score-tier">${tierLabel}</span>
          </div>
          <div class="lead-amount">${fmt$(r.market_value)}</div>
          <div class="lead-built">${r.build_year || '—'}</div>
          <div class="lead-action">
            <button class="btn-play" data-idx="${start + i}" title="View detail">▶</button>
          </div>
        </div>
      `;
    }).join('');
  }

  // Pagination
  const totalPages = Math.max(1, Math.ceil(STATE.filtered.length / STATE.perPage));
  document.getElementById('pageInfo').textContent =
    `PAGE ${STATE.page} / ${totalPages} · ${fmtN(STATE.filtered.length)} LEADS`;
  document.getElementById('prevPage').disabled = STATE.page <= 1;
  document.getElementById('nextPage').disabled = STATE.page >= totalPages;

  // Bind row clicks
  wrap.querySelectorAll('.btn-play, .lead-row').forEach(el => {
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      const idx = parseInt(el.dataset.idx, 10);
      if (!isNaN(idx)) showLeadDetail(STATE.filtered[idx]);
    });
  });
}

/* ------------- LEAD DETAIL MODAL ------------- */
function showLeadDetail(r) {
  const equity = r.market_value - r.last_sale_price;
  const equityPct = r.market_value > 0 ? Math.round((equity / r.market_value) * 100) : 0;
  const arvEstimate = Math.round(r.market_value * 1.05); // small ARV buffer
  const repairs = Math.round(r.market_value * 0.15); // assume 15% repair budget
  const mao = Math.round((arvEstimate * 0.70) - repairs);

  const body = document.getElementById('modalBody');
  body.innerHTML = `
    <h2 class="modal-title">${r.owner}</h2>
    <div class="modal-grid">
      <div class="modal-field"><b>PROPERTY ADDRESS</b><span>${r.address}, ${r.city}, MN ${r.zip}</span></div>
      <div class="modal-field"><b>MAILING ADDRESS</b><span>${r.mail_address || '—'}, ${r.mail_city || ''}</span></div>
      <div class="modal-field"><b>PARCEL ID (PID)</b><span>${r.pid}</span></div>
      <div class="modal-field"><b>PROPERTY TYPE</b><span>${r.property_type || '—'}</span></div>
      <div class="modal-field"><b>BUILT</b><span>${r.build_year || '—'}</span></div>
      <div class="modal-field"><b>PARCEL SQFT</b><span>${fmtN(r.parcel_sqft)}</span></div>
      <div class="modal-field"><b>MARKET VALUE</b><span>${fmt$(r.market_value)}</span></div>
      <div class="modal-field"><b>LAND / BLDG VALUE</b><span>${fmt$(r.land_value)} / ${fmt$(r.bldg_value)}</span></div>
      <div class="modal-field"><b>LAST SALE</b><span>${fmt$(r.last_sale_price)} (${r.last_sale_year || 'N/A'})</span></div>
      <div class="modal-field"><b>EST. EQUITY</b><span>${fmt$(equity)} (~${equityPct}%)</span></div>
      <div class="modal-field"><b>ANNUAL TAX</b><span>${fmt$(r.annual_tax)}</span></div>
      <div class="modal-field"><b>HOMESTEAD</b><span>${r.is_homestead ? 'YES' : 'NO (likely investor)'}</span></div>
    </div>
    <div class="modal-signals">
      <b>🔥 DISTRESS SIGNALS — SCORE: ${r.score} / 100</b>
      <ul>${r.signals.map(s => `<li>${s}</li>`).join('')}</ul>
    </div>
    <div class="modal-signals" style="border-color:var(--neon-cyan); background:rgba(0,240,255,0.05);">
      <b style="color:var(--neon-cyan);">🧮 QUICK MAO ESTIMATE (70% RULE)</b>
      <div style="font-family:var(--font-mono); font-size:15px; margin-top:10px; line-height:1.7;">
        Est. ARV (1.05× MV): <b style="color:var(--neon-yellow)">${fmt$(arvEstimate)}</b><br/>
        Est. Repairs (15% MV): <b style="color:var(--neon-yellow)">${fmt$(repairs)}</b><br/>
        Max Allowable Offer: <b style="color:var(--neon-pink); font-size:18px;">${fmt$(mao)}</b>
        <small style="display:block; margin-top:8px; color:var(--txt-dim)">Refine with the Deal Calculator using a real ARV from comps.</small>
      </div>
    </div>
    <div class="modal-actions">
      <button class="btn-control export-btn" onclick='exportSingleLead(${JSON.stringify(r)})'>EXPORT TO GHL</button>
      <button class="btn-control" onclick='runCompForLead(${JSON.stringify(r)})'>FIND COMPS</button>
      <button class="btn-control" onclick='loadIntoCalc(${JSON.stringify(r)})'>OPEN IN CALCULATOR</button>
    </div>
  `;
  document.getElementById('leadModal').classList.add('active');
}

/* ------------- COMP TOOL ------------- */
function findComps() {
  const q = document.getElementById('comp-search').value.trim().toUpperCase();
  const tol = parseFloat(document.getElementById('comp-sqft-tol').value);
  const zipStrict = document.getElementById('comp-zip-strict').value === '1';

  if (!q) {
    document.getElementById('comp-subject').innerHTML =
      `<div style="color:var(--neon-pink); font-family:var(--font-mono); padding:14px;">⚠ Enter an address or PID to find comps.</div>`;
    document.getElementById('comp-results').innerHTML = '';
    return;
  }

  // Find subject (within our 15K leads)
  const subj = STATE.raw.find(r => {
    const hay = `${r.address} ${r.city} ${r.zip} ${r.pid}`.toUpperCase();
    return hay.includes(q);
  });

  if (!subj) {
    document.getElementById('comp-subject').innerHTML =
      `<div style="color:var(--neon-pink); font-family:var(--font-mono); padding:14px;">⚠ Subject not found in current 15K leads. Try a broader search or use the full county tax roll for non-leads.</div>`;
    document.getElementById('comp-results').innerHTML = '';
    return;
  }

  document.getElementById('comp-subject').innerHTML = `
    <div class="subject-card">
      <h3>SUBJECT PROPERTY</h3>
      <div class="subject-grid">
        <div><b>ADDRESS</b>${subj.address}, ${subj.city} ${subj.zip}</div>
        <div><b>BUILT</b>${subj.build_year || '—'}</div>
        <div><b>PARCEL SQFT</b>${fmtN(subj.parcel_sqft)}</div>
        <div><b>MARKET VALUE</b>${fmt$(subj.market_value)}</div>
        <div><b>LAST SALE</b>${fmt$(subj.last_sale_price)} ${subj.last_sale_year ? `(${subj.last_sale_year})` : ''}</div>
        <div><b>PROPERTY TYPE</b>${subj.property_type || '—'}</div>
        <div><b>LAND / BLDG</b>${fmt$(subj.land_value)} / ${fmt$(subj.bldg_value)}</div>
        <div><b>SCORE</b>${subj.score}</div>
      </div>
    </div>
  `;

  // Find comps in our dataset (same property type, similar sqft, optionally same zip)
  const lo = subj.parcel_sqft * (1 - tol);
  const hi = subj.parcel_sqft * (1 + tol);
  const comps = STATE.raw.filter(r =>
    r.pid !== subj.pid &&
    r.property_type === subj.property_type &&
    (!zipStrict || r.zip === subj.zip) &&
    r.parcel_sqft >= lo && r.parcel_sqft <= hi &&
    r.last_sale_price > 10000 &&
    r.last_sale_year && r.last_sale_year >= 2018
  ).sort((a, b) => (b.last_sale_year || 0) - (a.last_sale_year || 0)).slice(0, 12);

  if (comps.length === 0) {
    document.getElementById('comp-results').innerHTML =
      `<div style="color:var(--txt-dim); font-family:var(--font-mono); padding:14px;">No comps found in current dataset. Loosen the sqft tolerance or expand outside zip.</div>`;
    return;
  }

  // Compute summary
  const prices = comps.map(c => c.last_sale_price);
  const avg = prices.reduce((s,p) => s+p, 0) / prices.length;
  const median = [...prices].sort((a,b) => a-b)[Math.floor(prices.length/2)];

  document.getElementById('comp-results').innerHTML = `
    <div style="margin-top:18px;">
      <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:14px;">
        <div class="modal-field"><b>COMPS FOUND</b><span style="color:var(--neon-cyan)">${comps.length}</span></div>
        <div class="modal-field"><b>AVG SALE</b><span style="color:var(--neon-green)">${fmt$(avg)}</span></div>
        <div class="modal-field"><b>MEDIAN SALE</b><span style="color:var(--neon-green)">${fmt$(median)}</span></div>
      </div>
      <div class="comp-results-table">
        <div class="comp-row comp-header">
          <div>ADDRESS</div>
          <div>SOLD</div>
          <div>SALE PRICE</div>
          <div>MV TODAY</div>
          <div>SQFT</div>
          <div>BUILT</div>
          <div></div>
        </div>
        ${comps.map(c => `
          <div class="comp-row">
            <div style="color:var(--neon-pink); font-family:var(--font-display); font-size:10px;">${c.address}, ${c.city}</div>
            <div>${c.last_sale_year || '—'}</div>
            <div style="color:var(--neon-green); font-weight:700;">${fmt$(c.last_sale_price)}</div>
            <div>${fmt$(c.market_value)}</div>
            <div>${fmtN(c.parcel_sqft)}</div>
            <div>${c.build_year || '—'}</div>
            <div><button class="btn-play" onclick='showLeadDetail(${JSON.stringify(c)})' title="View">▶</button></div>
          </div>
        `).join('')}
      </div>
      <div style="margin-top:14px; padding:12px; background:rgba(255,245,0,0.05); border:1px solid var(--neon-yellow); border-radius:4px; font-family:var(--font-mono); font-size:14px; color:var(--txt-base);">
        <b style="color:var(--neon-yellow); font-family:var(--font-display); font-size:10px;">⚠ ARV WARNING</b><br/>
        Tax-roll comps lag the market by 12-24 months. Use these as a SANITY CHECK against MLS comps from Zillow/Redfin/Realtor.com (those reflect current retail).
      </div>
    </div>
  `;
}

function runCompForLead(r) {
  closeModal();
  navigate('comps');
  document.getElementById('comp-search').value = r.address;
  setTimeout(findComps, 50);
}

/* ------------- DEAL CALCULATOR ------------- */
function calcDeal() {
  const arv = parseFloat(document.getElementById('calc-arv').value) || 0;
  const repairs = parseFloat(document.getElementById('calc-repairs').value) || 0;
  const fee = parseFloat(document.getElementById('calc-fee').value) || 0;
  const holdPct = parseFloat(document.getElementById('calc-hold').value) || 0;
  const closePct = parseFloat(document.getElementById('calc-close').value) || 0;
  const margin = parseFloat(document.getElementById('calc-margin').value) || 0.7;

  const holdCost = arv * (holdPct / 100);
  const closeCost = arv * (closePct / 100);
  const investorAllIn = arv * margin;
  const mao = investorAllIn - repairs - holdCost - closeCost;
  const offer = mao - fee;

  document.getElementById('calc-mao').textContent = fmt$(mao);
  document.getElementById('calc-offer').textContent = fmt$(offer);

  document.getElementById('calc-breakdown').innerHTML = `
    <div><b>ARV</b><span>${fmt$(arv)}</span></div>
    <div><b>× Investor Margin (${Math.round(margin*100)}%)</b><span>${fmt$(investorAllIn)}</span></div>
    <div><b>− Repairs</b><span>−${fmt$(repairs)}</span></div>
    <div><b>− Holding (${holdPct}%)</b><span>−${fmt$(holdCost)}</span></div>
    <div><b>− Close/Sell (${closePct}%)</b><span>−${fmt$(closeCost)}</span></div>
    <div style="border-top:1px solid var(--line-cyan); padding-top:6px; margin-top:6px;"><b style="color:var(--neon-pink)">= MAO</b><span style="color:var(--neon-pink)">${fmt$(mao)}</span></div>
    <div><b>− Wholesale Fee</b><span>−${fmt$(fee)}</span></div>
    <div style="border-top:1px solid var(--line-cyan); padding-top:6px; margin-top:6px;"><b style="color:var(--neon-pink)">= NET TO SELLER</b><span style="color:var(--neon-pink); font-size:18px;">${fmt$(offer)}</span></div>
  `;
}

function loadIntoCalc(r) {
  closeModal();
  navigate('calculator');
  document.getElementById('calc-arv').value = Math.round(r.market_value * 1.05);
  document.getElementById('calc-repairs').value = Math.round(r.market_value * 0.15);
  calcDeal();
}

/* ------------- MANUAL SOURCES ------------- */
const MANUAL_SOURCES = [
  {
    title: 'PRE-FORECLOSURE / NOTICE OF DEFAULT',
    difficulty: 'medium',
    summary: 'Minnesota is a non-judicial foreclosure state. Notices of Mortgage Foreclosure Sale are published in legal notices and recorded with the county. The earliest "I might lose my house" signal lives here.',
    steps: [
      'Go to <a href="https://www.startribune.com/legal-notices" target="_blank">startribune.com/legal-notices</a> — search "Notice of Mortgage Foreclosure Sale"',
      'Or check the <a href="https://finance-commerce.com/" target="_blank">Finance & Commerce</a> Public Notice section (Hennepin County legal paper)',
      'For each notice, copy: Mortgagor name, property address, sale date, original principal, attorney',
      'Cross-reference the address against this dashboard — if their score is already high, prioritize',
      'Search the owner on the dashboard search bar to enrich with mailing address + equity estimate',
      'Add a custom signal "+30 NOD" when entering into your CRM',
    ],
  },
  {
    title: 'PROBATE FILINGS',
    difficulty: 'hard',
    summary: 'When an owner dies, heirs often inherit property they don\'t want. Probate is filed in the county where the deceased lived. Heirs are typically ready to sell at a discount to settle the estate.',
    steps: [
      'Go to the <a href="https://www.mncourts.gov/Find-Courts/Hennepin.aspx" target="_blank">Hennepin County District Court</a> page',
      'Click "Court Calendar" → filter by "Probate"',
      'Or use <a href="https://publicaccess.courts.state.mn.us/CaseSearch" target="_blank">MN Court Records Online (MCRO)</a> — search by case type "Probate"',
      'Note: MCRO requires you to search case-by-case; there is no bulk export',
      'For each case, look for: decedent name, personal representative (PR) contact, property listed in inventory',
      'Match decedent address against this dashboard — if non-homestead and absentee, the heir likely lives elsewhere and wants to sell',
      'Mail the personal representative, NOT the decedent. Courteous, condolence-first messaging only.',
    ],
  },
  {
    title: 'CODE VIOLATIONS — MINNEAPOLIS / HENNEPIN CITIES',
    difficulty: 'easy',
    summary: 'Properties with open code violations are often owned by tired landlords or absentee owners who can\'t afford repairs. Open violations = motivation.',
    steps: [
      'Minneapolis: <a href="https://www2.minneapolismn.gov/government/departments/regulatory-services/" target="_blank">311 Regulatory Services</a> — request open violation list (FOIA-friendly)',
      'Or use <a href="https://opendata.minneapolismn.gov/" target="_blank">opendata.minneapolismn.gov</a> — look for "Housing Code Violations"',
      'For other Hennepin cities (Bloomington, Edina, etc.) check each city\'s open data portal individually',
      'Filter to "OPEN" status and addresses with 3+ violations',
      'Cross-reference against this dashboard to see which violators are also non-homestead / tax delinquent (double signal)',
      'Mail with a "we buy as-is, no repairs needed" angle',
    ],
  },
  {
    title: 'SKIP TRACING (PHONE + EMAIL APPEND)',
    difficulty: 'easy',
    summary: 'The county has owner names and mailing addresses but NO phone numbers or emails. Skip trace appends those so you can call/text/email instead of just mailing.',
    steps: [
      'Export the SKIPTRACE.csv from the Exports tab (already formatted)',
      'Upload to one of: <a href="https://batchskiptracing.com" target="_blank">BatchSkipTracing</a> ($0.10-0.15/lead), <a href="https://propstream.com" target="_blank">PropStream</a> (subscription), or <a href="https://www.tlo.com/" target="_blank">TLO</a> (subscription, premium)',
      'Hit rate: ~70-80% on phones, ~50% on emails for residential',
      'Re-import the enriched CSV into GoHighLevel via the GHL CSV format',
      'TCPA caution: cell phones cannot be cold-texted in MN without express written consent. Voicemail drops or RVMs only.',
      'Recommended cadence: 1 call → wait 3 days → 1 voicemail → wait 1 week → text after first verbal contact',
    ],
  },
  {
    title: 'DIVORCE FILINGS',
    difficulty: 'hard',
    summary: 'Divorces frequently force the sale of the marital home. These filings are public but searchable only one at a time.',
    steps: [
      'Use <a href="https://publicaccess.courts.state.mn.us/CaseSearch" target="_blank">MCRO</a> → search case type "Marriage Dissolution" in Hennepin County',
      'Look for cases filed in the last 90 days (older = already resolved)',
      'Note both parties\' names and any property addresses listed',
      'Cross-reference against this dashboard — if the home is in joint name and one party has moved out, you\'ve got motivation',
      'Approach gently — neither spouse wants to be cold-pitched mid-divorce',
    ],
  },
  {
    title: 'MLS / CURRENT MARKET COMPS (FOR ARV)',
    difficulty: 'medium',
    summary: 'Tax-roll comps lag 12-24 months. To accurately calculate MAO, you need REAL recent sale prices from the MLS.',
    steps: [
      'Free option: <a href="https://www.zillow.com" target="_blank">Zillow</a> Recently Sold filter — last 6 months, ½ mile radius',
      'Free option: <a href="https://www.redfin.com" target="_blank">Redfin</a> Sold tab — same approach',
      'Paid (best): <a href="https://propstream.com" target="_blank">PropStream</a> ($99/mo) — pulls actual MLS sold data with photos',
      'Paid (best for active investors): get a wholesaler-friendly REALTOR® who will pull MLS comps for you',
      'Take 3-5 closed sales within last 90 days, ±20% sqft, same school district, similar condition',
      'Use those numbers as ARV in the Deal Calculator → real MAO',
    ],
  },
  {
    title: 'EVICTION / UNLAWFUL DETAINER FILINGS',
    difficulty: 'medium',
    summary: 'Landlords filing evictions often want OUT of being landlords. Multiple evictions on one address = burned-out owner.',
    steps: [
      'Hennepin Housing Court: <a href="https://www.mncourts.gov/Find-Courts/Hennepin.aspx" target="_blank">mncourts.gov</a> → Housing Court',
      'Use <a href="https://publicaccess.courts.state.mn.us/CaseSearch" target="_blank">MCRO</a> → case type "Eviction (Unlawful Detainer)"',
      'Note plaintiff (landlord) name and property address',
      'Match against dashboard — landlords with 2+ evictions in 12 months are prime',
      'Pitch: "I buy rentals as-is, you keep the cash flow on closing day, no more late rent calls."',
    ],
  },
];

function renderManualCards() {
  const wrap = document.getElementById('manualCards');
  wrap.innerHTML = MANUAL_SOURCES.map(s => `
    <div class="manual-card">
      <h3>${s.title}<span class="difficulty diff-${s.difficulty === 'easy' ? 'easy' : s.difficulty === 'medium' ? 'med' : 'hard'}">${s.difficulty.toUpperCase()}</span></h3>
      <div class="summary">${s.summary}</div>
      <ol>
        ${s.steps.map(step => `<li>${step}</li>`).join('')}
      </ol>
    </div>
  `).join('');
}

/* ------------- EXPORTS ------------- */
function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function downloadCsv(filename, rows) {
  if (rows.length === 0) {
    alert('No leads match your current filters.');
    return;
  }
  const headers = Object.keys(rows[0]);
  const csv = [
    headers.join(','),
    ...rows.map(r => headers.map(h => csvEscape(r[h])).join(','))
  ].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

function exportGHL() {
  const rows = STATE.filtered.map(r => ({
    'First Name': r.first_name || '',
    'Last Name': r.last_name || '',
    'Email': '',
    'Phone': '',
    'Address 1': r.address,
    'City': r.city,
    'State': r.state,
    'Postal Code': r.zip,
    'Country': 'US',
    'Tags': `${r.lead_type.replace(/ /g, '_')},MS_INTEL,Hennepin,Score_${r.score >= 80 ? 'HOT' : r.score >= 60 ? 'WARM' : 'WATCH'}`,
    'MS_Score': r.score,
    'MS_Lead_Type': r.lead_type,
    'MS_Distress_Signals': r.signals.join(' | '),
    'MS_Market_Value': r.market_value,
    'MS_Last_Sale_Price': r.last_sale_price,
    'MS_Last_Sale_Year': r.last_sale_year || '',
    'MS_Build_Year': r.build_year || '',
    'MS_PID': r.pid,
    'MS_Mailing_Address': r.mail_address,
    'MS_Mailing_City': r.mail_city,
  }));
  downloadCsv(`ghl_jarvis_${todayStamp()}.csv`, rows);
}

function exportSkip() {
  const rows = STATE.filtered.map(r => ({
    'Owner Full Name': r.owner,
    'First Name': r.first_name,
    'Last Name': r.last_name,
    'Property Address': r.address,
    'Property City': r.city,
    'Property State': r.state,
    'Property Zip': r.zip,
    'Mailing Address': r.mail_address,
    'Mailing City': r.mail_city,
    'Mailing State': 'MN',
    'PID': r.pid,
  }));
  downloadCsv(`skiptrace_${todayStamp()}.csv`, rows);
}

function exportMail() {
  // Direct mail: use mailing address (where the owner actually lives)
  const rows = STATE.filtered.map(r => ({
    'Recipient Name': r.owner,
    'Address Line 1': r.mail_address || r.address,
    'City': r.mail_city || r.city,
    'State': 'MN',
    'Zip': '',
    'Property Address': `${r.address}, ${r.city} MN ${r.zip}`,
    'Lead Type': r.lead_type,
    'Score': r.score,
  }));
  downloadCsv(`directmail_${todayStamp()}.csv`, rows);
}

function exportJson() {
  const blob = new Blob([JSON.stringify(STATE.filtered, null, 2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `ms_intel_full_${todayStamp()}.json`;
  a.click();
  URL.revokeObjectURL(a.href);
}

function exportSingleLead(r) {
  const row = {
    'First Name': r.first_name,
    'Last Name': r.last_name,
    'Address 1': r.address,
    'City': r.city,
    'State': r.state,
    'Postal Code': r.zip,
    'Tags': `${r.lead_type.replace(/ /g, '_')},MS_INTEL,Score_${r.score}`,
    'MS_Score': r.score,
    'MS_Distress_Signals': r.signals.join(' | '),
  };
  downloadCsv(`ghl_${r.last_name || 'lead'}_${todayStamp()}.csv`, [row]);
}

function todayStamp() {
  const d = new Date();
  return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
}

function setRefreshAge(iso) {
  if (!iso) return;
  const then = new Date(iso);
  const now = new Date();
  const ageHrs = Math.floor((now - then) / 3600000);
  const ageDays = Math.floor(ageHrs / 24);
  const txt = ageDays > 0 ? `${ageDays}D AGO` : `${ageHrs}H AGO`;
  document.getElementById('dataAge').textContent = txt;
}

/* ------------- NAVIGATION ------------- */
function navigate(view) {
  STATE.view = view;
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById(`view-${view}`).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n => {
    n.classList.toggle('active', n.dataset.view === view);
  });
  // Keep "leads" view showing the dashboard table — they're the same thing here
  if (view === 'leads') {
    document.getElementById('view-dashboard').classList.add('active');
  }
}

function closeModal() {
  document.getElementById('leadModal').classList.remove('active');
}

/* ------------- EVENT BINDING ------------- */
function bindEvents() {
  document.getElementById('search').addEventListener('input', applyFilters);
  document.getElementById('f-type').addEventListener('change', applyFilters);
  document.getElementById('f-city').addEventListener('change', applyFilters);
  document.getElementById('f-score').addEventListener('input', e => {
    document.getElementById('scoreVal').textContent = e.target.value;
    applyFilters();
  });
  document.getElementById('f-sort').addEventListener('change', applyFilters);
  document.getElementById('resetBtn').addEventListener('click', () => {
    document.getElementById('search').value = '';
    document.getElementById('f-type').value = '';
    document.getElementById('f-city').value = '';
    document.getElementById('f-score').value = 60;
    document.getElementById('scoreVal').textContent = 60;
    document.getElementById('f-sort').value = 'score';
    applyFilters();
  });

  document.getElementById('prevPage').addEventListener('click', () => {
    if (STATE.page > 1) { STATE.page--; renderTable(); }
  });
  document.getElementById('nextPage').addEventListener('click', () => {
    const total = Math.ceil(STATE.filtered.length / STATE.perPage);
    if (STATE.page < total) { STATE.page++; renderTable(); }
  });

  document.querySelectorAll('.nav-item').forEach(n => {
    n.addEventListener('click', () => navigate(n.dataset.view));
  });

  document.getElementById('exportBtn').addEventListener('click', exportGHL);
  document.getElementById('exp-ghl').addEventListener('click', exportGHL);
  document.getElementById('exp-skip').addEventListener('click', exportSkip);
  document.getElementById('exp-mail').addEventListener('click', exportMail);
  document.getElementById('exp-json').addEventListener('click', exportJson);

  document.getElementById('comp-run').addEventListener('click', findComps);
  ['calc-arv','calc-repairs','calc-fee','calc-hold','calc-close','calc-margin'].forEach(id => {
    document.getElementById(id).addEventListener('input', calcDeal);
    document.getElementById(id).addEventListener('change', calcDeal);
  });
  calcDeal(); // initial calc

  document.getElementById('modalClose').addEventListener('click', closeModal);
  document.getElementById('leadModal').addEventListener('click', e => {
    if (e.target.id === 'leadModal') closeModal();
  });
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeModal();
  });
}

// Make modal functions globally accessible (for inline onclick)
window.showLeadDetail = showLeadDetail;
window.runCompForLead = runCompForLead;
window.loadIntoCalc = loadIntoCalc;
window.exportSingleLead = exportSingleLead;
window.closeModal = closeModal;

document.addEventListener('DOMContentLoaded', init);
