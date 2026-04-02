/* ──────────────────────────────────────────────────────────────────────────
   Marketing Dashboard — frontend logic
   Polls /api/data every 30s, /api/insights every 5min.
   Handles hierarchical table, sorting, filtering, charts.
────────────────────────────────────────────────────────────────────────── */

const DATA_INTERVAL     = 30_000;
const INSIGHTS_INTERVAL = 300_000;

let _currentData     = null;
let _currentInsights = null;
let _sortCol         = "spend";
let _sortDir         = "desc";
let _expandedRows    = new Set();
let _chart           = null;

// ── Fetch & refresh ──────────────────────────────────────────────────────────

async function fetchData(params = "") {
  try {
    const res = await fetch("/api/data" + params);
    if (!res.ok) return null;
    return await res.json();
  } catch { return null; }
}

async function fetchInsights() {
  try {
    const res = await fetch("/api/insights");
    if (!res.ok) return null;
    return await res.json();
  } catch { return null; }
}

async function loadData() {
  const params = buildFilterParams();
  const data   = await fetchData(params);
  if (!data || data.error) return;
  _currentData = data;
  renderKPIs(data.kpis);
  renderTableForLevel(data);
  renderRankings(data.creative_rankings);
  renderChart();
  updateSyncBadge(data.meta);
  updateFooter(data.meta);
  await populateCampaignFilter(data);
}

async function loadInsights() {
  const insights = await fetchInsights();
  if (!insights || insights.error) return;
  _currentInsights = insights;
  renderAlerts(insights.alerts);
  renderAIInsights(insights.ai_insights);
  renderRecommendations(insights.recommendations);
}

async function forceRefresh() {
  const btn = document.getElementById("refreshBtn");
  btn.textContent = "↻ Atualizando...";
  btn.disabled = true;
  try {
    const res  = await fetch("/api/refresh", { method: "POST" });
    const data = await res.json();
    if (data && !data.error) {
      _currentData = data;
      renderKPIs(data.kpis);
      renderTableForLevel(data);
      renderRankings(data.creative_rankings);
      renderChart();
      updateSyncBadge(data.meta);
    }
    await loadInsights();
  } finally {
    btn.textContent = "↻ Atualizar";
    btn.disabled = false;
  }
}

// ── Filters ──────────────────────────────────────────────────────────────────

function buildFilterParams() {
  const start    = document.getElementById("dateStart").value;
  const end      = document.getElementById("dateEnd").value;
  const campaign = document.getElementById("campaignFilter").value;
  const parts    = [];
  if (start)    parts.push(`date_start=${encodeURIComponent(start)}`);
  if (end)      parts.push(`date_end=${encodeURIComponent(end)}`);
  if (campaign) parts.push(`campaign=${encodeURIComponent(campaign)}`);
  return parts.length ? "?" + parts.join("&") : "";
}

function applyFilters() { loadData(); }

function clearFilters() {
  document.getElementById("dateStart").value    = "";
  document.getElementById("dateEnd").value      = "";
  document.getElementById("campaignFilter").value = "";
  loadData();
}

async function populateCampaignFilter(data) {
  const sel  = document.getElementById("campaignFilter");
  const current = sel.value;
  const names = [...new Set((data.performance_table || []).map(c => c.name))].sort();
  // Keep first "all" option
  while (sel.options.length > 1) sel.remove(1);
  names.forEach(n => {
    const opt = document.createElement("option");
    opt.value = n; opt.textContent = n;
    sel.appendChild(opt);
  });
  if (current) sel.value = current;
}

// ── KPI rendering ────────────────────────────────────────────────────────────

function fmt_currency(v) {
  if (!v && v !== 0) return "—";
  return "R$ " + Number(v).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function fmt_pct(v)    { return v == null ? "—" : Number(v).toFixed(2) + "%"; }
function fmt_x(v)      { return v == null ? "—" : Number(v).toFixed(2) + "x"; }
function fmt_num(v)    { return v == null ? "—" : Number(v).toLocaleString("pt-BR"); }

function renderKPIs(k) {
  if (!k) return;
  document.getElementById("val-spend").textContent    = fmt_currency(k.total_spend);
  document.getElementById("val-revenue").textContent  = fmt_currency(k.total_revenue);
  document.getElementById("val-roas").textContent     = fmt_x(k.overall_roas);
  document.getElementById("val-leads").textContent    = fmt_num(k.total_leads);
  document.getElementById("val-cpl").textContent      = fmt_currency(k.overall_cpl);
  document.getElementById("val-appts").textContent    = fmt_num(k.total_appointments);
  document.getElementById("val-sales").textContent    = fmt_num(k.total_sales);
  document.getElementById("val-cac").textContent      = fmt_currency(k.overall_cac);

  // Color ROAS card
  const roasCard = document.getElementById("kpi-roas");
  roasCard.className = "kpi-card highlight" + (k.overall_roas >= 3 ? " green" : k.overall_roas < 1 ? " red" : "");
}

// ── Table rendering ──────────────────────────────────────────────────────────

function renderTableForLevel(data) {
  const level = document.getElementById("levelFilter").value;
  const flatMap = {
    campaign: data.campaigns_flat,
    adset:    data.adsets_flat,
    creative: data.creatives_flat,
  };
  if (level !== "all" && flatMap[level] && flatMap[level].length) {
    renderFlatRows(flatMap[level]);
  } else {
    renderTable(data.performance_table);
  }
}

function renderFlatRows(rows) {
  const tbody = document.getElementById("perfTableBody");
  tbody.innerHTML = "";
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="11" class="loading-row">Sem dados disponíveis.</td></tr>`;
    return;
  }
  const sorted = sortRows([...rows], _sortCol, _sortDir);
  sorted.forEach(parent => {
    tbody.appendChild(buildRow(parent, true));
    const children = sortRows([...(parent.children || [])], _sortCol, _sortDir);
    children.forEach(child => {
      const tr = buildRow(child, false);
      tr.dataset.parent = parent.id;
      if (!_expandedRows.has(parent.id)) tr.classList.add("hidden");
      tbody.appendChild(tr);
      // one more level (e.g. campaign > adset > creative)
      const grandchildren = sortRows([...(child.children || [])], _sortCol, _sortDir);
      grandchildren.forEach(gc => {
        const gctr = buildRow(gc, false);
        gctr.dataset.parent = child.id;
        gctr.dataset.grandparent = parent.id;
        if (!_expandedRows.has(child.id) || !_expandedRows.has(parent.id)) gctr.classList.add("hidden");
        tbody.appendChild(gctr);
      });
    });
  });
}

function renderTable(campaignRows) {
  const tbody = document.getElementById("perfTableBody");
  tbody.innerHTML = "";
  if (!campaignRows || campaignRows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="11" class="loading-row">Sem dados disponíveis.</td></tr>`;
    return;
  }

  const sorted = sortRows([...campaignRows], _sortCol, _sortDir);

  sorted.forEach(camp => {
    tbody.appendChild(buildRow(camp, true));
    const sortedAdsets = sortRows([...(camp.children || [])], _sortCol, _sortDir);
    sortedAdsets.forEach(adset => {
      const tr = buildRow(adset, false);
      tr.dataset.parent = camp.id;
      if (!_expandedRows.has(camp.id)) tr.classList.add("hidden");
      tbody.appendChild(tr);

      const sortedCreatives = sortRows([...(adset.children || [])], _sortCol, _sortDir);
      sortedCreatives.forEach(cre => {
        const cr = buildRow(cre, false);
        cr.dataset.parent = adset.id;
        cr.dataset.grandparent = camp.id;
        if (!_expandedRows.has(adset.id) || !_expandedRows.has(camp.id)) cr.classList.add("hidden");
        tbody.appendChild(cr);
      });
    });
  });

  filterTable();
}

function buildRow(row, isTop) {
  const tr  = document.createElement("tr");
  tr.dataset.id    = row.id;
  tr.dataset.level = row.level;
  tr.dataset.sortName              = (row.name || "").toLowerCase();
  tr.dataset.sortSpend             = row.spend             ?? 0;
  tr.dataset.sortLeads             = row.leads             ?? 0;
  tr.dataset.sortCpl               = row.cpl               ?? 0;
  tr.dataset.sortCtr               = row.ctr               ?? 0;
  tr.dataset.sortConversionRate    = row.conversion_rate   ?? 0;
  tr.dataset.sortAppointments      = row.appointments      ?? 0;
  tr.dataset.sortSales             = row.sales             ?? 0;
  tr.dataset.sortRoas              = row.roas              ?? 0;
  tr.dataset.sortCac               = row.cac               ?? 0;
  tr.dataset.sortPerformanceScore  = row.performance_score ?? 0;

  const hasChildren = row.children && row.children.length > 0;
  const expanded    = _expandedRows.has(row.id);
  const expandBtn   = hasChildren
    ? `<button class="expand-btn" data-expand-id="${escapeHtml(row.id)}">${expanded ? "▾" : "▸"}</button>`
    : `<span style="display:inline-block;width:18px"></span>`;

  const levelClass = `level-${row.level}`;
  const name = escapeHtml(row.name || "");
  const scoreHtml = row.performance_score != null
    ? `<span class="score-pill score-${row.score_band || 'average'}">${row.performance_score}</span>`
    : "—";

  // Colour helpers relative to averages
  function cplClass(v) {
    if (!_currentData || !v) return "";
    const avg = _currentData.kpis.overall_cpl;
    if (v < avg * 0.8) return "val-good";
    if (v > avg * 1.3) return "val-bad";
    return "";
  }
  function ctrClass(v) {
    if (!v) return "";
    if (v >= 2)   return "val-good";
    if (v < 0.8)  return "val-bad";
    return "";
  }
  function roasClass(v) {
    if (!v) return "";
    if (v >= 4)  return "val-good";
    if (v < 1.5) return "val-bad";
    return "";
  }

  tr.innerHTML = `
    <td class="col-name ${levelClass}">${expandBtn}${name}</td>
    <td class="col-spend">${fmt_currency(row.spend)}</td>
    <td class="col-leads">${fmt_num(row.leads)}</td>
    <td class="col-cpl ${cplClass(row.cpl)}">${fmt_currency(row.cpl)}</td>
    <td class="col-ctr ${ctrClass(row.ctr)}">${fmt_pct(row.ctr)}</td>
    <td class="col-cvr">${fmt_pct(row.conversion_rate)}</td>
    <td class="col-appts">${fmt_num(row.appointments)}</td>
    <td class="col-sales">${fmt_num(row.sales)}</td>
    <td class="col-roas ${roasClass(row.roas)}">${row.roas > 0 ? fmt_x(row.roas) : "—"}</td>
    <td class="col-cac">${row.cac > 0 ? fmt_currency(row.cac) : "—"}</td>
    <td class="col-score">${scoreHtml}</td>
  `;
  return tr;
}

function toggleExpand(id) {
  if (_expandedRows.has(id)) {
    _expandedRows.delete(id);
  } else {
    _expandedRows.add(id);
  }
  if (_currentData) renderTableForLevel(_currentData);
}

document.addEventListener("click", e => {
  const btn = e.target.closest(".expand-btn");
  if (!btn) return;
  e.stopPropagation();
  toggleExpand(btn.dataset.expandId);
});

function expandAll() {
  (_currentData?.performance_table || []).forEach(c => {
    _expandedRows.add(c.id);
    (c.children || []).forEach(a => _expandedRows.add(a.id));
  });
  if (_currentData) renderTableForLevel(_currentData);
}

function collapseAll() {
  _expandedRows.clear();
  if (_currentData) renderTableForLevel(_currentData);
}

// ── Sorting ───────────────────────────────────────────────────────────────────

function sortRows(rows, col, dir) {
  return rows.sort((a, b) => {
    let va = a[col] ?? -Infinity;
    let vb = b[col] ?? -Infinity;
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return dir === "asc" ? -1 : 1;
    if (va > vb) return dir === "asc" ? 1 : -1;
    return 0;
  });
}

function sortTable(col) {
  if (_sortCol === col) {
    _sortDir = _sortDir === "asc" ? "desc" : "asc";
  } else {
    _sortCol = col;
    _sortDir = "desc";
  }
  document.querySelectorAll("thead th").forEach(th => {
    th.classList.remove("sorted-asc", "sorted-desc");
    if (th.dataset.col === col) th.classList.add(_sortDir === "asc" ? "sorted-asc" : "sorted-desc");
  });
  if (_currentData) renderTableForLevel(_currentData);
}

// ── Table search / level filter ──────────────────────────────────────────────

function filterTable() {
  const search = (document.getElementById("tableSearch").value || "").toLowerCase();
  const level  = document.getElementById("levelFilter").value;
  const tbody  = document.getElementById("perfTableBody");
  const rows   = document.querySelectorAll("#perfTableBody tr");

  rows.forEach(tr => {
    const rowLevel      = tr.dataset.level || "";
    const nameCell      = tr.querySelector(".col-name");
    const nameText      = (nameCell ? nameCell.textContent : "").toLowerCase();
    const parentId      = tr.dataset.parent;
    const grandparentId = tr.dataset.grandparent;

    const matchLevel          = level === "all" || rowLevel === level;
    const matchSearch         = !search || nameText.includes(search);
    const parentExpanded      = !parentId      || _expandedRows.has(parentId);
    const grandparentExpanded = !grandparentId || _expandedRows.has(grandparentId);

    tr.classList.toggle("hidden", !(matchLevel && matchSearch && parentExpanded && grandparentExpanded));
  });

  // When a specific level is selected, re-sort all visible rows globally in the DOM
  if (level !== "all") {
    const colKey = "sort" + _sortCol
      .split("_")
      .map((w, i) => i === 0 ? w[0].toUpperCase() + w.slice(1) : w[0].toUpperCase() + w.slice(1))
      .join("");
    const visible = [...rows].filter(tr => !tr.classList.contains("hidden"));
    visible.sort((a, b) => {
      const isName = _sortCol === "name";
      const va = isName ? (a.dataset.sortName || "") : parseFloat(a.dataset[colKey] ?? 0) || 0;
      const vb = isName ? (b.dataset.sortName || "") : parseFloat(b.dataset[colKey] ?? 0) || 0;
      if (va < vb) return _sortDir === "asc" ? -1 : 1;
      if (va > vb) return _sortDir === "asc" ? 1 : -1;
      return 0;
    });
    visible.forEach(tr => tbody.appendChild(tr));
  }
}

// ── Rankings ─────────────────────────────────────────────────────────────────

function renderRankings(rankings) {
  if (!rankings) return;
  renderRankList("top5List",    rankings.top_5 || [],    "top-item");
  renderRankList("bottom5List", rankings.bottom_5 || [], "bottom-item");
}

function renderRankList(containerId, items, cls) {
  const el = document.getElementById(containerId);
  if (!items.length) { el.innerHTML = `<p class="placeholder">Sem dados suficientes.</p>`; return; }
  el.innerHTML = items.map((it, i) => `
    <div class="rank-item ${cls}">
      <span class="rank-item-score score-pill score-${it.score_band || 'average'}">${it.performance_score ?? "?"}</span>
      <div>
        <div class="rank-item-name">${i + 1}. ${escapeHtml(it.name)}</div>
        <div class="rank-item-meta">CPL ${fmt_currency(it.cpl)} · CTR ${fmt_pct(it.ctr)} · Leads ${fmt_num(it.leads)}</div>
      </div>
    </div>
  `).join("");
}

// ── Chart ─────────────────────────────────────────────────────────────────────

function renderChart() {
  if (!_currentData) return;
  const metric   = document.getElementById("chartMetric").value;
  const campaigns = _currentData.performance_table || [];
  const labels    = campaigns.map(c => c.name);
  const values    = campaigns.map(c => c[metric] ?? 0);

  const ctx = document.getElementById("perfChart").getContext("2d");

  if (_chart) _chart.destroy();

  const metricLabels = {
    cpl: "CPL (R$)", roas: "ROAS (x)", leads: "Leads",
    ctr: "CTR (%)", spend: "Gasto (R$)",
  };

  // Color bars by performance score
  const colors = campaigns.map(c => {
    const s = c.performance_score ?? 50;
    if (s >= 80) return "rgba(34,197,94,0.75)";
    if (s >= 60) return "rgba(59,130,246,0.75)";
    if (s >= 40) return "rgba(245,158,11,0.75)";
    return "rgba(239,68,68,0.75)";
  });

  _chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label:           metricLabels[metric] || metric,
        data:            values,
        backgroundColor: colors,
        borderRadius:    5,
        borderSkipped:   false,
      }],
    },
    options: {
      responsive:          true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              if (metric === "cpl" || metric === "spend") return ` R$ ${v.toLocaleString("pt-BR", {minimumFractionDigits:2})}`;
              if (metric === "roas") return ` ${v.toFixed(2)}x`;
              if (metric === "ctr")  return ` ${v.toFixed(2)}%`;
              return ` ${v.toLocaleString("pt-BR")}`;
            },
          },
        },
      },
      scales: {
        x: { ticks: { color: "#8892a4", maxRotation: 40, font: { size: 11 } }, grid: { color: "#2e3348" } },
        y: { ticks: { color: "#8892a4", font: { size: 11 } }, grid: { color: "#2e3348" } },
      },
    },
  });
}

// ── Alerts ────────────────────────────────────────────────────────────────────

function renderAlerts(alerts) {
  const el = document.getElementById("alertsList");
  document.getElementById("alertCount").textContent = (alerts || []).length;

  if (!alerts || alerts.length === 0) {
    el.innerHTML = `<p class="placeholder">Nenhum alerta identificado. Continue monitorando!</p>`;
    return;
  }

  el.innerHTML = alerts.map(a => `
    <div class="alert-item ${a.type}">
      <div class="alert-header">
        <span class="alert-entity">${a.icon || ""} ${escapeHtml(a.entity)}</span>
        <span class="alert-metric">${escapeHtml(a.metric)}: <strong>${escapeHtml(a.value)}</strong></span>
      </div>
      <div>${escapeHtml(a.message)}</div>
      ${a.action ? `<span class="alert-action">→ ${escapeHtml(a.action)}</span>` : ""}
    </div>
  `).join("");
}

// ── AI Insights ───────────────────────────────────────────────────────────────

function renderAIInsights(ai) {
  const el      = document.getElementById("aiInsights");
  const modelEl = document.getElementById("aiModel");

  if (!ai) { el.innerHTML = `<p class="placeholder">Aguardando dados para análise...</p>`; return; }
  if (ai.error) {
    el.innerHTML = `<p class="placeholder">${escapeHtml(ai.error)}</p>`;
    modelEl.textContent = "";
    return;
  }
  if (!ai.text) { el.innerHTML = `<p class="placeholder">Gerando insights com IA...</p>`; return; }

  el.innerHTML = marked.parse(ai.text);
  modelEl.textContent = ai.model ? `via ${ai.model}` : "";
}

// ── Recommendations ───────────────────────────────────────────────────────────

function renderRecommendations(recs) {
  const el = document.getElementById("recsList");
  if (!recs || recs.length === 0) {
    el.innerHTML = `<p class="placeholder">Sem criativos suficientes para gerar recomendações.</p>`;
    return;
  }

  el.innerHTML = recs.map(r => `
    <div class="rec-item">
      <span class="rec-action ${r.action_color}">${escapeHtml(r.action)}</span>
      <div class="rec-body">
        <div class="rec-name">${escapeHtml(r.creative)}</div>
        <div class="rec-reason">${escapeHtml(r.reason)}</div>
      </div>
    </div>
  `).join("");
}

// ── Sync badge / footer ───────────────────────────────────────────────────────

function updateSyncBadge(meta) {
  const badge = document.getElementById("syncBadge");
  if (!meta) return;
  if (meta.stale) {
    badge.className = "sync-badge stale";
    badge.textContent = "⚠ Dados desatualizados";
  } else {
    badge.className = "sync-badge connected";
    badge.textContent = "✓ Conectado";
  }
}

function updateFooter(meta) {
  if (!meta || !meta.last_sync) return;
  const d = new Date(meta.last_sync);
  document.getElementById("footerSync").textContent =
    `Última sincronização: ${d.toLocaleTimeString("pt-BR")}`;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function escapeHtml(str) {
  if (str == null) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ── Init & polling ────────────────────────────────────────────────────────────

async function init() {
  await loadData();
  await loadInsights();

  setInterval(loadData,     DATA_INTERVAL);
  setInterval(loadInsights, INSIGHTS_INTERVAL);
}

document.addEventListener("DOMContentLoaded", init);
