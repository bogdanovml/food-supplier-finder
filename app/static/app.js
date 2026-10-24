/* ==========================================================================
   Food Supplier Finder — Application Logic (No emojis, Enterprise UX)
   ========================================================================== */
"use strict";

const STAGES_CONFIG = {
  plan:      { num: "01", name: "План запросов", note: "Запросы + ОКПД2 + ОКВЭД для ЕГРЮЛ" },
  serp:      { num: "02", name: "Поисковые системы", note: "Яндекс и Google XML по региону" },
  optlist:   { num: "03", name: "Каталог optlist.ru", note: "Сбор профильных компаний и карточек" },
  gosplan:   { num: "04", name: "Госконтракты 44-ФЗ", note: "Исполненные контракты по ОКПД2" },
  directory: { num: "05", name: "Реестр юрлиц (ОКВЭД)", note: "Каталог ЕГРЮЛ по отрасли: адрес, директор, выручка" },
  merge:     { num: "06", name: "Слияние и дедуп", note: "Матчинг по ИНН, ОГРН, доменам и названиям" },
  enrich:    { num: "07", name: "Обогащение сайтов", note: "Парсинг контактов, цен, ГОСТ/ХАССП" },
  company:   { num: "08", name: "Проверка юрлиц", note: "ЕГРЮЛ через DaData + опыт 44-ФЗ по ИНН" },
  structure: { num: "09", name: "Оценка релевантности", note: "Формирование обоснования выбора" },
  done:      { num: "10", name: "Готовность", note: "Фиксация результатов в аналитической БД" }
};

/* Города федерального значения — строгий гео-матчинг (зеркалит merge.py) */
const CITY_REGION_ALIASES = {
  "Москва": ["москва", "moscow"],
  "Санкт-Петербург": ["санкт-петербург", "спб", "петербург", "санкт петербург"],
  "Севастополь": ["севастополь"],
};
function isFederalCityRegion(region) { return Object.keys(CITY_REGION_ALIASES).includes(region); }
function cardInRegion(s) {
  if (!isFederalCityRegion(currentSearchRegion)) return true;
  const city = (s.city || "").toLowerCase().trim();
  if (!city) return true; // гео неизвестно — не фильтруем
  const aliases = CITY_REGION_ALIASES[currentSearchRegion];
  return aliases.some(a => a.includes(city) || city.includes(a));
}

const WORKFLOW_STATUSES = {
  new:         { label: "Новый", class: "badge-wf-new" },
  rfq_sent:    { label: "КП запрошено", class: "badge-wf-rfq_sent" },
  in_progress: { label: "В переговорах", class: "badge-wf-in_progress" },
  deal:        { label: "Контракт", class: "badge-wf-deal" },
  rejected:    { label: "Отказ", class: "badge-wf-rejected" }
};

let table = null;
let currentSearchId = null;
let currentSearchQuery = "";
let currentSearchRegion = "";
let suppliersMap = new Map(); // key -> supplier object
let searchTimerInterval = null;
let searchStartTime = 0;
let activeFilter = "all";

/* --------------------------------------------------------------------------
   Initialization
   -------------------------------------------------------------------------- */
async function init() {
  await loadRegions();
  await loadSearchHistory();
  setupEventListeners();
  buildTable();
}

async function loadRegions() {
  try {
    const res = await fetch("/api/regions");
    const regions = await res.json();
    const sel = document.getElementById("region");
    sel.innerHTML = "";
    regions.forEach(r => sel.add(new Option(r.name, r.name)));
    sel.value = "Москва";
  } catch (err) {
    console.error("Failed to load regions:", err);
  }
}

async function loadSearchHistory() {
  try {
    const res = await fetch("/api/searches");
    const list = await res.json();
    const menu = document.getElementById("history-menu");
    if (!list.length) {
      menu.innerHTML = '<li><span class="dropdown-item text-muted small">История пуста</span></li>';
      return;
    }
    menu.innerHTML = list.slice(0, 10).map(s => {
      const date = new Date(s.created_at).toLocaleString("ru", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
      return `<li><a class="dropdown-item small" href="#" onclick="restoreSearch(event, ${s.id})">
        <strong>${escapeHtml(s.query)}</strong> <span class="text-muted">(${escapeHtml(s.region)})</span>
        <div class="text-muted" style="font-size:10px">${date} • ${s.n_suppliers || 0} поставщиков</div>
      </a></li>`;
    }).join("");
  } catch (err) {
    console.error("Failed to load history:", err);
  }
}

function setupEventListeners() {
  document.getElementById("search-form").addEventListener("submit", onStartSearch);
  document.getElementById("btn-export-xlsx").addEventListener("click", () => exportCurrentSearch("xlsx"));
  document.getElementById("btn-export-csv").addEventListener("click", () => exportCurrentSearch("csv"));
  document.getElementById("btn-compare").addEventListener("click", openComparisonModal);
  document.getElementById("btn-rfq").addEventListener("click", openRfqModal);
  document.getElementById("compare-to-rfq-btn").addEventListener("click", () => {
    bootstrap.Modal.getInstance(document.getElementById("compare-modal")).hide();
    openRfqModal();
  });
  document.getElementById("rfq-regen-btn").addEventListener("click", regenerateRfq);
  document.getElementById("rfq-send-btn").addEventListener("click", dispatchRfqSmtp);
  document.getElementById("rfq-mailto-btn").addEventListener("click", dispatchRfqMailto);
  document.getElementById("btn-registry").addEventListener("click", openRegistry);

  // Filter pills
  document.querySelectorAll("#filter-pills button").forEach(btn => {
    btn.addEventListener("click", (e) => {
      document.querySelectorAll("#filter-pills button").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      applyTableFilter(btn.dataset.filter);
    });
  });
}

/* --------------------------------------------------------------------------
   Search Execution & SSE Streaming
   -------------------------------------------------------------------------- */
async function onStartSearch(e) {
  e.preventDefault();
  const query = document.getElementById("query").value.trim();
  const region = document.getElementById("region").value;
  if (query.length < 2) return;

  currentSearchQuery = query;
  currentSearchRegion = region;
  suppliersMap.clear();

  // гео-фильтр «В регионе» включен по умолчанию для городов федерального значения
  setDefaultFilter(isFederalCityRegion(region) ? "region" : "all");

  // UI state transition
  document.getElementById("zero-state").classList.add("d-none");
  document.getElementById("search-btn").disabled = true;
  document.getElementById("search-spinner").classList.remove("d-none");
  document.getElementById("search-btn-text").textContent = "Идет поиск...";
  
  document.getElementById("progress-section").classList.remove("d-none");
  document.getElementById("results-section").classList.remove("d-none");
  
  renderInitialStages();
  startSearchTimer();
  updateTableData();
  updateSelectionState();

  try {
    const response = await fetch("/api/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, region }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      // Normalize CRLF to LF so chunk split always works
      buffer = buffer.replace(/\r\n/g, "\n");

      let idx;
      while ((idx = buffer.indexOf("\n\n")) !== -1) {
        const chunk = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 2);
        if (chunk) {
          processSseChunk(chunk);
        }
      }
    }
  } catch (err) {
    console.error("Search pipeline error:", err);
    setTerminalText(`Ошибка соединения: ${err.message}`);
    updateStageStatus("done", "failed", err.message);
  } finally {
    document.getElementById("search-btn").disabled = false;
    document.getElementById("search-spinner").classList.add("d-none");
    document.getElementById("search-btn-text").textContent = "Найти поставщиков";
    stopSearchTimer();
    loadSearchHistory();
  }
}

function processSseChunk(chunk) {
  let eventType = null;
  let rawData = null;

  for (const line of chunk.split("\n")) {
    if (line.startsWith("event:")) eventType = line.slice(6).trim();
    if (line.startsWith("data:")) rawData = line.slice(5).trim();
  }

  if (!rawData || !eventType) return;

  let payload;
  try {
    payload = JSON.parse(rawData);
  } catch {
    return;
  }

  if (eventType === "stage") handleStageEvent(payload);
  else if (eventType === "supplier") handleSupplierEvent(payload.data);
  else if (eventType === "done") handleDoneEvent(payload);
  else if (eventType === "error") handlePipelineError(payload);
}

/* --------------------------------------------------------------------------
   Pipeline Stages Rendering
   -------------------------------------------------------------------------- */
function renderInitialStages() {
  const container = document.getElementById("stages");
  container.innerHTML = Object.entries(STAGES_CONFIG).map(([key, meta]) => `
    <div class="col">
      <div class="stage-card stage-pending" id="stage-card-${key}">
        <div>
          <div class="stage-card-num">${meta.num}</div>
          <div class="stage-card-title">${meta.name}</div>
        </div>
        <div class="stage-card-status" id="stage-status-${key}">В очереди</div>
      </div>
    </div>
  `).join("");

  document.getElementById("pipeline-status-badge").className = "badge badge-status-running";
  document.getElementById("pipeline-status-badge").textContent = "Выполняется";
  setTerminalText("Инициализация конвейера поиска...");
}

function handleStageEvent(ev) {
  updateStageStatus(ev.stage, ev.status, ev.message, ev.found);
}

function updateStageStatus(stageKey, status, message, found) {
  const card = document.getElementById(`stage-card-${stageKey}`);
  const statusEl = document.getElementById(`stage-status-${stageKey}`);
  if (!card || !statusEl) return;

  card.className = `stage-card stage-${status === "run" ? "run" : status}`;

  if (status === "start") {
    statusEl.textContent = "В процессе...";
    if (message) setTerminalText(message);
  } else if (status === "run") {
    statusEl.textContent = found ? `${found} найдено` : (message || "Обработка...");
    if (message) setTerminalText(message);
  } else if (status === "done") {
    statusEl.textContent = found != null ? `Готово (${found})` : "Завершено";
  } else if (status === "skipped") {
    statusEl.textContent = "Пропущено";
    if (message) setTerminalText(`Пропущен: ${message}`);
  } else if (status === "failed") {
    statusEl.textContent = "Ошибка";
    if (message) setTerminalText(`Сбой: ${message}`);
  }
}

function handlePipelineError(payload) {
  setTerminalText(`Ошибка конвейера: ${payload.message}`);
  document.getElementById("pipeline-status-badge").className = "badge bg-danger";
  document.getElementById("pipeline-status-badge").textContent = "Сбой";
}

function handleDoneEvent(payload) {
  currentSearchId = payload.search_id;
  document.getElementById("pipeline-status-badge").className = "badge badge-status-done";
  document.getElementById("pipeline-status-badge").textContent = "Завершено";
  setTerminalText(`Поиск завершен за ${payload.stats?.seconds || 0} с. Итого поставщиков: ${payload.count}.`);
  updateStageStatus("done", "done", null, payload.count);
  refreshKpis();
}

function setTerminalText(text) {
  const el = document.getElementById("terminal-ticker");
  if (el) el.textContent = text;
}

function startSearchTimer() {
  searchStartTime = Date.now();
  if (searchTimerInterval) clearInterval(searchTimerInterval);
  searchTimerInterval = setInterval(() => {
    const elapsed = Math.round((Date.now() - searchStartTime) / 1000);
    document.getElementById("progress-timer").textContent = `${elapsed} с`;
  }, 1000);
}

function stopSearchTimer() {
  if (searchTimerInterval) clearInterval(searchTimerInterval);
}

/* --------------------------------------------------------------------------
   Data Handling & KPIs
   -------------------------------------------------------------------------- */
function handleSupplierEvent(data) {
  const key = String(data.domain || data.inn || data.id || data.name);
  const existing = suppliersMap.get(key) || {};
  suppliersMap.set(key, { ...existing, ...data, id: data.id || existing.id || key });
  updateTableData();
  refreshKpis();
}

function refreshKpis(forcedSeconds = null) {
  const list = Array.from(suppliersMap.values());
  const total = list.length;
  const withContacts = list.filter(s => (s.emails && s.emails.length) || (s.phones && s.phones.length)).length;
  const withPrices = list.filter(s => s.price_min || (s.gov_contracts_value && s.gov_contracts_value > 0) || s.price_note).length;
  const govCount = list.filter(s => s.gov_contracts_count > 0).length;
  const optlistCount = list.filter(s => s.site && s.site.includes("optlist.ru")).length;

  document.getElementById("kpi-total").textContent = total;
  document.getElementById("kpi-contacts").textContent = withContacts;
  document.getElementById("kpi-prices").textContent = withPrices;
  document.getElementById("kpi-sources-breakdown").textContent = `44-ФЗ: ${govCount} • Каталог: ${optlistCount} • Органика: ${total - govCount - optlistCount}`;
  
  if (forcedSeconds != null) {
    document.getElementById("kpi-time").textContent = `${forcedSeconds} с`;
  } else if (searchStartTime > 0) {
    const elapsed = Math.round((Date.now() - searchStartTime) / 1000);
    document.getElementById("kpi-time").textContent = `${elapsed} с`;
  } else {
    document.getElementById("kpi-time").textContent = "—";
  }

  // Filter pill badges
  document.getElementById("count-all").textContent = total;
  document.getElementById("count-region").textContent = list.filter(cardInRegion).length;
  document.getElementById("count-gov").textContent = govCount;
  document.getElementById("count-optlist").textContent = optlistCount;
  document.getElementById("count-contacts").textContent = list.filter(s => s.emails && s.emails.length).length;
}

/* --------------------------------------------------------------------------
   Tabulator Table Configuration
   -------------------------------------------------------------------------- */
function buildTable() {
  table = new Tabulator("#suppliers-table", {
    layout: "fitColumns",
    responsiveLayout: false,
    height: "580px",
    pagination: "local",
    paginationSize: 25,
    selectableRows: true,
    placeholder: "Нет данных для отображения. Запустите поиск по категории.",
    rowSelectionChanged: (data, rows) => {
      updateSelectionState();
    },
    columns: [
      {
        formatter: "rowSelection",
        titleFormatter: "rowSelection",
        hozAlign: "center",
        headerHozAlign: "center",
        width: 36,
        headerSort: false
      },
      {
        title: "Поставщик",
        field: "name",
        widthGrow: 2.2,
        minWidth: 180,
        headerSort: true,
        formatter: (cell) => {
          const d = cell.getData();
          const srcValues = Object.values(d.sources || {}).join(" ");
          let sourceBadge = "";
          if (d.gov_contracts_count > 0) {
            sourceBadge = '<span class="badge badge-source badge-source-gov" title="Найден через реестр исполненных контрактов 44-ФЗ: подтверждённые поставки госзаказчикам">Госзакупки 44-ФЗ</span>';
          } else if (d.site && d.site.includes("optlist.ru")) {
            sourceBadge = '<span class="badge badge-source badge-source-catalog" title="Поставщик из B2B-каталога оптовиков optlist.ru">Каталог optlist</span>';
          } else if (srcValues.includes("ЕГРЮЛ")) {
            sourceBadge = '<span class="badge badge-source badge-source-okved" title="Действующее юрлицо отрасли из реестра ЕГРЮЛ по ОКВЭД: адрес, директор, выручка">Реестр ОКВЭД</span>';
          } else {
            sourceBadge = '<span class="badge badge-source badge-source-serp" title="Сайт компании найден через региональный поиск (Яндекс/Google); данные обогащены обходом сайта">Поиск (сайт)</span>';
          }
          const innSub = d.inn ? `<span class="text-muted small ms-1">(ИНН ${escapeHtml(d.inn)})</span>` : "";
          return `<div><div class="fw-semibold text-dark">${sourceBadge} ${escapeHtml(d.name)}</div>${innSub ? '<div style="font-size:11px">' + innSub + '</div>' : ''}</div>`;
        }
      },
      {
        title: "Город",
        field: "city",
        width: 110,
        headerSort: true,
        sorter: "string",
        formatter: (cell) => {
          const d = cell.getData();
          const val = d.city || d.region || "";
          if (!val) return '<span class="text-muted">—</span>';
          // мусорные города (индексы/мусор) показываем приглушённо как регион
          return /^[А-ЯЁA-Z]/.test(val) ? escapeHtml(val) : escapeHtml(d.region || "—");
        }
      },
      {
        title: "Контакты",
        field: "emails",
        widthGrow: 1.5,
        minWidth: 130,
        headerSort: false,
        formatter: (cell) => {
          const d = cell.getData();
          const items = [];
          if (d.emails && d.emails.length) items.push(`<span>Email: ${escapeHtml(d.emails[0])}</span>`);
          if (d.phones && d.phones.length) items.push(`<span class="text-muted">Тел: ${escapeHtml(d.phones[0])}</span>`);
          return items.length ? `<div style="font-size:12px;line-height:1.3">${items.join("<br>")}</div>` : '<span class="text-muted">—</span>';
        }
      },
      {
        title: "Мин. заказ",
        field: "min_order",
        width: 95,
        headerSort: false,
        formatter: (c) => c.getValue() ? `<span class="badge bg-light text-dark border">${escapeHtml(c.getValue())}</span>` : '<span class="text-muted">—</span>'
      },
      {
        title: "Цена по запросу",
        field: "price_note",
        widthGrow: 1.6,
        minWidth: 150,
        headerSort: false,
        formatter: (cell) => {
          const d = cell.getData();
          const note = d.price_note || (d.price_min ? `от ${d.price_min} ₽` : "");
          if (!note) return '<span class="text-muted small">По запросу КП</span>';
          const isGov = (d.price_note || "").startsWith("контракты:");
          const title = isGov ? "Фактические цены исполненных госконтрактов (44-ФЗ)" : "Цены с сайта поставщика по предмету запроса";
          return `<div class="text-truncate" style="font-size:12px" title="${escapeHtml(title)}: ${escapeHtml(note)}">${isGov ? '<span class="text-success">' : ''}${escapeHtml(note.slice(0, 90))}${isGov ? '</span>' : ''}</div>`;
        }
      },
      {
        title: "Опыт 44-ФЗ",
        field: "gov_contracts_count",
        width: 100,
        hozAlign: "center",
        headerSort: true,
        sorter: "number",
        formatter: (cell) => {
          const d = cell.getData();
          if (!d.gov_contracts_count) return '<span class="text-muted">—</span>';
          const valMln = d.gov_contracts_value ? (d.gov_contracts_value / 1_000_000).toFixed(1) : "0";
          return `<div style="line-height:1.2"><span class="fw-bold text-success">${d.gov_contracts_count} контр.</span><div style="font-size:10px" class="text-muted">${valMln} млн ₽</div></div>`;
        }
      },
      {
        title: "Обоснование ИИ",
        field: "reasons",
        widthGrow: 2.2,
        minWidth: 160,
        headerSort: false,
        formatter: (cell) => {
          const d = cell.getData();
          const reason = (d.reasons && d.reasons.length) ? d.reasons[0] : (d.description ? d.description.slice(0, 100) : "—");
          return `<div class="text-truncate text-secondary" style="font-size:12px" title="${escapeHtml(reason)}">${escapeHtml(reason)}</div>`;
        }
      },
      {
        title: "Статус",
        field: "status",
        width: 110,
        hozAlign: "center",
        headerSort: true,
        formatter: (cell) => {
          const val = cell.getValue() || "new";
          const st = WORKFLOW_STATUSES[val] || WORKFLOW_STATUSES.new;
          return `<span class="badge badge-wf ${st.class}">${st.label}</span>`;
        },
        cellClick: (e, cell) => {
          e.stopPropagation();
          cycleSupplierStatus(cell.getData());
        }
      },
      {
        title: "Баллы",
        field: "relevance",
        width: 75,
        hozAlign: "center",
        headerSort: true,
        sorter: "number",
        formatter: (cell) => {
          const val = Number(cell.getValue()) || 0;
          return `<span class="fw-bold">${val}</span><span class="text-muted small">/100</span>`;
        }
      },
      {
        title: "",
        width: 65,
        hozAlign: "center",
        headerSort: false,
        formatter: () => `<button class="btn btn-sm btn-outline-secondary" style="font-size:11px;padding:2px 8px">Досье</button>`,
        cellClick: (e, cell) => {
          e.stopPropagation();
          openDossier(cell.getData());
        }
      }
    ],
    initialSort: [{ column: "relevance", dir: "desc" }]
  });

  table.on("rowSelectionChanged", (data, rows) => {
    updateSelectionState();
  });

  table.on("rowClick", (e, row) => {
    openDossier(row.getData());
  });
}

function updateTableData() {
  if (!table) return;
  const list = Array.from(suppliersMap.values());
  table.setData(list);
  applyTableFilter(activeFilter);
}

function setDefaultFilter(key) {
  document.querySelectorAll("#filter-pills button").forEach(b => {
    b.classList.toggle("active", b.dataset.filter === key);
  });
  applyTableFilter(key);
}

function applyTableFilter(filterKey) {
  activeFilter = filterKey;
  if (!table) return;
  if (filterKey === "all") {
    table.clearFilter();
  } else if (filterKey === "region") {
    table.setFilter((data) => cardInRegion(data));
  } else if (filterKey === "gov") {
    table.setFilter("gov_contracts_count", ">", 0);
  } else if (filterKey === "optlist") {
    table.setFilter((data) => data.site && data.site.includes("optlist.ru"));
  } else if (filterKey === "contacts") {
    table.setFilter((data) => data.emails && data.emails.length > 0);
  }
}

function updateSelectionState() {
  const selected = table ? table.getSelectedData() : [];
  const count = selected.length;
  const pill = document.getElementById("selection-pill");
  const countEl = document.getElementById("selection-count");
  const compareBtn = document.getElementById("btn-compare");
  const rfqBtn = document.getElementById("btn-rfq");

  if (count > 0) {
    pill.classList.remove("d-none");
    countEl.textContent = count;
    compareBtn.disabled = count < 2 || count > 5;
    compareBtn.textContent = count >= 2 ? `Сравнить (${count})` : "Сравнить выбранных";
    rfqBtn.disabled = false;
  } else {
    pill.classList.add("d-none");
    compareBtn.disabled = true;
    compareBtn.textContent = "Сравнить выбранных";
    rfqBtn.disabled = suppliersMap.size === 0;
  }
}

/* --------------------------------------------------------------------------
   Dossier (Offcanvas) & Provenance
   -------------------------------------------------------------------------- */
async function openDossier(row) {
  let supplier = row;
  if (row.id && typeof row.id === "number") {
    try {
      const res = await fetch(`/api/suppliers/${row.id}`);
      if (res.ok) supplier = await res.json();
    } catch {}
  }

  document.getElementById("sc-name").textContent = supplier.name;
  document.getElementById("sc-pretitle").textContent = supplier.inn ? `ИНН ${supplier.inn}` : "Карточка контрагента";

  const st = WORKFLOW_STATUSES[supplier.status || "new"] || WORKFLOW_STATUSES.new;
  const container = document.getElementById("sc-body");

  container.innerHTML = `
    <!-- Workflow status -->
    <div class="dossier-section">
      <div class="d-flex justify-content-between align-items-center mb-2">
        <span class="dossier-section-title mb-0 border-0 p-0">Статус в воронке закупки</span>
        <select class="form-select form-select-sm" style="width:180px" id="dossier-status-select">
          <option value="new" ${supplier.status === "new" ? "selected" : ""}>Новый</option>
          <option value="rfq_sent" ${supplier.status === "rfq_sent" ? "selected" : ""}>КП запрошено</option>
          <option value="in_progress" ${supplier.status === "in_progress" ? "selected" : ""}>В переговорах</option>
          <option value="deal" ${supplier.status === "deal" ? "selected" : ""}>Контракт</option>
          <option value="rejected" ${supplier.status === "rejected" ? "selected" : ""}>Отказ</option>
        </select>
      </div>
    </div>

    <!-- Corporate info -->
    <div class="dossier-section">
      <div class="dossier-section-title">Юридическое лицо (ЕГРЮЛ)</div>
      <div class="dossier-row">
        <span class="dossier-row-label">Наименование</span>
        <span class="dossier-row-value">${escapeHtml(supplier.legal_name || supplier.name)}</span>
      </div>
      ${supplier.inn ? `
      <div class="dossier-row">
        <span class="dossier-row-label">ИНН</span>
        <span class="dossier-row-value font-monospace">${escapeHtml(supplier.inn)}</span>
      </div>` : ""}
      ${supplier.ogrn ? `
      <div class="dossier-row">
        <span class="dossier-row-label">ОГРН</span>
        <span class="dossier-row-value font-monospace">${escapeHtml(supplier.ogrn)}</span>
      </div>` : ""}
      ${supplier.director ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Руководитель</span>
        <span class="dossier-row-value">${escapeHtml(supplier.director)}</span>
      </div>` : ""}
      ${supplier.okved ? `
      <div class="dossier-row">
        <span class="dossier-row-label">ОКВЭД</span>
        <span class="dossier-row-value">${escapeHtml(String(supplier.okved).slice(0, 90))}</span>
      </div>` : ""}
      ${supplier.reg_date ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Дата регистрации</span>
        <span class="dossier-row-value">${escapeHtml(supplier.reg_date)}</span>
      </div>` : ""}
      ${supplier.company_status ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Статус</span>
        <span class="dossier-row-value">${supplier.company_status === "Действующая" ? '<span class="text-success fw-semibold">Действующая</span>' : `<span class="text-danger fw-semibold">${escapeHtml(supplier.company_status)}</span>`}</span>
      </div>` : ""}
      ${supplier.revenue_note ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Выручка</span>
        <span class="dossier-row-value fw-semibold">${escapeHtml(supplier.revenue_note)}</span>
      </div>` : ""}
      ${supplier.staff ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Штат</span>
        <span class="dossier-row-value">${escapeHtml(supplier.staff)} чел.</span>
      </div>` : ""}
      <div class="dossier-row">
        <span class="dossier-row-label">Регион / Город</span>
        <span class="dossier-row-value">${escapeHtml(supplier.city || supplier.region || "Не указан")}</span>
      </div>
      ${supplier.site ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Сайт</span>
        <span class="dossier-row-value"><a href="${escapeHtml(supplier.site)}" target="_blank" class="text-decoration-none">${escapeHtml(supplier.site)}</a></span>
      </div>` : ""}
    </div>

    <!-- Contacts -->
    <div class="dossier-section">
      <div class="dossier-section-title">Прямые контакты</div>
      <div class="dossier-row">
        <span class="dossier-row-label">Email адреса</span>
        <span class="dossier-row-value">${(supplier.emails && supplier.emails.length) ? supplier.emails.map(e => `<a href="mailto:${escapeHtml(e)}">${escapeHtml(e)}</a>`).join(", ") : "Не обнаружен"}</span>
      </div>
      <div class="dossier-row">
        <span class="dossier-row-label">Телефоны</span>
        <span class="dossier-row-value">${(supplier.phones && supplier.phones.length) ? supplier.phones.map(escapeHtml).join("<br>") : "Не обнаружен"}</span>
      </div>
    </div>

    <!-- Commercial terms -->
    <div class="dossier-section">
      <div class="dossier-section-title">Коммерческие условия</div>
      <div class="dossier-row">
        <span class="dossier-row-label">Мин. заказ (MOQ)</span>
        <span class="dossier-row-value fw-semibold">${escapeHtml(supplier.min_order || "По согласованию")}</span>
      </div>
      <div class="dossier-row">
        <span class="dossier-row-label">Ценовой ориентир</span>
        <span class="dossier-row-value">${escapeHtml(supplier.price_note || (supplier.price_min ? supplier.price_min + " ₽" : "По запросу"))}</span>
      </div>
      <div class="dossier-row">
        <span class="dossier-row-label">Опыт 44-ФЗ</span>
        <span class="dossier-row-value">${supplier.gov_contracts_count ? `${supplier.gov_contracts_count} контрактов на сумму ${(supplier.gov_contracts_value || 0).toLocaleString("ru")} ₽` : "Нет данных в реестре"}</span>
      </div>
      <div class="dossier-row">
        <span class="dossier-row-label">Условия доставки</span>
        <span class="dossier-row-value">${escapeHtml(supplier.delivery || "Уточняются при запросе КП")}</span>
      </div>
      <div class="dossier-row">
        <span class="dossier-row-label">Стандарты / ГОСТ</span>
        <span class="dossier-row-value">${(supplier.certificates && supplier.certificates.length) ? supplier.certificates.map(escapeHtml).join(", ") : "Декларация соответствия ТР ТС"}</span>
      </div>
    </div>

    <!-- Justification -->
    ${(supplier.maps_url || supplier.checko_url) ? `
    <div class="dossier-section">
      <div class="dossier-section-title">Внешние профили и репутация</div>
      ${supplier.maps_rating ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Яндекс Карты</span>
        <span class="dossier-row-value">
          <span class="rating-badge">${supplier.maps_rating.toFixed(1)}</span>
          <span class="text-muted small">${supplier.maps_reviews || 0} отзывов</span>
        </span>
      </div>` : ""}
      ${supplier.maps_url ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Профиль на Картах</span>
        <span class="dossier-row-value"><a href="${escapeHtml(supplier.maps_url)}" target="_blank" class="text-decoration-none">Открыть отзывы и адрес</a></span>
      </div>` : ""}
      ${supplier.checko_url ? `
      <div class="dossier-row">
        <span class="dossier-row-label">Карточка ЕГРЮЛ (checko)</span>
        <span class="dossier-row-value"><a href="${escapeHtml(supplier.checko_url)}" target="_blank" class="text-decoration-none">Реквизиты, учредители, арбитражи</a></span>
      </div>` : ""}
    </div>` : ""}

    ${supplier.reasons && supplier.reasons.length ? `
    <div class="dossier-section">
      <div class="dossier-section-title">Аналитическое обоснование выбора</div>
      <div class="p-3 bg-light rounded text-secondary" style="font-size:13px;line-height:1.45">
        ${supplier.reasons.map(r => `• ${escapeHtml(r)}`).join("<br>")}
      </div>
    </div>` : ""}

    <!-- Provenance / Source Audit -->
    <div class="dossier-section">
      <div class="dossier-section-title">Аудит источников данных (Provenance)</div>
      <div class="bg-light p-3 rounded" style="font-size:12px">
        ${supplier.sources && Object.keys(supplier.sources).length ? Object.entries(supplier.sources).map(([f, src]) => `
          <div class="mb-1"><strong>${escapeHtml(f)}:</strong> <span class="text-muted font-monospace">${escapeHtml(src)}</span></div>
        `).join("") : '<span class="text-muted">Первичный сбор через региональный поисковый индекс</span>'}
      </div>
    </div>

    <!-- Private Notes -->
    <div class="dossier-section">
      <div class="dossier-section-title">Внутренние заметки закупщика</div>
      <div id="notes-list" class="mb-3">
        ${(supplier.notes && supplier.notes.length) ? supplier.notes.map(n => `
          <div class="p-2 border rounded mb-2 bg-light">
            <div style="font-size:13px">${escapeHtml(n.text)}</div>
            <div class="text-muted" style="font-size:10px">${new Date(n.created_at).toLocaleString("ru")}</div>
          </div>
        `).join("") : '<div class="text-muted small">Нет сохраненных заметок</div>'}
      </div>
      <form id="add-note-form" class="d-flex gap-2">
        <input type="text" id="new-note-text" class="form-control form-control-sm" placeholder="Добавить комментарий по поставщику..." required ${supplier.id ? "" : "disabled"}>
        <button type="submit" class="btn btn-sm btn-primary-custom" ${supplier.id ? "" : "disabled"}>Сохранить</button>
      </form>
    </div>
  `;

  // Status select handler
  document.getElementById("dossier-status-select").addEventListener("change", async (e) => {
    const nextStatus = e.target.value;
    if (supplier.id) {
      await updateSupplierStatus(supplier.id, nextStatus);
      supplier.status = nextStatus;
      suppliersMap.set(String(supplier.domain || supplier.inn || supplier.id), supplier);
      updateTableData();
      showToast("Статус контрагента обновлен");
    }
  });

  // Notes submit handler
  document.getElementById("add-note-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("new-note-text");
    const text = input.value.trim();
    if (!text || !supplier.id) return;
    
    try {
      const res = await fetch(`/api/suppliers/${supplier.id}/notes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text })
      });
      const data = await res.json();
      input.value = "";
      document.getElementById("notes-list").innerHTML = data.notes.map(n => `
        <div class="p-2 border rounded mb-2 bg-light">
          <div style="font-size:13px">${escapeHtml(n.text)}</div>
          <div class="text-muted" style="font-size:10px">${new Date(n.created_at).toLocaleString("ru")}</div>
        </div>
      `).join("");
      showToast("Заметка сохранена");
    } catch (err) {
      alert("Не удалось сохранить заметку: " + err.message);
    }
  });

  new bootstrap.Offcanvas(document.getElementById("supplier-canvas")).show();
}

async function cycleSupplierStatus(supplier) {
  if (!supplier.id) return;
  const order = ["new", "rfq_sent", "in_progress", "deal", "rejected"];
  const currentIdx = order.indexOf(supplier.status || "new");
  const nextStatus = order[(currentIdx + 1) % order.length];
  await updateSupplierStatus(supplier.id, nextStatus);
  supplier.status = nextStatus;
  suppliersMap.set(String(supplier.domain || supplier.inn || supplier.id), supplier);
  updateTableData();
  showToast(`Статус ${supplier.name}: ${WORKFLOW_STATUSES[nextStatus].label}`);
}

async function updateSupplierStatus(id, status) {
  try {
    await fetch(`/api/suppliers/${id}/status`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status })
    });
  } catch (err) {
    console.error("Failed to patch status:", err);
  }
}

/* --------------------------------------------------------------------------
   Side-by-Side Comparison Matrix
   -------------------------------------------------------------------------- */
function openComparisonModal() {
  const selected = table.getSelectedData();
  if (selected.length < 2) {
    alert("Выберите минимум 2 поставщиков для сравнения (галочки слева).");
    return;
  }
  if (selected.length > 5) {
    alert("Для наглядного сравнения выберите не более 5 поставщиков.");
    return;
  }

  const modalBody = document.getElementById("compare-modal-body");
  const headers = selected.map(s => `
    <td class="header-col">
      <div class="fw-bold">${escapeHtml(s.name)}</div>
      <div class="small text-muted">${s.city || s.region || "—"}</div>
    </td>
  `).join("");

  const makeRow = (label, fn) => `
    <tr>
      <th>${label}</th>
      ${selected.map(s => `<td>${fn(s)}</td>`).join("")}
    </tr>
  `;

  modalBody.innerHTML = `
    <table class="comparison-table">
      <thead>
        <tr>
          <th>Параметр</th>
          ${headers}
        </tr>
      </thead>
      <tbody>
        ${makeRow("Юрлицо / ИНН", s => s.inn ? `ИНН ${escapeHtml(s.inn)}` : "—")}
        ${makeRow("Мин. заказ", s => s.min_order ? `<span class="badge bg-light text-dark border">${escapeHtml(s.min_order)}</span>` : "По согласованию")}
        ${makeRow("Ценовой диапазон", s => s.price_note ? escapeHtml(s.price_note.slice(0, 100)) : (s.price_min ? `${s.price_min} ₽` : "По запросу"))}
        ${makeRow("Опыт в 44-ФЗ", s => s.gov_contracts_count ? `<strong class="text-success">${s.gov_contracts_count} контр.</strong> (${(s.gov_contracts_value/1_000_000).toFixed(1)} млн ₽)` : "Нет в реестре")}
        ${makeRow("Доставка", s => escapeHtml((s.delivery || "Самовывоз / ТК").slice(0, 120)))}
        ${makeRow("Стандарты качества", s => (s.certificates && s.certificates.length) ? s.certificates.map(escapeHtml).join(", ") : "Декларация ТР ТС")}
        ${makeRow("Прямой Email", s => (s.emails && s.emails.length) ? `<a href="mailto:${s.emails[0]}">${escapeHtml(s.emails[0])}</a>` : '<span class="text-muted">Не найден</span>')}
        ${makeRow("Телефон", s => (s.phones && s.phones.length) ? escapeHtml(s.phones[0]) : "—")}
        ${makeRow("Рейтинг (Я.Карты)", s => s.maps_rating ? `<strong>${s.maps_rating.toFixed(1)}</strong> <span class="text-muted small">(${s.maps_reviews || 0} отз.)</span>` : "—")}
        ${makeRow("Обоснование ИИ", s => (s.reasons && s.reasons.length) ? escapeHtml(s.reasons[0]) : "—")}
      </tbody>
    </table>
  `;

  new bootstrap.Modal(document.getElementById("compare-modal")).show();
}

/* --------------------------------------------------------------------------
   RFQ Modal & Dispatch
   -------------------------------------------------------------------------- */
async function openRfqModal() {
  if (!currentSearchId) {
    alert("Сначала дождитесь сохранения поисковой сессии.");
    return;
  }
  let selected = table ? table.getSelectedData() : [];
  if (!selected.length) {
    selected = Array.from(suppliersMap.values()).filter(s => s.emails && s.emails.length).slice(0, 5);
  }
  if (!selected.length) {
    alert("Среди найденных поставщиков нет подтвержденных email-адресов.");
    return;
  }

  const chipsEl = document.getElementById("rfq-recipients-chips");
  chipsEl.innerHTML = selected.map(s => `
    <span class="badge bg-white text-dark border p-2">
      <strong>${escapeHtml(s.name)}</strong>: ${(s.emails && s.emails.length) ? escapeHtml(s.emails[0]) : "нет email"}
    </span>
  `).join("");

  document.getElementById("rfq-send-btn").dataset.ids = JSON.stringify(selected.map(s => s.id));
  new bootstrap.Modal(document.getElementById("rfq-modal")).show();
  await generateRfqDraft(selected.map(s => s.id));
}

async function regenerateRfq() {
  const ids = JSON.parse(document.getElementById("rfq-send-btn").dataset.ids || "[]");
  await generateRfqDraft(ids);
}

async function generateRfqDraft(ids) {
  const extra = document.getElementById("rfq-extra").value;
  document.getElementById("rfq-subject").value = "Генерация темы письма...";
  document.getElementById("rfq-body").value = "Генерация текста делового запроса через ИИ...";
  document.getElementById("rfq-send-btn").disabled = true;

  try {
    const res = await fetch("/api/rfq/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        search_id: currentSearchId,
        supplier_ids: ids,
        extra_requirements: extra
      })
    });
    if (!res.ok) throw new Error("Сбой формирования письма");
    const data = await res.json();
    document.getElementById("rfq-subject").value = data.subject;
    document.getElementById("rfq-body").value = data.body;
    document.getElementById("rfq-send-btn").disabled = false;
    document.getElementById("rfq-send-btn").dataset.recipients = JSON.stringify(data.recipients || []);
  } catch (err) {
    document.getElementById("rfq-body").value = `Ошибка генерации черновика: ${err.message}`;
  }
}

async function dispatchRfqSmtp() {
  const btn = document.getElementById("rfq-send-btn");
  const ids = JSON.parse(btn.dataset.ids || "[]");
  const subject = document.getElementById("rfq-subject").value;
  const body = document.getElementById("rfq-body").value;
  const extra = document.getElementById("rfq-extra").value;

  btn.disabled = true;
  btn.textContent = "Отправка...";
  document.getElementById("rfq-dispatch-status").textContent = "Установка защищенного SMTP-соединения...";

  try {
    const res = await fetch("/api/rfq/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        search_id: currentSearchId,
        supplier_ids: ids,
        extra_requirements: extra,
        subject,
        body
      })
    });
    const result = await res.json();
    if (!res.ok) throw new Error(result.detail || "Сбой отправки");
    
    document.getElementById("rfq-dispatch-status").textContent = `Успешно отправлено: ${result.sent}, сбоев: ${result.failed}`;
    showToast(`Запрос отправлен ${result.sent} адресатам`);
    
    // Update local statuses
    ids.forEach(id => {
      const sup = Array.from(suppliersMap.values()).find(s => s.id === id);
      if (sup) sup.status = "rfq_sent";
    });
    updateTableData();
  } catch (err) {
    document.getElementById("rfq-dispatch-status").textContent = `Ошибка: ${err.message}`;
    alert(`Не удалось отправить через SMTP: ${err.message}. Воспользуйтесь кнопкой "Открыть в почтовой программе".`);
  } finally {
    btn.disabled = false;
    btn.textContent = "Отправить через SMTP";
  }
}

function dispatchRfqMailto() {
  const recipients = JSON.parse(document.getElementById("rfq-send-btn").dataset.recipients || "[]");
  const subject = encodeURIComponent(document.getElementById("rfq-subject").value);
  const body = encodeURIComponent(document.getElementById("rfq-body").value);
  const to = recipients.join(",");
  window.open(`mailto:${to}?subject=${subject}&body=${body}`, "_blank");
}

/* --------------------------------------------------------------------------
   Export & Persistence
   -------------------------------------------------------------------------- */
function exportCurrentSearch(type) {
  if (!currentSearchId) {
    alert("Сначала выполните поиск или выберите сессию из истории.");
    return;
  }
  window.location.href = `/api/searches/${currentSearchId}/export/${type}`;
}

window.restoreSearch = async function (e, searchId) {
  e.preventDefault();
  try {
    const [searchesRes, suppliersRes] = await Promise.all([
      fetch("/api/searches"),
      fetch(`/api/searches/${searchId}/suppliers`)
    ]);
    const searches = await searchesRes.json();
    const suppliers = await suppliersRes.json();
    const s = searches.find(item => item.id === searchId);
    
    if (s) {
      document.getElementById("query").value = s.query;
      document.getElementById("region").value = s.region;
      currentSearchId = s.id;
      currentSearchQuery = s.query;
      currentSearchRegion = s.region;
    }

    suppliersMap.clear();
    suppliers.forEach(sup => suppliersMap.set(String(sup.domain || sup.inn || sup.id), sup));
    
    document.getElementById("zero-state").classList.add("d-none");
    document.getElementById("progress-section").classList.add("d-none");
    document.getElementById("results-section").classList.remove("d-none");

    updateTableData();
    let durationSec = null;
    try {
      const statsObj = typeof s.stats === "string" ? JSON.parse(s.stats) : s.stats;
      durationSec = statsObj?.seconds;
    } catch {}
    refreshKpis(durationSec);
    updateSelectionState();
    showToast(`Восстановлен поиск: ${s ? s.query : searchId}`);
  } catch (err) {
    alert("Ошибка загрузки данных сессии: " + err.message);
  }
};

/* --------------------------------------------------------------------------
   Utility Functions
   -------------------------------------------------------------------------- */
function escapeHtml(str) {
  if (str == null) return "";
  const div = document.createElement("div");
  div.textContent = String(str);
  return div.innerHTML;
}

function showToast(msg) {
  const toastEl = document.getElementById("app-toast");
  document.getElementById("toast-message").textContent = msg;
  bootstrap.Toast.getOrCreateInstance(toastEl, { delay: 3500 }).show();
}

// Start application on load
document.addEventListener("DOMContentLoaded", init);

/* --------------------------------------------------------------------------
   Global Supplier Registry (накопительная база поставщиков)
   -------------------------------------------------------------------------- */
let registryTable = null;

async function openRegistry() {
  try {
    const res = await fetch("/api/registry");
    const data = await res.json();
    document.getElementById("registry-stats").innerHTML = `
      <div class="d-flex gap-3 mb-2 small text-muted">
        <span>Поставщиков в базе: <strong class="text-dark">${data.stats.suppliers}</strong></span>
        <span>Категорий товаров: <strong class="text-dark">${data.stats.product_queries}</strong></span>
        <span>Связей поставщик-товар: <strong class="text-dark">${data.stats.links}</strong></span>
      </div>
    `;
    if (!registryTable) {
      registryTable = new Tabulator("#registry-table", {
        layout: "fitColumns",
        height: "65vh",
        pagination: "local",
        paginationSize: 20,
        placeholder: "База пуста — выполните первый поиск, поставщики накопятся автоматически.",
        columns: [
          { title: "Поставщик", field: "name", widthGrow: 1.8, formatter: (cell) => {
              const d = cell.getData();
              const inn = d.inn ? ` <span class="text-muted small">(${escapeHtml(d.inn)})</span>` : "";
              return `<div class="fw-semibold">${escapeHtml(d.name)}${inn}</div>`;
            } },
          { title: "Город", field: "city", width: 110, formatter: (c) => escapeHtml(c.getValue() || "—") },
          { title: "Товары / категории", field: "products", widthGrow: 2, formatter: (cell) => {
              const arr = cell.getValue() || [];
              return arr.length ? `<div class="small">${arr.map(escapeHtml).join(", ")}</div>` : '<span class="text-muted">—</span>';
            } },
          { title: "Контракты 44-ФЗ", field: "gov_contracts_count", width: 120, hozAlign: "center", sorter: "number", formatter: (cell) => {
              const d = cell.getData();
              if (!d.gov_contracts_count) return '<span class="text-muted">—</span>';
              return `<div class="fw-bold text-success">${d.gov_contracts_count} шт<div class="small text-muted" style="font-weight:400">${(d.gov_contracts_value/1_000_000).toFixed(1)} млн ₽</div></div>`;
            } },
          { title: "Email", field: "emails", widthGrow: 1.2, formatter: (cell) => {
              const arr = cell.getValue() || [];
              return arr.length ? `<a href="mailto:${escapeHtml(arr[0])}">${escapeHtml(arr[0])}</a>` : '<span class="text-muted">—</span>';
            } },
          { title: "Поисков", field: "searches_count", width: 90, hozAlign: "center", sorter: "number",
            formatter: (c) => `<span class="badge bg-light text-dark border">${c.getValue()}</span>` },
        ],
        initialSort: [{ column: "gov_contracts_count", dir: "desc" }],
      });
    }
    const modalEl = document.getElementById("registry-modal");
    const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    modalEl.addEventListener("shown.bs.modal", () => {
      registryTable.setData(data.suppliers).then(() => registryTable.redraw(true));
    }, { once: true });
    modal.show();
  } catch (err) {
    alert("Не удалось загрузить базу поставщиков: " + err.message);
  }
}
