/* Food Supplier Finder — SPA: SSE-поиск, Tabulator, карточка, RFQ, экспорт. */
"use strict";

const $ = (sel) => document.querySelector(sel);

const STAGE_META = {
  plan:     { name: "1. Планирование запросов", note: "LLM строит поисковые запросы" },
  serp:     { name: "2. Поисковики", note: "Яндекс + Google XML (регион)" },
  optlist:  { name: "3. Каталог optlist.ru", note: "оптовики + их карточки" },
  gosplan:  { name: "4. Госконтракты 44-ФЗ", note: "реальные поставки и цены" },
  merge:    { name: "5. Слияние", note: "дедуп: домен + ИНН + имя" },
  enrich:   { name: "6. Обход сайтов", note: "контакты, цены, сертификаты" },
  structure:{ name: "7. Оценка", note: "почему с каждым стоит связаться" },
  done:     { name: "8. Готово", note: "результаты в базе, экспорт доступен" },
};

let table = null;
let currentSearchId = null;
let supplierRows = new Map();   // domain|name -> row
let selectedRows = new Set();
let searchTimer = null;

/* ------------------------------- init ------------------------------- */

async function init() {
  const regions = await (await fetch("/api/regions")).json();
  const sel = $("#region");
  for (const r of regions) sel.add(new Option(r.name, r.name));
  sel.value = "Москва";

  const searches = await (await fetch("/api/searches")).json();
  renderHistory(searches);

  $("#search-form").addEventListener("submit", startSearch);
  $("#btn-export-xlsx").addEventListener("click", () => exportFile("xlsx"));
  $("#btn-export-csv").addEventListener("click", () => exportFile("csv"));
  $("#btn-rfq").addEventListener("click", openRfq);
  $("#rfq-regen").addEventListener("click", openRfq);
  $("#rfq-send-btn").addEventListener("click", sendRfq);
}

function renderHistory(searches) {
  if (!searches.length) { $("#history-box").textContent = ""; return; }
  const links = searches.slice(0, 5).map(s => {
    const d = new Date(s.created_at + "Z");
    return `<a href="#" class="d-block" data-sid="${s.id}" onclick="loadSearch(event, ${s.id})">
      «${escapeHtml(s.query)}» · ${escapeHtml(s.region)} · ${s.n_suppliers} пост.</a>`;
  }).join("");
  $("#history-box").innerHTML = `<div class="text-secondary-emphasis fw-semibold">История поисков</div>${links}`;
}

window.loadSearch = async function (event, searchId) {
  event.preventDefault();
  const search = (await (await fetch("/api/searches")).json()).find(s => s.id === searchId);
  const suppliers = await (await fetch(`/api/searches/${searchId}/suppliers`)).json();
  currentSearchId = searchId;
  $("#progress-section").classList.add("d-none");
  $("#results-section").classList.remove("d-none");
  suppliersRows(suppliers);
  $("#results-count").textContent = suppliers.length + " поставщиков";
  enableResultButtons(true);
};

/* ------------------------------- поиск (SSE) ------------------------------- */

async function startSearch(e) {
  e.preventDefault();
  const query = $("#query").value.trim();
  if (query.length < 2) return;
  $("#search-btn").disabled = true;
  $("#results-section").classList.add("d-none");
  $("#progress-section").classList.remove("d-none");
  supplierRows = new Map();
  selectedRows = new Set();
  renderStagesStart();
  startTimer();

  try {
    const resp = await fetch("/api/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, region: $("#region").value }),
    });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf("\n\n")) !== -1) {
        const chunk = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        handleSseChunk(chunk);
      }
    }
  } catch (err) {
    stageStatus("done", "failed", "соединение потеряно: " + err.message);
  } finally {
    $("#search-btn").disabled = false;
    stopTimer();
  }
}

function handleSseChunk(chunk) {
  let eventType = null, data = null;
  for (const line of chunk.split("\n")) {
    if (line.startsWith("event:")) eventType = line.slice(6).trim();
    if (line.startsWith("data:")) data = line.slice(5).trim();
  }
  if (!data) return;
  let ev;
  try { ev = JSON.parse(data); } catch { return; }
  if (eventType === "stage") return handleStage(ev);
  if (eventType === "supplier") return handleSupplier(ev.data);
  if (eventType === "done") return handleDone(ev);
  if (eventType === "error") return stageStatus("done", "failed", ev.message);
  if (eventType === "ping") return;
}

/* ------------------------------- этапы ------------------------------- */

function renderStagesStart() {
  const box = $("#stages");
  box.innerHTML = Object.entries(STAGE_META).map(([k, m]) => `
    <div class="col"><div class="stage-card stage-pending" id="stage-${k}">
      <div class="stage-name">${m.name}</div>
      <div class="stage-note" id="stage-${k}-note">ожидает…</div>
    </div></div>`).join("");
  $("#progress-log").innerHTML = "";
}

function handleStage(ev) {
  const meta = STAGE_META[ev.stage];
  const el = $("#stage-" + ev.stage);
  if (!el) return;
  el.className = "stage-card stage-" + (ev.status === "run" ? "run" : ev.status === "pending" ? "pending" : ev.status);
  const note = $("#stage-" + ev.stage + "-note");
  let text = "";
  if (ev.status === "done") text = ev.found != null ? `✓ найдено: ${ev.found}` : "✓ готово";
  else if (ev.status === "start") text = "выполняется…";
  else if (ev.status === "run") text = ev.message || "…";
  else if (ev.status === "skipped") text = "пропущен: " + (ev.message || "");
  else if (ev.status === "failed") text = "✗ ошибка";
  note.textContent = text;
  if (ev.message && ev.status !== "skipped") logLine(`${meta?.name || ev.stage}: ${ev.message}`);
  if (ev.status === "start" && ev.message) logLine(`▶ ${meta?.name}: ${ev.message}`);
}

function stageStatus(stage, status, msg) {
  handleStage({ stage, status, message: msg });
}

function logLine(text) {
  const log = $("#progress-log");
  const line = document.createElement("div");
  line.textContent = "· " + text;
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}

function startTimer() {
  const t0 = Date.now();
  stopTimer();
  searchTimer = setInterval(() => {
    const sec = Math.round((Date.now() - t0) / 1000);
    $("#progress-timer").textContent = sec + " с";
  }, 1000);
}
function stopTimer() { if (searchTimer) clearInterval(searchTimer); }

/* ------------------------------- таблица ------------------------------- */

function handleSupplier(data) {
  const key = data.domain || data.inn || data.name;
  supplierRows.set(key, { ...data, id: key });
  if (!table) buildTable();
  table.setData(Array.from(supplierRows.values()));
  $("#results-section").classList.remove("d-none");
  $("#results-count").textContent = supplierRows.size + " поставщиков";
}

function suppliersRows(rows) {
  supplierRows = new Map(rows.map(r => [String(r.domain || r.inn || r.id), { ...r, id: r.domain || r.inn || r.id }]));
  if (!table) buildTable();
  table.setData(Array.from(supplierRows.values()));
}

function buildTable() {
  table = new Tabulator("#suppliers-table", {
    layout: "fitColumns",
    height: "70vh",
    pagination: "local", paginationSize: 25,
    selectable: "highlight",
    rowClick: (e, row) => openSupplier(row.getData()),
    columns: [
      { formatter: "rownum", hozAlign: "center", width: 45, headerSort: false },
      {
        title: "Поставщик", field: "name", widthGrow: 2, headerSort: true,
        formatter: (cell) => {
          const d = cell.getData();
          const src = d.gov_contracts_count ? `<span class="badge text-bg-success badge-src">гос</span> `
            : d.site?.includes("optlist.ru") ? `<span class="badge text-bg-info badge-src">каталог</span> `
            : `<span class="badge text-bg-secondary badge-src">поиск</span> `;
          return src + escapeHtml(d.name);
        },
      },
      { title: "Город", field: "city", width: 110, headerSort: true },
      {
        title: "Контакты", field: "emails", widthGrow: 1.2, headerSort: false,
        formatter: (cell) => {
          const d = cell.getData();
          const parts = [];
          if (d.emails?.length) parts.push("✉ " + escapeHtml(d.emails[0]));
          if (d.phones?.length) parts.push("☎ " + escapeHtml(d.phones[0]));
          return parts.join("<br>") || "<span class='text-secondary'>—</span>";
        },
      },
      { title: "Мин. заказ", field: "min_order", width: 100, headerSort: false,
        formatter: (c) => c.getValue() ? escapeHtml(c.getValue()) : "—" },
      { title: "Цены", field: "price_note", widthGrow: 1.4, headerSort: false,
        formatter: (c) => c.getValue() ? escapeHtml(c.getValue()) : "—" },
      { title: "Гос. контракты", field: "gov_contracts_count", width: 90, hozAlign: "center", headerSort: true,
        formatter: (c) => c.getValue() ? `<b class="text-success">${c.getValue()}</b>` : "—" },
      { title: "Рейтинг", field: "relevance", width: 85, hozAlign: "center", headerSort: true,
        formatter: (c) => {
          const v = c.getValue();
          const color = v >= 40 ? "success" : v >= 15 ? "secondary" : "light";
          return `<span class="badge rounded-pill text-bg-${color}">${v}</span>`;
        },
        sorter: "number",
      },
      {
        title: "", width: 55, hozAlign: "center", headerSort: false,
        formatter: (cell) => `<button class="btn btn-sm btn-outline-success" title="Подробнее">⋯</button>`,
        cellClick: (e, cell) => { e.stopPropagation(); openSupplier(cell.getData()); },
      },
    ],
    initialSort: [{ column: "relevance", dir: "desc" }],
  });
}

function handleDone(ev) {
  currentSearchId = ev.search_id;
  enableResultButtons(true);
  const stats = ev.stats || {};
  logLine(`ГОТОВО за ${stats.seconds}s: ${ev.count} поставщиков, ` +
    `с контактами: ${stats.with_contacts}, с ценами: ${stats.with_prices}`);
}

function enableResultButtons(on) {
  $("#btn-export-xlsx").disabled = !on;
  $("#btn-export-csv").disabled = !on;
  $("#btn-rfq").disabled = !on;
}

/* ------------------------------- карточка ------------------------------- */

async function openSupplier(d) {
  // полный карточный record из БД (с id и заметками)
  const full = await (await fetch(`/api/suppliers/${d.id}`)).json().catch(() => d);
  const s = full.id ? full : d;
  $("#sc-name").textContent = s.name;
  const prov = (field) => s.sources?.[field]
    ? `<span class="src" title="источник: ${escapeHtml(s.sources[field])}">⟨${escapeHtml(shortSrc(s.sources[field]))}⟩</span>`
    : "";
  const contact = (label, arr, field) => arr?.length
    ? `<div class="sc-fact"><b>${label}:</b> ${arr.map(escapeHtml).join("<br>")} ${prov(field)}</div>` : "";
  $("#sc-body").innerHTML = `
    <div class="sc-section">
      ${s.inn ? `<div class="sc-fact"><b>ИНН:</b> ${escapeHtml(s.inn)} ${prov("имя и ИНН")}</div>` : ""}
      ${s.city || s.region ? `<div class="sc-fact"><b>География:</b> ${escapeHtml(s.city || "")} ${s.region ? "(" + escapeHtml(s.region) + ")" : ""}</div>` : ""}
      ${s.site ? `<div class="sc-fact"><b>Сайт:</b> <a href="${escapeHtml(s.site)}" target="_blank">${escapeHtml(s.site)}</a></div>` : ""}
    </div>
    ${s.description ? `<div class="sc-section"><h6>О компании</h6><div>${escapeHtml(s.description).slice(0, 600)}</div></div>` : ""}
    <div class="sc-section"><h6>Коммерческие условия</h6>
      ${contact("Телефон", s.phones, "контакты")}
      ${contact("Email", s.emails, "email")}
      ${s.min_order ? `<div class="sc-fact"><b>Мин. заказ:</b> ${escapeHtml(s.min_order)} ${prov("мин. заказ")}</div>` : ""}
      ${s.price_note ? `<div class="sc-fact"><b>Цены:</b> ${escapeHtml(s.price_note)} ${prov("цена")}</div>` : ""}
      ${s.gov_contracts_count ? `<div class="sc-fact"><b>Госконтракты:</b> ${s.gov_contracts_count} шт, ${Number(s.gov_contracts_value).toLocaleString("ru")} ₽ ${prov("цена")}</div>` : ""}
      ${s.delivery ? `<div class="sc-fact"><b>Доставка:</b> ${escapeHtml(s.delivery)} ${prov("доставка")}</div>` : ""}
      ${s.certificates?.length ? `<div class="sc-fact"><b>Сертификаты:</b> ${s.certificates.map(escapeHtml).join(", ")}</div>` : ""}
      ${s.docs_verified ? `<div class="sc-fact text-success"><b>✓ Документы проверены</b> ${prov("документы")}</div>` : ""}
    </div>
    ${s.reasons?.length ? `<div class="sc-section"><h6>Почему связаться</h6><div>${s.reasons.map(r => "• " + escapeHtml(r)).join("<br>")}</div></div>` : ""}
    <div class="sc-section"><h6>Источники данных (прозрачность)</h6>
      ${Object.entries(s.sources || {}).map(([f, src]) => `<div class="sc-fact"><b>${escapeHtml(f)}:</b> ${escapeHtml(src)}</div>`).join("") || "<span class='text-secondary'>—</span>"}
    </div>
    <div class="sc-section"><h6>Заметки (сохраняются в БД)</h6>
      <div id="notes-list">${(s.notes || []).map(n => `<div class="border rounded p-2 mb-1 bg-light">${escapeHtml(n.text)}<div class="text-secondary" style="font-size:.7rem">${escapeHtml(n.created_at)}</div></div>`).join("")}</div>
      <form id="note-form" class="d-flex gap-2 mt-2">
        <input class="form-control form-control-sm" id="note-text" placeholder="Например: звонить после 14:00, спросить про опт от 500кг" ${s.id ? "" : "disabled"}>
        <button class="btn btn-sm btn-success" ${s.id ? "" : "disabled"}>Сохранить</button>
      </form>
    </div>`;
  if (s.id) {
    $("#note-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const text = $("#note-text").value.trim();
      if (!text) return;
      const res = await fetch(`/api/suppliers/${s.id}/notes`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      }).then(r => r.json());
      $("#notes-list").innerHTML = res.notes.map(n =>
        `<div class="border rounded p-2 mb-1 bg-light">${escapeHtml(n.text)}<div class="text-secondary" style="font-size:.7rem">${escapeHtml(n.created_at)}</div></div>`).join("");
      $("#note-text").value = "";
    });
  }
  new bootstrap.Offcanvas($("#supplier-canvas")).show();
}

function shortSrc(src) {
  if (src.includes("госконтракт")) return "44-ФЗ";
  if (src.includes("optlist")) return "каталог";
  if (src.includes("сайт")) return "сайт";
  if (src.includes("поиск")) return "поиск";
  return "ист.";
}

/* ------------------------------- экспорт ------------------------------- */

function exportFile(kind) {
  if (!currentSearchId) return;
  window.location.href = `/api/searches/${currentSearchId}/export/${kind}`;
}

/* ------------------------------- RFQ ------------------------------- */

const rfqModal = () => bootstrap.Modal.getOrCreateInstance($("#rfq-modal"));

async function openRfq() {
  if (!currentSearchId) return;
  const rows = table ? table.getSelectedData() : [];
  let suppliers = rows.length ? rows : Array.from(supplierRows.values()).filter(r => r.emails?.length).slice(0, 5);
  if (!suppliers.length) {
    alert("Выберите поставщиков в таблице (клик по строке выделяет), у кого есть email — или дождитесь обхода сайтов.");
    return;
  }
  const ids = suppliers.map(s => s.id).filter(Boolean);
  if (!ids.length) { alert("У выбранных поставщиков нет записи в БД (поиск ещё не завершён)."); return; }
  $("#rfq-recipients").innerHTML = "<b>Кому:</b> " + suppliers.map(s => escapeHtml(s.name)).join(", ");
  $("#rfq-body").textContent = "Генерация письма…";
  $("#rfq-send-btn").disabled = true;
  rfqModal().show();
  await draftRfq(ids);
}

async function draftRfq(ids) {
  const resp = await fetch("/api/rfq/preview", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ search_id: currentSearchId, supplier_ids: ids, extra_requirements: $("#rfq-extra").value }),
  });
  const data = await resp.json();
  if (!resp.ok) { $("#rfq-body").textContent = data.detail || "Ошибка генерации"; return; }
  $("#rfq-subject").textContent = "Тема: " + data.subject;
  $("#rfq-body").textContent = data.body;
  $("#rfq-send-btn").disabled = false;
  $("#rfq-send-btn").dataset.ids = JSON.stringify(ids);
  $("#rfq-send-btn").dataset.to = JSON.stringify(data.recipients);
}

async function sendRfq() {
  const btn = $("#rfq-send-btn");
  btn.disabled = true;
  btn.textContent = "Отправка…";
  const resp = await fetch("/api/rfq/send", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ search_id: currentSearchId, supplier_ids: JSON.parse(btn.dataset.ids),
                           extra_requirements: $("#rfq-extra").value }),
  });
  const data = await resp.json();
  btn.textContent = "Отправить поставщикам";
  if (!resp.ok) {
    alert(data.detail || "Ошибка отправки");
    btn.disabled = false;
    return;
  }
  $("#rfq-body").textContent =
    `Отправлено писем: ${data.sent}, ошибок: ${data.failed}\n\n` +
    (data.detail || []).map(d => `${d.to}: ${d.status}`).join("\n");
  if (data.sent === 0) btn.disabled = false;
}

/* ------------------------------- utils ------------------------------- */

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = String(s ?? "");
  return d.innerHTML;
}

init();
