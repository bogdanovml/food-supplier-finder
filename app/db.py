"""SQLite-слой: схема + DAO. Прототип — один файл, без миграторов."""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import get_settings

_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    region TEXT NOT NULL,
    lr INTEGER NOT NULL DEFAULT 213,
    status TEXT NOT NULL DEFAULT 'running',
    stats TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER NOT NULL REFERENCES searches(id),
    name TEXT NOT NULL,
    domain TEXT,
    inn TEXT,
    city TEXT,
    region TEXT,
    phones TEXT,
    emails TEXT,
    site TEXT,
    description TEXT,
    categories TEXT,
    price_min REAL,
    price_max REAL,
    price_note TEXT,
    min_order TEXT,
    delivery TEXT,
    certificates TEXT,
    gov_contracts_count INTEGER DEFAULT 0,
    gov_contracts_value REAL DEFAULT 0,
    gov_contract_examples TEXT,
    relevance INTEGER DEFAULT 0,
    reasons TEXT,
    sources TEXT,
    enrichment_status TEXT DEFAULT 'pending',
    status TEXT DEFAULT 'new',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_suppliers_search ON suppliers(search_id);
CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id INTEGER NOT NULL REFERENCES suppliers(id),
    text TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checko_cache (
    ref TEXT PRIMARY KEY,
    json TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS nalog_cache (
    inn TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS excluded (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER NOT NULL REFERENCES searches(id),
    name TEXT NOT NULL,
    domain TEXT,
    city TEXT,
    inn TEXT,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_excluded_search ON excluded(search_id);
CREATE TABLE IF NOT EXISTS registry (
    key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    inn TEXT,
    domain TEXT,
    city TEXT,
    region TEXT,
    emails TEXT,
    phones TEXT,
    site TEXT,
    min_order TEXT,
    certificates TEXT,
    gov_contracts_count INTEGER DEFAULT 0,
    gov_contracts_value REAL DEFAULT 0,
    searches_count INTEGER DEFAULT 1,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS registry_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    registry_key TEXT NOT NULL REFERENCES registry(key),
    query TEXT NOT NULL,
    category TEXT,
    seen_at TEXT NOT NULL,
    UNIQUE (registry_key, query)
);
CREATE TABLE IF NOT EXISTS rfq_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER REFERENCES searches(id),
    supplier_ids TEXT,
    subject TEXT,
    body TEXT,
    status TEXT,
    detail TEXT,
    sent_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rfq_daily_counter (
    day TEXT PRIMARY KEY,
    count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS rfq_inbox (
    id TEXT PRIMARY KEY,
    from_email TEXT,
    from_name TEXT,
    to_email TEXT,
    subject TEXT,
    body TEXT,
    received_at TEXT,
    is_read INTEGER DEFAULT 0
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _conn() -> sqlite3.Connection:
    """Одно соединение на поток. SQLite: WAL не нужен для прототипа."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        Path(get_settings().db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(get_settings().db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        try:
            conn.execute("ALTER TABLE suppliers ADD COLUMN status TEXT DEFAULT 'new'")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE suppliers ADD COLUMN gov_contract_examples TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        for col in ("legal_name TEXT", "ogrn TEXT", "director TEXT", "okved TEXT",
                    "reg_date TEXT", "company_status TEXT", "staff TEXT", "revenue_note TEXT",
                    "profit_note TEXT",
                    "assets_note TEXT",
                    "delivery_geo TEXT",
                    "checko_url TEXT", "maps_url TEXT", "maps_rating REAL", "maps_reviews INTEGER",
                    "product_links TEXT", "order_terms TEXT"):
            try:
                conn.execute(f"ALTER TABLE suppliers ADD COLUMN {col}")
                conn.commit()
            except sqlite3.OperationalError:
                pass
        _local.conn = conn
    return conn


@contextmanager
def db() -> Iterator[sqlite3.Connection]:
    yield _conn()


def _j(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def _uj(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return default


# ------------------------------ searches ------------------------------

def create_search(query: str, region: str, lr: int) -> int:
    with db() as c:
        cur = c.execute(
            "INSERT INTO searches (query, region, lr, status, created_at) VALUES (?,?,?,?,?)",
            (query, region, lr, "running", _now()),
        )
        c.commit()
        return cur.lastrowid


def finish_search(search_id: int, status: str, stats: dict) -> None:
    with db() as c:
        c.execute(
            "UPDATE searches SET status=?, stats=? WHERE id=?",
            (status, _j(stats), search_id),
        )
        c.commit()


def list_searches(limit: int = 20) -> list[dict]:
    with db() as c:
        rows = c.execute(
            "SELECT s.*, (SELECT COUNT(*) FROM suppliers sp WHERE sp.search_id=s.id) AS n_suppliers "
            "FROM searches s "
            "WHERE (SELECT COUNT(*) FROM suppliers sp WHERE sp.search_id=s.id) > 0 "
            "ORDER BY s.id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_search(search_id: int) -> dict | None:
    with db() as c:
        row = c.execute("SELECT * FROM searches WHERE id=?", (search_id,)).fetchone()
        return dict(row) if row else None


# ------------------------------ suppliers ------------------------------

SUPPLIER_COLS = [
    "search_id", "name", "domain", "inn", "legal_name", "ogrn", "director",
    "okved", "reg_date", "company_status", "staff", "revenue_note", "profit_note", "assets_note", "delivery_geo",
    "checko_url", "maps_url", "maps_rating", "maps_reviews",
    "product_links", "order_terms",
    "city", "region", "phones", "emails",
    "site", "description", "categories", "price_min", "price_max", "price_note",
    "min_order", "delivery", "certificates", "gov_contracts_count",
    "gov_contracts_value", "gov_contract_examples", "relevance", "reasons", "sources",
    "enrichment_status", "status",
]


def insert_supplier(data: dict) -> int:
    payload = {k: data.get(k) for k in SUPPLIER_COLS}
    if not payload.get("status"):
        payload["status"] = "new"
    for k in ("phones", "emails", "categories", "certificates", "reasons", "sources", "gov_contract_examples", "product_links"):
        payload[k] = _j(payload[k]) if payload[k] is not None else None
    cols = ", ".join(SUPPLIER_COLS)
    ph = ", ".join("?" * len(SUPPLIER_COLS))
    with db() as c:
        cur = c.execute(
            f"INSERT INTO suppliers ({cols}, created_at) VALUES ({ph}, ?)",
            [payload[k] for k in SUPPLIER_COLS] + [_now()],
        )
        c.commit()
        return cur.lastrowid


def get_suppliers(search_id: int) -> list[dict]:
    with db() as c:
        rows = c.execute(
            "SELECT * FROM suppliers WHERE search_id=? ORDER BY relevance DESC, id", (search_id,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("phones", "emails", "categories", "certificates", "reasons", "sources", "product_links"):
                d[k] = _uj(d.get(k), [] if k in ("phones", "emails", "categories", "certificates") else {})
            out.append(d)
        return out


def get_supplier(supplier_id: int) -> dict | None:
    with db() as c:
        r = c.execute("SELECT * FROM suppliers WHERE id=?", (supplier_id,)).fetchone()
        if not r:
            return None
        d = dict(r)
        for k in ("phones", "emails", "categories", "certificates", "reasons", "sources", "product_links"):
            d[k] = _uj(d.get(k), [] if k in ("phones", "emails", "categories", "certificates") else {})
        return d


def update_supplier_status(supplier_id: int, status: str) -> bool:
    with db() as c:
        cur = c.execute("UPDATE suppliers SET status=? WHERE id=?", (status, supplier_id))
        c.commit()
        return cur.rowcount > 0


# ------------------------------ notes ------------------------------

def add_note(supplier_id: int, text: str) -> int:
    with db() as c:
        cur = c.execute(
            "INSERT INTO notes (supplier_id, text, created_at) VALUES (?,?,?)",
            (supplier_id, text, _now()),
        )
        c.commit()
        return cur.lastrowid


def get_notes(supplier_id: int) -> list[dict]:
    with db() as c:
        rows = c.execute(
            "SELECT * FROM notes WHERE supplier_id=? ORDER BY id DESC", (supplier_id,)
        ).fetchall()
        return [dict(r) for r in rows]


# ------------------------------ rfq ------------------------------

def log_rfq(search_id: int, supplier_ids: list[int], subject: str, body: str, status: str, detail: str = "") -> int:
    with db() as c:
        cur = c.execute(
            "INSERT INTO rfq_log (search_id, supplier_ids, subject, body, status, detail, sent_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (search_id, _j(supplier_ids), subject, body, status, detail, _now()),
        )
        c.commit()
        return cur.lastrowid


def get_daily_rfq_count(day: str | None = None) -> int:
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with db() as c:
        row = c.execute("SELECT count FROM rfq_daily_counter WHERE day=?", (day,)).fetchone()
        return row[0] if row else 0


def increment_daily_rfq_count(count: int, day: str | None = None) -> int:
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with db() as c:
        c.execute(
            "INSERT INTO rfq_daily_counter (day, count) VALUES (?, ?) "
            "ON CONFLICT(day) DO UPDATE SET count = count + ?",
            (day, count, count)
        )
        c.commit()
        return get_daily_rfq_count(day)


def save_inbox_message(msg: dict) -> bool:
    with db() as c:
        try:
            c.execute(
                "INSERT OR IGNORE INTO rfq_inbox (id, from_email, from_name, to_email, subject, body, received_at, is_read) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    msg["id"],
                    msg.get("from_email", ""),
                    msg.get("from_name", ""),
                    msg.get("to_email", ""),
                    msg.get("subject", ""),
                    msg.get("body", ""),
                    msg.get("received_at", _now()),
                    msg.get("is_read", 0),
                )
            )
            c.commit()
            return True
        except Exception:
            return False


def get_inbox_messages(limit: int = 50) -> list[dict]:
    with db() as c:
        rows = c.execute(
            "SELECT * FROM rfq_inbox ORDER BY received_at DESC, id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def mark_inbox_read(msg_id: str) -> None:
    with db() as c:
        c.execute("UPDATE rfq_inbox SET is_read = 1 WHERE id = ?", (msg_id,))
        c.commit()


def get_inbox_unread_count() -> int:
    with db() as c:
        row = c.execute("SELECT COUNT(*) FROM rfq_inbox WHERE is_read = 0").fetchone()
        return row[0] if row else 0


# ------------------------------ реестр поставщиков (глобальная база) ------------------------------

def _registry_key(s: dict) -> str:
    """Ключ глобального реестра: ИНН надёжнее, иначе домен, иначе имя."""
    return f"inn:{s['inn']}" if s.get("inn") else (f"dom:{s['domain']}" if s.get("domain") else f"name:{s.get('name','').lower()}")


def upsert_registry(supplier: dict, query: str) -> None:
    """Идемпотентное слияние карточки поиска в глобальный реестр.

    Один поставщик может поставлять сколько угодно видов товаров:
    каждый поиск дописывает товар в registry_products и освежает факты.
    """
    key = _registry_key(supplier)
    now = _now()
    with db() as c:
        row = c.execute("SELECT * FROM registry WHERE key=?", (key,)).fetchone()
        if row is None:
            c.execute(
                "INSERT INTO registry (key, name, inn, domain, city, region, emails, phones, "
                "site, min_order, certificates, gov_contracts_count, gov_contracts_value, "
                "searches_count, first_seen, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)",
                (key, supplier["name"], supplier.get("inn"), supplier.get("domain"),
                 supplier.get("city"), supplier.get("region"),
                 _j(supplier.get("emails") or []), _j(supplier.get("phones") or []),
                 supplier.get("site"), supplier.get("min_order"),
                 _j(supplier.get("certificates") or []),
                 supplier.get("gov_contracts_count") or 0,
                 supplier.get("gov_contracts_value") or 0, now, now),
            )
        else:
            # не перезаписываем заполненное пустым; копим контрактную статистику
            def coalesce(old, new):
                return new if new not in (None, "", []) else old
            c.execute(
                "UPDATE registry SET name=?, emails=COALESCE(NULLIF(?,'null'), emails), "
                "phones=COALESCE(NULLIF(?,'null'), phones), min_order=?, "
                "certificates=COALESCE(NULLIF(?,'null'), certificates), "
                "gov_contracts_count=MAX(gov_contracts_count, ?), "
                "gov_contracts_value=MAX(gov_contracts_value, ?), "
                "searches_count=searches_count+1, last_seen=? WHERE key=?",
                (supplier["name"],
                 _j(supplier.get("emails") or []) or "null",
                 _j(supplier.get("phones") or []) or "null",
                 coalesce(row["min_order"], supplier.get("min_order")),
                 _j(supplier.get("certificates") or []) or "null",
                 supplier.get("gov_contracts_count") or 0,
                 supplier.get("gov_contracts_value") or 0, now, key),
            )
        try:
            c.execute("INSERT INTO registry_products (registry_key, query, seen_at) VALUES (?,?,?)",
                      (key, query, now))
        except sqlite3.IntegrityError:
            pass  # UNIQUE (registry_key, query) — уже знаем, что он это поставляет
        c.commit()


def list_registry(limit: int = 500) -> list[dict]:
    """Реестр с агрегированными товарами: один поставщик -> список категорий."""
    with db() as c:
        rows = c.execute(
            "SELECT r.*, GROUP_CONCAT(rp.query, '||') AS products "
            "FROM registry r LEFT JOIN registry_products rp ON rp.registry_key = r.key "
            "GROUP BY r.key "
            "ORDER BY r.gov_contracts_value DESC, r.searches_count DESC, r.last_seen DESC "
            "LIMIT ?", (limit,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("emails", "phones", "certificates"):
                d[k] = _uj(d.get(k), [])
            d["products"] = [p.strip() for p in (d.get("products") or "").split("||") if p.strip()]
            out.append(d)
        return out


def registry_stats() -> dict:
    with db() as c:
        n_sup = c.execute("SELECT COUNT(*) FROM registry").fetchone()[0]
        n_prod = c.execute("SELECT COUNT(DISTINCT query) FROM registry_products").fetchone()[0]
        n_links = c.execute("SELECT COUNT(*) FROM registry_products").fetchone()[0]
        return {"suppliers": n_sup, "product_queries": n_prod, "links": n_links}


# ------------------------------ кэш checko (анти-рейт-лимит) ------------------------------

def checko_cache_get(ref: str) -> dict | None:
    """Кэш карточек ЕГРЮЛ: контакты юрлица меняются редко —
    повторные запросы к checko по тому же ИНН/ОГРН бессмысленны."""
    with db() as c:
        row = c.execute("SELECT json FROM checko_cache WHERE ref=?", (ref,)).fetchone()
        if not row:
            return None
        import json as _json
        return _json.loads(row[0])


def checko_cache_put(ref: str, data: dict) -> None:
    with db() as c:
        c.execute("INSERT OR REPLACE INTO checko_cache (ref, json, fetched_at) VALUES (?,?,?)",
                  (ref, _j(data), _now()))
        c.commit()


# ------------------------------ отсеянные карточки ------------------------------

REASON_LABELS = {
    "no_legal": "Нет юрлица (ИНН/ОГРН не найдены)",
    "inactive": "Юрлицо неактивно (ликвидация/реорганизация)",
    "geo": "Другой город при поиске по федеральному городу",
    "no_contacts": "Нет полных контактов (обрубок телефона — не контакт)",
    "not_supplier": "Не поставщик (товар/декор/розница/агрегатор) — LLM-вычитка",
    "offtopic": "Чужой профиль товаров / зомби-сайт (вердикт LLM обхода)",
    "no_substance": "Слепая карточка (нет сайта, прайсов, условий и контрактов)",
}


def insert_excluded(search_id: int, cards: list, reason: str) -> None:
    """Отсеянные карточки — для вкладки прозрачности."""
    with db() as c:
        for card in cards:
            c.execute(
                "INSERT INTO excluded (search_id, name, domain, city, inn, reason, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (search_id, card.get("name") or "?", card.get("domain"),
                 card.get("city"), card.get("inn"), reason, _now()),
            )
        c.commit()


def get_excluded(search_id: int) -> list[dict]:
    with db() as c:
        rows = c.execute(
            "SELECT * FROM excluded WHERE search_id=? ORDER BY id", (search_id,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["reason_label"] = REASON_LABELS.get(d.get("reason"), d.get("reason"))
            out.append(d)
        return out
