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
    "search_id", "name", "domain", "inn", "city", "region", "phones", "emails",
    "site", "description", "categories", "price_min", "price_max", "price_note",
    "min_order", "delivery", "certificates", "gov_contracts_count",
    "gov_contracts_value", "relevance", "reasons", "sources",
    "enrichment_status", "status",
]


def insert_supplier(data: dict) -> int:
    payload = {k: data.get(k) for k in SUPPLIER_COLS}
    if not payload.get("status"):
        payload["status"] = "new"
    for k in ("phones", "emails", "categories", "certificates", "reasons", "sources"):
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
            for k in ("phones", "emails", "categories", "certificates", "reasons", "sources"):
                d[k] = _uj(d.get(k), [] if k in ("phones", "emails", "categories", "certificates") else {})
            out.append(d)
        return out


def get_supplier(supplier_id: int) -> dict | None:
    with db() as c:
        r = c.execute("SELECT * FROM suppliers WHERE id=?", (supplier_id,)).fetchone()
        if not r:
            return None
        d = dict(r)
        for k in ("phones", "emails", "categories", "certificates", "reasons", "sources"):
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
