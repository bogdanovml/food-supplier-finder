"""Экспорт результатов: XLSX (xlsxwriter — форматирование) и CSV (для Google Sheets)."""
from __future__ import annotations

import csv
import io
from typing import Any

import xlsxwriter


def _rows(suppliers: list[dict]) -> list[list[Any]]:
    rows = []
    for s in suppliers:
        rows.append([
            s["name"],
            s["city"] or s["region"],
            "; ".join(s["phones"]),
            "; ".join(s["emails"]),
            s["site"] or s["domain"],
            s["min_order"] or "",
            s["price_note"] or "",
            s["gov_contracts_count"],
            (s["gov_contracts_value"] or 0) / 1_000_000,
            "; ".join(s["certificates"]),
            s["delivery"] or "",
            "; ".join(s["reasons"]),
            s.get("status") or "новый",
            s["relevance"],
        ])
    return rows


COLS = [
    ("Поставщик", 40), ("Город", 16), ("Телефоны", 20), ("Email", 26),
    ("Сайт", 28), ("Мин. заказ", 14), ("Цены", 22),
    ("Госконтракты, шт", 12), ("Госконтракты, млн ₽", 12),
    ("Сертификаты", 20), ("Доставка", 24), ("Почему связаться", 40),
    ("Статус", 14), ("Релевантность", 12),
]


def to_xlsx(search: dict, suppliers: list[dict], buf: io.BytesIO) -> int:
    """Красивый xlsx: автофильтр, freeze, зебра, гиперссылки на сайты."""
    wb = xlsxwriter.Workbook(buf, {"in_memory": True})
    ws = wb.add_worksheet("Поставщики")

    title = wb.add_format({"bold": True, "font_size": 14})
    sub = wb.add_format({"font_color": "#666666"})
    head = wb.add_format({"bold": True, "bg_color": "#2E5E4E", "font_color": "white",
                          "border": 1, "text_wrap": True, "valign": "vcenter"})
    row_a = wb.add_format({"bg_color": "#F2F7F5", "valign": "top", "text_wrap": True, "border": 1})
    row_b = wb.add_format({"valign": "top", "text_wrap": True, "border": 1})
    link = wb.add_format({"font_color": "#0563C1", "underline": 1})

    ws.write(0, 0, "Поставщики продуктов питания", title)
    ws.write(1, 0, f"Запрос: «{search['query']}» • {search['region']} • "
                   f"экспорт из Food Supplier Finder", sub)

    ws.set_row(2, 8)
    ws.set_row(3, 30)
    for col, (name, width) in enumerate(COLS):
        ws.write(3, col, name, head)
        ws.set_column(col, col, width)

    for i, row in enumerate(_rows(suppliers)):
        fmt = row_a if i % 2 == 0 else row_b
        ws.set_row(4 + i, 34)
        for col, value in enumerate(row):
            if col == 4 and value:  # сайт — гиперссылка
                ws.write_url(4 + i, col, value if "://" in value else f"http://{value}", fmt)
            else:
                ws.write(4 + i, col, value, fmt)

    ws.autofilter(3, 0, 3 + len(suppliers), len(COLS) - 1)
    ws.freeze_panes(4, 1)
    ws.hide_gridlines(2)
    wb.close()
    return len(suppliers)


def to_csv(search: dict, suppliers: list[dict]) -> str:
    """CSV для импорта в Google Sheets (Файл → Импорт)."""
    out = io.StringIO()
    w = csv.writer(out, delimiter=";")
    w.writerow([search["query"], search["region"], "Food Supplier Finder"])
    w.writerow([c for c, _ in COLS])
    for row in _rows(suppliers):
        w.writerow(row)
    return out.getvalue()
