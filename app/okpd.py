"""Справочник ОКПД2 + матчинг товарного запроса к классификаторам.

Зачем: госконтракты ищутся не только полнотекстово («чипсы»), но и по
ОКПД2/КТРУ-кодам предмета закупки. Полнотекст по узкому товару часто
мажет или пуст; классификатор даёт контракт-выборку всей категории,
а поставщики категории — это и есть кандидаты.

Источники кода:
  * data/okpd2.csv — официальный справочник (19К строк), вендорен из проекта
    tender/nmck;
  * LLM предлагает коды, мы ВАЛИДИРУЕМ их по справочнику (анти-галлюцинация)
    и дополняем fuzzy-матчингом названия товара.
"""
from __future__ import annotations

import csv
import logging
from functools import lru_cache
from pathlib import Path

from rapidfuzz import fuzz, process as fz_process

from .config import get_settings

logger = logging.getLogger(__name__)


@lru_cache
def _load() -> list[tuple[str, str]]:
    """[(код, название)] из справочника. Пропускаем верхнеуровневые разделы."""
    path = Path(__file__).resolve().parent.parent / "data" / "okpd2.csv"
    rows: list[tuple[str, str]] = []
    for enc in ("cp1251", "utf-8", "utf-8-sig"):
        try:
            with open(path, encoding=enc, newline="") as fh:
                rdr = csv.reader(fh, delimiter=";")
                rows = []
                for r in rdr:
                    if len(r) >= 3 and "." in r[1] and r[2].strip():
                        rows.append((r[1].strip(), r[2].strip()))
            if rows:
                break
        except UnicodeDecodeError:
            continue
    # нижние уровни детализации: код с >= 5 символами ("10.84")
    return [(c, n) for c, n in rows if len(c) >= 5]


def lookup_name(code: str) -> str | None:
    for c, n in _load():
        if c == code:
            return n
    return None


def suggest_codes(query: str, *, limit: int = 4) -> list[tuple[str, str]]:
    """Fuzzy-матчинг товара к названиям ОКПД2. Возвращает [(код, название)]."""
    rows = _load()
    names = [n for _, n in rows]
    hits = fz_process.extract(query, names, scorer=fuzz.partial_ratio, limit=limit * 3)
    seen: dict[str, tuple[str, str]] = {}
    for name, score, idx in hits:
        if score < 70:
            continue
        code = rows[idx][0]
        # более детальный код приоритетнее группы
        if code not in seen or len(code) > len(seen[code][0]):
            seen[code] = (code, rows[idx][1])
        if len(seen) >= limit:
            break
    return list(seen.values())[:limit]


def validate_codes(codes: list[str]) -> list[str]:
    """Оставляем только существующие в справочнике коды (анти-галлюцинация LLM)."""
    valid = {c for c, _ in _load()}
    out = []
    for c in codes:
        c = str(c).strip().rstrip(".")
        if c in valid and c not in out:
            out.append(c)
    return out
