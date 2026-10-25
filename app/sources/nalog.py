"""ФНС: Ресурс БФО (bo.nalog.gov.ru) — официальный статус юрлица и выручка
по ИНН. Открытый API без ключей и авторизации (эндпоинт расширенного поиска
возвращает statusCode + последний отчёт bfo.gainSum).

Это самый надёжный источник фактов «статус» и «выручка»: первоисточник ФНС,
обновляется при сдаче годовой отчётности (март следующего года).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time

import httpx

from app.config import get_settings
from app.db import db

logger = logging.getLogger(__name__)

BO_BASE = "https://bo.nalog.gov.ru"
_TTL = 30 * 86400  # статус/выручка меняются редко: кэш 30 дней


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def fmt_sum(rub: int | None) -> str | None:
    """1417564 -> «1,4 млн ₽»"""
    if not rub:
        return None
    if rub >= 1_000_000_000:
        return f"{rub / 1_000_000_000:.1f} млрд ₽".replace(".", ",")
    if rub >= 1_000_000:
        return f"{rub / 1_000_000:.1f} млн ₽".replace(".", ",")
    if rub >= 1_000:
        return f"{rub // 1_000} тыс ₽"
    return f"{rub} ₽"


async def company_profile_by_inn(inn: str) -> dict | None:
    """Статус + выручка + ОГРН/ОКВЭД/город по ИНН из ФНС БФО. Кэш 30 дней."""
    inn = re.sub(r"\D", "", inn or "")
    if len(inn) not in (10, 12):
        return None

    with db() as c:  # кэш: выручка обновляется раз в год — живёт в БД
        row = c.execute(
            "SELECT payload, ts FROM nalog_cache WHERE inn=?", (inn,)
        ).fetchone()
        if row and row["ts"] and time.time() - time.mktime(
            time.strptime(row["ts"][:19], "%Y-%m-%dT%H:%M:%S")
        ) < _TTL:
            return json.loads(row["payload"])

    try:
        async with httpx.AsyncClient(
                timeout=httpx.Timeout(20, connect=10), verify=False) as hc:
            r = await hc.get(
                f"{BO_BASE}/advanced-search/organizations/search",
                params={"query": inn, "page": 0, "size": 20},
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 Chrome/156.0 Safari/537.36",
                    "Accept": "application/json",
                    "Referer": f"{BO_BASE}/",
                },
            )
            if r.status_code != 200:
                logger.debug("bo.nalog %s -> %s", inn, r.status_code)
                return None
            for item in r.json().get("content") or []:
                # ИНН в ответе обёрнут в <strong>…</strong> — подсветка
                if re.sub(r"<[^>]+>", "", item.get("inn") or "") != inn:
                    continue
                bfo = item.get("bfo") or {}
                prof = {
                    "name": re.sub(r"<[^>]+>", "", item.get("shortName") or ""),
                    "status": "Действующая"
                    if item.get("statusCode") == "ACTIVE" else item.get("statusCode"),
                    "revenue": bfo.get("gainSum"),
                    "revenue_year": bfo.get("period"),
                    "ogrn": item.get("ogrn"),
                    "okved": item.get("okved2"),
                    "city": (item.get("city") or "").title(),
                    "reg_date": item.get("statusDate"),
                }
                with db() as c:
                    c.execute(
                        "INSERT OR REPLACE INTO nalog_cache (inn, payload, ts) "
                        "VALUES (?,?,?)", (inn, json.dumps(prof), _now()),
                    )
                    c.commit()
                return prof
    except Exception as exc:
        logger.debug("bo.nalog %s failed: %s", inn, exc)
    return None


async def enrich_companies(cards, emit=None) -> int:
    """Пройтись по карточкам с ИНН: официальный статус + выручка ФНС.
    Возвращает число обновлённых карточек. Вежливо, последовательно."""
    todo = [c for c in cards
            if c.inn and (not c.company_status
                          or c.company_status == "Действующая"
                          and not c.revenue_note)]
    updated = 0
    for c in todo:
        prof = await company_profile_by_inn(c.inn)
        if not prof:
            continue
        if prof["status"] and not c.company_status:
            c.company_status = prof["status"]
        if not c.ogrn and prof["ogrn"]:
            c.ogrn = prof["ogrn"]
        if not c.reg_date and prof["reg_date"]:
            c.reg_date = prof["reg_date"]
        if not c.revenue_note and prof["revenue"]:
            year = f" ({prof['revenue_year']})" if prof["revenue_year"] else ""
            c.revenue_note = f"{fmt_sum(prof['revenue'])}{year}"
            c.sources["выручка"] = (
                f"ФНС БФО (bo.nalog.gov.ru): {c.revenue_note}"
            )
            updated += 1
        if prof["status"] and prof["status"] != "Действующая":
            c.relevance = 0  # фильтр неактивных исключит и запишет причину
        await asyncio.sleep(0.3)  # вежливость к официальному ресурсу
    return updated
