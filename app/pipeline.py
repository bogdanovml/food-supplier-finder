"""Пайплайн поиска: 8 этапов, параллельные источники, события прогресса.

Событийная модель (SSE в main.py):
  {"type": "stage",   "stage": "...", "status": "start|done|skipped|failed",
   "found": N, "message": "..."}        — этапный чек-лист
  {"type": "supplier", "data": {...}}  — карточка (обновляется по мере enrich)
  {"type": "done", "search_id": N, "count": N, "stats": {...}}
  {"type": "error",  "message": "..."}

Источник阶段 параллельны: SERP + optlist + gosplan стартуют одновременно —
пользователь видит три полосы прогресса, а не последовательное ожидание."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Callable

from . import db
from .config import get_settings
from .extraction import enrich_sites
from .llm import ask_json, LLMError
from .merge import merge_all, apply_enrichment
from .regions import resolve
from .sources import gosplan as gosplan_src
from .sources import optlist as optlist_src
from .sources import xmlstock as serp_src

logger = logging.getLogger(__name__)

Event = dict[str, Any]
Emit = Callable[[Event], None]

STAGES = ["plan", "serp", "optlist", "gosplan", "merge", "enrich", "structure", "done"]


async def _plan_queries(query: str, emit: Emit) -> list[str]:
    """LLM строит 2-3 поисковых запроса из пользовательского ТЗ (дешёвая модель)."""
    await emit({"type": "stage", "stage": "plan", "status": "start"})
    try:
        data = await ask_json(
            f'Товар: "{query}". Составь 2-3 поисковых запроса для Яндекса, '
            f'чтобы найти ОПТОВЫХ поставщиков/производителей этого товара в России. '
            f'Ответь JSON: {{"queries": ["...", "...", "..."]}}',
            smart=False, temperature=0.3, max_tokens=300)
        queries = [q for q in data.get("queries", []) if isinstance(q, str) and len(q) > 3]
    except LLMError:
        queries = []
    queries = queries[:3] or [f"{query} оптом", f"{query} производитель купить"]
    await emit({"type": "stage", "stage": "plan", "status": "done",
                "found": len(queries), "message": f"Запросы: {'; '.join(queries)}"})
    return queries


async def _run_serp(queries: list[str], lr: int, emit: Emit) -> list:
    await emit({"type": "stage", "stage": "serp", "status": "start",
                "message": f"Яндекс + Google XML по {len(queries)} запросам (регион lr={lr})"})

    async def prog(engine: str, q: str, found: int) -> None:
        if found:
            await emit({"type": "stage", "stage": "serp", "status": "run",
                        "found": found, "message": f"{engine}: «{q}» → {found} результатов"})

    hits = await serp_src.fetch_serp(queries, lr=lr, progress=prog)
    await emit({"type": "stage", "stage": "serp", "status": "done", "found": len(hits)})
    return hits


async def _run_optlist(query: str, emit: Emit) -> list:
    await emit({"type": "stage", "stage": "optlist", "status": "start",
                "message": "optlist.ru: поиск + карточки компаний (Playwright, ~1-2 мин)"})
    try:
        hits = await optlist_src.search_suppliers(query, with_details=4)
        status = "done" if hits else "skipped"
        await emit({"type": "stage", "stage": "optlist", "status": status, "found": len(hits),
                    "message": "" if hits else "optlist: ничего не найдено/недоступен"})
        return hits
    except Exception as exc:
        logger.warning("optlist stage failed: %s", exc)
        await emit({"type": "stage", "stage": "optlist", "status": "skipped",
                    "message": f"optlist пропущен: {exc}"})
        return []


async def _run_gosplan(query: str, region_code: int, emit: Emit) -> list:
    await emit({"type": "stage", "stage": "gosplan", "status": "start",
                "message": "44-ФЗ: исполненные контракты (Госплан API)"})
    try:
        hits = await gosplan_src.search_food_contracts(query, region_code=region_code or None)
        await emit({"type": "stage", "stage": "gosplan", "status": "done", "found": len(hits)})
        return hits
    except Exception as exc:
        logger.warning("gosplan stage failed: %s", exc)
        await emit({"type": "stage", "stage": "gosplan", "status": "skipped",
                    "message": f"госконтракты пропущены: {exc}"})
        return []


async def _structure_reasons(cards: list, query: str, emit: Emit) -> None:
    """LLM: человеческие «причины связаться» (одна batched-вызов — быстро и дёшево)."""
    await emit({"type": "stage", "stage": "structure", "status": "start"})
    top = sorted(cards, key=lambda c: c.relevance, reverse=True)[:12]
    if not top:
        await emit({"type": "stage", "stage": "structure", "status": "skipped"})
        return
    compact = [{"i": i, "name": c.name, "city": c.city,
                 "contracts": c.gov_contracts_count, "value": c.gov_contracts_value,
                 "min_order": c.min_order, "desc": (c.description or "")[:120]}
                for i, c in enumerate(top)]
    try:
        data = await ask_json(
            f'Пользователь ищет поставщиков: "{query}". Ниже — список поставщиков (JSON). '
            f"Для КАЖДОГО верни одну краткую причину, почему с ним стоит связаться "
            f"(из его данных, без общих слов). Ответь JSON: "
            f'{{"reasons": [{{"i": 0, "why": "..."}}, ...]}}\n\n{json.dumps(compact, ensure_ascii=False)}',
            smart=True, temperature=0.4, max_tokens=1200)
        for r in data.get("reasons", []):
            try:
                card = top[int(r.get("i", -1))]
                why = str(r.get("why", "")).strip()
                if why:
                    card.reasons.append(why[:200])
            except (ValueError, IndexError):
                continue
    except (LLMError, ValueError) as exc:
        logger.warning("structure stage failed: %s", exc)
        for c in top:
            c.reasons.append(_fallback_reason(c))
    await emit({"type": "stage", "stage": "structure", "status": "done", "found": len(top)})


def _fallback_reason(card) -> str:
    if card.gov_contracts_count:
        return (f"Поставлял госзаказчикам ({card.gov_contracts_count} контракт(ов) "
                f"на {card.gov_contracts_value:,.0f} ₽)")
    if card.min_order:
        return f"Оптовый поставщик, мин. заказ {card.min_order}"
    if card.emails:
        return "Есть прямые контакты для запроса КП"
    return "Найден по профильному запросу в органике"


async def run_search(query: str, region_name: str, emit: Emit) -> AsyncIterator[None]:
    """Полный цикл: создаёт search в БД, гоняет этапы, пишет поставщиков."""
    s = get_settings()
    lr, gos_region = resolve(region_name)
    search_id = db.create_search(query, region_name, lr)
    stats: dict[str, Any] = {"search_id": search_id}
    t0 = time.monotonic()

    try:
        queries = await _plan_queries(query, emit)

        await emit({"type": "stage", "stage": "merge", "status": "pending"})
        # три источника параллельно
        serp_hits, opt_hits, gos_hits = await asyncio.gather(
            _run_serp(queries, lr, emit),
            _run_optlist(query, emit),
            _run_gosplan(query, gos_region, emit),
        )
        stats["serp"] = len(serp_hits)
        stats["optlist"] = len(opt_hits)
        stats["gosplan"] = len(gos_hits)

        # --- merge ---
        await emit({"type": "stage", "stage": "merge", "status": "start"})
        cards = merge_all(serp_hits, opt_hits, gos_hits, region_name)
        await emit({"type": "stage", "stage": "merge", "status": "done", "found": len(cards)})

        # карточки сразу в UI (enrichment_status=pending)
        by_domain = {c.domain: c for c in cards if c.domain}
        for c in sorted(cards, key=lambda x: x.relevance, reverse=True):
            await emit({"type": "supplier", "data": _row(c)})

        # --- enrich: только карточки из SERP без контактов ---
        await emit({"type": "stage", "stage": "enrich", "status": "start"})
        to_enrich = [c for c in cards
                     if c.domain and c.enrichment_status == "pending"
                     and not (c.emails and c.phones)]
        to_enrich = sorted(to_enrich, key=lambda x: x.relevance, reverse=True)[:s.enrich_max_sites]

        async def prog(url: str, facts) -> None:
            card = by_domain.get(_domain_of(url))
            if card is None:
                return
            apply_enrichment(card, facts)
            await emit({"type": "supplier", "data": _row(card)})
            await emit({"type": "stage", "stage": "enrich", "status": "run",
                        "found": sum(1 for c in cards if c.enrichment_status == "done"),
                        "message": f"обход сайтов: {url}"})

        if to_enrich:
            await enrich_sites([c.site for c in to_enrich], query, progress=prog)
        for c in to_enrich:
            if c.enrichment_status == "pending":
                c.enrichment_status = "failed"
        await emit({"type": "stage", "stage": "enrich", "status": "done",
                    "found": sum(1 for c in cards if c.enrichment_status == "done")})

        # --- structure + финальные карточки ---
        await _structure_reasons(cards, query, emit)
        final = sorted(cards, key=lambda c: c.relevance, reverse=True)
        for c in final:
            await emit({"type": "supplier", "data": _row(c)})

        # --- запись в БД ---
        ids = [db.insert_supplier({"search_id": search_id, **_row(c)}) for c in final]
        stats["suppliers"] = len(final)
        stats["with_contacts"] = sum(1 for c in final if c.emails or c.phones)
        stats["with_prices"] = sum(1 for c in final if c.price_min or c.gov_contracts_value)
        stats["seconds"] = round(time.monotonic() - t0, 1)
        db.finish_search(search_id, "done", stats)
        await emit({"type": "done", "search_id": search_id,
                    "count": len(final), "stats": stats})
    except Exception as exc:
        logger.exception("search pipeline failed")
        db.finish_search(search_id, "failed", {"error": str(exc)})
        await emit({"type": "error", "message": f"Поиск упал: {exc}"})


def _domain_of(url: str) -> str:
    from .merge import domain_key
    return domain_key(url)


def _row(card) -> dict:
    return card.to_row()
