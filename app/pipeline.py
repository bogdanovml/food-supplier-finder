"""Пайплайн поиска: 8 этапов, параллельные источники, события прогресса.

Событийная модель (SSE в main.py):
  {"type": "stage",   "stage": "...", "status": "start|done|skipped|failed",
   "found": N, "message": "..."}        — этапный чек-лист
  {"type": "supplier", "data": {...}}  — карточка (обновляется по мере enrich)
  {"type": "done", "search_id": N, "count": N, "stats": {...}}
  {"type": "error",  "message": "..."}

Этапы источников параллельны: SERP + optlist + gosplan стартуют вместе —
пользователь видит три полосы прогресса, а не последовательное ожидание.

На финише карточки сливаются в ГЛОБАЛЬНЫЙ реестр поставщиков: один
поставщик может поставлять много товаров — база копится между поисками."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable

from . import db
from .config import get_settings
from .extraction import enrich_sites
from .llm import ask_json, LLMError
from .merge import merge_all, apply_enrichment
from .okpd import suggest_codes, validate_codes
from .regions import resolve
from .sources import gosplan as gosplan_src
from .sources import optlist as optlist_src
from .sources import xmlstock as serp_src

logger = logging.getLogger(__name__)

Event = dict[str, Any]
Emit = Callable[[Event], None]

STAGES = ["plan", "serp", "optlist", "gosplan", "merge", "enrich", "structure", "done"]


async def _plan_queries(query: str, emit: Emit) -> tuple[list[str], list[str]]:
    """LLM строит поисковые запросы + предлагает ОКПД2-коды категории товара.

    Коды валидируются по справочнику (анти-галлюцинация) и дополняются
    fuzzy-матчингом названия по справочнику.
    """
    await emit({"type": "stage", "stage": "plan", "status": "start"})
    queries: list[str] = []
    codes_raw: list[str] = []
    try:
        data = await ask_json(
            f'Товар: "{query}". '
            f'1) Составь 2-3 поисковых запроса для Яндекса, чтобы найти ОПТОВЫХ '
            f'поставщиков/производителей этого товара в России. '
            f'2) Назови 1-3 кода ОКПД2 (российский классификатор продукции), '
            f'к которым относится этот товар (например, мука -> 10.61.21). '
            f'Ответь JSON: {{"queries": ["..."], "okpd2": ["10.xx.xx"]}}',
            smart=True, temperature=0.2, max_tokens=400)
        queries = [q for q in data.get("queries", []) if isinstance(q, str) and len(q) > 3][:3]
        codes_raw = [str(c).strip() for c in data.get("okpd2", []) if c][:3]
    except LLMError:
        pass
    queries = queries or [f"{query} оптом", f"{query} производитель купить"]

    codes = validate_codes(codes_raw)
    for c, _n in suggest_codes(query, limit=2):
        if c not in codes:
            codes.append(c)
    codes = codes[:3]

    await emit({"type": "stage", "stage": "plan", "status": "done", "found": len(queries),
                "message": f"Запросы: {'; '.join(queries)}. ОКПД2: {', '.join(codes) or '—'}"})
    return queries, codes


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


async def _run_gosplan(query: str, region_code: int, classifiers: list[str],
                       emit: Emit) -> list:
    await emit({"type": "stage", "stage": "gosplan", "status": "start",
                "message": f"44-ФЗ: контракты по товару + ОКПД2 "
                           f"({', '.join(classifiers) if classifiers else 'полный текст'})"})
    try:
        sup = await gosplan_src.search_gov_suppliers(
            query, region_code=region_code or None,
            classifiers=classifiers or None, limit=20, max_details=10)
        contracts_total = sum(g.count for g in sup)
        await emit({"type": "stage", "stage": "gosplan", "status": "done",
                    "found": len(sup),
                    "message": f"{len(sup)} поставщиков, {contracts_total} контрактов "
                               f"на {sum(g.total_value for g in sup):,.0f} ₽"})
        return sup
    except Exception as exc:
        logger.warning("gosplan stage failed: %s", exc)
        await emit({"type": "stage", "stage": "gosplan", "status": "skipped",
                    "message": f"госконтракты пропущены: {exc}"})
        return []


async def _structure_reasons(cards: list, query: str, emit: Emit) -> None:
    """LLM: «почему связаться» — строго по фактам, без пересказа описаний."""
    await emit({"type": "stage", "stage": "structure", "status": "start"})
    top = sorted(cards, key=lambda c: c.relevance, reverse=True)[:12]
    if not top:
        await emit({"type": "stage", "stage": "structure", "status": "skipped"})
        return
    compact = [{"i": i, "name": c.name, "city": c.city,
                "contracts": c.gov_contracts_count,
                "value": c.gov_contracts_value,
                "contract_example": (c.gov_contract_examples or [{}])[0].get("subject", "")[:80],
                "min_order": c.min_order, "price_note": (c.price_note or "")[:80],
                "certificates": "; ".join(c.certificates[:3]),
                "has_email": bool(c.emails), "docs": c.docs_verified}
                for i, c in enumerate(top)]
    try:
        data = await ask_json(
            f'Пользователь ищет поставщиков: "{query}". Ниже — JSON поставщиков с их '
            f"подтверждёнными фактами (контракты 44-ФЗ, цены, МОК, сертификаты, контакты).\n\n"
            f"Для КАЖДОГО поставщика сформулируй ОДНУ причину связаться, опираясь ТОЛЬКО на "
            f"факты из данных (цифры контрактов, цены, МОК, сертификаты, документы). "
            f"НЕ описывай компанию, объясни закупщику конкретную выгоду. Если фактов нет — "
            f"прямо скажи, что данные не подтверждены и нужна ручная проверка.\n\n"
            f'Ответь JSON: {{"reasons": [{{"i": 0, "why": "..."}}, ...]}}\n\n'
            f"{json.dumps(compact, ensure_ascii=False)}",
            smart=True, temperature=0.3, max_tokens=1400)
        for r in data.get("reasons", []):
            try:
                card = top[int(r.get("i", -1))]
                why = str(r.get("why", "")).strip()
                if why:
                    card.reasons.append(why[:250])
            except (ValueError, IndexError):
                continue
    except (LLMError, ValueError) as exc:
        logger.warning("structure stage failed: %s", exc)
        for c in top:
            c.reasons.append(_fallback_reason(c))
    # fallback для тех, кому LLM не дал причину
    for c in top:
        if not c.reasons:
            c.reasons.append(_fallback_reason(c))
    await emit({"type": "stage", "stage": "structure", "status": "done", "found": len(top)})


def _fallback_reason(card) -> str:
    if card.gov_contracts_count:
        ex = (card.gov_contract_examples or [{}])[0]
        subj = (ex.get("subject") or "продукцию")[:60]
        return (f"Подтверждённый поставщик госзаказчиков: {card.gov_contracts_count} "
                f"контракт(ов) на {card.gov_contracts_value:,.0f} ₽, включая «{subj}»")
    if card.price_min:
        note = card.price_note[:80]
        return f"Есть ценовой ориентир: {note} — можно сравнивать до запроса КП"
    if card.min_order:
        return f"Известен минимальный заказ: {card.min_order} — сразу видно, подходит ли объём"
    if card.docs_verified or card.certificates:
        return "Документы проверены каталогом, есть сертификаты — ниже риск по качеству"
    if card.emails:
        return "Есть прямой email — быстрый канал запроса КП"
    return "Данные не подтверждены (органика): нужен ручный звонок до запроса КП"


def _pessimize_thin(cards) -> None:
    """Карточки без единого полезного факта — вниз рейтинга.

    Пустышки из органики (JS-оболочки, витрины без данных) не выбрасываем —
    вдруг сайт заработает, — но и не даём им сидеть выше компаний
    с реальными ценами и контактами."""
    for c in cards:
        useful = (c.emails or c.phones or c.price_min or c.gov_contracts_count
                  or c.min_order or c.certificates)
        if not useful and c.enrichment_status != "done":
            c.relevance = min(c.relevance, 10)


async def run_search(query: str, region_name: str, emit: Emit) -> None:
    """Полный цикл: создаёт search в БД, гоняет этапы, пишет поставщиков + реестр."""
    s = get_settings()
    lr, gos_region = resolve(region_name)
    search_id = db.create_search(query, region_name, lr)
    stats: dict[str, Any] = {"search_id": search_id}
    t0 = time.monotonic()

    try:
        queries, codes = await _plan_queries(query, emit)

        await emit({"type": "stage", "stage": "merge", "status": "pending"})
        # три источника параллельно
        serp_hits, opt_hits, gov_sup = await asyncio.gather(
            _run_serp(queries, lr, emit),
            _run_optlist(query, emit),
            _run_gosplan(query, gos_region, codes, emit),
        )
        stats["serp"] = len(serp_hits)
        stats["optlist"] = len(opt_hits)
        stats["gosplan"] = len(gov_sup)

        # --- merge ---
        await emit({"type": "stage", "stage": "merge", "status": "start"})
        cards = merge_all(serp_hits, opt_hits, gov_sup, region_name)
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
        _pessimize_thin(cards)
        await emit({"type": "stage", "stage": "enrich", "status": "done",
                    "found": sum(1 for c in cards if c.enrichment_status == "done")})

        # --- structure + финальные карточки ---
        await _structure_reasons(cards, query, emit)
        final = sorted(cards, key=lambda c: c.relevance, reverse=True)
        for c in final:
            await emit({"type": "supplier", "data": _row(c)})

        # --- запись в БД + глобальный реестр ---
        for c in final:
            row = {"search_id": search_id, **_row(c)}
            db.insert_supplier(row)
            try:
                db.upsert_registry(row, query)
            except Exception as exc:
                logger.warning("registry upsert failed for %s: %s", c.name, exc)

        stats["suppliers"] = len(final)
        stats["with_contacts"] = sum(1 for c in final if c.emails or c.phones)
        stats["with_prices"] = sum(1 for c in final if c.price_min or c.gov_contracts_value)
        stats["with_gov"] = sum(1 for c in final if c.gov_contracts_count)
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
