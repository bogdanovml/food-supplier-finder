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
import re
import time
from typing import Any, Callable

from . import db
from .config import get_settings
from .extraction import enrich_sites
from .llm import ask_json, LLMError
from .merge import merge_all, apply_enrichment, region_ok
from .okpd import suggest_codes, validate_codes
from .regions import resolve
from .sources import companyinfo as companyinfo_src
from .sources import directory as directory_src
from .sources import gosplan as gosplan_src
from .sources import optlist as optlist_src
from .sources import xmlstock as serp_src

logger = logging.getLogger(__name__)

Event = dict[str, Any]
Emit = Callable[[Event], None]

STAGES = ["plan", "serp", "optlist", "gosplan", "directory", "merge",
          "enrich", "company", "structure", "done"]


async def _plan_queries(query: str, emit: Emit) -> tuple[list[str], list[str], list[str]]:
    """LLM строит поисковые запросы + ОКПД2 (госконтракты) + ОКВЭД (ЕГРЮЛ-каталог).

    ОКПД2 валидируется по справочнику; ОКВЭД самовалидируется каталогом
    (страница без компаний = код не существует)."""
    await emit({"type": "stage", "stage": "plan", "status": "start"})
    queries: list[str] = []
    codes_raw: list[str] = []
    okved_raw: list[str] = []
    try:
        data = await ask_json(
            f'Товар: "{query}". '
            f'1) Составь 2-3 поисковых запроса для Яндекса, чтобы найти ОПТОВЫХ '
            f'поставщиков/производителей этого товара в России. '
            f'2) Назови 1-3 кода ОКПД2 (российский классификатор продукции), '
            f'к которым относится товар (мука -> 10.61.21). '
            f'3) Назови 1-2 кода ОКВЭД2 (классификатор видов деятельности), '
            f'подходящих для ПРОИЗВОДСТВА или ОПТОВОЙ ТОРГОВЛИ этим товаром '
            f'(производство муки -> 10.61, оптовая торговля мукой -> 46.21). '
            f'Ответь JSON: {{"queries": ["..."], "okpd2": ["10.xx"], "okved2": ["10.xx"]}}',
            smart=True, temperature=0.2, max_tokens=500)
        queries = [q for q in data.get("queries", []) if isinstance(q, str) and len(q) > 3][:3]
        codes_raw = [str(c).strip() for c in data.get("okpd2", []) if c][:3]
        okved_raw = [str(c).strip() for c in data.get("okved2", []) if c][:2]
    except LLMError:
        pass
    queries = queries or [f"{query} оптом", f"{query} производитель купить"]

    codes = validate_codes(codes_raw)
    for c, _n in suggest_codes(query, limit=2):
        if c not in codes:
            codes.append(c)
    codes = codes[:3]
    okved = [c for c in okved_raw if re.match(r"^\d{2}\.\d{2}", c)][:2]

    await emit({"type": "stage", "stage": "plan", "status": "done", "found": len(queries),
                "message": f"Запросы: {'; '.join(queries)}. ОКПД2: {', '.join(codes) or '—'}. ОКВЭД: {', '.join(okved) or '—'}"})
    return queries, codes, okved


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


async def _run_directory(okved_codes: list[str], region_name: str, emit: Emit) -> list:
    await emit({"type": "stage", "stage": "directory", "status": "start",
                "message": f"ЕГРЮЛ-каталог по ОКВЭД: {', '.join(okved_codes) or '—'} "
                           f"(действующие юрлица отрасли, фильтр: {region_name})"})
    if not okved_codes:
        await emit({"type": "stage", "stage": "directory", "status": "skipped",
                    "message": "LLM не предложил ОКВЭД-кодов"})
        return []
    try:
        hits = await directory_src.search_okved_directory(
            okved_codes, region_name=region_name, max_per_code=100)
        status = "done" if hits else "skipped"
        await emit({"type": "stage", "stage": "directory", "status": status,
                    "found": len(hits),
                    "message": "" if hits else "по кодам/региону ничего не найдено"})
        return hits
    except Exception as exc:
        logger.warning("directory stage failed: %s", exc)
        await emit({"type": "stage", "stage": "directory", "status": "skipped",
                    "message": f"каталог ОКВЭД пропущен: {exc}"})
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
                "revenue": (c.revenue_note or "")[:30],
                "okved": (c.okved or "")[:20],
                "director": (c.director or "")[:40],
                "certificates": "; ".join(c.certificates[:3]),
                "kind": (c.sources.get("тип") or "")[:40],
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
    if card.revenue_note:
        okved_part = f", ОКВЭД {card.okved}" if card.okved else ""
        return (f"Действующее юрлицо отрасли{okved_part}, выручка {card.revenue_note} —"
                f" масштаб соответствует оптовым поставкам")
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
                  or c.min_order or c.certificates or c.inn or c.revenue_note)
        if not useful and c.enrichment_status != "done":
            c.relevance = min(c.relevance, 10)


def _pessimize_all(cards) -> None:
    _pessimize_thin(cards)


async def _run_company(cards: list, emit: Emit, region_name: str = "", limit: int = 18) -> None:
    """Юрлица: dadata/checko по ИНН/ОГРН/названию + полный опыт 44/223-ФЗ.

    Приоритет: (1) карточки с известным ИНН — быстрая валидация ЕГРЮЛ;
    (2) домен-карточки (бренд-лукап через DaData, кейс auchan.ru);
    (3) остальные по рейтингу."""
    await emit({"type": "stage", "stage": "company", "status": "start",
                "message": "Проверка юрлиц через ЕГРЮЛ (DaData) и опыт 44/223-ФЗ по ИНН"})
    with_inn = [c for c in cards if c.inn]
    rest = [c for c in cards if not c.inn]
    # домен-карточки бустим: бренд-лукап дешёвый и даёт юрлицо даже пустышке
    rest.sort(key=lambda c: (c.relevance + (10 if c.domain else 0)), reverse=True)
    top = (with_inn + rest)[:limit]

    def is_bl(d: str) -> bool:
        from .merge import is_blacklisted
        return is_blacklisted(d)

    # --- параллельный проход: контакты/сайт из карточек ЕГРЮЛ (checko)
    # для ВСЕХ юрлиц с ОГРН без контактов (кейс ООО «АВИСТРОН»: контакты
    # есть в карточке компании, но карточка с низким рейтингом не попадала
    # в top-N основной проверки) ---
    from .merge import domain_key

    async def _fill_contacts(c):
        try:
            cc = await companyinfo_src.contacts_by_ogrn(c.ogrn)
        except Exception as exc:
            logger.debug("contacts_by_ogrn %s failed: %s", c.ogrn, exc)
            return
        if not cc:
            return
        for p in cc.get("phones", [])[:2]:
            if p not in c.phones:
                c.phones.append(p)
        for e in cc.get("emails", [])[:2]:
            if e not in c.emails:
                c.emails.append(e)
        if cc.get("site") and not c.site:
            d = domain_key(cc["site"])
            if d and not is_bl(d):
                c.domain = d
                c.site = "https://" + cc["site"]
                c.sources["сайт"] = f"ЕГРЮЛ (checko.ru): {d}"
        if cc.get("phones") or cc.get("emails"):
            c.sources["контакты"] = "ЕГРЮЛ: карточка checko.ru"
        if cc.get("checko_url"):
            c.checko_url = cc["checko_url"]

    ogr_cards = [c for c in cards if c.ogrn and not (c.emails or c.phones)][:25]
    if ogr_cards:
        await emit({"type": "stage", "stage": "company", "status": "run",
                    "message": f"контакты из карточек ЕГРЮЛ: {len(ogr_cards)} юрлиц"})
        await asyncio.gather(*(_fill_contacts(c) for c in ogr_cards))

    # у всех карточек с ОГРН — прямая ссылка на карточку ЕГРЮЛ (через редирект)
    for c in cards:
        if c.ogrn and not c.checko_url:
            c.checko_url = f"https://checko.ru/search?query={c.ogrn}"

    # --- Яндекс Карты: рейтинг и отзывы для топ-карточек в регионе ---
    from .sources.maps import org_profile
    from .merge import region_ok as _rok
    maps_candidates = [c for c in cards
                        if c.city and _rok(c.city, region_name)
                        and not c.maps_url][:8]
    if maps_candidates:
        await emit({"type": "stage", "stage": "company", "status": "run",
                    "message": f"Яндекс Карты: рейтинг {len(maps_candidates)} компаний"})

        async def _maps(c):
            query_name = (c.legal_name or c.name).replace('"', "")
            prof = await org_profile(query_name, c.city)
            if prof:
                c.maps_url = prof.get("url")
                c.maps_rating = prof.get("rating")
                c.maps_reviews = prof.get("reviews")
                c.sources["рейтинг"] = (f"Яндекс Карты: {prof.get('rating')} "
                                       f"({prof.get('reviews')} отзывов)")

        await asyncio.gather(*(_maps(c) for c in maps_candidates))

    for c in top:
        try:
            info = None
            if c.inn:
                # ЕГРЮЛ — единственный источник истины: сайт может называть
                # себя брендом или устаревшим именем. DaData всегда перекрывает.
                info = await companyinfo_src.by_inn(c.inn)
            elif c.ogrn:
                info = await companyinfo_src.by_inn(c.ogrn)   # dadata находит и по ОГРН
                if info:
                    c.inn = info.inn
                    c.sources["ИНН"] = f"ЕГРЮЛ: найден по ОГРН {c.ogrn}"
            elif not c.ogrn:
                # у органики имя = домен: ищем юрлицо по бренду из домена
                # (кейс auchan.ru -> 'auchan' -> ООО «АШАН») или по названию
                if c.domain:
                    brand = c.domain.split(".")[0]
                    info = await companyinfo_src.by_name(brand, trust_first=True)
                if not info:
                    info = await companyinfo_src.by_name(c.name)
                if info:
                    c.inn = info.inn
                    c.sources["юрлицо"] = f"ЕГРЮЛ (dadata.ru): найдено по бренду сайта"
            if info and info.inn:
                if not c.inn:
                    c.inn = info.inn
                if not c.legal_name and info.legal_name:
                    c.legal_name = info.legal_name
                elif info.legal_name and c.legal_name and info.legal_name.upper() != c.legal_name.upper():
                    # сайт назвался иначе, чем ЕГРЮЛ (бренд/переименование) —
                    # показываем оба: ЕГРЮЛ как юрлицо, сайтовое как «бренд»
                    c.sources["бренд/название сайта"] = f"сайт: {c.legal_name}"
                    c.legal_name = info.legal_name
                if not c.ogrn and info.ogrn:
                    c.ogrn = info.ogrn
                if not c.director and info.director:
                    c.director = info.director
                if not c.okved and info.okved:
                    c.okved = info.okved
                if not c.reg_date and info.reg_date:
                    c.reg_date = info.reg_date
                if not c.company_status and info.status:
                    c.company_status = info.status
                if not c.staff and info.staff:
                    c.staff = info.staff
                src = (info.source_url or "").split("/")[2] if info.source_url else "ЕГРЮЛ"
                c.sources["юрлицо"] = f"ЕГРЮЛ ({src}): {info.legal_name or c.name}"
                if info.status and info.status != "Действующая":
                    c.relevance = 0
            # (контакты из карточек ЕГРЮЛ заполняются отдельным параллельным
            #  проходом выше — _fill_contacts — для ВСЕХ карточек с ОГРН,
            #  а не только попавших в top-N)

            # узнав ИНН — проверяем полный опыт 44/223-ФЗ
            if c.inn and c.gov_contracts_count == 0:
                stat = await gosplan_src.contracts_by_inn(c.inn, limit=5)
                if stat["count"]:
                    c.gov_contracts_count = stat["count"]
                    c.gov_contracts_value = stat["value"]
                    ex = stat.get("example") or {}
                    if ex:
                        c.gov_contract_examples = [ex]
                    c.sources["опта 44-ФЗ"] = (
                        f"поставщик {stat['count']} контрактов 44-ФЗ "
                        f"на {stat['value']:,.0f} ₽ (поиск по ИНН)")
                    c.relevance = min(100, c.relevance + 10)
            await emit({"type": "supplier", "data": _row(c)})
        except Exception as exc:
            logger.debug("company stage card %s failed: %s", c.name, exc)
    await emit({"type": "stage", "stage": "company", "status": "done",
                "found": sum(1 for c in top if c.inn)})


async def _find_sites_for(cards: list, emit: Emit, limit: int = 8) -> None:
    """Сайт-поиск для карточек из ЕГРЮЛ-каталога без домена (кейс МАРИТАЙМ ГРУП:
    «сайт не обнаружен», хотя гуглится первой строкой).

    Кого ищем: карточки с юрлицом/выручкой (реальные компании), но без сайта.
    Как: одна Яндекс-выдача по «"<юрлицо>" официальный сайт» — первый
    не-блэклистовый домен становится сайтом компании.
    """
    from .merge import domain_key, is_blacklisted
    candidates = [c for c in cards if (c.legal_name or c.name) and not c.domain and not c.site]
    candidates.sort(key=lambda c: c.relevance, reverse=True)
    for c in candidates[:limit]:
        try:
            legal = (c.legal_name or c.name).replace("«", " ").replace("»", " ").replace('"', " ")
            legal = re.sub(r"\s+", " ", legal).strip()
            hits = await serp_src.fetch_serp(
                [f'{legal} официальный сайт'], lr=225, top=5)
            for h in hits:
                d = domain_key(h.url)
                if not d or is_blacklisted(d):
                    continue
                # «официальный сайт» от прямого вхождения имени в сниппете
                if legal.lower() not in (h.title + h.snippet).lower():
                    continue
                c.domain = d
                c.site = h.url
                c.sources["сайт"] = f"найден поиском по названию юрлица: {d}"
                await emit({"type": "stage", "stage": "enrich", "status": "run",
                            "message": f"сайт для {legal}: {d}"})
                break
        except Exception as exc:
            logger.debug("find site for %s failed: %s", legal, exc)


async def run_search(query: str, region_name: str, emit: Emit) -> None:
    """Полный цикл: создаёт search в БД, гоняет этапы, пишет поставщиков + реестр."""
    s = get_settings()
    lr, gos_region = resolve(region_name)
    search_id = db.create_search(query, region_name, lr)
    stats: dict[str, Any] = {"search_id": search_id}
    t0 = time.monotonic()

    try:
        queries, codes, okved = await _plan_queries(query, emit)

        await emit({"type": "stage", "stage": "merge", "status": "pending"})
        # четыре источника параллельно
        serp_hits, opt_hits, gov_sup, dir_hits = await asyncio.gather(
            _run_serp(queries, lr, emit),
            _run_optlist(query, emit),
            _run_gosplan(query, gos_region, codes, emit),
            _run_directory(okved, region_name, emit),
        )
        stats["serp"] = len(serp_hits)
        stats["optlist"] = len(opt_hits)
        stats["gosplan"] = len(gov_sup)
        stats["directory"] = len(dir_hits)

        # --- merge ---
        await emit({"type": "stage", "stage": "merge", "status": "start"})
        cards = merge_all(serp_hits, opt_hits, gov_sup, dir_hits, region_name)
        _pessimize_all(cards)
        await emit({"type": "stage", "stage": "merge", "status": "done", "found": len(cards)})

        # карточки сразу в UI (enrichment_status=pending)
        by_domain = {c.domain: c for c in cards if c.domain}
        for c in sorted(cards, key=lambda x: x.relevance, reverse=True):
            await emit({"type": "supplier", "data": _row(c)})

        # --- юрлица без сайта (ЕГРЮЛ-каталог) — ищем сайт по названию ---
        await _find_sites_for(cards, emit)

        # --- enrich: карточки без контактов, включая каталоговые с найденным сайтом ---
        await emit({"type": "stage", "stage": "enrich", "status": "start"})
        to_enrich = [c for c in cards
                     if c.site and c.enrichment_status == "pending"
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

        # --- юрлица: dadata/checko + опыт 44-ФЗ по ИНН ---
        await _run_company(cards, emit, region_name)

        # --- гео-несовпадение: поставщик из другого города при поиске
        #     по городу федерального значения — минус к рейтингу + заметка ---
        from .merge import region_ok as _region_ok
        geo_mismatches = 0
        for c in cards:
            if not _region_ok(c.city, region_name):
                c.relevance = max(0, c.relevance - 15)
                c.sources["гео"] = f"{c.city} — вне региона запроса ({region_name})"
                geo_mismatches += 1
        if geo_mismatches:
            await emit({"type": "stage", "stage": "company", "status": "run",
                        "message": f"гео-несовпадений: {geo_mismatches} (пессимизированы)"})

        # --- неактивные юрлица (ликвидированы/в процессе) — НЕ показываем:
        # поставки от мёртвой компании невозможны, нет смысла в выборке ---
        before = len(cards)
        cards = [c for c in cards if not (c.company_status and c.company_status != "Действующая")]
        excluded = before - len(cards)
        if excluded:
            stats["excluded_inactive"] = excluded
            await emit({"type": "stage", "stage": "company", "status": "done",
                        "found": sum(1 for c in cards if c.inn),
                        "message": f"скрыто неактивных юрлиц: {excluded}"})

        _pessimize_all(cards)

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
