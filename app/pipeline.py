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
        hits = await optlist_src.search_suppliers(query, with_details=6)
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
        msg = "" if hits else (
            "checko ограничивает запросы (капча) — реестр юрлиц временно "
            "недоступен, выдача компенсируется другими источниками"
            if directory_src.CHECKO_BLOCKED else
            f"по ОКВЭД {', '.join(okved_codes)} в регионе ничего не найдено")
        await emit({"type": "stage", "stage": "directory", "status": status,
                    "found": len(hits), "message": msg})
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


_TR = {"а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
       "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
       "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
       "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch",
       "э": "e", "ю": "yu", "я": "ya"}


def _brand_matches(brand: str, legal_name: str) -> bool:
    """Бренд-матч валиден, если бренд сидит в транслите имени из ЕГРЮЛ
    как сегмент/подстрока, либо почти равен сегменту (fuzzy>=85 —
    ловит defagroup/ДЕФАГРУПП=89). Короткие сегменты (<4) игнорируются:
    иначе '23' из «ОПТИМУМ 23» ложно сидит в 'opt23'."""
    from rapidfuzz import fuzz
    translit = "".join(_TR.get(ch, ch) for ch in legal_name.lower())
    translit = re.sub(r"[^a-z0-9]", " ", translit)
    b = brand.lower()
    for seg in translit.split():
        if len(seg) < 4:
            continue
        if b == seg or b in seg or seg in b:
            return True
        if len(b) >= 5 and fuzz.ratio(b, seg) >= 85:
            return True
    return False


async def _resolve_legal(cards: list, emit: Emit, limit: int = 30) -> None:
    """Водопад, этап 1: идентификаторы и статус (DaData, ЕГРЮЛ).

    Обрабатываем ВСЕ карточки с дефицитом (нет ИНН/ОГРН/юрлица/статуса).
    DaData — единственный платный (метрованный) источник, поэтому здесь
    сохраняется потолок: приоритет — карточки с ИНН (дешёвая валидация),
    затем домен-карточки (бренд-лукап), затем остальные по релевантности."""
    await emit({"type": "stage", "stage": "company", "status": "start",
                "message": "ЕГРЮЛ (DaData): идентификаторы и статус юрлиц"})
    deficient = [c for c in cards
                 if c.inn or not c.ogrn or not c.legal_name or not c.company_status]
    with_inn = [c for c in deficient if c.inn]
    # домен-карточки резолвим ВСЕ: бренд-лукап — 1 дешёвый DaData-запрос,
    # а карточка без юрлица для закупщика бесполезна (принцип «обрубка»)
    with_domain = [c for c in deficient if not c.inn and c.domain]
    rest = [c for c in deficient if not c.inn and not c.domain]
    rest.sort(key=lambda c: c.relevance, reverse=True)
    top = (with_inn + with_domain + rest)[:limit]
    for c in top:
        try:
            info = None
            if c.inn:
                # ЕГРЮЛ — источник истины: сайт может называть себя брендом
                # или устаревшим именем. DaData всегда перекрывает.
                info = await companyinfo_src.by_inn(c.inn)
            elif c.ogrn:
                info = await companyinfo_src.by_inn(c.ogrn)  # dadata ищет и по ОГРН
                if info:
                    c.inn = info.inn
                    c.sources["ИНН"] = f"ЕГРЮЛ: найден по ОГРН {c.ogrn}"
            else:
                # органика: имя = домен -> бренд-лукап (auchan.ru -> ООО «АШАН»).
                # Валидация обязательна: короткий бренд находит тёзок
                # (кейс opt23.ru -> чужое ООО «ОПТИМУМ 23», ликвидировано!)
                if c.domain:
                    brand = c.domain.split(".")[0]
                    info = await companyinfo_src.by_name(brand, trust_first=True)
                    if info and not _brand_matches(brand, info.legal_name or ""):
                        info = None  # тёзка, не наша компания
                if not info and c.name and not c.name.startswith("http"):
                    info = await companyinfo_src.by_name(c.name)
                if info:
                    c.inn = info.inn
                    c.sources["юрлицо"] = "ЕГРЮЛ (dadata.ru): найдено по бренду сайта"
            if info and info.inn:
                if not c.inn:
                    c.inn = info.inn
                if not c.legal_name and info.legal_name:
                    c.legal_name = info.legal_name
                elif info.legal_name and c.legal_name \
                        and info.legal_name.upper() != c.legal_name.upper():
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
                src_host = (info.source_url or "").split("/")[2] if info.source_url else "ЕГРЮЛ"
                c.sources["юрлицо"] = f"ЕГРЮЛ ({src_host}): {info.legal_name or c.name}"
                if info.status and info.status != "Действующая":
                    c.relevance = 0
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
            logger.debug("resolve legal %s failed: %s", c.name, exc)
    await emit({"type": "stage", "stage": "company", "status": "run",
                "message": f"ЕГРЮЛ проверен: {sum(1 for c in cards if c.company_status)} юрлиц"})


async def _checko_contacts_pass(cards: list, emit: Emit) -> None:
    """Водопад, этап 2 (самый дешёвый и урожайный): checko-карточки.

    ВСЕ карточки с ИНН/ОГРН без контактов: один визит в карточку ЕГРЮЛ
    даёт телефоны, email, сайт и прямую ссылку на карточку — 70-80% всех
    контактов берётся именно здесь (~0.5с на карточку)."""
    from .merge import domain_key, is_blacklisted

    def _real_site(c) -> bool:
        return bool(c.site and "optlist.ru" not in c.site)

    async def _fill(c):
        try:
            cc = await companyinfo_src.contacts_by_ogrn(c.ogrn or c.inn)
        except Exception as exc:
            logger.debug("contacts_by_ogrn %s failed: %s", c.ogrn or c.inn, exc)
            return
        if not cc:
            return
        for ph in cc.get("phones", [])[:2]:
            if ph not in c.phones:
                c.phones.append(ph)
        for e in cc.get("emails", [])[:2]:
            if e not in c.emails:
                c.emails.append(e)
        if cc.get("site") and not _real_site(c):
            d = domain_key(cc["site"])
            if d and not is_blacklisted(d):
                c.domain = d
                c.site = "https://" + cc["site"]
                c.sources["сайт"] = f"ЕГРЮЛ (checko.ru): {d}"
        if cc.get("phones") or cc.get("emails"):
            c.sources["контакты"] = "ЕГРЮЛ: карточка checko.ru"
        if cc.get("checko_url"):
            c.checko_url = cc["checko_url"]

    deficient = [c for c in cards if (c.ogrn or c.inn) and not (c.emails or c.phones)]
    deficient.sort(key=lambda c: c.relevance, reverse=True)
    if deficient:
        from .sources.companyinfo import _CheckoState
        import time as _time
        blocked = _CheckoState.blocked_until > _time.time()
        msg = (f"карточки ЕГРЮЛ (checko): {min(len(deficient), 40)} компаний"
               if not blocked else
               "checko временно ограничил запросы (лимит) — контакты из "
               "карточек ЕГРЮЛ пропущены, работают сайт-обход и другие источники")
        await emit({"type": "stage", "stage": "company", "status": "run", "message": msg})
        # троттлинг 1.2с/запрос: 40 карточек ≈ 50с; результаты кэшируются в БД
        await asyncio.gather(*(_fill(c) for c in deficient[:40]))
        for c in deficient[:40]:
            if c.emails or c.phones:
                await emit({"type": "supplier", "data": _row(c)})
    # ссылка на карточку ЕГРЮЛ — всем с идентификатором
    for c in cards:
        ident = c.ogrn or c.inn
        if ident and not c.checko_url:
            c.checko_url = f"https://checko.ru/search?query={ident}"


async def _maps_pass(cards: list, emit: Emit, region_name: str, limit: int = 10) -> None:
    """Водопад, финальный (самый тяжёлый и низкоурожайный): Яндекс Карты."""
    from .sources.maps import org_profile
    from .merge import region_ok as _rok
    candidates = [c for c in cards if c.city and _rok(c.city, region_name) and not c.maps_url]
    candidates.sort(key=lambda c: c.relevance, reverse=True)
    candidates = candidates[:limit]
    if candidates:
        await emit({"type": "stage", "stage": "company", "status": "run",
                    "message": f"Яндекс Карты: рейтинг {len(candidates)} компаний"})

    async def _maps(c):
        query_name = (c.legal_name or c.name).replace('"', "")
        prof = await org_profile(query_name, c.city)
        if prof:
            c.maps_url = prof.get("url")
            c.maps_rating = prof.get("rating")
            c.maps_reviews = prof.get("reviews")
            if c.maps_rating:
                c.sources["рейтинг"] = (f"Яндекс Карты: {c.maps_rating} "
                                       f"({c.maps_reviews or 0} отзывов)")
            # контакты из профиля организации — страховка от лимита checko
            for ph in prof.get("phones", [])[:2]:
                if ph not in c.phones:
                    c.phones.append(ph)
            if prof.get("email") and prof["email"] not in c.emails:
                c.emails.append(prof["email"])
            if (prof.get("phones") or prof.get("email")) \
                    and "контакты" not in c.sources:
                c.sources["контакты"] = "Яндекс Карты (профиль организации)"
        else:
            # профиля нет (типично для B2B) — глубокая ссылка на поиск
            from urllib.parse import quote
            c.maps_url = "https://yandex.ru/maps/?text=" + quote(f"{query_name} {c.city}")

    await asyncio.gather(*(_maps(c) for c in candidates))


def _translit_key(tokens: list[str]) -> str:
    """Транслит названия для домен-матчинга: «морфиш» -> 'morfish',
    «знатная мука» -> 'znatnayamuka' (дефисы/точки домена игнорируются)."""
    lat = {"а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
           "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
           "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
           "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch",
           "э": "e", "ю": "yu", "я": "ya"}
    return "".join(lat.get(ch, "") for t in tokens for ch in t.lower())


def _has_real_site(c) -> bool:
    """Профиль optlist.ru/company/<slug> — не сайт компании: каталог.
    Каталожные ссылки НЕ считаем сайтом при поиске/обходе."""
    return bool(c.site and "optlist.ru" not in c.site)


async def _find_sites_for(cards: list, emit: Emit, limit: int = 60) -> None:
    """Сайт компании по юрлицу: один Яндекс-запрос через XML-провайдера.

    Практика: если у компании сайт вообще есть — юрлицо выводит его в топ-10
    выдачи. Сканируем все 10 позиций, матчинг по токенам названия
    (строгое вхождение «ООО Морфиш» редко встречается на их же сайте —
    там пишут «Морфиш» или брендом), агрегаторы отрезает блэклист.

    Фолбэк-запрос по короткому имени (без «ООО») если топ-10 по полному
    юрлицу не сматчился. Параллельно (4 запроса одновременно).
    """
    from .merge import domain_key, is_blacklisted

    def core_tokens(name: str) -> list[str]:
        """Значимые токены названия: без организационно-правовых форм."""
        stop = {"ооо", "ао", "пао", "зао", "оао", "ип", "ано", "фгуп", "гк", "тд",
                "официальный", "сайт", "компания", "группа", "торговый", "дом"}
        cleaned = re.sub(r"[«»\"']", " ", name.lower())
        toks = [t for t in re.split(r"[\s,.]+", cleaned) if len(t) > 2 and t not in stop]
        return toks

    sem = asyncio.Semaphore(4)
    found = 0

    async def _find_one(c):
        nonlocal found
        legal = (c.legal_name or c.name).replace("«", " ").replace("»", " ").replace('"', " ")
        legal = re.sub(r"\s+", " ", legal).strip()
        tokens = core_tokens(legal)
        if not tokens:
            return
        # запрос строго по полному юрлицу: короткое имя ловит тёзок
        # (кейс «Морфиш» -> мореходный институт morfish.ru) — не используем
        for query in (legal,):
            # Яндекс Live — круглосуточный; Google — фолбэк
            hits: list = []
            for engine in ("yandexlive", "google"):
                async with sem:
                    try:
                        hits = await serp_src.fetch_serp([query], lr=225, top=10,
                                                         engines=[engine])
                    except Exception as exc:
                        logger.debug("site search '%s' via %s failed: %s", query, engine, exc)
                        hits = []
                if hits:
                    break
            # --- скоринг кандидатов: не «первый в топе», а лучший ---
            # 3 сигнала: домен-транслит названия (сильнейший), вхождение
            # в title, вхождение в сниппет; отсекаем страницы-профили
            # отраслевых каталогов (/company/123) — они всегда выше
            # сайтов мелких компаний в выдаче
            best = None  # (score, hit, domain)
            for h in hits:
                d = domain_key(h.url)
                if not d or is_blacklisted(d):
                    continue
                low_url = h.url.lower()
                if re.search(r"/(compan(y|ies)|firm|org|catalog|supplier|postavshhik|"
                             r"vendo?rs?|b2b|contragent|kontragent|proverka|"
                             r"organization|organizaci|find\?|search\?)", low_url) \
                        or re.search(r"/(id|firm)/\d+", low_url):
                    continue  # досье-страница/каталог, не сайт компании
                blob = (h.title + " " + h.snippet).lower()
                matched = sum(1 for t in tokens if t in blob)
                if matched < max(1, len(tokens) // 2):
                    continue  # компания лишь упомянута на чужой странице
                # транслит по ПОЛНОМУ хосту: сайты мелких компаний часто
                # живут на поддоменах билдеров (морфиш -> morfish.clients.site)
                host = h.url.split("//", 1)[-1].split("/", 1)[0]
                host_clean = re.sub(r"[^a-z0-9]", "", host)
                score = matched * 2 - h.position * 0.3
                if _translit_key(tokens) and _translit_key(tokens) in host_clean:
                    score += 10  # хост = транслит названия
                if any(t in h.title.lower() for t in tokens):
                    score += 3  # в title сильнее, чем в сниппете
                if best is None or score > best[0]:
                    best = (score, h, d)
            # порог 3: сниппет-матчей недостаточно (мусор), нужен
            # транслит-хост или вхождение названия в title страницы
            if best and best[0] >= 3:
                _s, h, d = best
                # билдер-поддомены (морфиш.clients.site): хранить полный хост,
                # иначе разные компании на одном билдере склеятся в дедупе
                host = h.url.split("//", 1)[-1].split("/", 1)[0]
                host = host.removeprefix("www.")
                c.domain = host if host != d else d
                c.site = h.url
                c.sources["сайт"] = f"найден поиском по юрлицу: {d}"
                found += 1
                await emit({"type": "stage", "stage": "company", "status": "run",
                            "message": f"сайт найден: {legal[:40]} -> {d}"})
                return

    candidates = [c for c in cards
                  if (c.legal_name or c.name) and not _has_real_site(c)]
    candidates.sort(key=lambda c: c.relevance, reverse=True)
    await asyncio.gather(*(_find_one(c) for c in candidates[:limit]))




async def _late_legal_resolve(cards: list, emit: Emit) -> None:
    """Резолв юрлица ПОСЛЕ обхода сайтов: футер сайта даёт точное название
    («ООО Югоптторг-23» для opt23.ru), по нему DaData находит правильную
    компанию. Бренд-лукап на раннем этапе мог промазать или не найти."""
    deficient = [c for c in cards
                 if not (c.inn or c.ogrn) and c.legal_name]
    if not deficient:
        return
    await emit({"type": "stage", "stage": "company", "status": "run",
                "message": f"юрлицо по реквизитам с сайтов: {len(deficient)} карточек"})
    for c in deficient[:30]:
        try:
            info = await companyinfo_src.by_name(c.legal_name)
            if not info or not info.inn:
                continue
            c.inn = info.inn
            if not c.ogrn and info.ogrn:
                c.ogrn = info.ogrn
            if not c.company_status and info.status:
                c.company_status = info.status
            if not c.director and info.director:
                c.director = info.director
            if not c.reg_date and info.reg_date:
                c.reg_date = info.reg_date
            c.sources["юрлицо"] = f"ЕГРЮЛ (dadata.ru): по реквизитам сайта — {info.legal_name or c.name}"
            if info.status and info.status != "Действующая":
                c.relevance = 0
            await emit({"type": "supplier", "data": _row(c)})
        except Exception as exc:
            logger.debug("late resolve %s failed: %s", c.name, exc)




async def _legal_facts_pass(cards: list, emit: Emit) -> int:
    """Дочитать досье юрлица для карточек, чей ИНН появился ПОЗЖЕ резолва
    (реквизиты с сайта — кейс bik-fish.ru — или поздний резолв): к этому
    моменту этапы «по ИНН» уже отработали вхолостую. DaData по ИНН —
    реквизиты и статус, ФНС БФО — статус и выручка/прибыль/активы."""
    todo = [c for c in cards
            if c.inn and (not c.company_status or not c.legal_name
                           or not c.revenue_note)]
    if not todo:
        return 0
    from app.sources import nalog as nalog_src
    await emit({"type": "stage", "stage": "company", "status": "run",
                "message": f"дочитать досье по ИНН: {len(todo)} карточек"})
    done = 0
    for c in todo[:40]:
        try:
            if not c.company_status or not c.legal_name or not c.director:
                info = await companyinfo_src.by_inn(c.inn)
                if info:
                    if not c.legal_name and info.legal_name:
                        c.legal_name = info.legal_name
                    if not c.ogrn and info.ogrn:
                        c.ogrn = info.ogrn
                    if not c.company_status and info.status:
                        c.company_status = info.status
                    if not c.director and info.director:
                        c.director = info.director
                    if not c.reg_date and info.reg_date:
                        c.reg_date = info.reg_date
                    if not c.okved and info.okved:
                        c.okved = info.okved
                    if "юрлицо" not in c.sources:
                        c.sources["юрлицо"] = (
                            f"ЕГРЮЛ (dadata.ru): по ИНН с сайта — {info.legal_name or c.inn}")
                    if info.status and info.status != "Действующая":
                        c.relevance = 0
                    done += 1
            await nalog_src.enrich_companies([c], emit)
            await emit({"type": "supplier", "data": _row(c)})
        except Exception as exc:
            logger.debug("facts pass %s failed: %s", c.name, exc)
    return done


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

        # =================================================================
        # ВОДОПАД ДОСБОРА: все карточки с дефицитом, источники от дешёвых
        # урожайных к тяжёлым; каждый этап снижает работу следующего.
        # =================================================================

        # --- 1. Идентификаторы и статус: ЕГРЮЛ через DaData (метрованный
        #     API — единственный осознанный потолок), гособход 44/223-ФЗ ---
        await _resolve_legal(cards, emit, limit=60)

        # --- 1.5 ФНС БФО: официальный статус + выручка по ИНН
        #     (bo.nalog.gov.ru, без ключей; статус кормит фильтр неактивных) ---
        from app.sources import nalog as nalog_src
        n_f = await nalog_src.enrich_companies(cards, emit)
        if n_f:
            await emit({"type": "stage", "stage": "company", "status": "run",
                        "message": f"ФНС БФО: выручка получена для {n_f} юрлиц"})

        # --- 2. Неактивные юрлица — сразу вон: нет смысла тратить обходы ---
        before = len(cards)
        inactive = [c for c in cards
                    if c.company_status and c.company_status != "Действующая"]
        if inactive:
            db.insert_excluded(search_id, [_row(c) for c in inactive], "inactive")
        cards = [c for c in cards
                 if not (c.company_status and c.company_status != "Действующая")]
        excluded = before - len(cards)
        if excluded:
            stats["excluded_inactive"] = excluded
            await emit({"type": "stage", "stage": "company", "status": "run",
                        "message": f"скрыто неактивных юрлиц: {excluded}"})

        # --- 3. Checko-карточки: телефоны/email/сайт одним визитом —
        #     самый дешёвый источник (0.5с/шт), 70-80% контактов здесь ---
        await _checko_contacts_pass(cards, emit)

        # --- 4. Сайт-поиск по названию юрлица: для тех, кто ещё без
        #     контактов и без сайта (Яндекс XML ~1.5с/шт) ---
        siteless = [c for c in cards
                    if not (c.emails or c.phones) and not _has_real_site(c)
                    and (c.legal_name or c.name)]
        if siteless:
            siteless.sort(key=lambda c: c.relevance, reverse=True)
            await emit({"type": "stage", "stage": "company", "status": "run",
                        "message": f"поиск сайтов по названию: {min(len(siteless), 40)} компаний"})
            await _find_sites_for(siteless[:40], emit, limit=40)

        # --- 5. Обход сайтов: ВСЕ дефицитные с сайтом (самый тяжёлый
        #     источник ~10-20с/шт, поэтому последним). Дефицит-приоритет:
        #     бесконтактные — первыми, потом недообогащённые (нет цен/МОК) ---
        await emit({"type": "stage", "stage": "enrich", "status": "start"})
        by_domain = {c.domain: c for c in cards if c.domain}
        contactless = [c for c in cards
                       if _has_real_site(c) and not (c.emails or c.phones)]
        partial = [c for c in cards
                   if _has_real_site(c) and (c.emails or c.phones)
                   and not (c.price_min or c.min_order)]
        contactless.sort(key=lambda x: x.relevance, reverse=True)
        partial.sort(key=lambda x: x.relevance, reverse=True)
        to_enrich = (contactless + partial)[:s.enrich_max_sites]

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

        # --- 5.5 Поздний резолв: юрлицо из реквизитов сайтов + повторный
        #     отсев неактивных (ранний бренд-лукап мог и промазать/промолчать)
        await _late_legal_resolve(cards, emit)
        await _legal_facts_pass(cards, emit)
        before = len(cards)
        late_inactive = [c for c in cards
                         if c.company_status and c.company_status != "Действующая"]
        if late_inactive:
            db.insert_excluded(search_id, [_row(c) for c in late_inactive], "inactive")
            cards = [c for c in cards
                     if not (c.company_status and c.company_status != "Действующая")]
            stats["excluded_inactive"] = stats.get("excluded_inactive", 0) + (before - len(cards))

        # --- 6. Гео: для городов федерального значения карточки с ИЗВЕСТНЫМ
        #     чужим городом — исключаем целиком (кейс ТрансСибЛизинг/Новосибирск
        #     в поиске по Москве): нам нужен поставщик, который здесь работает.
        #     Для областей/краёв — пессимизация, не исключение.
        from .merge import region_ok as _region_ok, CITY_REGION_ALIASES
        is_federal = region_name in CITY_REGION_ALIASES
        geo_mismatches = 0
        kept_geo = []
        for c in cards:
            if c.city and not _region_ok(c.city, region_name):
                if is_federal:
                    continue  # исключение из результатов
                c.relevance = max(0, c.relevance - 15)
                c.sources["гео"] = f"{c.city} — вне региона запроса ({region_name})"
                geo_mismatches += 1
            kept_geo.append(c)
        geo_dump = [c for c in cards if c not in kept_geo]
        if geo_dump:
            db.insert_excluded(search_id, [_row(c) for c in geo_dump], "geo")
        excluded_geo = len(cards) - len(kept_geo)
        if excluded_geo:
            stats["excluded_geo"] = excluded_geo
        cards = kept_geo
        if geo_mismatches or excluded_geo:
            await emit({"type": "stage", "stage": "company", "status": "run",
                        "message": f"гео-фильтр: исключено {excluded_geo}, пессимизировано {geo_mismatches}"})

        # --- 7. Яндекс Карты (самый тяжёлый и низкоурожайный): рейтинг ---
        await _maps_pass(cards, emit, region_name)
        await emit({"type": "stage", "stage": "company", "status": "done",
                    "found": sum(1 for c in cards if c.inn)})

        # --- карточки БЕЗ юрлица (ИНН/ОГРН) не показываем: для закупщика
        #     аноним «interfoodgroup.ru» = обрубок телефона — с ним ничего
        #     нельзя сделать (ни договор, ни проверку, ни оплату) ---
        before_no_legal = len(cards)
        no_legal = [c for c in cards if not (c.inn or c.ogrn)]
        if no_legal:
            db.insert_excluded(search_id, [_row(c) for c in no_legal], "no_legal")
        cards = [c for c in cards if c.inn or c.ogrn]
        excluded_no_legal = len(no_legal)
        if excluded_no_legal:
            stats["excluded_no_legal"] = excluded_no_legal
            await emit({"type": "stage", "stage": "company", "status": "run",
                        "message": f"скрыто карточек без юрлица: {excluded_no_legal}"})

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
