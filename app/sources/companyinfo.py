"""Юрлица поставщиков: извлечение с сайтов + обогащение из открытых реестров.

Поток:
  1. Юрлицо с сайта поставщика: «ООО «Ромашка»», ИНН/ОГРН в футере, на
     страницах «Реквизиты»/«О компании» (regex, бесплатно).
  2. Если ИНН на сайте нет — поиск по названию компании на checko.ru
     (открытый агрегатор ЕГРЮЛ; поиск по ИНН — мгновенный 301-редирект
     на карточку). Fuzzy-валидация имени, чтобы не склеить тёзок.
  3. Карточка checko: полное и краткое наименование, ОГРН/КПП,
     руководитель, юр. адрес, ОКВЭД, дата регистрации, статус, штат.

list-org.com тоже полезен, но агрессивно лимитит («слишком часто
обращались» уже на первом запросе) — не используем, checko хватает.

Graceful degradation: недоступность checko не роняет поиск."""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field

import httpx
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36")
BASE = "https://checko.ru"

_sem = asyncio.Semaphore(2)   # не DDoS-им открытые сервисы

from ..config import get_settings


def _dadata_headers() -> dict:
    s = get_settings()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {s.dadata_api_key}",
        "X-Secret": s.dadata_secret,
    }


def _party_to_info(party: dict, source: str) -> CompanyInfo:
    """DaData party -> CompanyInfo (структурированный ЕГРЮЛ)."""
    data = party.get("data") or {}
    state = data.get("state") or {}
    mgmt = data.get("management") or {}
    addr = (data.get("address") or {}).get("unrestricted_value") \
        or (data.get("address") or {}).get("value")
    reg_ts = state.get("registration_date")
    reg_date = None
    if reg_ts:
        import datetime
        reg_date = datetime.date.fromtimestamp(reg_ts / 1000).strftime("".join(["%d.%m.", "%Y"]))
    return CompanyInfo(
        inn=data.get("inn") or "",
        ogrn=data.get("ogrn"),
        legal_name=data.get("name", {}).get("short_with_opf") or party.get("value"),
        director=(f"{mgmt.get('post')}: {mgmt.get('name')}" if mgmt.get("name") else None),
        address=addr,
        okved=(data.get("okved") or "") + (" " + data.get("okved_name", "") if data.get("okved_name") else ""),
        okved_code=data.get("okved"),
        reg_date=reg_date,
        status={"ACTIVE": "Действующая", "LIQUIDATED": "Ликвидирована",
                "REORGANIZING": "Реорганизация", "LIQUIDATING": "В процессе ликвидации"}
            .get(state.get("status"), state.get("status")),
        staff=None,
        source_url=source,
    )


async def _dadata(endpoint: str, payload: dict) -> list[dict] | None:
    s = get_settings()
    if not s.dadata_api_key:
        return None
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15, connect=10)) as client:
            resp = await client.post(f"https://suggestions.dadata.ru/suggestions/api/4_1/rs/{endpoint}",
                                     headers=_dadata_headers(), json=payload)
            if resp.status_code == 200:
                return resp.json().get("suggestions") or []
            logger.debug("dadata %s -> %s", endpoint, resp.status_code)
            return None
    except httpx.HTTPError as exc:
        logger.debug("dadata %s failed: %s", endpoint, exc)
        return None


# ------------------------- 1. юрлицо с сайта -------------------------

RE_INN = re.compile(r"ИНН[:\s]*(\d{10,12})")
RE_OGRN = re.compile(r"ОГРН[:\s]*(\d{13,15})")
RE_LEGAL = re.compile(
    r"(ООО|АО|ПАО|ЗАО|ИП|АНО|ФГУП|ОАО)\s*«?([А-ЯЁA-Z][А-ЯЁа-яёA-Za-z0-9\-\s.]{2,50})»?")
# мусорные имена из адресов: «муниципальный округ», «шоссе», «тер. г.»...
RE_NAME_JUNK = re.compile(
    r"округ|район|обл(асть)?\b|тер\.?\b|шоссе|улица|ул\.|проспект|д\.\s|дом\b|кварт", re.I)
RE_IP_FIO = re.compile(r"^[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?$")


def _valid_legal(form: str, name: str) -> str | None:
    """Валидное юрлицо или None: отсекаем адресный мусор и голые ИП без ФИО."""
    name = name.strip().strip('"«»').strip()
    if not (2 < len(name) < 60) or re.match(r"^(опт|руб|кг|шт)$", name.lower()):
        return None
    if RE_NAME_JUNK.search(name):
        return None
    if form == "ИП" and not RE_IP_FIO.match(name):
        return None
    return f"{form} «{name}»"


def extract_legal_from_text(text: str) -> dict:
    """Юрлицо/ИНН/ОГРН из текста сайта.

    Правило контекста (кейс bik-fish.ru): «ООО «Восточный Берег»» на странице
    может быть чьим угодно — поставщиком, брендом, партнёром. Своё юрлицо
    компании пишут в РЕКВИЗИТНОМ БЛОКЕ — рядом с ИНН/ОГРН. Поэтому:
      1. нашли ИНН -> юрлицо ищем только в окне ±250 символов вокруг него;
      2. не нашли в окне -> не выдумываем имя: ИНН уйдёт в DaData за точным;
      3. ИНН вообще нет -> первое «ООО...» (низкая уверенность).
    """
    out: dict = {}
    m_inn = RE_INN.search(text)
    m_ogrn = RE_OGRN.search(text)
    if m_ogrn:
        out["ogrn"] = m_ogrn.group(1)
    if not m_inn:
        m = RE_LEGAL.search(text)
        if m:
            legal = _valid_legal(m.group(1), m.group(2))
            if legal:
                out["legal_name"] = legal
        return out

    out["inn"] = m_inn.group(1)
    # юрлицо ТОЛЬКО в реквизитном окне вокруг ИНН
    lo = max(0, m_inn.start() - 250)
    hi = min(len(text), m_inn.end() + 250)
    window = text[lo:hi]
    for m in RE_LEGAL.finditer(window):
        legal = _valid_legal(m.group(1), m.group(2))
        if legal:
            out["legal_name"] = legal
            break
    return out




# ------------------------- 2-3. checko -------------------------

@dataclass(slots=True)
class CompanyInfo:
    inn: str
    ogrn: str | None = None
    legal_name: str | None = None
    director: str | None = None
    address: str | None = None
    okved: str | None = None
    okved_code: str | None = None
    reg_date: str | None = None
    status: str | None = None
    staff: str | None = None
    source_url: str | None = None

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


async def _fetch(client: httpx.AsyncClient, url: str) -> str:
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return ""
    except httpx.HTTPError as exc:
        logger.debug("checko fetch %s failed: %s", url, exc)
        return ""


def _parse_card(html: str, url: str) -> CompanyInfo | None:
    """Карточка компании checko. Title содержит: 'Имя - Город - ИНН х - Должник ФИО'."""
    import html as html_mod
    text = html_mod.unescape(re.sub(r"<[^>]+>", "|", html))
    text = re.sub(r"[|\s]+", " ", text)
    raw_unescaped = html_mod.unescape(html)

    inn = None
    m = re.search(r"ИНН (\d{10,12})", text)
    if m:
        inn = m.group(1)
    m = re.search(r'ИНН=|inn=(\d{10,12})', html)
    if not inn and m:
        inn = m.group(1)
    if not inn:
        return None

    info = CompanyInfo(inn=inn, source_url=url)
    m = re.search(r"ОГРН (\d{13,15})", text)
    if m:
        info.ogrn = m.group(1)
    m = re.search(r'<title>([^<]+)</title>', raw_unescaped)
    if m:
        title = m.group(1)
        # 'ООО "КАЛИНА ГРУПП" - Москва - ИНН 7734464104 - Гендиректор Федоров А. В.'
        m2 = re.match(r"(.+?)\s*-\s*[^-]+?-\s*ИНН", title)
        if m2:
            info.legal_name = m2.group(1).strip()[:120]
        m2 = re.search(r"(?:Гендиректор|Директор|Руководитель)\s+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ]\.\s?[А-ЯЁ]\.)?)", title)
        if m2:
            info.director = m2.group(1)
    if not info.legal_name:
        m = re.search(r"(ООО|АО|ПАО|ИП|АНО)\s*[«\"]([^\"]{2,60})[»\"]", text)
        if m:
            info.legal_name = f"{m.group(1)} «{m.group(2)}»"
    m = re.search(r"Дата регистрации ([^|]{3,40}года)", text)
    if m:
        info.reg_date = m.group(1).strip()
    m = re.search(r"(Действующая|Недействующая|Ликвидирована|В процессе ликвидации)", text)
    if m:
        info.status = m.group(1)
    m = re.search(r"(\d{6},\s*г\.[^|]{5,120})", text)
    if m:
        info.address = m.group(1).strip()[:140]
    m = re.search(r"ОКВЭД-?2?\s*(\d{2}\.\d{2})", text)
    if m:
        info.okved_code = m.group(1)
    m = re.search(r"Вид деятельности\s*([А-ЯЁ][^|]{5,90})", text)
    if m:
        info.okved = m.group(1).strip()[:100]
    m = re.search(r"[Сс]реднесписочная численность работников\s*(\d+)", text)
    if m:
        info.staff = m.group(1)
    return info


async def by_inn(inn: str) -> CompanyInfo | None:
    """Юрлицо по ИНН: DaData (структурированный ЕГРЮЛ) -> checko (фолбэк)."""
    suggestions = await _dadata("findById/party", {"query": inn, "count": 1,
                                                 "branch_type": "MAIN"})
    if suggestions:
        info = _party_to_info(suggestions[0], "dadata.ru (ЕГРЮЛ)")
        if info.inn:
            return info
    async with _sem:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10),
                                      headers={"User-Agent": UA}) as client:
            html = await _fetch(client, f"{BASE}/search?query={inn}")
    if not html:
        return None
    return _parse_card(html, url=f"{BASE}/search?query={inn}")


async def by_name(name: str, *, min_score: int = 75, trust_first: bool = False) -> CompanyInfo | None:
    """Юрлицо по названию: DaData suggest -> fuzzy-валидация -> checko (фолбэк).

    Валидация обязательна: «Ромашка» найдёт десятки тёзок по всей стране —
    берём только уверенное совпадение имени.
    trust_first=True — пропустить fuzzy (для латинских брендов из домена:
    «auchan.ru» -> 'auchan' -> ООО «АШАН»: кириллица против латиницы
    fuzzy всегда отвергнет, а поисковый движок DaData уже сделал матчинг).
    """
    clean = re.sub(r"[«»\"']", "", name).strip()
    if len(clean) < 4:
        return None

    suggestions = await _dadata("suggest/party", {"query": clean, "count": 5})
    if suggestions:
        best = None
        for s in suggestions:
            score = fuzz.token_sort_ratio(clean.lower(), s.get("value", "").lower())
            if best is None or score > best[1]:
                best = (s, score)
        if best and (trust_first or best[1] >= min_score):
            info = _party_to_info(best[0], "dadata.ru (ЕГРЮЛ, поиск по названию)")
            if info.inn:
                return info

    # --- фолбэк: checko ---
    import html as html_mod
    async with _sem:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10),
                                      headers={"User-Agent": UA}) as client:
            html = await _fetch(client, f"{BASE}/search?query={clean}")
    if not html:
        return None

    # результаты: /company/<slug>-<ОГРН>
    candidates: list[tuple[str, str]] = []
    for href, label in re.findall(r'href="(/company/[^"]+)"[^>]*>([^<]{3,90})', html):
        label = html_mod.unescape(label).strip()
        if href not in [h for h, _ in candidates]:
            candidates.append((href, label))
    best: tuple[str, str, float] | None = None
    for href, label in candidates[:10]:
        label_clean = re.sub(r"[«»\"']", "", label)
        score = fuzz.token_sort_ratio(clean.lower(), label_clean.lower())
        if best is None or score > best[2]:
            best = (href, label, score)
    if not best or best[2] < min_score:
        return None
    href, label, _score = best
    async with _sem:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10),
                                      headers={"User-Agent": UA}) as client:
            card_html = await _fetch(client, f"{BASE}{href}")
    if not card_html:
        return None
    return _parse_card(card_html, url=f"{BASE}{href}")


async def contacts_by_ogrn(ogrn: str) -> dict:
    """Контакты и сайт юрлица из карточки ЕГРЮЛ на checko.

    Принимает ОГРН (13 цифр) или ИНН (10-12): поиск редиректит на карточку
    по любому идентификатору.

    Анти-рейт-лимит (урок 429+SmartCaptcha): checko ограничивает частые
    запросы, поэтому:
      * результат кэшируется в БД навсегда (контакты юрлиц меняются редко) —
        повторные поиски не ходят в checko за тем же юрлицом;
      * троттлинг: не более одного запроса в ~1.2с (глобальный семафор);
      * при 429/капче — ставим блок-флаг на 30 минут и честно возвращаем
        пусто: обход капчи не вариант, остальные источники продолжают.
    """
    if not ogrn or not re.match(r"^\d{10,15}$", ogrn or ""):
        return {}
    from .. import db as _db

    cached = _db.checko_cache_get(ogrn)
    if cached is not None:
        return cached

    if _CheckoState.blocked_until > _now_ts():
        return {}

    out: dict = {}
    card_url = f"{BASE}/search?query={ogrn}"
    async with _checko_sem:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10),
                                          headers={"User-Agent": UA}) as client:
                resp = await client.get(f"{BASE}/search?query={ogrn}", follow_redirects=True)
                if resp.status_code == 429 or "подтвердите" in resp.text[:3000].lower():
                    _CheckoState.blocked_until = _now_ts() + 30 * 60
                    logger.warning("checko rate-limited (429/капча): блок на 30 минут")
                    return {}
                if resp.status_code == 200 and len(resp.text) > 2000:
                    html = resp.text
                    if "/company/" in str(resp.url):
                        card_url = str(resp.url)
                else:
                    html = ""
                await asyncio.sleep(0.7)   # вежливый темп
        except httpx.HTTPError as exc:
            logger.debug("checko ogrn fetch failed: %s", exc)
            return {}
    if not html:
        return {}

    import html as html_mod
    text = html_mod.unescape(re.sub(r"<[^>]+>", " ", html))
    text = re.sub(r"\s+", " ", text)

    phones: list[str] = []
    for m in re.finditer(r"\+7[\s\-()]*\d{3}[\s\-]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}", text[:80000]):
        p = m.group(0)
        digits = re.sub(r"\D", "", p)
        if len(digits) == 11 and digits[0] == "7":
            formatted = f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:]}"
            if formatted not in phones:
                phones.append(formatted)
        if len(phones) >= 2:
            break
    if phones:
        out["phones"] = phones

    emails: list[str] = []
    for m in re.finditer(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,6}", text[:80000]):
        e = m.group(0).lower()
        if e not in emails and not e.endswith(("@checko.ru", "@example.com")):
            emails.append(e)
        if len(emails) >= 2:
            break
    if emails:
        out["emails"] = emails

    m = re.search(r"Веб-сайт[ы]*\s+([a-z0-9\-]+\.[a-z0-9\-.]{2,30})\s", text[:80000], re.I)
    if m:
        out["site"] = m.group(1).strip(".")
    if "/company/" in card_url:
        out["checko_url"] = card_url
    if out:
        _db.checko_cache_put(ogrn, out)
    return out


# ------------------------------ состояние checko ------------------------------

import time as _time_mod


def _now_ts() -> float:
    return _time_mod.time()


class _CheckoState:
    """Блок-флаг checko: после 429/капчи молчим 30 минут, не долбим."""
    blocked_until: float = 0.0


# троттлинг: один запрос за раз + пауза внутри (см. contacts_by_ogrn)
_checko_sem = asyncio.Semaphore(1)
