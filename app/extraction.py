"""Извлечение данных с сайтов-кандидатов: контакты, цены, сертификаты.

Стратегия — дешёвое → дорогое (переиспользуем подход price_extractor
из ai-tools.su/search, адаптированный под food-прототип):
  1. regex-проход по HTML: телефоны, email, цены («42 руб/кг», «от 850 ₽»),
     маркеры сертификатов (ХАССП, ГОСТ, декларация);
  2. LLM-fallback (LLM_MODEL_SMART) для сложных страниц: структурирует
     описание, МОК и условия доставки, которых regex не видит.

Каждый извлечённый факт помечается источником (provenance) — в UI видно,
откуда что взято."""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx
from price_parser import Price
from trafilatura import extract as traf_extract

from .config import get_settings
from .llm import ask_json, LLMError
from .renderer import render_html, THIN_TEXT_THRESHOLD
from .sources.companyinfo import extract_legal_from_text

logger = logging.getLogger(__name__)

FETCH_TIMEOUT = httpx.Timeout(15, connect=8)
HTML_TRIM = 6000          # столько текста достаточно LLM для вердикта
PAGE_LIMIT = 1_500_000       # не тянем гигантские страницы (мегабайт — потолок)

RE_PHONE = re.compile(
    r"(?:\+7|8)[\s(-]{0,3}\d{3}[\s)-]{0,3}\d{3}[\s-]{0,2}\d{2}[\s-]{0,2}\d{2}\b")
RE_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[A-Za-z]{2,6}")
RE_PRICE = re.compile(
    r"(?:от\s*)?(\d[\d\s.,]{1,9})\s*(?:₽|руб\.?|RUB)\s*(?:[/за]\s*([а-яёa-z²³.]{1,10}))?",
    re.IGNORECASE)
RE_CERT = re.compile(
    r"(ХАССП|HACCP|ИСО\s?22000|ISO\s?22000|ГОСТ\s?\d+|декларац\w+ соответстви\w+|"
    r"сертификат\w* соответстви\w+|Меркурий|Ветеринарн\w+ свидетельств\w+)", re.IGNORECASE)
RE_MOK = re.compile(
    r"(?:мин\w*\s+заказ\w*|заказ\s+от|минимальн\w+ партия\w*)\s*[—:от]*\s*(\d[\d\s.,]{0,7}\s*(?:кг|т|шт|л|уп|руб\w*|₽)?)",
    re.IGNORECASE)
RE_DELIVERY = re.compile(
    r"(доставк\w+[^.]{5,120})", re.IGNORECASE)

BAD_EMAIL_DOMAINS = {"sentry.io", "example.com", "wixpress.com", "yandex.ru"}


def _clean_phone(raw: str) -> str | None:
    """Форматирует телефон, отбраковывая мусор (кейс: «8 2017 2016 2015»).

    Валидность: ровно 11 цифр, начиная с 7/8 (федеральный формат РФ),
    либо 10 цифр. Год-последовательности и обрывки отбрасываются."""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits[0] in "78":
        return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:]}"
    if len(digits) == 10:
        return f"+7 ({digits[:3]}) {digits[3:6]}-{digits[6:8]}-{digits[8:]}"
    return None


def _text_of(html: str) -> str:
    """Полный видимый текст (включая футер с контактами) — для regex-слоя."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    import html as html_mod
    return html_mod.unescape(re.sub(r"\s+", " ", text)).strip()


def _main_text(html: str) -> str:
    """Основной контент страницы без навигации/рекламы — для LLM-слоя.

    trafilatura — индустриальный стандарт main-content extraction, топ
    бенчмарков: убирает boilerplate, оставляет сигнальный текст о компании/ценах.
    Плюс: в разы меньше токенов на LLM-вызов. Фолбэк на полный текст."""
    clean = traf_extract(html, include_tables=False, include_comments=False)
    return clean.strip() if clean else _text_of(html)


@dataclass(slots=True)
class SiteFacts:
    url: str
    ok: bool = False
    phones: list[str] = field(default_factory=list)  # валидированные: 11 цифр от 7/8
    emails: list[str] = field(default_factory=list)
    prices: list[str] = field(default_factory=list)      # «чипсы 43 руб/шт» — по предмету запроса
    min_order: str | None = None
    certificates: list[str] = field(default_factory=list)
    delivery: str | None = None
    description: str | None = None
    match: str = ""                                     # LLM: profile|adjacent|offtopic
    irrelevant: bool = False                            # вычисляется: match == offtopic
    method: str = "regex"                               # regex | llm | failed
    kind: str | None = None                              # производитель/дистрибьютор/магазин
    wholesale: bool | None = None                        # LLM: работает ли с оптом
    rendered: bool = False                               # потребовался ли JS-рендер
    inn: str | None = None                                # юрлицо с сайта (футер/реквизиты)
    ogrn: str | None = None
    legal_name: str | None = None

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def normalize_price(raw: str) -> tuple[float, str] | None:
    """'от 850 руб./кг' -> (850.0, 'руб./кг'). price-parser (Zyte) + юнит."""
    try:
        p = Price.fromstring(raw)
        if p.amount is None:
            return None
        unit = ""
        m = re.search(r"(?:/\s*|за\s+)([а-яёa-z²³.]{1,10})", raw, re.IGNORECASE)
        if m:
            unit = m.group(1)
        if unit.lower() == "за":
            unit = ""
        return (float(p.amount), f"{p.currency or 'руб'}" + (f"/{unit}" if unit else ""))
    except (ValueError, TypeError):
        return None


async def fetch_html(url: str) -> str:
    """GET страницы с таймаутом; ошибки и редиректы-помойки -> ''."""
    try:
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0 Safari/537.36"}) as c:
            resp = await c.get(url)
            if resp.status_code != 200 or len(resp.content) > PAGE_LIMIT:
                return ""
            ctype = resp.headers.get("content-type", "")
            if "html" not in ctype and "text" not in ctype:
                return ""
            return resp.text
    except httpx.HTTPError:
        return ""


def regex_facts(url: str, text: str) -> SiteFacts:
    """Детерминированный слой: бесплатно, покрывает ~70% случаев."""
    f = SiteFacts(url=url, ok=bool(text))
    f.phones = [p for m in RE_PHONE.findall(text)[:5] if (p := _clean_phone(m))][:3]
    emails = []
    for e in RE_EMAIL.findall(text):
        if e.split("@")[-1].lower() not in BAD_EMAIL_DOMAINS and e.lower() not in emails:
            emails.append(e.lower())
    f.emails = emails[:3]
    for m in RE_PRICE.finditer(text):
        val = re.sub(r"[.,]\s", " ", m.group(0)).strip()
        if any(c.isdigit() for c in val) and val not in f.prices:
            f.prices.append(re.sub(r"\s+", " ", val))
        if len(f.prices) >= 4:
            break
    if not f.prices:
        f.prices = []
    m = RE_MOK.search(text)
    if m:
        f.min_order = re.sub(r"\s+", " ", m.group(0))[:80]
    f.certificates = sorted(set(x.strip().upper() if len(x) < 12 else x.strip()
                                for x in RE_CERT.findall(text)))[:4]
    m = RE_DELIVERY.search(text)
    if m:
        candidate = re.sub(r"\s+", " ", m.group(1))[:140]
        nav_keywords = ("прайс-лист", "акции", "корзина", "личный кабинет", "каталог", "контакты", "главная")
        if not any(kw in candidate.lower() for kw in nav_keywords):
            f.delivery = candidate
    return f



RE_CONTACT_KW = re.compile(
    r"(контакт|телефон|тел\.|почт|email|e-mail|связать|позвон|адрес\b|офис|"
    r"whatsapp|telegram|вайбер|мессендж|график работы)", re.IGNORECASE)


def contact_text(full_text: str, limit: int = 8000) -> str:
    """Текст, пригодный для извлечения контактов LLM.

    trafilatura (main content) срезает футер — а у российских B2B-сайтов
    контакты живут именно там. Берём окна вокруг контакт-ключевых слов
    плюс хвост страницы (футер) — то, что regex-ям мешает, а гемме нет."""
    if len(full_text) <= limit:
        return full_text
    windows: list[str] = []
    for m in RE_CONTACT_KW.finditer(full_text):
        lo = max(0, m.start() - 500)
        hi = min(len(full_text), m.end() + 700)
        if windows and lo <= windows[-1][1]:
            windows[-1] = (windows[-1][0], hi)  # слить пересекающиеся окна
        else:
            windows.append((lo, hi))
    slices = [full_text[lo:hi] for lo, hi in windows]
    tail = full_text[-2500:]  # футер
    out = "\n...\n".join(slices) + "\n...\n" + tail
    return out[:limit]



async def llm_contacts(url: str, text: str) -> SiteFacts:
    """Контакты через дешёвую модель (gemma): regex не тянет по вариативности
    футеров, а gemini получает только очищенный main content без футера."""
    f = SiteFacts(url=url, method="llm-contacts")
    if len(text) < 60:
        return f
    prompt = f"""Из текста страницы сайта компании извлеки контактные данные.

Ответь СТРОГО JSON:
{{"phones": ["телефоны в формате как на сайте"], "emails": ["email"],
  "messengers": ["whatsapp/telegram если есть"]}}

Если чего-то нет — пустой массив. Не выдумывай.

Текст:
{text[:6000]}"""
    try:
        data = await ask_json(prompt, smart=False, temperature=0.1, max_tokens=400)
        f.phones = [str(x)[:25] for x in (data.get("phones") or [])][:3]
        f.emails = [str(x).lower()[:60] for x in (data.get("emails") or [])][:2]
        if f.phones or f.emails:
            f.ok = True
    except LLMError as exc:
        logger.debug("llm_contacts failed for %s: %s", url, exc)
    return f


async def llm_facts(url: str, text: str, query: str) -> SiteFacts:
    """LLM-слой: понимает контекст, отсекает нерелевантные и розничные страницы.

    Ключевое отличие от regex: цены извлекаются ТОЛЬКО по предмету запроса
    (запрос «чипсы» -> «чипсы картофельные 43 ₽/шт»), а не любые ₽ на странице.
    Плюс вердикт «опт/розница» — фильтр мусора типа интернет-магазинов."""
    f = SiteFacts(url=url, method="llm")
    prompt = f"""Товар, который ищет пользователь: "{query}".
Ниже — текст с сайта потенциального поставщика. Определи, торгует ли сайт этим
или близким товаром, и извлеки данные.

Ответь JSON:
{{"match": "profile|adjacent|offtopic",
  "kind": "производитель|дистрибьютор|магазин|неясно",
  "wholesale": true/false,
  "wholesale_evidence": "признак оптовой торговли: 'опт', 'мелкий опт', прайс для юрлиц... иначе null",
  "description": "1-2 предложения, чем занимается компания",
  "price_examples": ["цены ТОЛЬКО на искомый товар, в формате как на сайте: '43 руб/шт', 'от 850 руб за кг'"],
  "min_order": "мин. заказ/партия если указан, иначе null",
  "delivery": "условия доставки если указаны, иначе null",
  "certificates": ["найденные сертификаты/ГОСТы"],
  "phones": ["телефоны"], "emails": ["email"]}}

Класс match:
- "profile" — сайт ПРОДАЁТ или ПРОИЗВОДИТ именно "{query}" (или прямые варианты);
- "adjacent" — сайт не про "{query}", но это пищевой оптовик/производsteller/дистрибьютор
  (смежная еда, ингредиенты, упаковка) — полезный контакт для закупщика;
- "offtopic" — не поставщик вообще (блог, услуга, частный сервис, чужая тематика).

ВАЖНО:
1. В price_examples включай цены только на "{query}" или его прямые
   варианты. Цены на другие товары, услуги, доставку — НЕ включай.
2. wholesale=true если сайт работает с оптом: признаки — «опт», «мелкий
   опт», прайс для юрлиц, минимальная партия, а ТАКЖЕ оптовая упаковка:
   «Коробка: 8,00 кг», «ящик 20 шт», «упаковка 10 кг» и т.п. — фасовка
   ящиками/коробками от 3 кг это опт или мелкий опт.
3. wholesale=false только если явная розница частным лицам без оптовых признаков.

Текст сайта (обрезан):
{text[:HTML_TRIM]}"""
    try:
        data = await ask_json(prompt, smart=True, temperature=0.1)
        match = str(data.get("match") or "").strip().lower()
        f.match = match if match in ("profile", "adjacent", "offtopic") else ""
        f.irrelevant = (f.match == "offtopic")
        if not f.match and "relevant" in data:  # обратная совместимость формата
            f.irrelevant = not bool(data.get("relevant", True))
            f.match = "offtopic" if f.irrelevant else "profile"
        f.kind = (data.get("kind") or data.get("supplier_kind") or "неясно")[:20]
        f.wholesale = bool(data.get("wholesale")) if "wholesale" in data else None
        f.description = (data.get("description") or "")[:300] or None
        f.prices = [str(x)[:60] for x in (data.get("price_examples") or [])[:4]]
        f.min_order = (data.get("min_order") or None)
        f.delivery = (data.get("delivery") or None)
        f.certificates = [str(x)[:40] for x in (data.get("certificates") or [])[:4]]
        f.phones = [str(x) for x in (data.get("phones") or [])[:4]]
        f.emails = [str(x) for x in (data.get("emails") or [])[:3]]
        f.ok = True
    except LLMError as exc:
        logger.warning("llm_facts failed for %s: %s", url, exc)
        f.ok = False
    return f


def merge_facts(a: SiteFacts, b: SiteFacts) -> SiteFacts:
    """regex + LLM: пересечение/дополнение. regex — приоритет для контактов."""
    out = a
    out.method = "regex+llm" if a.ok and b.ok else (b.method if b.ok and not a.ok else a.method)
    out.irrelevant = b.irrelevant
    out.kind = b.kind or a.kind
    out.wholesale = b.wholesale if b.wholesale is not None else a.wholesale
    out.rendered = a.rendered or b.rendered
    if b.description and not out.description:
        out.description = b.description
    for p in b.phones:
        if p not in out.phones:
            out.phones.append(p)
    for e in b.emails:
        if e not in out.emails:
            out.emails.append(e)
    for p in b.prices:
        if p not in out.prices:
            out.prices.append(p)
    out.min_order = out.min_order or b.min_order
    out.delivery = out.delivery or b.delivery
    for c in b.certificates:
        if c not in out.certificates:
            out.certificates.append(c)
    return out


async def enrich_site(url: str, query: str, *, use_llm: bool = True) -> SiteFacts:
    """Полный enrichment: httpx -> (Playwright-рендер если httpx заблокирован или SPA)
    -> страницы контактов -> LLM (цены по предмету + опт/розница)."""
    html = await fetch_html(url)
    if not html:
        # httpx заблокирован анти-ботом (кейс auchan.ru: 0 байт) — рендер браузером
        rendered = await render_html(url)
        if not rendered:
            return SiteFacts(url=url, method="failed")
        html = rendered
    full_text = _text_of(html)
    facts = regex_facts(url, full_text)
    facts.rendered = True

    # --- JS-оболочка? видимого текста критически мало -> рендер браузером.
    # (regex-«цены» из навигации не считаем данными: после рендера переизвлекаем)
    if len(full_text) < THIN_TEXT_THRESHOLD:
        rendered = await render_html(url)
        if rendered:
            html = rendered
            full_text = _text_of(html)
            facts = regex_facts(url, full_text)
            facts.rendered = True

    # --- юрлицо с сайта: футер/«Реквизиты» (после рендера — на итоговом тексте) ---
    legal = extract_legal_from_text(full_text)
    facts.inn = legal.get("inn")
    facts.ogrn = legal.get("ogrn")
    facts.legal_name = legal.get("legal_name")

    # --- нет email -> типовые страницы контактов ---
    if not facts.emails:
        for suffix in ("kontakty", "contacts", "kontakty.html"):
            contact_html = await fetch_html(url.rstrip("/") + "/" + suffix)
            if not contact_html:
                continue
            contact_text = _text_of(contact_html)
            if len(contact_text) < 200:  # и там SPA — рендерим
                contact_html = await render_html(url.rstrip("/") + "/" + suffix) or contact_html
                contact_text = _text_of(contact_html)
            contact_facts = regex_facts(url + "/" + suffix, contact_text)
            for e in contact_facts.emails:
                if e not in facts.emails:
                    facts.emails.append(e)
            for p in contact_facts.phones:
                if p not in facts.phones:
                    facts.phones.append(p)
            if facts.emails:
                facts.ok = True
                break

    # --- контакты всё ещё нет: гемма по контакт-окнам (включая футер,
    #     который trafilatura срезает) — regex не тянет вариативность ---
    if use_llm and not (facts.emails and facts.phones):
        ct = contact_text(full_text)
        contact_facts = await llm_contacts(url, ct)
        if contact_facts.ok:
            for ph in contact_facts.phones:
                cleaned = _clean_phone(ph)
                if cleaned and cleaned not in facts.phones:
                    facts.phones.append(cleaned)
            for e in contact_facts.emails:
                if e not in facts.emails:
                    facts.emails.append(e)
            facts.ok = True

    # --- LLM: цены строго по предмету, опт/розница, описание ---
    main_text = _main_text(html)
    if use_llm and len(main_text) > 200:
        llm = await llm_facts(url, main_text, query)
        if llm.ok:
            facts = merge_facts(facts, llm)
            # цены LLM релевантнее regex-мусора из навигации: приоритет LLM
            if llm.prices:
                facts.prices = llm.prices
    return facts


async def enrich_sites(urls: list[str], query: str, *, concurrency: int | None = None,
                       progress=None) -> list[SiteFacts]:
    """Параллельный обход с семафором. progress: async-callback(url, facts)."""
    s = get_settings()
    sem = asyncio.Semaphore(concurrency or s.enrich_concurrency)

    async def _task(u: str) -> SiteFacts:
        async with sem:
            facts = await enrich_site(u, query)
            if progress:
                await progress(u, facts)
            return facts

    return await asyncio.gather(*[_task(u) for u in urls])
