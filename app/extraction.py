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

logger = logging.getLogger(__name__)

FETCH_TIMEOUT = httpx.Timeout(15, connect=8)
HTML_TRIM = 6000          # столько текста достаточно LLM для вердикта
PAGE_LIMIT = 1_500_000       # не тянем гигантские страницы (мегабайт — потолок)

RE_PHONE = re.compile(r"(?:\+7|8)[\s(-]?\d{3}[\s)-]?[\d\s(-]{7,12}\d")
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


def _clean_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits[0] in "78":
        return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:]}"
    return raw.strip()


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
    phones: list[str] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)
    prices: list[str] = field(default_factory=list)      # «42 руб/кг»
    min_order: str | None = None
    certificates: list[str] = field(default_factory=list)
    delivery: str | None = None
    description: str | None = None
    irrelevant: bool = False                            # LLM: сайт не про наш товар
    method: str = "regex"                               # regex | llm | failed

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
    f.phones = [_clean_phone(m) for m in RE_PHONE.findall(text)[:5]]
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


async def llm_facts(url: str, text: str, query: str) -> SiteFacts:
    """LLM-слой: понимает контекст, отсекает нерелевантные страницы."""
    f = SiteFacts(url=url, method="llm")
    prompt = f"""Товар, который ищет пользователь: "{query}".
Ниже — текст с сайта потенциального поставщика. Определи, торгует ли сайт этим
или близким товаром, и извлеки данные.

Ответь JSON:
{{"relevant": true/false,
  "supplier_kind": "производитель|дистрибьютор|магазин|неясно",
  "description": "1-2 предложения, чем занимается компания",
  "price_examples": ["строки с ценами как на сайте, если видны"],
  "min_order": "мин. заказ/партия если указан, иначе null",
  "delivery": "условия доставки если указаны, иначе null",
  "certificates": ["найденные сертификаты/ГОСТы"],
  "phones": ["телефоны"], "emails": ["email"]}}

Текст сайта (обрезан):
{text[:HTML_TRIM]}"""
    try:
        data = await ask_json(prompt, smart=True, temperature=0.1)
        f.irrelevant = not bool(data.get("relevant", True))
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
    """Полный enrichment одного сайта: fetch -> regex -> (LLM если надо)."""
    html = await fetch_html(url)
    if not html:
        return SiteFacts(url=url, method="failed")
    full_text = _text_of(html)
    facts = regex_facts(url, full_text)
    # LLM нужен если: ничего не извлекли regex-ом ИЛИ страница мелкая/сложная,
    # или нет описания. Иначе не тратим дорогие вызовы.
    needs_llm = use_llm and (not facts.prices and not facts.min_order or not facts.emails)
    if needs_llm and len(full_text) > 200:
        llm = await llm_facts(url, _main_text(html), query)
        if llm.ok:
            facts = merge_facts(facts, llm)
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
