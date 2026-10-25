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
from .llm import ask_json

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
# маркеры условий заказа: оплата и логистика
RE_ORDER_TERMS = re.compile(
    r"(безналичн\w+ расч\w*|нал\w* при получении|отсрочк\w+ платеж\w*|предоплат\w*|"
    r"самовывоз\w*|транспортной компанией|ТК\b|собственн\w+ транспорт\w*|"
    r"отгрузк\w+ со склад\w+|со склада в \w+|минимальн\w+ сумм\w+ заказ\w*)", re.IGNORECASE)

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
    city: str | None = None                              # город компании с сайта
    product_links: list[str] = field(default_factory=list)  # разделы/прайсы по запросу
    order_terms: str | None = None                        # условия заказа и доставки
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
    markers = []
    for m in RE_ORDER_TERMS.finditer(text):
        val = m.group(1).strip().lower()
        if val not in markers:
            markers.append(val)
        if len(markers) >= 5:
            break
    if markers:
        f.order_terms = " · ".join(markers[:5])
    return f



RE_CONTACT_KW = re.compile(
    r"(контакт|телефон|тел\.|почт|email|e-mail|связать|позвон|адрес\b|офис|"
    r"whatsapp|telegram|вайбер|мессендж|график работы)", re.IGNORECASE)





# ---------------------- классификация ссылок главной страницы ----------------------
# Все внутренние ссылки с якорями -> словарь по категориям (цены/контакты/
# реквизиты/доставка); недостающие категории добирает LLM (дешёвый проход).
from urllib.parse import urljoin as _uj, urlparse as _up

_LINK_RULES: list[tuple[str, re.Pattern]] = [
    ("prices", re.compile(r"прайс|цен[аык]|price|katalog|каталог|продук|товар|опт", re.I)),
    ("contacts", re.compile(r"контакт|связь|телефон|почт|email|адрес|офис|написать", re.I)),
    ("legal", re.compile(r"реквизит|о[- ]компани|about|компани|documents|документ", re.I)),
    ("delivery", re.compile(r"достав|оплата|условия|dostavka|shipping|самовывоз", re.I)),
]
_NAV_NOISE = re.compile(r"войти|регистр|корзин|личн|политик|cookie|ваканс", re.I)


def extract_site_links(html: str, base_url: str, limit: int = 40) -> list[tuple[str, str]]:
    """Все внутренние ссылки с текстами якорей: [(abs_url, anchor), ...]."""
    base_host = _up(base_url).netloc.removeprefix("www.")
    out, seen = [], set()
    for m in re.finditer(r'<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>',
                         html, re.I | re.S):
        href, anchor_html = m.group(1), m.group(2)
        if href.startswith(("mailto:", "tel:", "javascript:", "data:")):
            continue
        absu = _uj(base_url, href)
        host = _up(absu).netloc.removeprefix("www.")
        if host != base_host:
            continue
        if absu.rstrip("/") == base_url.rstrip("/") or absu in seen:
            continue
        anchor = re.sub(r"<[^>]+>", " ", anchor_html)
        anchor = re.sub(r"\s+", " ", anchor).strip()
        if not anchor or len(anchor) > 120 or _NAV_NOISE.search(anchor):
            continue
        seen.add(absu)
        out.append((absu, anchor))
        if len(out) >= limit:
            break
    return out


def classify_links_regex(links: list[tuple[str, str]]) -> dict[str, list[str]]:
    """Дешёвый слой: категории по словам якоря/URL. Ничего не стоит."""
    out: dict[str, list[str]] = {k: [] for k, _ in _LINK_RULES}
    for absu, anchor in links:
        blob = f"{anchor} {absu}"
        for cat, rx in _LINK_RULES:
            if rx.search(blob) and absu not in out[cat]:
                out[cat].append(absu)
    for cat in out:
        out[cat] = out[cat][:3]
    return out


async def llm_classify_links(base_url: str,
                             links: list[tuple[str, str]]) -> dict[str, list[str]]:
    """LLM-слой для ссылок, которые словарь не понял: «Продукция_1» тоже
    прайс-страница, «Как нас найти» — контакты, «Юр. информация» — реквизиты."""
    out: dict[str, list[str]] = {k: [] for k, _ in _LINK_RULES}
    if not links:
        return out
    lines = "\n".join(f"{i+1}. \"{anchor}\" — {absu}"
                      for i, (absu, anchor) in enumerate(links[:40]))
    prompt = f"""Ниже список ссылок с главной страницы сайта поставщика "{base_url}".
Определи, какие ссылки ведут на страницы:
- prices: прайсы, каталоги, витрины товаров с ценами (ИМЕННО страницы с перечнем товаров/цен)
- contacts: контакты, телефоны, адрес офиса
- legal: реквизиты компании, «О компании», юридическая информация
- delivery: условия доставки и оплаты

{lines}

Ответ — только JSON: {{"prices": [номера], "contacts": [номера], "legal": [номера], "delivery": [номера]}}.
Не придумывай номера: пустой список, если ничего подходящего."""
    data = await ask_json(prompt, system="Ты аккуратный классификатор ссылок B2B-сайтов.")
    if not isinstance(data, dict):
        return out
    for cat in out:
        for n in data.get(cat) or []:
            try:
                idx = int(n) - 1
                if 0 <= idx < len(links) and links[idx][0] not in out[cat]:
                    out[cat].append(links[idx][0])
            except (ValueError, TypeError):
                continue
        out[cat] = out[cat][:3]
    return out


async def classify_site_links(html: str, base_url: str) -> dict[str, list[str]]:
    """Словарь -> LLM на дефицит: сначала бесплатно, недостающие категории
    добираем вычиткой якорей (цены у оптовика бывают и «Витрина», и «Продукция»)."""
    links = extract_site_links(html, base_url)
    cats = classify_links_regex(links)
    deficit = [c for c in ("prices", "contacts", "legal", "delivery") if not cats[c]]
    if deficit and links:
        llm_cats = await llm_classify_links(base_url, links)
        for c in deficit:
            cats[c] = llm_cats.get(c, [])[:3]
    return cats


def extract_product_links(html: str, query: str, base_url: str, limit: int = 2) -> list[str]:
    """Ссылки на разделы/каталоги/прайсы, релевантные запросу.

    Скоры: совпадение слов запроса в тексте ссылки + bonus за
    «прайс|каталог|цена|katalog|price» в href/анкоре. Абсолютные URL,
    только внутренние (тот же домен)."""
    from urllib.parse import urljoin, urlparse
    base_host = urlparse(base_url).netloc.removeprefix("www.")
    tokens = [t for t in re.split(r"\W+", query.lower()) if len(t) > 2]
    scored: list[tuple[float, str]] = []
    for m in re.finditer(r'<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL):
        href, anchor_html = m.group(1), m.group(2)
        if href.startswith(("mailto:", "tel:", "javascript:", "http")):
            continue
        absu = urljoin(base_url, href)
        host = urlparse(absu).netloc.removeprefix("www.")
        if host != base_host or absu.rstrip("/") == base_url.rstrip("/"):
            continue
        anchor = re.sub(r"<[^>]+>", " ", anchor_html)
        anchor = re.sub(r"\s+", " ", anchor).strip()
        if not anchor or len(anchor) > 120:
            continue
        blob = (anchor + " " + href).lower()
        score = sum(1 for t in tokens if t in blob)
        if re.search(r"прайс|katalog|каталог|цен|price|optov|оптов", blob):
            score += 2
        if score >= 2:
            scored.append((score, absu))
    scored.sort(key=lambda x: -x[0])
    out: list[str] = []
    for _s, u in scored:
        if u not in out:
            out.append(u)
        if len(out) >= limit:
            break
    return out



# коды крупнейших городов (мобильные 9xx не матчатся — только стационарные)
PHONE_CODE_CITIES: dict[str, str] = {
    "495": "Москва", "499": "Москва", "498": "Москва",
    "812": "Санкт-Петербург",
    "343": "Екатеринбург", "342": "Пермь", "383": "Новосибирск",
    "861": "Краснодар", "863": "Ростов-на-Дону",
    "843": "Казань", "846": "Самара", "831": "Нижний Новгород",
    "351": "Челябинск", "381": "Омск", "347": "Уфа",
    "391": "Красноярск", "423": "Владивосток", "473": "Воронеж",
    "421": "Хабаровск", "395": "Иркутск", "341": "Ижевск",
    "836": "Йошкар-Ола", "867": "Владикавказ",
}


def _city_from_phone(phone: str | None) -> str | None:
    """Город по стационарному коду: +7 (342)... -> Пермь."""
    if not phone:
        return None
    m = re.search(r"\+7\s*\((\d{3})\)", phone)
    return PHONE_CODE_CITIES.get(m.group(1)) if m else None


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
  "messengers": ["whatsapp/telegram если есть"],
  "city": "ГОРОД компании по адресу склада/офиса/реквизитам на странице (НЕ город покупателя!); null если не указан"}}

Если чего-то нет — пустой массив/null. Не выдумывай.

Текст:
{text[:6000]}"""
    try:
        data = await ask_json(prompt, smart=False, temperature=0.1, max_tokens=400)
        f.phones = [str(x)[:25] for x in (data.get("phones") or [])][:3]
        f.emails = [str(x).lower()[:60] for x in (data.get("emails") or [])][:2]
        f.city = (str(data.get("city")).strip()[:40] or None) if data.get("city") else None
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
  "city": "ГОРОД компании по контактным данным/реквизитам/адресу склада на странице (не город покупателя!); если не указан — null",
  "delivery": "условия доставки если указаны, иначе null",
  "order_terms": "условия заказа: оплата (безнал/нал/отсрочка), самовывоз/ТК/свой транспорт, сроки поставки — если указаны; иначе null",
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
        f.city = (str(data.get("city")).strip()[:40] or None) if data.get("city") else None
        f.delivery = (data.get("delivery") or None)
        f.order_terms = (str(data.get("order_terms"))[:250] or None) if data.get("order_terms") else None
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
    out.match = b.match or a.match
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

    # --- классификация ссылок: словарь -> LLM на дефицит ---
    #     заменяет хардкод-пробы «/kontakty» и догадки о прайсах:
    #     «Как нас найти» — контакты, «Витрина» — цены, «Юр. информация» — реквизиты
    import asyncio as _aio
    try:
        link_cats = await classify_site_links(html, url)
    except Exception:
        link_cats = {k: [] for k in ("prices", "contacts", "legal", "delivery")}

    # --- дефицит email -> адресные контакт-страницы (по классификации; ---
    #     фолбэк — старые типовые суффиксы, если сайт молчит про контакты)
    if not facts.emails:
        contact_pages = link_cats["contacts"][:2]
        probe_urls = contact_pages or [url.rstrip("/") + "/" + sfx for sfx in
                                        ("kontakty", "contacts", "dostavka")]

        async def _cpage(c_url: str, is_probe: bool) -> SiteFacts:
            contact_html = await fetch_html(c_url)
            cpage_text = _text_of(contact_html) if contact_html else ""
            # рендер — только для первичной пробы /kontakty: у антиспам-сайтов
            # каждая страница — 5-10с рендера, шесть проб = минута на сайт
            if len(cpage_text) < 200 and is_probe and "kontakty" in c_url:
                contact_html = await render_html(c_url) or contact_html
                cpage_text = _text_of(contact_html)
            cf = regex_facts(c_url, cpage_text)
            # контакт-страница идёт и гемме — regex не вытаскивает из
            # контакт-окон/скрытых блоков (кейс АВИСТРОН)
            if use_llm and (cf.emails or cf.phones or len(cpage_text) > 400):
                lcf = await llm_contacts(c_url, contact_text(cpage_text))
                if lcf and lcf.ok:
                    for ph in lcf.phones:
                        cleaned = _clean_phone(ph)
                        if cleaned and cleaned not in cf.phones:
                            cf.phones.append(cleaned)
                    for e in lcf.emails:
                        if e not in cf.emails:
                            cf.emails.append(e)
                    if lcf.city and not cf.city:
                        cf.city = lcf.city
                    cf.ok = True
            return cf

        cf_list = await _aio.gather(*(_cpage(u, u not in contact_pages)
                                       for u in probe_urls))
        for cf in cf_list:
            for e in cf.emails:
                if e not in facts.emails:
                    facts.emails.append(e)
            for ph in cf.phones:
                if ph not in facts.phones:
                    facts.phones.append(ph)
            if cf.order_terms and not facts.order_terms:
                facts.order_terms = cf.order_terms
            if cf.delivery and not facts.delivery:
                facts.delivery = cf.delivery
            if cf.city and not facts.city:
                facts.city = cf.city
        if facts.emails or facts.phones:
            facts.ok = True

    # --- дефицит ИНН -> страница реквизитов («О компании») ---
    if not facts.inn and link_cats["legal"]:
        try:
            ltext = _text_of(await fetch_html(link_cats["legal"][0]) or "")
            legal2 = extract_legal_from_text(ltext)
            if legal2.get("inn"):
                facts.inn = legal2["inn"]
                if not facts.ogrn and legal2.get("ogrn"):
                    facts.ogrn = legal2["ogrn"]
                if not facts.legal_name and legal2.get("legal_name"):
                    facts.legal_name = legal2["legal_name"]
        except Exception:
            pass

    # --- дефицит доставки -> адресная страница доставки/оплаты ---
    if (not facts.delivery or not facts.order_terms) and link_cats["delivery"]:
        try:
            dtext = _text_of(await fetch_html(link_cats["delivery"][0]) or "")
            df = regex_facts(url, dtext)
            if df.delivery and not facts.delivery:
                facts.delivery = df.delivery
            if df.order_terms and not facts.order_terms:
                facts.order_terms = df.order_terms
            if df.min_order and not facts.min_order:
                facts.min_order = df.min_order
        except Exception:
            pass

    # --- два LLM-вызова ПАРАЛЛЕЛЬНО (это половина времени обхода):
    #     1) контакты: гемма по контакт-окнам (футер включён, regex не тянет);
    #     2) цены/классификация: gemini по очищенному main content.
    import asyncio as _aio
    main_text = _main_text(html)
    need_contacts_llm = use_llm and not (facts.emails and facts.phones)
    need_facts_llm = use_llm and len(main_text) > 200

    async def _contacts_call():
        if not need_contacts_llm:
            return None
        return await llm_contacts(url, contact_text(full_text))

    async def _facts_call():
        if not need_facts_llm:
            return None
        return await llm_facts(url, main_text, query)

    contact_facts, llm = await _aio.gather(_contacts_call(), _facts_call())

    if contact_facts and contact_facts.ok:
        for ph in contact_facts.phones:
            cleaned = _clean_phone(ph)
            if cleaned and cleaned not in facts.phones:
                facts.phones.append(cleaned)
        for e in contact_facts.emails:
            if e not in facts.emails:
                facts.emails.append(e)
        if contact_facts.city:
            facts.city = contact_facts.city
        facts.ok = True
    # город по коду стационарного телефона — надёжнее каталога (кейс
    # «Русская Рыба»: каталог дал Москву, телефоны +7 (342) Пермь)
    if not facts.city and facts.phones:
        facts.city = _city_from_phone(facts.phones[0]) or facts.city

    if llm and llm.ok:
        facts = merge_facts(facts, llm)
        # цены LLM релевантнее regex-мусора из навигации: приоритет LLM
        if llm.prices:
            facts.prices = _polish_prices(llm.prices)
    # ссылки на разделы/прайсы по запросу
    facts.product_links = extract_product_links(html, query, url)

    # --- дефицит цен на главной -> прайс-страницы: классифицированные ссылки
    #     («Витрина», «Продукция») + релевантные запросу разделы product_links
    price_targets = list(dict.fromkeys((link_cats["prices"] or [])[:1]
                                       + facts.product_links[:2]))
    if use_llm and not facts.prices and price_targets:
        fetched = []
        for pl in price_targets[:2]:
            try:
                sub_html = await fetch_html(pl)
                if not sub_html:
                    continue
                sub_text = _main_text(sub_html)
                if len(sub_text) < 200:
                    continue
                sub = await llm_facts(pl, sub_text, query)
                fetched.append(sub)
                if sub.prices:
                    facts.prices = sub.prices
                    facts.method = "llm+price-page"
                    break
                for ph in sub.phones:
                    if _clean_phone(ph) and _clean_phone(ph) not in facts.phones:
                        facts.phones.append(_clean_phone(ph))
                for e in sub.emails:
                    if e not in facts.emails:
                        facts.emails.append(e)
                if sub.city and not facts.city:
                    facts.city = sub.city
            except Exception:
                continue
    return facts




def _polish_prices(raw: list[str]) -> list[str]:
    """Косметика LLM-цен: «140 0 ₽ За» -> «140,00 ₽», пустые «0 руб» — вон."""
    out = []
    for s in raw or []:
        s = re.sub(r"\s+(за|шт|кг|упак)\b.*$", "", (s or "").strip(), flags=re.I)
        m = re.match(r"^(\d[\d\s]*)\s(\d)\s*₽$", s)
        if m:
            s = f"{m.group(1).strip()},{m.group(2)}0 ₽"
        if s and not re.match(r"^0\s*(руб|₽)", s) and s not in out:
            out.append(s)
        if len(out) >= 4:
            break
    return out


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
