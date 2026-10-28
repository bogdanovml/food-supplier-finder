"""Извлечение данных с сайтов-кандидатов: контакты, цены, сертификаты.

Стратегия — дешёвое → дорогое (переиспользуем подход price_extractor
из раннего прототипа поиска, адаптированный под food-прототип):
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
from html.parser import HTMLParser
from urllib.parse import urlparse, urljoin

import httpx
from price_parser import Price
from trafilatura import extract as traf_extract

from .config import get_settings
from .llm import ask_json, LLMError
from .renderer import render_html, THIN_TEXT_THRESHOLD
from .sources.companyinfo import extract_legal_from_text
from .llm import ask_json

logger = logging.getLogger(__name__)

FETCH_TIMEOUT = httpx.Timeout(connect=3.5, read=6.0, write=4.0, pool=8.0)
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
    delivery_geo: str | None = None  # география доставки: «Москва и МО», «вся РФ»
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
    """GET страницы с таймаутом; ошибки и редиректы-помойки -> ''.
    Поддерживает fallback https -> http, игнорирует невалидные SSL-сертификаты (verify=False),
    корректно декодирует windows-1251 / cp1251 (часто у старых оптовых баз)."""
    if not url:
        return ""
    urls_to_try = [url]
    if url.startswith("https://"):
        urls_to_try.append("http://" + url[8:])

    for u in urls_to_try:
        try:
            async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=True,
                                         verify=False,
                                         headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0 Safari/537.36"}) as c:
                resp = await c.get(u)
                if resp.status_code == 200 and len(resp.content) <= PAGE_LIMIT:
                    raw = resp.content
                    ctype = resp.headers.get("content-type", "").lower()
                    if "html" not in ctype and "text" not in ctype:
                        continue
                    if "windows-1251" in ctype or "cp1251" in ctype or b"charset=windows-1251" in raw[:1000].lower() or b"charset=\"windows-1251\"" in raw[:1000].lower():
                        try:
                            return raw.decode("cp1251", errors="replace")
                        except Exception:
                            pass
                    return resp.text
        except httpx.TimeoutException:
            # Хост не отвечает по таймауту — не тратим время на второй круг по http
            break
        except Exception:
            continue
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
    ("prices", re.compile(r"прайс|цен[аык]|price|prais|katalog|каталог|продук|товар|опт|products", re.I)),
    ("contacts", re.compile(r"контакт|связь|телефон|почт|email|адрес|офис|написать|kontakty|contacts", re.I)),
    # строго: «Компания» без «О» — слишком общий якорь, маскирует дефицит
    # и не пускает карту сайта со страницей /requisites/ (кейс abri-kos.ru)
    ("legal", re.compile(r"реквизит|о[- ]компани|о[- _]компан|about|rekv|requisites|документы|documents", re.I)),
    ("delivery", re.compile(r"достав|оплата|условия|dostavka|delivery|oplata|shipping|самовывоз", re.I)),
]
_NAV_NOISE = re.compile(r"войти|регистр|корзин|личн|политик|cookie|ваканс", re.I)


def _normalize_host(h: str) -> str:
    """Нормализует хост с учетом Punycode/IDN для кириллических .рф доменов.
    Без этого ссылки на дарысибири.рф (xn--80achrba6cdf3h.xn--p1ai) отбрасываются!"""
    if not h:
        return ""
    h = h.strip().lower().removeprefix("www.")
    try:
        return h.encode("idna").decode("ascii")
    except Exception:
        return h


def extract_site_links(html: str, base_url: str, limit: int = 100) -> list[tuple[str, str]]:
    """Все внутренние ссылки с текстами якорей: [(abs_url, anchor), ...]."""
    base_host = _normalize_host(_up(base_url).netloc)
    out, seen = [], set()
    for m in re.finditer(r'<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>',
                         html, re.I | re.S):
        href, anchor_html = m.group(1), m.group(2)
        if href.startswith(("mailto:", "tel:", "javascript:", "data:")):
            continue
        absu = _uj(base_url, href)
        host = _normalize_host(_up(absu).netloc)
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
                      for i, (absu, anchor) in enumerate(links[:80]))
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


_SITEMAP_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
# якорь по сегментам пути: /company/requisites/ -> «rekv» и т.п.
_PATH_RULES: list[tuple[str, re.Pattern]] = [
    ("legal", re.compile(r"rekv|requisites|about|company/o[-_ ]?kompani|kompaniy", re.I)),
    ("contacts", re.compile(r"kontakty|contacts|svyaz", re.I)),
    ("delivery", re.compile(r"dostavka|oplata|delivery|usloviya", re.I)),
    ("prices", re.compile(r"price|prais|price-list|katalog|catalog|products|produkciya", re.I)),
]


async def sitemap_pages(base_url: str) -> dict[str, list[str]]:
    """Карта сайта как источник страниц: у жирных каталогов (Bitrix) главная
    на 500КБ и 120 якорей не хватает до «Реквизитов» в меню — а sitemap.xml
    честно перечисляет всё. Возвращает URL по категориям."""
    from urllib.parse import urlparse
    out: dict[str, list[str]] = {k: [] for k, _ in _PATH_RULES}
    try:
        root = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
        seen_maps: set[str] = set()

        def _sm_priority(u: str) -> int:
            # триаж: статические страницы (page/misc) содержат реквизиты,
            # доставку, контакты; товарные sitemap — тысячи ссылок без пользы
            low = u.lower()
            if "misc" in low:
                return 0
            if "page" in low:
                return 1
            if re.search(r"product|tax|pt-product|item", low):
                return 99  # пропускаем
            if "post" in low:
                return 3
            return 2

        queue = [root + "/sitemap.xml"]
        urls: list[str] = []
        while queue and len(seen_maps) < 7:
            sm_url = queue.pop(0)
            if sm_url in seen_maps:
                continue
            seen_maps.add(sm_url)
            raw = await fetch_html(sm_url)
            if not raw or "xml" not in (raw[:200] or ""):
                continue
            locs = _SITEMAP_RE.findall(raw)
            for loc in locs:
                if loc.endswith(".xml"):
                    if _sm_priority(loc) < 90:
                        queue.append(loc)
                else:
                    urls.append(loc)
            # страничные подсitemap — вперёд товарных
            queue.sort(key=_sm_priority)
            if len(urls) > 400:
                break
        if not urls:  # sitemap нет — попробуем robots.txt
            raw = await fetch_html(root + "/robots.txt") or ""
            for m in re.finditer(r"Sitemap:\s*(\S+)", raw, re.I):
                smr = await fetch_html(m.group(1)) or ""
                urls += _SITEMAP_RE.findall(smr)
        for u in urls[:400]:
            path = urlparse(u).path
            for cat, rx in _PATH_RULES:
                if rx.search(path) and u not in out[cat]:
                    out[cat].append(u)
        for cat in out:
            out[cat] = out[cat][:3]
        return out
    except Exception:
        return out


TARGET_PATTERNS: dict[str, re.Pattern] = {
    "delivery": re.compile(
        r'<a[^>]+href="([^"#]+)"[^>]*>(?:<[^>]+>)*\s*([^<]*(?:доставк|оплат|услови|delivery|shipping)[^<]*)\s*(?:</[^>]+>)*</a>',
        re.I,
    ),
    "contacts": re.compile(
        r'<a[^>]+href="([^"#]+)"[^>]*>(?:<[^>]+>)*\s*([^<]*(?:контакт|связ|адрес|contact)[^<]*)\s*(?:</[^>]+>)*</a>',
        re.I,
    ),
    "legal": re.compile(
        r'<a[^>]+href="([^"#]+)"[^>]*>(?:<[^>]+>)*\s*([^<]*(?:реквизит|о[- ]компани|о нас|документ|requisite|about)[^<]*)\s*(?:</[^>]+>)*</a>',
        re.I,
    ),
    "prices": re.compile(
        r'<a[^>]+href="([^"#]+)"[^>]*>(?:<[^>]+>)*\s*([^<]*(?:прайс|цен[ыа]|каталог|price|catalog)[^<]*)\s*(?:</[^>]+>)*</a>',
        re.I,
    ),
}


def scan_target_links(html: str, base_url: str) -> dict[str, list[str]]:
    """Прямой целевой поиск ключевых ссылок (меню, футер, навигация) по всему HTML.
    Не зависит от лимитов длины списков ссылок и находит /delivery/, /contacts/, /requisites/
    даже на страницах каталогов на 500 КБ."""
    from urllib.parse import urlparse, urljoin
    base_host = _normalize_host(urlparse(base_url).netloc)
    out: dict[str, list[str]] = {k: [] for k in TARGET_PATTERNS}
    for cat, rx in TARGET_PATTERNS.items():
        seen = set()
        for m in rx.finditer(html):
            href = m.group(1).strip()
            if href.startswith(("mailto:", "tel:", "javascript:", "data:")):
                continue
            absu = urljoin(base_url, href)
            host = _normalize_host(urlparse(absu).netloc)
            if host != base_host or absu.rstrip("/") == base_url.rstrip("/"):
                continue
            if absu not in seen:
                seen.add(absu)
                out[cat].append(absu)
            if len(out[cat]) >= 3:
                break
    return out


async def classify_site_links(html: str, base_url: str) -> dict[str, list[str]]:
    """Каскад поиска целевых страниц компании:
    1. Прямой целевой скан HTML по меню, навигации и футеру (мгновенно);
    2. Извлечение ссылок страницы + словарная классификация;
    3. sitemap.xml / robots.txt для жирных каталогов;
    4. LLM-классификация оставшихся ссылок при дефиците.
    """
    cats = scan_target_links(html, base_url)
    deficit = [c for c in ("prices", "contacts", "legal", "delivery") if not cats[c]]
    links = []
    if deficit:
        links = extract_site_links(html, base_url)
        regex_cats = classify_links_regex(links)
        for c in deficit:
            if regex_cats.get(c):
                cats[c] = regex_cats[c]
        deficit = [c for c in deficit if not cats[c]]
    if deficit:
        sm = await sitemap_pages(base_url)
        for c in deficit:
            if sm.get(c):
                cats[c] = sm[c]
        deficit = [c for c in deficit if not cats[c]]
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
    base_host = _normalize_host(urlparse(base_url).netloc)
    tokens = [t for t in re.split(r"\W+", query.lower()) if len(t) > 2]
    scored: list[tuple[float, str]] = []
    for m in re.finditer(r'<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL):
        href, anchor_html = m.group(1), m.group(2)
        if href.startswith(("mailto:", "tel:", "javascript:", "http")):
            continue
        absu = urljoin(base_url, href)
        host = _normalize_host(urlparse(absu).netloc)
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
    """Контакты через дешёвую модель: regex не тянет по вариативности
    футеров, а дорогая — получает только очищенный main content без футера."""
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




async def llm_delivery(url: str, text: str) -> tuple[str | None, str | None, str | None]:
    """Условия доставки выжимкой: география, бесплатность/порог, сроки,
    транспорт. Regex даёт обрывки вроде «доставка готовых рационов...» —
    закупщику нужна суть, а не первая строка с кейвордом."""
    if len(text) < 120:
        return None
    prompt = f"""Из текста страницы сайта оптового поставщика извлеки УСЛОВИЯ ДОСТАВКИ И ОПЛАТЫ.
Сведи в 1-2 предложения 220 символов максимум: география доставки (город/регион),
стоимость или порог бесплатной доставки, сроки, самовывоз, транспортные компании,
условия оплаты (нал/безнал/отсрочка) если есть.

Раздели данные на поля:
- geo: ГЕОГРАФИЯ доставки кратко — «Москва и МО», «Пермский край», «вся РФ»,
  «РФ и СНГ», «Санкт-Петербург и ЛО». null если не указана.
- delivery: УСЛОВИЯ доставки БЕЗ географии: порог бесплатности, стоимость,
  сроки, самовывоз, транспортные компании, оплата (нал/безнал/отсрочка).
- city: если доставка явно ЛОКАЛЬНАЯ (конкретный город/регион — «по Перми
  и краю», «по Москве и МО») — этот город в именительном падеже.
  Если федеральная («по России», «в любой город») — null.
  Это НЕ город покупателя: только то, что написано в тексте.

Ответ СТРОГО JSON: {{"geo": "география или null", "delivery": "условия без географии или null", "city": "город или null"}}

Текст:
{text[:7000]}"""
    try:
        data = await ask_json(prompt, smart=False, temperature=0.1, max_tokens=300)
        d = (data or {}).get("delivery")
        if not (isinstance(d, str) and len(d.strip()) > 10) and not data.get("geo"):
            return (None, None, None)
        geo = ((str(data.get("geo")).strip()[:40] or None)
               if data.get("geo") else None)
        city = ((str(data.get("city")).strip()[:40] or None)
                if data.get("city") else None)
        return ((d.strip()[:240] if isinstance(d, str) else None), city, geo)
    except LLMError as exc:
        logger.debug("llm_delivery failed for %s: %s", url, exc)
    return (None, None, None)


async def llm_facts(url: str, text: str, query: str) -> SiteFacts:
    """LLM-слой: понимает контекст, отсекает нерелевантные и розничные страницы.

    Ключевое отличие от regex: цены извлекаются ТОЛЬКО по предмету запроса
    (запрос «чипсы» -> «чипсы картофельные 43 ₽/шт»), а не любые ₽ на странице.
    Плюс вердикт «опт/розница» — фильтр мусора типа интернет-магазинов."""
    f = SiteFacts(url=url, method="llm")
    prompt = f"""Товар, который ищет закупщик: "{query}".
Ниже — текст с сайта компании. Твоя задача — критически оценить, РЕАЛЬНО ли это действующий поставщик именно "{query}", или это спам/дорвей/зомби-страница/случайный мусор/чужая тематика.

ВНИМАТЕЛЬНО ПОСМОТРИ НА КАТАЛОГ И СОДЕРЖИМОЕ СТРАНИЦЫ:
- Если в заголовке написано про "{query}", но В КАТАЛОГЕ И ТЕКСТЕ продаются совершенно другие товары (например: моторные/индустриальные масла, смазки, запчасти, стройматериалы, одежда, инструменты), а "{query}" упомянуто лишь формально или в старом шаблоне — это СТРОГО "offtopic"!
- Если это дохлый сайт, каталог-агрегатор, дорвей или заброшенная шаблонная страница на бесплатном поддомене — это СТРОГО "offtopic"!
- "profile" — ТОЛЬКО если сайт РЕАЛЬНО и преимущественно занимается производством или оптовой торговлей "{query}" (продукты питания, грибы, сельхозпродукция).
- "adjacent" — если это реальный оптовик других продуктов питания (овощи, фрукты, бакалея, заморозка).
- "offtopic" — не поставщик еды вообще (масла/химия, запчасти, услуги, блоги, дорвеи, спам).

Ответь JSON:
{{"match": "profile|adjacent|offtopic",
  "reasoning": "краткое объяснение решения на 1 предложение",
  "kind": "производитель|дистрибьютор|магазин|неясно",
  "wholesale": true/false,
  "wholesale_evidence": "признак оптовой торговли... иначе null",
  "description": "1-2 предложения, чем РЕАЛЬНО занимается компания",
  "price_examples": ["цены ТОЛЬКО на искомый товар {query}, не на чужие товары!"],
  "min_order": "мин. заказ если указан, иначе null",
  "city": "ГОРОД компании по контактным данным/реквизитам/адресу склада на странице (не город покупателя!); если не указан — null",
  "delivery": "условия доставки если указаны, иначе null",
  "order_terms": "условия заказа: оплата (безнал/нал/отсрочка), самовывоз/ТК/свой транспорт, сроки поставки — если указаны; иначе null",
  "certificates": ["найденные сертификаты/ГОСТы"],
  "phones": ["телефоны"], "emails": ["email"]}}

ВАЖНО:
1. В price_examples включай цены ТОЛЬКО на "{query}". Цены на другие товары (масла, запчасти, услуги, доставку) — НЕ включай.
2. wholesale=true если сайт работает с оптом: признаки — «опт», «мелкий опт», прайс для юрлиц, минимальная партия, оптовая фасовка коробками/ящиками.
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




class DOMNavExtractor(HTMLParser):
    """Извлечение всех внутренних ссылок из DOM без багов проглатывания тегов."""
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.base_host = _normalize_host(urlparse(base_url).netloc)
        self.links: list[tuple[str, str]] = []
        self._curr_href: str | None = None
        self._curr_text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "").strip()
            if href and not href.startswith(("mailto:", "tel:", "javascript:", "data:", "#")):
                absu = urljoin(self.base_url, href)
                if _normalize_host(urlparse(absu).netloc) == self.base_host:
                    self._curr_href = absu
                    self._curr_text = []

    def handle_data(self, data):
        if self._curr_href is not None:
            self._curr_text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self._curr_href is not None:
            text = " ".join("".join(self._curr_text).split())
            if text and len(text) < 100:
                self.links.append((self._curr_href, text))
            self._curr_href = None
            self._curr_text = []


_NAV_KEYWORD_PATTERNS = {
    "delivery": re.compile(r"доставк|оплат|delivery|shipping|dostavka|oplata", re.I),
    "about": re.compile(r"о нас|о компани|о предприяти|информаци|about|company|info", re.I),
    "contacts": re.compile(r"контакт|связ|contact|kontakty|где купить|адрес", re.I),
    "legal": re.compile(r"реквизит|requisite|rekv|документ", re.I),
    "prices": re.compile(r"прайс|цен[ыа]|каталог|price|catalog|товары|продукц", re.I),
}


def extract_key_subpages(html: str, base_url: str) -> dict[str, str]:
    """Находит в DOM навигации ключевые подстраницы: доставка, о компании, контакты, реквизиты, прайсы."""
    parser = DOMNavExtractor(base_url)
    try:
        parser.feed(html)
    except Exception:
        pass

    found: dict[str, str] = {}
    for url, anchor in parser.links:
        path = urlparse(url).path.lower()
        combo = f"{anchor} {path}"
        for cat, rx in _NAV_KEYWORD_PATTERNS.items():
            if cat not in found and rx.search(combo):
                if url.rstrip("/") != base_url.rstrip("/"):
                    found[cat] = url
    return found


def extract_embedded_payload(raw_html: str) -> dict:
    """Извлекает структурированные данные и состояние SSR гидратации из сырого HTML:
    - Schema.org JSON-LD (<script type="application/ld+json">): Organization, LocalBusiness, Store, Product
    - Next.js (<script id="__NEXT_DATA__">)
    - Inertia.js (<div id="app" data-page="...">)
    - Nuxt 2/3 (<script id="__NUXT_DATA__">, window.__NUXT__)
    - Redux / Vuex / Pinia SSR (window.__INITIAL_STATE__, window.__PRELOADED_STATE__)
    - Remix (window.__remixContext)
    - Apollo GraphQL SSR (window.__APOLLO_STATE__)
    - Meta description / OpenGraph (og:description)

    Работает мгновенно (0.1 мс), извлекая контакты, юрлицо, город, цены и чистый каталог
    БЕЗ надобности запускать тяжелый Playwright Chromium.
    """
    import html as _html_mod
    res = {
        "phones": set(),
        "emails": set(),
        "inn": None,
        "ogrn": None,
        "legal_name": None,
        "org_name": None,
        "city": None,
        "prices": [],
        "text_snippets": []
    }
    if not raw_html:
        return res

    def collect_strings(obj, acc, depth=0, max_items=40):
        if len(acc) >= max_items or depth > 4:
            return
        if isinstance(obj, str):
            clean = obj.strip()
            if 10 <= len(clean) <= 300:
                if not (clean.startswith("http") or clean.startswith("/_") or clean.startswith("data:")
                        or clean.startswith("{") or clean.startswith("<")):
                    acc.append(clean)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if len(acc) >= max_items:
                    break
                if k in ("css", "chunks", "runtimeConfig", "polyfilled", "modules", "webpack"):
                    continue
                collect_strings(v, acc, depth + 1, max_items)
        elif isinstance(obj, list):
            for v in obj[:20]:
                if len(acc) >= max_items:
                    break
                collect_strings(v, acc, depth + 1, max_items)

    # 1. Schema.org JSON-LD
    ld_blocks = re.findall(r'<script[^>]*type=[\'"]application/ld\+json[\'"][^>]*>(.*?)</script>', raw_html, re.DOTALL | re.I)
    for block in ld_blocks:
        try:
            data = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for it in items:
                if not isinstance(it, dict):
                    continue
                nodes = it.get("@graph") if isinstance(it.get("@graph"), list) else [it]
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    ntype = str(node.get("@type", ""))
                    if any(t in ntype for t in ("Organization", "LocalBusiness", "Store", "WholesaleStore", "Company", "FoodEstablishment")):
                        if node.get("name") and not res["org_name"]:
                            res["org_name"] = str(node["name"]).strip()
                        if node.get("legalName") and not res["legal_name"]:
                            res["legal_name"] = str(node["legalName"]).strip()
                        for key in ("taxID", "vatID", "inn"):
                            if node.get(key) and not res["inn"]:
                                val = re.sub(r"\D", "", str(node[key]))
                                if len(val) in (10, 12):
                                    res["inn"] = val
                        if node.get("ogrn") and not res["ogrn"]:
                            val = re.sub(r"\D", "", str(node["ogrn"]))
                            if len(val) in (13, 15):
                                res["ogrn"] = val
                        if node.get("telephone"):
                            tels = node["telephone"] if isinstance(node["telephone"], list) else [node["telephone"]]
                            for t in tels:
                                res["phones"].add(str(t).strip())
                        if node.get("email"):
                            ems = node["email"] if isinstance(node["email"], list) else [node["email"]]
                            for e in ems:
                                res["emails"].add(str(e).strip().lower())
                        addr = node.get("address")
                        if isinstance(addr, dict):
                            loc = addr.get("addressLocality") or addr.get("addressRegion")
                            if loc and not res["city"]:
                                res["city"] = str(loc).strip()
                            st = addr.get("streetAddress") or ""
                            res["text_snippets"].append(f"Адрес компании: {loc or ''} {st}".strip())
                    if "ContactPoint" in ntype:
                        if node.get("telephone"):
                            tels = node["telephone"] if isinstance(node["telephone"], list) else [node["telephone"]]
                            for t in tels:
                                res["phones"].add(str(t).strip())
                        if node.get("email"):
                            ems = node["email"] if isinstance(node["email"], list) else [node["email"]]
                            for e in ems:
                                res["emails"].add(str(e).strip().lower())
                    if "Product" in ntype:
                        pname = str(node.get("name") or "").strip()
                        offers = node.get("offers")
                        off_list = offers if isinstance(offers, list) else ([offers] if isinstance(offers, dict) else [])
                        for off in off_list:
                            if isinstance(off, dict):
                                pr = off.get("price")
                                cur = off.get("priceCurrency") or "RUB"
                                if pr:
                                    res["prices"].append(f"{pname}: {pr} {cur}".strip())
        except Exception:
            pass

    # 2. Next.js: <script id="__NEXT_DATA__">
    m_next = re.search(r'<script[^>]*id=[\'"]__NEXT_DATA__[\'"][^>]*>(.*?)</script>', raw_html, re.DOTALL | re.I)
    if m_next:
        try:
            d = json.loads(m_next.group(1).strip())
            pp = d.get("props", {}).get("pageProps", {})
            collected = []
            collect_strings(pp, collected)
            if collected:
                res["text_snippets"].append(" ".join(collected))
        except Exception:
            pass

    # 3. Inertia.js: data-page
    m_inertia = re.search(r'data-page=([\'"])(.*?)\1', raw_html, re.DOTALL | re.I)
    if m_inertia:
        try:
            d = json.loads(_html_mod.unescape(m_inertia.group(2)))
            props = d.get("props", {})
            collected = []
            collect_strings(props, collected)
            if collected:
                res["text_snippets"].append(" ".join(collected))
        except Exception:
            pass

    # 4. Redux / Vuex / Pinia / Remix / Nuxt State
    state_patterns = [
        r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});?\s*</script>',
        r'window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});?\s*</script>',
        r'window\.__remixContext\s*=\s*(\{.*?\});?\s*</script>',
        r'<script[^>]*id=[\'"]__NUXT_DATA__[\'"][^>]*>(.*?)</script>',
        r'window\.__NUXT__\s*=\s*(\{.*?\});?\s*</script>',
        r'window\.__APOLLO_STATE__\s*=\s*(\{.*?\});?\s*</script>'
    ]
    for sp in state_patterns:
        m = re.search(sp, raw_html, re.DOTALL | re.I)
        if m:
            raw_blob = m.group(1).strip()
            try:
                d = json.loads(raw_blob)
                collected = []
                collect_strings(d, collected)
                if collected:
                    res["text_snippets"].append(" ".join(collected))
            except Exception:
                pass

    # 5. OpenGraph & Meta description
    m_desc = re.search(r'<meta[^>]*name=[\'"]description[\'"][^>]*content=[\'"]([^\'"]+)[\'"]', raw_html, re.I)
    if m_desc:
        res["text_snippets"].append(f"Описание: {_html_mod.unescape(m_desc.group(1))}")
    m_og = re.search(r'<meta[^>]*property=[\'"]og:description[\'"][^>]*content=[\'"]([^\'"]+)[\'"]', raw_html, re.I)
    if m_og:
        res["text_snippets"].append(f"О компании: {_html_mod.unescape(m_og.group(1))}")

    return res


async def fetch_clean_page(url: str, *, allow_render: bool = True) -> tuple[str, str, dict]:
    """Скачивает страницу, извлекает встроенные данные гидратации (Schema.org JSON-LD,
    Next.js __NEXT_DATA__, Nuxt, Inertia.js, Redux, Remix), а при реальной необходимости
    в JS-рендере — запускает браузер Playwright.
    Возвращает (html, text, embedded)."""
    html = await fetch_html(url)
    if not html or len(html.strip()) < 50:
        return "", "", {}

    low_html = html.lower()
    if any(err in low_html for err in ("404 not found", "страница не найдена", "ошибка 404",
                                       "403 forbidden", "403 доступ запрещен", "доступ ограничен")):
        return html, "", {}

    text = _text_of(html)
    embedded = extract_embedded_payload(html)

    # Обогащаем текстовый слой плоским текстом из гидратации/метатегов
    if embedded.get("text_snippets"):
        extra = "\n".join(embedded["text_snippets"])
        text = f"{text}\n{extra}".strip()

    # Полноценная проверка надобности в Playwright Chromium:
    # 1. Если allow_render=False — Chromium запрещен (например, догадки подстраниц).
    # 2. Если в гидратации уже найдены прямые контакты (телефоны/email) — Chromium НЕ нужен.
    # 3. Если текст уже достаточный (>= 250 символов) — Chromium НЕ нужен.
    # 4. Chromium нужен ТОЛЬКО если allow_render=True, контактов нет, а разметка — явная SPA-пустышка.
    has_contacts_in_embedded = bool(embedded.get("phones") or embedded.get("emails"))
    is_spa_shell = ('<div id="root">' in html or '<div id="app">' in html)
    needs_render = allow_render and not has_contacts_in_embedded and (
        (is_spa_shell and len(text) < 250) or (len(text) < 120 and not embedded.get("text_snippets"))
    )

    if needs_render:
        rendered = await render_html(url)
        if rendered:
            html = rendered
            text = _text_of(html)
            if not has_contacts_in_embedded:
                emb_rendered = extract_embedded_payload(html)
                for k, v in emb_rendered.items():
                    if isinstance(v, set):
                        embedded[k].update(v)
                    elif isinstance(v, list):
                        embedded[k].extend(v)
                    elif v and not embedded.get(k):
                        embedded[k] = v

    return html, text, embedded

async def enrich_site(url: str, query: str, *, use_llm: bool = True) -> SiteFacts:
    """Комплексный анализ компании по сайту:
    1. Читает посадочную страницу (с авто-рендером React/Vue SPA и парсингом SSR гидратации);
    2. Извлекает из меню и футера ссылки на Доставку, О компании, Контакты;
    3. Скачивает и рендерит эти страницы параллельно;
    4. Извлекает контакты, телефоны, email, ИНН и цены (включая Schema.org / State);
    5. Передает комплексный текст (товар + компания + доставка) в LLM,
       чтобы модель видела полную картину, а не обрывок товарного каталога!
    """
    facts = SiteFacts(url=url, method="holistic")

    # 1. Посадочная страница
    landing_html, landing_text, landing_emb = await fetch_clean_page(url)
    if not landing_html:
        root_url = f"{urlparse(url).scheme or 'https'}://{urlparse(url).netloc}/"
        if root_url.rstrip("/") != url.rstrip("/"):
            landing_html, landing_text, landing_emb = await fetch_clean_page(root_url)

    if not landing_html or len(landing_text) < 40:
        facts.method = "failed"
        return facts

    facts.ok = True
    facts.rendered = True

    # Применяем прямые факты из гидратации посадочной страницы
    for p in landing_emb.get("phones", []):
        cp = _clean_phone(p)
        if cp and cp not in facts.phones:
            facts.phones.append(cp)
    for e in landing_emb.get("emails", []):
        if e and e not in facts.emails:
            facts.emails.append(e)
    if landing_emb.get("inn"):
        facts.inn = landing_emb["inn"]
    if landing_emb.get("ogrn"):
        facts.ogrn = landing_emb["ogrn"]
    if landing_emb.get("legal_name"):
        facts.legal_name = landing_emb["legal_name"]
    if landing_emb.get("city"):
        facts.city = landing_emb["city"]
    if landing_emb.get("prices"):
        facts.prices.extend(landing_emb["prices"])

    # 2. Поиск ключевых страниц в DOM навигации (меню, футер)
    subpages = extract_key_subpages(landing_html, url)

    # Если посадочная страница глубокая (товар), а меню там нет — проверим корень сайта
    root_url = f"{urlparse(url).scheme or 'https'}://{urlparse(url).netloc}/"
    if ("delivery" not in subpages or "about" not in subpages) and root_url.rstrip("/") != url.rstrip("/"):
        root_html, _, root_emb = await fetch_clean_page(root_url)
        if root_html:
            root_subpages = extract_key_subpages(root_html, root_url)
            for k, v in root_subpages.items():
                if k not in subpages:
                    subpages[k] = v
            # добираем реквизиты с корня
            for p in root_emb.get("phones", []):
                cp = _clean_phone(p)
                if cp and cp not in facts.phones:
                    facts.phones.append(cp)
            for e in root_emb.get("emails", []):
                if e and e not in facts.emails:
                    facts.emails.append(e)
            if root_emb.get("inn") and not facts.inn:
                facts.inn = root_emb["inn"]
            if root_emb.get("legal_name") and not facts.legal_name:
                facts.legal_name = root_emb["legal_name"]

    # Фолбэк типовых путей доставки/контактов, если в меню их нет (без Playwright!)
    guessed_keys = set()
    if "delivery" not in subpages:
        subpages["delivery"] = f"{root_url}delivery/"
        guessed_keys.add("delivery")
    if "contacts" not in subpages:
        subpages["contacts"] = f"{root_url}contacts/"
        guessed_keys.add("contacts")

    # 3. Параллельное скачивание ключевых подстраниц (с авто-рендером SPA и гидратацией!)
    fetch_tasks = {
        cat: fetch_clean_page(u, allow_render=(cat not in guessed_keys))
        for cat, u in subpages.items()
        if cat in ("delivery", "about", "contacts", "legal")
    }
    sub_texts: dict[str, str] = {}
    if fetch_tasks:
        res = await asyncio.gather(*fetch_tasks.values())
        for cat, (h, t, emb) in zip(fetch_tasks.keys(), res):
            if len(t) >= 60:
                sub_texts[cat] = t
                for p in emb.get("phones", []):
                    cp = _clean_phone(p)
                    if cp and cp not in facts.phones:
                        facts.phones.append(cp)
                for e in emb.get("emails", []):
                    if e and e not in facts.emails:
                        facts.emails.append(e)
                if emb.get("inn") and not facts.inn:
                    facts.inn = emb["inn"]
                if emb.get("legal_name") and not facts.legal_name:
                    facts.legal_name = emb["legal_name"]
                if emb.get("city") and not facts.city:
                    facts.city = emb["city"]

    # 4. Регулярки по ВСЕМ скачанным страницам (контакты, ИНН, ОГРН)
    all_combined_text = f"{landing_text}\n" + "\n".join(sub_texts.values())
    rg_facts = regex_facts(url, all_combined_text)
    facts.phones = list(dict.fromkeys(facts.phones + rg_facts.phones))
    facts.emails = list(dict.fromkeys(facts.emails + rg_facts.emails))
    facts.certificates = rg_facts.certificates

    legal = extract_legal_from_text(all_combined_text)
    facts.inn = legal.get("inn")
    facts.ogrn = legal.get("ogrn")
    facts.legal_name = legal.get("legal_name")

    # 5. Ссылки на прайсы/каталоги
    price_page_url = subpages.get("prices")
    pl = extract_product_links(landing_html, query, url)
    facts.product_links = list(dict.fromkeys(([price_page_url] if price_page_url else []) + pl))[:3]

    # 6. Комплексный LLM-анализ компании
    if use_llm:
        about_snippet = sub_texts.get("about", sub_texts.get("legal", ""))[:3500]
        delivery_snippet = sub_texts.get("delivery", "")[:3500]
        contacts_snippet = sub_texts.get("contacts", "")[:1500]

        prompt = f"""Товар, который ищет закупщик: "{query}".
Ниже — сводная информация с официального сайта компании (страница товара, «О нас», «Доставка и оплата», «Контакты»):

--- ПОСАДОЧНАЯ СТРАНИЦА ТОВАРА ---
{landing_text[:3500]}

--- О КОМПАНИИ / О НАС ---
{about_snippet or "—"}

--- ДОСТАВКА И ОПЛАТА ---
{delivery_snippet or "—"}

--- КОНТАКТЫ И РЕКВИЗИТЫ ---
{contacts_snippet or "—"}

Проанализируй сайт КОМПЛЕКСНО и ответь СТРОГО JSON:
{{
  "match": "profile|adjacent|offtopic",
  "reasoning": "краткое объяснение решения (1 предложение)",
  "kind": "производитель|дистрибьютор|магазин|неясно",
  "wholesale": true/false,
  "description": "1-2 предложения, чем реально занимается компания",
  "company_city": "в каком городе/регионе находится офис/склад/производство компании (НЕ город покупателя!); null если не указан",
  "delivery_geo": "куда доставляют («Москва и МО», «вся РФ», «Новосибирск и пригород» и т.п.)",
  "delivery": "условия доставки (порог бесплатности, стоимость, сроки, самовывоз, оплата)",
  "prices": ["цены ТОЛЬКО на {query}"],
  "min_order": "мин. заказ если указан",
  "inn": "ИНН если указан в реквизитах, иначе null",
  "legal_name": "ООО/ИП/АО если указано в реквизитах, иначе null",
  "phones": ["телефоны если есть"],
  "emails": ["email если есть"]
}}"""

        try:
            data = await ask_json(prompt, smart=True, temperature=0.1)
            if isinstance(data, dict):
                match = str(data.get("match") or "").strip().lower()
                facts.match = match if match in ("profile", "adjacent", "offtopic") else "offtopic"
                facts.irrelevant = (facts.match == "offtopic")
                facts.kind = (data.get("kind") or "неясно")[:20]
                facts.wholesale = bool(data.get("wholesale")) if "wholesale" in data else None
                if data.get("description"):
                    facts.description = str(data["description"])[:300]
                if data.get("company_city"):
                    facts.city = str(data["company_city"]).strip()[:40]
                if data.get("delivery_geo"):
                    facts.delivery_geo = str(data["delivery_geo"]).strip()[:50]
                if data.get("delivery"):
                    facts.delivery = str(data["delivery"]).strip()[:240]
                if data.get("min_order"):
                    facts.min_order = str(data["min_order"]).strip()[:80]
                if data.get("prices"):
                    facts.prices = _polish_prices([str(p) for p in data["prices"][:4]])
                if data.get("inn") and not facts.inn:
                    facts.inn = re.sub(r"\D", "", str(data["inn"]))[:12]
                if data.get("legal_name") and not facts.legal_name:
                    facts.legal_name = str(data["legal_name"]).strip()[:80]
                for p in (data.get("phones") or []):
                    cp = _clean_phone(str(p))
                    if cp and cp not in facts.phones:
                        facts.phones.append(cp)
                for e in (data.get("emails") or []):
                    se = str(e).lower().strip()
                    if se and se not in facts.emails:
                        facts.emails.append(se)
        except Exception as exc:
            logger.warning("Holistic LLM analysis failed for %s: %s", url, exc)

    # Город по коду стационарного телефона (резерв)
    if not facts.city and facts.phones:
        facts.city = _city_from_phone(facts.phones[0]) or facts.city

    facts.prices = _polish_prices(facts.prices)
    return facts


def _polish_prices(raw: list[str]) -> list[str]:
    """Косметика LLM-цен: «140 0 ₽ За» -> «140,00 ₽», нулевые цены («0 руб./кг», «0 ₽») — вон."""
    out = []
    for s in raw or []:
        s = re.sub(r"\s+(за|шт|упак)\b.*$", "", (s or "").strip(), flags=re.I)
        m = re.match(r"^(\d[\d\s]*)\s(\d)\s*₽$", s)
        if m:
            s = f"{m.group(1).strip()},{m.group(2)}0 ₽"
        # Проверка на ноль (0 руб, 0.00 руб, 0 руб./кг)
        clean_num = re.sub(r"[^\d.]", "", s.replace(",", "."))
        try:
            if float(clean_num) <= 0:
                continue
        except (ValueError, TypeError):
            pass
        if re.match(r"^0[\s\u00a0]*(руб|₽|\.0)", s, re.I):
            continue
        if s and s not in out:
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
