"""Источник: справочник организаций OrgPage (orgpage.ru).

Открытый JSON API поиска по локации и рубрикам:
  * URL: POST https://www.orgpage.ru/Search/GetSearchCompany/
  * payload: {"number": 1, "loc": город/регион, "searchText": запрос, "isForReplies": "false"}
  * отдает 30 компаний на страницу с реальными телефонами, адресами в регионе,
    сайтами и описаниями деятельности.
  * без капчи, быстрый HTTP POST (~0.3с), нативная регионализация (loc).

Дополнительно: для типовых категорий (грибы, овощи, мясо) проверяет
прямые страницы рубрик (/{region_slug}/{rubric}/).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import httpx

from ..extraction import _clean_phone, _text_of, fetch_html

logger = logging.getLogger(__name__)

ORG_BASE = "https://www.orgpage.ru"

REGION_TO_CITY: dict[str, str] = {
    "Москва": "Москва",
    "Московская область": "Московская область",
    "Санкт-Петербург": "Санкт-Петербург",
    "Ленинградская область": "Ленинградская область",
    "Новосибирская область": "Новосибирск",
    "Екатеринбург / Свердловская область": "Екатеринбург",
    "Нижегородская область": "Нижний Новгород",
    "Казань / Татарстан": "Казань",
    "Краснодарский край": "Краснодар",
    "Ростовская область": "Ростов-на-Дону",
    "Самарская область": "Самара",
    "Воронежская область": "Воронеж",
    "Волгоградская область": "Волгоград",
    "Саратовская область": "Саратов",
    "Тюменская область": "Тюмень",
    "Пермский край": "Пермь",
    "Челябинская область": "Челябинск",
    "Омская область": "Омск",
    "Приморский край": "Владивосток",
    "Иркутская область": "Иркутск",
    "Башкортостан": "Уфа",
    "Тульская область": "Тула",
    "Ярославская область": "Ярославль",
    "Тверская область": "Тверь",
    "Калининградская область": "Калининград",
    "Ставропольский край": "Ставрополь",
    "Крым": "Симферополь",
    "Вся Россия": "Россия",
}

REGION_TO_SLUG: dict[str, str] = {
    "Челябинская область": "chelyabinskaya-obl",
    "Екатеринбург / Свердловская область": "sverdlovskaya-obl",
    "Пермский край": "permskiy-kray",
    "Москва": "moskva",
    "Московская область": "moskovskaya-obl",
    "Санкт-Петербург": "spb",
    "Ленинградская область": "leningradskaya-obl",
    "Новосибирская область": "novosibirskaya-obl",
    "Нижегородская область": "nizhegorodskaya-obl",
    "Казань / Татарстан": "tatarstan",
    "Краснодарский край": "krasnodarskiy-kray",
    "Ростовская область": "rostovskaya-obl",
    "Самарская область": "samarskaya-obl",
    "Воронежская область": "voronezhskaya-obl",
    "Тульская область": "tulskaya-obl",
    "Башкортостан": "bashkortostan",
}

# Подбор рубрик для частых продуктовых категорий
KEYWORD_RUBRICS: list[tuple[re.Pattern, list[str]]] = [
    (re.compile(r"шампиньон|вешенк|гриб", re.I),
     ["proizvodstvo-shampinonov", "gribovodstvo", "gribnoe-proizvodstvo"]),
    (re.compile(r"мук[аеи]|зерн|мукомол", re.I),
     ["zernovodstvo-2", "muka"]),
    (re.compile(r"овощ|зелен|укроп|петрушк|салат", re.I),
     ["ovoschevodstvo-i-bakhchevodstvo", "selkhozproduktsiya"]),
    (re.compile(r"мяс|птиц|говядин|свинин|куриц", re.I),
     ["zhivotnovodstvo", "ptitsevodstvo"]),
    (re.compile(r"рыб|икр|морепродукт", re.I),
     ["akvakultura"]),
]


@dataclass(slots=True)
class OrgpageHit:
    name: str
    city: str
    address: str = ""
    phones: list[str] = field(default_factory=list)
    site: str = ""
    domain: str = ""
    description: str = ""
    orgpage_url: str = ""

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def _parse_blocks(html: str, default_city: str) -> list[OrgpageHit]:
    """Унифицированный парсинг карточек (одинаковый для search API и рубрик)."""
    blocks = html.split('class="object-item similar-item"')
    hits: list[OrgpageHit] = []
    seen_names = set()
    for b in blocks[1:]:
        title_m = re.search(
            r'<div class="similar-item__title">\s*<a[^>]*href="([^"]*)"[^>]*>(?:<span[^>]*>.*?</span>)?(.*?)</a>',
            b, re.DOTALL)
        if not title_m:
            continue
        purl = title_m.group(1).strip()
        name = _text_of(title_m.group(2)).strip()
        if not name or name in seen_names:
            continue
        seen_names.add(name)

        desc_m = re.search(r'<div class="similar-item__description">(.*?)</div>', b, re.DOTALL)
        desc = _text_of(desc_m.group(1)).strip() if desc_m else ""

        raw_phones = re.findall(r'<div class="phone\s*">\s*([^<]+)\s*</div>', b)
        phones = [_clean_phone(p) for p in raw_phones if _clean_phone(p)]

        site_m = re.search(r'<div class="site"[^>]*>(.*?)</div>', b, re.DOTALL)
        raw_site = _text_of(site_m.group(1)).strip() if site_m else ""
        from ..merge import domain_key, is_blacklisted
        d = domain_key(raw_site) if raw_site else ""
        if d and is_blacklisted(d):
            raw_site = ""
            d = ""
        site = ("https://" + raw_site) if raw_site and not raw_site.startswith("http") else raw_site

        addr_m = re.search(r'<div class="similar-item__address-col col-lg-6[^>]*>(.*?)</div>', b, re.DOTALL)
        addr = _text_of(addr_m.group(1)).strip() if addr_m else ""

        # Город: из адреса если там указан другой субъект (для межрегиональных в рубрике),
        # иначе default_city
        city = default_city
        if addr:
            m_city = re.search(r'(?:^|,\s*)(?:г\.?\s*|город\s+)([А-ЯЁ][а-яё\-]+)', addr)
            if m_city:
                city = m_city.group(1)

        hits.append(OrgpageHit(
            name=name,
            city=city,
            address=addr,
            phones=phones,
            site=site,
            domain=d,
            description=desc,
            orgpage_url=purl if purl.startswith("http") else (ORG_BASE + purl),
        ))
    return hits


async def search_orgpage(query: str, region_name: str, limit: int = 30) -> list[OrgpageHit]:
    """Сбор компаний из справочника OrgPage по поисковому запросу и региону."""
    loc = REGION_TO_CITY.get(region_name, region_name.split("/")[0].strip())
    hits_by_key: dict[str, OrgpageHit] = {}

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Referer": "https://www.orgpage.ru/search.html",
        "X-Requested-With": "XMLHttpRequest",
    }

    # 1. Поиск через JSON API OrgPage
    search_queries = [
        f"купить {query}",
        query,
    ]
    # Если запрос многословный (например, «шампиньоны свежие опт»), добавим базовое слово
    first_word = query.split()[0].strip()
    if len(first_word) >= 4 and first_word not in query:
        search_queries.append(first_word)

    async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
        for sq in search_queries[:2]:
            payload = {
                "number": 1,
                "loc": loc,
                "searchText": sq,
                "isForReplies": "false",
            }
            try:
                r = await client.post(f"{ORG_BASE}/Search/GetSearchCompany/",
                                      json=payload, headers=headers)
                if r.status_code == 200:
                    found = _parse_blocks(r.text, loc)
                    for h in found:
                        k = h.domain or h.name
                        if k not in hits_by_key:
                            hits_by_key[k] = h
                    if len(hits_by_key) >= limit:
                        break
            except Exception as exc:
                logger.debug("orgpage search error %s: %s", sq, exc)

    # 2. Если есть релевантная рубрика — проверим прямую страницу рубрики
    reg_slug = REGION_TO_SLUG.get(region_name)
    if reg_slug:
        rubrics_to_check: list[str] = []
        for rx, r_list in KEYWORD_RUBRICS:
            if rx.search(query):
                rubrics_to_check.extend(r_list)
        for r_slug in rubrics_to_check[:2]:
            try:
                url = f"{ORG_BASE}/{reg_slug}/{r_slug}/"
                r_html = await fetch_html(url)
                if r_html and "object-item" in r_html:
                    r_hits = _parse_blocks(r_html, loc)
                    for h in r_hits:
                        k = h.domain or h.name
                        if k not in hits_by_key:
                            hits_by_key[k] = h
            except Exception as exc:
                logger.debug("orgpage rubric error %s: %s", r_slug, exc)

    return list(hits_by_key.values())[:limit]
