"""Merge-слой: слияние карточек поставщиков из 3 источников + provenance.

Ключи слияния (по убыванию надёжности):
  1. ИНН (госконтракт ↔ optlist) — точное совпадение;
  2. домен (SERP ↔ optlist site) — tldextract;
  3. fuzzy-имя (rapidfuzz partial_ratio ≥ 93).

Санитария данных:
  * DOMAIN_BLACKLIST — поисковики, маркетплейсы, агрегаторы и сам каталог
    никогда не становятся «поставщиками» (в органике их полно);
  * имена-заголовки типа «Купить чипсы оптом в СПб» заменяются доменом —
    это честно: поисковик знает домен, а не юрлицо.

Provenance: каждый атрибут хранит источник («цена ← госконтракт №…»)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import tldextract
from rapidfuzz import fuzz

from .extraction import normalize_price
from .sources.gosplan import GovSupplier
from .sources.optlist import OptlistHit
from .sources.xmlstock import SerpHit

_tld = tldextract.TLDExtract(suffix_list_urls=())  # встроенный снапшот PSL — офлайн

# Всё, что не может быть поставщиком: поисковики, маркетплейсы, агрегаторы,
# отраслевые порталы-справочники, соцсети. Проверяется по регистрируемому домену.
DOMAIN_BLACKLIST: set[str] = {
    # поисковики/порталы
    "yandex.ru", "ya.ru", "google.com", "google.ru", "bing.com", "mail.ru",
    "list-org.com", "checko.ru", "rusprofile.ru", "sbis.ru", "kontur.ru",
    "zakupki.gov.ru", "gosplan.info",
    # маркетплейсы/объявления
    "avito.ru", "ozon.ru", "wildberries.ru", "market.yandex.ru", "sbermegamarket.ru",
    "allegro.ru", "ebay.com", "aliexpress.ru",
    # b2b-агрегаторы и каталоги (они — витрины, не поставщики)
    "optlist.ru", "tiu.ru", "pulscen.ru", "prom.ru", "all.biz", "satom.ru",
    "fl.ru", "b2b-center.ru", "prodirectory.ru", "foodbay.com", "prodal.ru",
    "suplfood.ru", "foods.ru", "mosex.name", "agrorus.com", "opt-union.ru",
    "sakh.com", "bloha.ru", "tenderguru.ru", "sdelanogroup.ru",
    "agroserver.ru", "suppliers.ru", "postavshhikov.net", "1sept.ru",
    # соцсети/видео/справка
    "vk.com", "ok.ru", "youtube.com", "rutube.ru", "dzen.ru", "instagram.com",
    "telegram.org", "t.me", "whatsapp.com", "wikipedia.org", "drom.ru",
    # досье/новости
    " Kommersant", "rbc.ru", "vedomosti.ru", "ria.ru", "tass.ru",
}


def domain_key(url: str) -> str:
    """Регистрируемый домен из URL: 'https://www.opt23.ru/cat?x' -> 'opt23.ru'."""
    if not url:
        return ""
    if "://" not in url:
        url = "http://" + url
    try:
        ext = _tld(url)
        return f"{ext.domain}.{ext.suffix}".lower() if ext.domain else ""
    except Exception:
        return ""


def is_blacklisted(domain: str) -> bool:
    return domain in DOMAIN_BLACKLIST


# «продающие» заголовки — мусорное имя, используем домен
RE_TITLE_JUNK = re.compile(r"^(купить|купим|продажа|цена|оптом|дешево|заказать|прайс|каталог|товары)", re.I)


@dataclass
class SupplierCard:
    """Унифицированная карточка поставщика до записи в БД."""
    name: str
    domain: str = ""
    site: str = ""
    inn: str | None = None
    city: str = ""
    region: str = ""
    phones: list[str] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)
    description: str = ""
    categories: list[str] = field(default_factory=list)
    price_min: float | None = None
    price_max: float | None = None
    price_note: str = ""
    min_order: str = ""
    delivery: str = ""
    certificates: list[str] = field(default_factory=list)
    docs_verified: bool = False
    gov_contracts_count: int = 0
    gov_contracts_value: float = 0
    gov_contract_examples: list[dict] = field(default_factory=list)
    relevance: int = 0
    reasons: list[str] = field(default_factory=list)
    sources: dict[str, str] = field(default_factory=dict)   # field -> "источник"
    enrichment_status: str = "pending"
    status: str = "new"
    serp_urls: list[str] = field(default_factory=list)

    def to_row(self) -> dict:
        return {
            "name": self.name, "domain": self.domain, "site": self.site, "inn": self.inn,
            "city": self.city, "region": self.region, "phones": self.phones,
            "emails": self.emails, "description": self.description,
            "categories": self.categories, "price_min": self.price_min,
            "price_max": self.price_max, "price_note": self.price_note,
            "min_order": self.min_order, "delivery": self.delivery,
            "certificates": self.certificates, "docs_verified": self.docs_verified,
            "gov_contracts_count": self.gov_contracts_count,
            "gov_contracts_value": self.gov_contracts_value,
            "gov_contract_examples": self.gov_contract_examples,
            "relevance": self.relevance, "reasons": self.reasons,
            "sources": self.sources, "enrichment_status": self.enrichment_status,
            "status": self.status,
        }


def merge_all(serp: list[SerpHit], optlist: list[OptlistHit], gov: list[GovSupplier],
              region_name: str) -> list[SupplierCard]:
    """Слияние трёх источников в список карточек."""
    cards: list[SupplierCard] = []

    # --- 1. госконтракты: самый сильный источник (реальные сделки) ---
    gos_by_inn: dict[str, SupplierCard] = {}
    for g in gov:
        examples = [c.to_dict() for c in g.contracts[:5]]
        note_parts = [f"«{(c.subject or 'контракт')[:60]}» — {(c.price or 0):,.0f} ₽"
                      for c in g.contracts[:2]]
        card = SupplierCard(
            name=g.name, inn=g.inn,
            region=region_name, city=(g.address or "").split(",")[0].strip(),
            description=g.address or "",
            gov_contracts_count=g.count, gov_contracts_value=g.total_value,
            gov_contract_examples=examples,
            price_note="контракты: " + "; ".join(note_parts) if note_parts else "",
            sources={
                "имя, ИНН, адрес": f"реестр контрактов 44-ФЗ (Госплан API), ИНН {g.inn}",
                "цены": f"{g.count} исполненных контрактов на {g.total_value:,.0f} ₽",
            },
        )
        gos_by_inn[g.inn] = card
        cards.append(card)

    # --- 2. optlist: отраслевой каталог ---
    for oh in optlist:
        target = gos_by_inn.get(oh.inn) if oh.inn else None
        if target is None:
            best, best_score = None, 0.0
            for c in gos_by_inn.values():
                score = fuzz.partial_ratio(oh.name.lower(), c.name.lower())
                if score > best_score:
                    best, best_score = c, score
            if best is not None and best_score >= 93:
                target = best
        if target is not None:
            _absorb_optlist(target, oh)
            continue
        card = SupplierCard(
            name=oh.name, city=oh.city, region=region_name,
            inn=oh.inn, site=f"https://optlist.ru/company/{oh.slug}",
            emails=[oh.email] if oh.email else [],
            phones=[oh.phone_masked] if oh.phone_masked else [],
            description=oh.description, min_order=oh.min_order or "",
            delivery=oh.delivery or "",
            certificates=[], docs_verified=oh.docs_verified,
            categories=[oh.tags] if oh.tags else [],
            sources={"имя, город": f"каталог optlist.ru/company/{oh.slug}"},
        )
        if oh.email:
            card.sources["email"] = f"optlist.ru/company/{oh.slug}"
        if oh.min_order:
            card.sources["мин. заказ"] = f"optlist.ru/company/{oh.slug}"
        if oh.docs_verified:
            card.sources["документы"] = "optlist.ru: документы проверены"
        cards.append(card)

    # --- 3. SERP: органический поиск, merge по домену с уже созданными ---
    have_domains = {c.domain for c in cards if c.domain}
    for hit in serp:
        d = domain_key(hit.url)
        if not d or d in have_domains or is_blacklisted(d):
            continue
        # имя = домен: честно (поисковик знает домен, а не юрлицо).
        # Заголовки-«купить оптом в СПб» — мусор, не имя поставщика.
        card = SupplierCard(
            name=d,
            domain=d, site=hit.url, region=region_name,
            description=hit.snippet, serp_urls=[hit.url],
            sources={"домен, сниппет": f"поиск {hit.engine}: позиция {hit.position}"},
        )
        cards.append(card)
        have_domains.add(d)

    _score_all(cards, serp)
    return cards


def _absorb_optlist(card: SupplierCard, oh: OptlistHit) -> None:
    """optlist-данные дополняют карточку (не перезаписывают госконтрактные)."""
    if not card.city:
        card.city = oh.city
    if not card.domain and oh.site:
        card.domain = domain_key(oh.site)
    if oh.email and oh.email not in card.emails:
        card.emails.append(oh.email)
        card.sources["email"] = f"optlist.ru/company/{oh.slug}"
    if oh.phone_masked and oh.phone_masked not in card.phones:
        card.phones.append(oh.phone_masked)
    if oh.min_order:
        card.min_order = oh.min_order
        card.sources["мин. заказ"] = f"optlist.ru/company/{oh.slug}"
    if oh.delivery:
        card.delivery = oh.delivery
        card.sources["доставка"] = f"optlist.ru/company/{oh.slug}"
    if oh.docs_verified:
        card.docs_verified = True
        card.sources["документы"] = "optlist.ru: документы проверены"
    if oh.description and not card.description:
        card.description = oh.description


def _score_all(cards: list[SupplierCard], serp: list[SerpHit]) -> None:
    """Детерминированный рейтинг 0-100 с внятным распределением.

    Компоненты (сумма до 100):
      госконтракты  до 40   — подтверждённый опыт поставок (главный сигнал)
      каталог       15      — проверенный B2B-профиль
      SERP-позиция  до 15   — топ органики региона
      email         10      — есть прямой канал связи
      цена предмета до 15   — известен ценовой ориентир по запросу
      телефон      5       — второй канал
      МОК/сертификаты/докум. до 15
    """
    pos_by_domain: dict[str, int] = {}
    for h in serp:
        d = domain_key(h.url)
        if d and (d not in pos_by_domain or h.position < pos_by_domain[d]):
            pos_by_domain[d] = h.position
    for c in cards:
        score = 0
        if c.gov_contracts_count:
            score += 30 + min(5 * c.gov_contracts_count, 10)
        if "optlist.ru" in str(c.sources.values()):
            score += 15
        pos = pos_by_domain.get(c.domain)
        if pos:
            score += 15 if pos <= 5 else (10 if pos <= 10 else 5)
        if c.emails:
            score += 10
        if c.price_min or c.gov_contracts_value:
            score += 15
        if c.phones:
            score += 5
        if c.min_order:
            score += 5
        if c.certificates:
            score += 5
        if c.docs_verified:
            score += 5
        c.relevance = min(score, 100)


def apply_enrichment(card: SupplierCard, facts) -> None:
    """Дополнить карточку фактами с сайта (enrichment-слой)."""
    if not facts.ok:
        card.enrichment_status = "skipped"
        return
    if facts.irrelevant:
        card.enrichment_status = "irrelevant"
        card.relevance = max(0, card.relevance - 25)
        return
    card.enrichment_status = "done"
    new_phones = [p for p in facts.phones if p not in card.phones]
    card.phones.extend(new_phones[:2])
    new_emails = [e for e in facts.emails if e not in card.emails]
    card.emails.extend(new_emails[:2])
    if facts.description and not card.description:
        card.description = facts.description
    if facts.min_order and not card.min_order:
        card.min_order = facts.min_order
        card.sources["мин. заказ"] = f"сайт {facts.url}"
    if facts.delivery and not card.delivery:
        card.delivery = facts.delivery
        card.sources["доставка"] = f"сайт {facts.url}"
    for cert in facts.certificates:
        if cert not in card.certificates:
            card.certificates.append(cert)
    if card.emails or card.phones:
        card.sources.setdefault("контакты", f"сайт {facts.url}")
    # числовая вилка цен ПО ПРЕДМЕТУ ЗАПРОСА (LLM извлекает только релевантные)
    values = [v for p in facts.prices if (v := normalize_price(p))]
    if values and not card.price_min:
        nums = [v for v, _ in values]
        card.price_min = min(nums)
        card.price_max = max(nums) if len(nums) > 1 else None
        card.price_note = "; ".join(f"{v:g} {u}" for v, u in values[:3])
        card.sources["цена"] = f"сайт {facts.url} (по предмету запроса)"
