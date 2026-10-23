"""Merge-слой: слияние карточек поставщиков из 3 источников + provenance.

Ключи слияния (по убыванию надёжности):
  1. ИНН (госконтракт ↔ optlist) — точное совпадение;
  2. домен (SERP ↔ optlist site) — tldextract;
  3. fuzzy-имя (rapidfuzz partial_ratio ≥ 93) — «ООО «РисОптТорг»» ≈ «РисОптТорг».

Provenance: каждый атрибут хранит источник («цена ← госконтракт №…»),
в UI это показывает пользователь, откуда что взято — прозрачность."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from rapidfuzz import fuzz
import tldextract

from .extraction import normalize_price
from .sources.gosplan import ContractHit
from .sources.optlist import OptlistHit
from .sources.xmlstock import SerpHit

_tld = tldextract.TLDExtract(suffix_list_urls=())  # встроенный снапшот PSL — офлайн


def domain_key(url: str) -> str:
    """Регистрированный домен из URL: 'https://www.opt23.ru/cat?x' -> 'opt23.ru'."""
    if not url:
        return ""
    if "://" not in url:
        url = "http://" + url
    try:
        ext = _tld(url)
        return f"{ext.domain}.{ext.suffix}".lower() if ext.domain else ""
    except Exception:
        return ""


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
    relevance: int = 0
    reasons: list[str] = field(default_factory=list)
    sources: dict[str, str] = field(default_factory=dict)   # field -> "источник"
    enrichment_status: str = "pending"
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
            "relevance": self.relevance, "reasons": self.reasons,
            "sources": self.sources, "enrichment_status": self.enrichment_status,
        }


def merge_all(serp: list[SerpHit], optlist: list[OptlistHit], gosplan: list[ContractHit],
              region_name: str) -> list[SupplierCard]:
    """Слияние трёх источников в список карточек."""
    cards: list[SupplierCard] = []

    # --- 1. госконтракты: самый сильный источник (реальные сделки) ---
    gos_by_inn: dict[str, SupplierCard] = {}
    for ch in gosplan:
        card = SupplierCard(
            name=ch.supplier_name, inn=ch.inn,
            region=region_name, city=ch.address.split(",")[0].strip() if ch.address else "",
            description=ch.address or "",
            gov_contracts_count=1, gov_contracts_value=ch.price or 0,
            price_note=f"контракт: {(ch.subject or '')[:80]}",
            sources={"имя и ИНН": f"госконтракт 44-ФЗ №{ch.reg_num}",
                     "цена": f"исполненный контракт №{ch.reg_num}: {(ch.price or 0):,.0f} ₽"},
        )
        gos_by_inn[ch.inn] = card
        cards.append(card)

    # --- 2. optlist: отраслевой каталог ---
    for oh in optlist:
        # (а) merge по ИНН с госконтрактом
        target = gos_by_inn.get(oh.inn) if oh.inn else None
        # (б) merge по fuzzy-имени с госконтрактом (только если нет ИНН-матча)
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
        if not d or d in have_domains:
            continue
        card = SupplierCard(
            name=hit.title.split("—")[0].split("—")[0].strip() or d,
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
    if not card.site:
        card.site = f"https://optlist.ru/company/{oh.slug}"
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
    """Детерминированный рейтинг (объяснимый, без магии).

    госконтракты — самый сильный сигнал; каталог — средний; SERP-позиция —
    мягкий буст. Контакты/документы добавляют доверия."""
    pos_by_domain: dict[str, int] = {}
    for h in serp:
        d = domain_key(h.url)
        if d and (d not in pos_by_domain or h.position < pos_by_domain[d]):
            pos_by_domain[d] = h.position
    for c in cards:
        score = 0
        if c.gov_contracts_count:
            score += 40 + min(c.gov_contracts_count * 5, 20)
            score += min(int(c.gov_contracts_value / 1_000_000), 10)  # масштаб сделок
        if "optlist.ru" in str(c.sources.values()):
            score += 15
        if c.domain in pos_by_domain:
            score += max(0, (20 - pos_by_domain[c.domain])) // 2
        if c.emails:
            score += 8
        if c.phones:
            score += 5
        if c.docs_verified:
            score += 7
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
    card.sources.setdefault("контакты", f"сайт {facts.url}")
    # числовая вилка цен из строковых цен (price-parser)
    values = [v for p in facts.prices if (v := normalize_price(p))]
    if values and not card.price_min:
        nums = [v for v, _ in values]
        card.price_min = min(nums)
        card.price_max = max(nums) if len(nums) > 1 else None
        card.price_note = "; ".join(f"{v:g} {u}" for v, u in values[:3])
        card.sources["цена"] = f"сайт {facts.url}"
    if card.emails and card.phones:
        card.relevance = min(100, card.relevance + 5)
