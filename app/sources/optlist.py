"""Источник: optlist.ru — каталог оптовых поставщиков.

Особенности (проверено живыми запросами):
  * сайт за Anubis (JS proof-of-work) -> обычный httpx бесполезен,
    работаем через Playwright (headless chromium, poll до прохождения PoW);
  * поиск: /wholesales?q=<текст> (поставщики) и /products?q=<текст> (товары) —
    оба сервер-сайд рендерят карточки компаний со ссылками /company/<slug>;
  * карточка /company/<slug>: schema.org microdata (name, legalName, email,
    url, адрес) + текстовые блоки «Минимальный заказ», «Способы доставки»,
    «Документы проверены», ИНН/ОГРН. Телефон замаскирован — берём префикс
    (городской код) + email + официальный сайт.

Graceful degradation: любая проблема (PoW не прошёл, таймаут, нет Playwright)
-> этап помечается пропущенным, остальной пайплайн работает."""
from __future__ import annotations

import asyncio
import html as html_mod
import logging
import re
from dataclasses import dataclass, field

from ..config import get_settings

logger = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36")

POW_POLL_INTERVAL = 4.0
POW_MAX_POLLS = 15          # ~60 сек на прохождение proof-of-work
PAGE_MIN_LEN = 40000        # карточные страницы ~100-200КБ; пустые < 30КБ


@dataclass(slots=True)
class OptlistHit:
    slug: str
    name: str
    city: str = ""
    description: str = ""
    tags: str = ""
    # детали (заполняются отдельно через company_details)
    inn: str | None = None
    email: str | None = None
    site: str | None = None
    address: str | None = None
    min_order: str | None = None
    delivery: str | None = None
    docs_verified: bool = False
    phone_masked: str | None = None

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


class OptlistClient:
    """Playwright-клиент с переиспользуемым браузером (Anubis-cookie живёт в контексте)."""

    def __init__(self) -> None:
        self._browser = None
        self._context = None
        self._lock = asyncio.Lock()      # старт браузера (однократно)
        self._broken = False

    async def _ensure_browser(self):
        if self._browser is not None:
            return
        async with self._lock:
            if self._browser is not None:   # double-check после ожидания лока
                return
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().start()
            self._browser = await self._pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            self._context = await self._browser.new_context(user_agent=UA, locale="ru-RU")

    async def _get(self, url: str, *, min_len: int = PAGE_MIN_LEN,
                   must_contain: list[str] | None = None,
                   max_polls: int = POW_MAX_POLLS) -> str:
        """GET с ожиданием прохождения Anubis PoW. Возвращает HTML или ''.

        must_contain: если задано — ждём, пока в HTML не появятся все эти
        подстроки (Angular-страницы дописывают контент после «load»)."""
        await self._ensure_browser()
        page = await self._context.new_page()
        try:
            try:
                await page.goto(url, wait_until="commit", timeout=30000)
            except Exception:
                pass  # PoW-редиректы ломают load-события — это нормально
            for _ in range(max_polls):
                await asyncio.sleep(POW_POLL_INTERVAL)
                try:
                    html = await page.content()
                except Exception:
                    continue
                low = html.lower()
                if "anubis" in low or "went wrong" in low:
                    continue
                if len(html) < min_len:
                    continue
                if must_contain and not all(k in html for k in must_contain):
                    continue
                return html
            return ""
        finally:
            await page.close()

    # ------------------------------ поиск ------------------------------

    async def search(self, query: str) -> list[OptlistHit]:
        """Оба поиска: по поставщикам и по товарам. Ошибки -> [] (не роняем пайплайн)."""
        if self._broken:
            return []
        try:
            hits: dict[str, OptlistHit] = {}
            for path in (f"/wholesales?q={query}", f"/products?q={query}"):
                html = await self._get(f"https://optlist.ru{path}", min_len=30000)
                for hit in self._parse_search(html):
                    if hit.slug not in hits:
                        hits[hit.slug] = hit
            return list(hits.values())
        except Exception as exc:
            logger.warning("optlist search failed: %s", exc)
            return []

    def _parse_search(self, html: str) -> list[OptlistHit]:
        """Карточки в блоках itemprop="itemListElement" (schema.org Organization)."""
        blocks = re.split(r'itemprop="itemListElement"', html)
        out: list[OptlistHit] = []
        for block in blocks[1:]:
            m = re.search(r'href="(/company/[a-z0-9-]+)"', block)
            if not m:
                continue
            slug = m.group(1).rsplit("/", 1)[-1]
            text = re.sub(r"<[^>]+>", "|", block)
            text = re.sub(r"[|\s]+", " ", text)
            name = ""
            nm = re.search(r'itemprop="name"[^>]*>([^<]+)', block)
            if nm:
                name = re.sub(r"\s+", " ", nm.group(1)).strip()
            else:
                # имя — первый осмысленный текст после ссылки
                seg = text.split("Подробнее")[0]
                parts = [p.strip() for p in seg.split("|") if len(p.strip()) > 2]
                name = parts[0] if parts else slug
            city = ""
            cm = re.search(r"([А-ЯЁ][а-яёA-Za-z\- ]+,\s*Россия)", text)
            if cm:
                city = cm.group(1).split(",")[0].strip()
            desc = ""
            dm = re.search(r"(Специализация|ООО|Компания|Производство|Оптовая|Постав)[^.]{0,400}\.", text)
            if dm:
                desc = dm.group(0).strip()[:300]
            out.append(OptlistHit(slug=slug, name=name, city=city, description=desc))
        return out

    # ------------------------------ детали ------------------------------

    async def company_details(self, hit: OptlistHit) -> OptlistHit:
        """Дополнение карточки: ИНН, email, сайт, МОК, доставка, документы."""
        if self._broken:
            return hit
        try:
            html = await self._get(f"https://optlist.ru/company/{hit.slug}", min_len=25000,
                                   must_contain=["Телефон", "Адрес"], max_polls=6)
        except Exception as exc:
            logger.debug("optlist details %s failed: %s", hit.slug, exc)
            return hit
        if not html:
            return hit
        text = html_mod.unescape(re.sub(r"<[^>]+>", "|", html))
        text = re.sub(r"[|\s]+", " ", text)

        def prop(name: str) -> str | None:
            m = re.search(rf'itemprop="{name}"[^>]*>([^<]+)', html)
            return m.group(1).strip() if m else None

        if prop("name") and not hit.name:
            hit.name = prop("name")
        m = re.search(r"ИНН\s*([\dXx-]{8,})", text)
        hit.inn = m.group(1).replace(" ", "") if m else hit.inn
        # email: microdata, mailto или текстовое поле
        hit.email = prop("email") or hit.email
        if not hit.email:
            m = re.search(r'href="mailto:([^"\s]+)"', html)
            if not m:
                m = re.search(r"(?:E-?mail)\s*\|\s*([\w.+-]+@[\w.-]+\.[A-Za-z]{2,})", text)
            if m:
                hit.email = m.group(1)
        site = prop("url")
        hit.site = site if site and "." in site else hit.site
        if not hit.city:
            loc = prop("addressLocality")
            if loc:
                hit.city = loc.split(",")[0].strip()
        # МОК: «Минимальный заказ | 50 000 руб.» или «Минимальный заказ — 5000 руб.»
        m = (re.search(r"Минимальный заказ\s*\|\s*([^|]{2,40}?)(?:\s*\||$)", text)
             or re.search(r"Минимальный заказ\s*[—:-]\s*([\d\s.,]+\s*\S{0,8})", text))
        if m:
            hit.min_order = re.sub(r"\s+", " ", m.group(1)).strip(" .!")
        # доставка: список способов между «Способы доставки» и следующим разделом
        m = re.search(r"Способы доставки\s*\|\s*(.{3,140}?)\s*(?:\||$)", text)
        if m:
            ways = [w.strip() for w in m.group(1).split(",") if w.strip()]
            hit.delivery = " / ".join(ways) if ways else None
        hit.docs_verified = "Документы проверены" in text and "не проверены" not in text
        # телефон: маска вида «+7 913 970...» или «+7 (351)...»
        m = re.search(r"\+7\s*\(?[\d]{3}\)?\s*[\d]{0,7}\.\.\.", text)
        hit.phone_masked = m.group(0).strip() if m else None
        return hit

    async def close(self):
        try:
            if self._browser:
                await self._browser.close()
            if getattr(self, "_pw", None):
                await self._pw.stop()
        except Exception:
            pass
        self._browser = self._context = None


# Синглтон на процесс: браузер живёт между поисками, Anubis-cookie переиспользуется
_client: OptlistClient | None = None


def get_optlist() -> OptlistClient:
    global _client
    if _client is None:
        _client = OptlistClient()
    return _client


async def search_suppliers(query: str, *, with_details: int = 5) -> list[OptlistHit]:
    """Удобная обёртка для пайплайна: поиск + детали топ-N карточек."""
    client = get_optlist()
    hits = await client.search(query)
    if not hits:
        return []
    # детали топ-N параллельно (страницы независимы, браузер один)
    top = hits[:with_details]
    await asyncio.gather(*(client.company_details(h) for h in top))
    return hits
