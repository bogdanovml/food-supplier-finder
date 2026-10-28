"""Источник: XMLstock — региональная выдача Яндекс XML и Google XML.

Оба эндпоинта используют одинаковую (яндексоидную) XML-схему `<yandexsearch>`,
поэтому один парсер на оба. Яндекс умеет `lr=` (регион), Google — нет.

Graceful degradation: любая ошибка -> пустой список + предупреждение,
пайплайн продолжает работу с другими источниками."""
from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import httpx

from ..config import get_settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SerpHit:
    query: str
    engine: str                 # 'yandex' | 'google'
    position: int
    url: str
    domain: str
    title: str
    snippet: str

    def to_dict(self) -> dict:
        return {"query": self.query, "engine": self.engine, "position": self.position,
                "url": self.url, "domain": self.domain, "title": self.title, "snippet": self.snippet}


def _domain_of(url: str) -> str:
    try:
        return (url.split("//", 1)[1].split("/", 1)[0]).lower()
    except (IndexError, ValueError):
        return ""


def parse_yandex_xml(xml_text: str, *, engine: str, query: str, top: int, start_pos: int = 1) -> list[SerpHit]:
    """Парсер единой xmlstock-схемы. Ошибки парсинга -> [] (не роняем пайплайн)."""
    hits: list[SerpHit] = []
    try:
        root = ET.fromstring(xml_text)
        pos = start_pos
        for group in root.iter("group"):
            doc = group.find("doc")
            if doc is None:
                continue
            url_el = doc.find("url")
            title_el = doc.find("title")
            passage_el = doc.find(".//passage")
            url = (url_el.text or "").strip() if url_el is not None else ""
            if not url:
                continue
            title = "".join(title_el.itertext()) if title_el is not None else ""
            snippet = "".join(passage_el.itertext()) if passage_el is not None else ""
            hits.append(SerpHit(
                query=query, engine=engine, position=pos,
                url=url, domain=_domain_of(url), title=title.strip(), snippet=snippet.strip(),
            ))
            pos += 1
            if len(hits) >= top:
                break
    except ET.ParseError as exc:
        logger.warning("SERP XML parse failed (%s): %s", engine, exc)
    return hits


async def _fetch_page(client: httpx.AsyncClient, *, engine: str, url: str, query: str,
                      lr: int, page: int) -> list[SerpHit]:
    s = get_settings()
    params = {"user": s.xmlstock_user, "key": s.xmlstock_key, "query": query, "sortby": "rlv"}
    if "yandex" in engine:
        params["lr"] = lr
        params["maxpassages"] = 2
    if page > 0:
        params["page"] = page
    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return parse_yandex_xml(resp.text, engine=engine, query=query, top=10, start_pos=page * 10 + 1)
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("xmlstock %s page %d failed for '%s': %s", engine, page, query, exc)
        return []


async def _fetch_one(client: httpx.AsyncClient, *, engine: str, url: str, query: str,
                     lr: int, top: int = 20) -> list[SerpHit]:
    """Сбор выдачи по запросу: 2 страницы (позиции 1-20) параллельно."""
    num_pages = max(1, (top + 9) // 10)
    pages = await asyncio.gather(*[
        _fetch_page(client, engine=engine, url=url, query=query, lr=lr, page=p)
        for p in range(num_pages)
    ])
    return [h for chunk in pages for h in chunk][:top]


async def fetch_serp(queries: list[str], *, lr: int, top: int | None = None,
                     progress=None, engines: list[str] | None = None) -> list[SerpHit]:
    """Параллельно собираем SERP по всем запросам из двух движков.

    Движки:
      * 'yandex'      — ночной XML-тариф (работает 00:00–06:00 MSK);
      * 'yandexlive'  — Яндекс Live, круглосуточно (основной по умолчанию);
      * 'google'      — Google XML.
    progress: async-callback(engine, query, found) для live-обновления UI.
    """
    s = get_settings()
    top = top or s.serp_top
    sem = asyncio.Semaphore(6)  # параллельные запросы к XML-шлюзу
    engines = engines or ["yandexlive", "google"]

    endpoints = {
        "yandex": ("yandex", s.xmlstock_yandex_url),
        "yandexlive": ("yandex", s.xmlstock_yandex_live_url),
        "google": ("google", s.xmlstock_google_url),
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(25, connect=10)) as client:
        async def _task(engine: str, endpoint: str, query: str):
            async with sem:
                hits = await _fetch_one(client, engine=engine, url=endpoint, query=query, lr=lr, top=top)
                if progress:
                    await progress(engine, query, len(hits))
                return hits

        tasks = [
            _task(engine, endpoints[engine][1], q)
            for engine in engines
            for q in queries
        ]
        results = await asyncio.gather(*tasks)

    return [h for chunk in results for h in chunk]
