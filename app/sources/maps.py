"""Яндекс Карты: профиль организации, рейтинг и отзывы (Playwright-рендер).

Зачем: рейтинг на Картах — народная оценка поставщика. Платный Search API
не подключаем; страницы Карт корректно рендерятся браузером и редиректят
сразу на организацию: /?text=<имя> <город> -> /maps/org/<slug>/<id>/.

Валидация: совпадение имени — редирект должен вести на org-страницу,
иначе это просто поисковая выдача (несколько тёзок) — возвращаем None.
Graceful degradation: капча/таймаут -> None, поиск не падает."""
from __future__ import annotations

import logging
import re
from urllib.parse import quote

from ..renderer import render_page

logger = logging.getLogger(__name__)

RE_ORG_URL = re.compile(r"/maps/org/([^/]+)/(\d+)")
RE_RATING = re.compile(r"rating[^>]*>(\d[.,]\d)<")
RE_REVIEWS = re.compile(r'"reviewCount"\s*:\s*(\d+)')

_sem = __import__("asyncio").Semaphore(3)


async def org_profile(name: str, city: str) -> dict | None:
    """Профиль организации на Яндекс Картах: url, рейтинг, отзывы.

    Рендерим поиск «<имя> <город>»; если Яндекс редиректит на /maps/org/...,
    организация найдена однозначно — берём рейтинг и число отзывов.
    """
    query = quote(f"{name} {city}")
    html, final_url = await render_page(
        f"https://yandex.ru/maps/?text={query}", wait_ms=4500)
    if not html:
        return None
    m = RE_ORG_URL.search(final_url)
    if not m:
        return None  # не редиректнуло на конкретную организацию — тёзки/нет данных
    out = {"url": f"https://yandex.ru/maps/org/{m.group(1)}/{m.group(2)}/"}
    rm = RE_RATING.search(html)
    if rm:
        try:
            out["rating"] = float(rm.group(1).replace(",", "."))
        except ValueError:
            pass
    vm = RE_REVIEWS.search(html)
    if vm:
        out["reviews"] = int(vm.group(1))
    if "rating" not in out and "reviews" not in out:
        return None
    return out
