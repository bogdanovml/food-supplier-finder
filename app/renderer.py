"""Общий JS-рендерер (Playwright) для enrichment-обхода сайтов.

Зачем: заметная часть поставщиков — SPA/JS-оболочки (httpx получает 90KB HTML,
а видимого текста — 1KB пустышки). Для таких страниц после httpx-фаса делаем
fallback-рендер браузером: контент подгружается, цены/контакты появляются.

Переиспользуется один браузер на процесс (как и в optlist-клиенте),
поэтому накладные расходы — только ~3-5с на страницу."""
from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36")

# Порог «JS-оболочки»: меньше столько видимого текста — почти наверняка SPA
THIN_TEXT_THRESHOLD = 1500

_browser = None
_lock = asyncio.Lock()


async def _ensure_browser():
    global _browser
    if _browser is not None:
        return
    async with _lock:
        if _browser is not None:
            return
        from playwright.async_api import async_playwright
        _pw = await async_playwright().start()
        _browser = await _pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )


async def render_page(url: str, *, wait_ms: int = 4000, max_bytes: int = 3_000_000) -> tuple[str, str]:
    """Отрендерить страницу браузером. Возвращает (html, final_url).

    final_url нужен, когда сайт редиректит на профиль организации
    (Яндекс Карты: /?text=... -> /maps/org/<slug>/<id>/)."""
    try:
        await _ensure_browser()
        page = await _browser.new_page(user_agent=UA, locale="ru-RU")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=25000)
            await page.wait_for_timeout(wait_ms)
            html = await page.content()
            if len(html) > max_bytes:
                return "", url
            return html, page.url
        finally:
            await page.close()
    except Exception as exc:
        logger.debug("render_page failed for %s: %s", url, exc)
        return "", url


_render_failures = 0


async def render_html(url: str, *, wait_ms: int = 4000, max_bytes: int = 3_000_000) -> str:
    """Отрендерить страницу браузером. Ошибки/таймауты -> '' (не роняем пайплайн).

    Жёсткий внешний кап: если браузер умер (OOM после десятков рендеров),
    Playwright-транспорт может висеть БЕСКОНЕЧНО — wait_for лечит зависание
    всего обхода. Три подряд провала -> перезапуск браузера."""
    global _browser, _render_failures
    import asyncio
    try:
        html, final_url = await asyncio.wait_for(
            render_page(url, wait_ms=wait_ms, max_bytes=max_bytes),
            timeout=45)
        _render_failures = 0
        return html
    except asyncio.TimeoutError:
        logger.warning("render_page висит >45с (%s) — браузер подозревается", url)
        _render_failures += 1
        if _render_failures >= 3:
            logger.warning("3 зависания подряд: перезапуск браузера")
            try:
                if _browser:
                    await _browser.close()
            except Exception:
                pass
            _browser = None
        return ""
    except Exception as exc:
        logger.debug("render failed %s: %s", url, exc)
        return ""


async def close():
    global _browser
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    _browser = None
