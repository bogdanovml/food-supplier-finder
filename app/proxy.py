"""Менеджер прокси для работы со строгими источниками (checko.ru).

Список прокси хранится в локальном файле `proxies.txt` (добавлен в .gitignore
и не попадает в git).
Формат строк: ip:port:user:pass или http://user:pass@ip:port.
Протокол: HTTP (HTTPS CONNECT) с поддержкой HTTP/2 (обходит 429/SmartCaptcha).
"""

from __future__ import annotations

import itertools
import logging
import time
from pathlib import Path

import httpx

from .config import BASE_DIR, get_settings

logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

_proxy_pool: list[str] = []
_proxy_cycle: itertools.cycle | None = None
_proxy_cooldowns: dict[str, float] = {}  # proxy_url -> timestamp when available again
_initialized = False


def _parse_proxy_line(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "://" in line:
        return line
    parts = line.split(":")
    if len(parts) == 4:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    if len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    return None


def load_proxies() -> list[str]:
    """Загружает список прокси из файла proxies.txt или .env."""
    global _proxy_pool, _proxy_cycle, _initialized
    s = get_settings()
    p_path = Path(getattr(s, "proxies_file", "proxies.txt"))
    if not p_path.is_absolute():
        p_path = BASE_DIR / p_path

    pool: list[str] = []
    if p_path.exists():
        try:
            for l in p_path.read_text(encoding="utf-8").splitlines():
                parsed = _parse_proxy_line(l)
                if parsed and parsed not in pool:
                    pool.append(parsed)
        except Exception as exc:
            logger.warning("Failed to load %s: %s", p_path, exc)

    _proxy_pool = pool
    _proxy_cycle = itertools.cycle(pool) if pool else None
    _initialized = True
    if pool:
        logger.info("Loaded %d proxies for external services", len(pool))
    return pool


def get_proxy() -> str | None:
    """Возвращает следующий рабочий прокси с ротацией (round-robin)."""
    global _proxy_cycle
    if not _initialized:
        load_proxies()
    if not _proxy_pool:
        return None

    now = time.time()
    # Ищем прокси, который не на кулдауне
    for _ in range(len(_proxy_pool)):
        p = next(_proxy_cycle)
        cd = _proxy_cooldowns.get(p, 0)
        if now >= cd:
            return p

    # Если все на кулдауне — берем тот, у которого кулдаун истекает раньше всех
    return min(_proxy_pool, key=lambda p: _proxy_cooldowns.get(p, 0))


def report_proxy_failure(proxy_url: str, cooldown_sec: int = 120) -> None:
    """Пометить прокси как временно заблокированный (429/капча/таймаут)."""
    if proxy_url:
        _proxy_cooldowns[proxy_url] = time.time() + cooldown_sec
        logger.warning("Proxy %s on cooldown for %ds", proxy_url.split("@")[-1], cooldown_sec)


def report_proxy_success(proxy_url: str) -> None:
    """Снять кулдаун при успешном ответе."""
    if proxy_url and proxy_url in _proxy_cooldowns:
        _proxy_cooldowns.pop(proxy_url, None)


async def fetch_checko(url: str, *, timeout: float = 12.0) -> tuple[int, str, str]:
    """Выполняет запрос к checko.ru через ротируемые прокси с HTTP/2.

    Возвращает (status_code, html_text, final_url).
    Автоматически перебирает прокси при ошибках или 429.
    """
    if not _initialized:
        load_proxies()

    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    max_attempts = min(3, len(_proxy_pool)) if _proxy_pool else 1
    for attempt in range(max_attempts):
        proxy = get_proxy()
        try:
            async with httpx.AsyncClient(
                proxy=proxy,
                http2=True,
                timeout=timeout,
                follow_redirects=True,
                headers=headers,
            ) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    report_proxy_success(proxy)
                    return resp.status_code, resp.text, str(resp.url)
                elif resp.status_code == 429:
                    report_proxy_failure(proxy, cooldown_sec=180)
                    continue
                else:
                    return resp.status_code, resp.text, str(resp.url)
        except Exception as exc:
            logger.debug("checko fetch failed via %s: %s", proxy, exc)
            report_proxy_failure(proxy, cooldown_sec=60)

    # Фолбэк без прокси (напрямую)
    try:
        async with httpx.AsyncClient(
            http2=True,
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
        ) as client:
            resp = await client.get(url)
            return resp.status_code, resp.text, str(resp.url)
    except Exception as exc:
        logger.debug("checko direct fetch failed: %s", exc)
        return 0, "", url
