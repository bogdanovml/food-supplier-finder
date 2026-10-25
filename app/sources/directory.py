"""Источник: каталог юридических лиц по ОКВЭД (checko.ru/company/select).

Зачем: это «огромный список поставщиков», который пользователь просил —
по ОКВЭД-коду отрасли (например, 10.61 — мукомольное производство) выдаётся
реестр ДЕЙСТВУЮЩИХ юрлиц с адресом, директором, датой регистрации и
финансовыми показателями (выручка, прибыль, капитал).

Форматы (проверено живыми запросами):
  * URL: checko.ru/company/select?code=<ОКВЭД без точки>00  (10.61 -> 106100)
  * пагинация: &page=2 (50 компаний на страницу)
  * страница — SPA (JS-рендер, поэтому Playwright из renderer.py)
  * строка: название, адрес (регион -> локальная фильтрация), директор,
    дата регистрации, выручка/прибыль/капитал
  * slug ссылки содержит ОГРН: /company/<name>-<ОГРН 13 цифр>

Регион: серверного фильтра нет — фильтруем локально по адресу строки.

Graceful degradation: недоступность каталога не роняет поиск."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from ..renderer import render_html

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DirectoryHit:
    ogrn: str
    name: str                     # ООО "РИСОПТТОРГ"
    address: str = ""
    city: str = ""
    director: str = ""
    reg_date: str = ""
    revenue: str = ""             # «1,6 млрд руб.»
    profit: str = ""
    okved_code: str = ""

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def okved_to_url_code(okved: str) -> str:
    """'10.61' -> '106100', '10.61.2' -> '106102' (группа -> двухзначный хвост)."""
    parts = [p for p in okved.split(".") if p]
    if not parts:
        return ""
    code = "".join(parts)
    while len(code) < 6:
        code += "0"
    return code[:6]


# «453120, Республика Башкортостан, г. Стерлитамак, ул. Глинки, д. 1» -> город
RE_CITY = re.compile(r"г\.?\s+([А-ЯЁ][а-яё\-]+)")
# строка каталога: ссылка + блок данных
RE_ROW = re.compile(
    r'href="(/company/[a-z0-9-]+-(\d{13}))"[^>]*>([^<]+)</a>\s*'
    r"<div>([^<]+)</div>"          # адрес
    r"(?:\s*<div>(.*?)</div>)?",   # директор / дата регистрации
    re.DOTALL,
)
RE_DIRECTOR = re.compile(r"(?:Генеральный директор|Директор|директор|Руководитель)(?:</span>)?\s*([А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+)")
RE_REGDATE = re.compile(r"Дата регистрации(?:</span>)?\s*([^&<|]{3,30}года)")
RE_REVENUE = re.compile(r"Выручка(?:</span>)?\s*([\d.,]+\s*(?:тыс|млн|млрд)[^&<\s]*\s*руб)")
RE_PROFIT = re.compile(r"Чистая прибыль(?:</span>)?\s*([\d.,]+\s*(?:тыс|млн|млрд)[^&<\s]*\s*руб)")


def parse_catalog(html: str, okved_code: str) -> list[DirectoryHit]:
    """Страница каталога -> список юрлиц. HTML уже отрендерен."""
    hits: list[DirectoryHit] = []
    for m in RE_ROW.finditer(html):
        href, ogrn, name, address, extra = m.groups()
        name = name.strip()
        address = re.sub(r"\s+", " ", address).strip()
        city = ""
        cm = RE_CITY.search(address)
        if cm:
            city = cm.group(1)
        hit = DirectoryHit(ogrn=ogrn, name=name, address=address, city=city,
                           okved_code=okved_code)
        blob = extra or ""
        dm = RE_DIRECTOR.search(blob) or RE_DIRECTOR.search(address + " " + blob)
        if dm:
            hit.director = dm.group(1)
        rm = RE_REGDATE.search(blob)
        if rm:
            hit.reg_date = rm.group(1).strip()
        rev = RE_REVENUE.search(blob)
        if rev:
            hit.revenue = rev.group(1)
        pr = RE_PROFIT.search(blob)
        if pr:
            hit.profit = pr.group(1)
        # выручка может быть дальше строки — ищем в следующем куске html
        if not hit.revenue:
            tail = html[m.end():m.end() + 1200]
            rev = RE_REVENUE.search(tail)
            if rev:
                hit.revenue = rev.group(1)
            pr = RE_PROFIT.search(tail)
            if pr:
                hit.profit = pr.group(1)
        hits.append(hit)
    return hits


def _region_matches(address: str, region_name: str) -> bool:
    """Локальная фильтрация: адрес содержит регион запроса."""
    if not region_name or region_name in ("Вся Россия",):
        return True
    low = address.lower()
    region = region_name.lower().strip()
    if " " in region_name and "область" in region_name or "край" in region_name or "республик" in region_name.lower():
        return region in low
    # город федерального значения: «москва» должен матчить «г. Москва»,
    # но НЕ «Московская область»
    return bool(re.search(rf"г\.?\s+{region}(?:[^а-яё]|$)", low))


CHECKO_BLOCKED = False  # капча на каталоге checko: страницу не спарсить


async def search_okved_directory(okved_codes: list[str], *, region_name: str,
                                  max_per_code: int = 100) -> list[DirectoryHit]:
    """Каталог юрлиц по ОКВЭД-кодам, отфильтрованный по региону.

    2 страницы по 50 строк на код; региональный фильтр режет объём сам.
    """
    out: dict[str, DirectoryHit] = {}
    for okved in okved_codes[:2]:
        url_code = okved_to_url_code(okved)
        if not url_code:
            continue
        for page in (1, 2):
            html = await render_html(
                f"https://checko.ru/company/select?code={url_code}"
                + (f"&page={page}" if page > 1 else ""),
                wait_ms=4500,
            )
            if not html:
                break
            if "подтвердите" in html[:3000].lower() or "smartcaptcha" in html.lower():
                global CHECKO_BLOCKED
                CHECKO_BLOCKED = True
                logger.warning("checko catalog: captcha — каталог недоступен")
                break
            hits = parse_catalog(html, okved)
            if not hits:
                break  # пустая страница = конец списка
            for h in hits:
                if h.ogrn in out:
                    continue
                if _region_matches(h.address, region_name):
                    out[h.ogrn] = h
            if len(out) >= max_per_code:
                break
    return list(out.values())
