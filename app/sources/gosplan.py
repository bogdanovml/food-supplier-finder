"""Источник: Госплан API — исполненные контракты 44-ФЗ (тестовый сервер без ключа).

Ключевая ценность для закупщика:
  * поставщики, которые РЕАЛЬНО поставляли искомый товар госзаказчикам;
  * ФАКТИЧЕСКИЕ ЦЕНЫ исполнения контрактов (не прайс с сайта, а деньги);
  * ИНН + юр. название + адрес поставщика из карточки контракта.

Логика поиска (двойная, по убыванию точности):
  1. classifier: по ОКПД2/КТРУ-кодам предмета закупки — коды предлагают
     LLM + локальный fuzzy-справочник, валидируются по справочнику
     (app/okpd.py). Категорийный поиск — главный: он находит контракты,
     где предмет сформулирован иначе, чем «чипсы» («картофельные снеки»).
  2. subject: полнотекстовый по фразе (точная -> упрощённая).

Агрегация: контракты группируются ПОСТАВЩИКОМ (ИНН): количество, сумма,
примеры предметов с ценами — карточка закупщика сразу видит
«ООО Ромашка: 3 контракта, 2.1 млн ₽, поставляла картофельные чипсы в школы».

Факты API (проверено по Swagger + живыми запросами):
  * GET /fz44/contracts?subject=<полнотекст>&classifier=<окпд2>&stage=E|EC
    &region=<KLADR>&limit= -> индекс: subject, price, region, suppliers=[ИНН]
  * GET /fz44/contracts/{reg_num} -> docs[].source.suppliersInfo.supplierInfo
     .legalEntityRF.EGRULInfo = {fullName, shortName, INN, KPP, address}

Graceful degradation: недоступность API не роняет поиск — этап пропускается."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import httpx

from ..config import get_settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ContractExample:
    reg_num: str | None
    price: float | None          # цена исполненного контракта, руб
    subject: str | None

    def to_dict(self) -> dict:
        return {"reg_num": self.reg_num, "price": self.price, "subject": self.subject}


@dataclass(slots=True)
class GovSupplier:
    """Агрегат по поставщику: все его контракты по теме запроса."""
    inn: str
    name: str                     # shortName из карточки контракта
    address: str | None = None
    contracts: list[ContractExample] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.contracts)

    @property
    def total_value(self) -> float:
        return sum(c.price or 0 for c in self.contracts)

    def to_dict(self) -> dict:
        return {"inn": self.inn, "name": self.name, "address": self.address,
                "count": self.count, "total_value": self.total_value,
                "contracts": [c.to_dict() for c in self.contracts]}


def _client() -> httpx.AsyncClient:
    # Госплан «думает» до 14с на сложных запросах (prod-опыт tender/nmck)
    return httpx.AsyncClient(timeout=httpx.Timeout(connect=10, read=45, write=30, pool=60))


async def _get(client: httpx.AsyncClient, path: str, params: dict | None = None,
               retries: int = 2) -> list | dict | None:
    base = get_settings().gosplan_base_url.rstrip("/")
    for attempt in range(retries + 1):
        try:
            resp = await client.get(f"{base}{path}", params=params)
            if resp.status_code == 429:  # у тестового сервера жёсткий rate limit
                await asyncio.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            logger.debug("gosplan %s failed (attempt %d): %s", path, attempt + 1, exc)
            if attempt < retries:
                await asyncio.sleep(1.5 * (attempt + 1))
    return None


def _index_to_raw(data, stage: str) -> list[dict]:
    out = []
    for it in data if isinstance(data, list) else []:
        try:
            out.append({
                "inn": (it.get("suppliers") or [""])[0],
                "customer": it.get("customer"),
                "reg_num": it.get("reg_num"),
                "price": float(it.get("price") or 0) or None,
                "subject": it.get("subject"),
                "stage": stage,
            })
        except (TypeError, ValueError):
            continue
    return out


async def _fill_supplier_names(client: httpx.AsyncClient, raws: list[dict],
                               max_details: int) -> None:
    """Имена/адреса поставщиков из карточек контрактов (топ-N по цене)."""
    regs = []
    seen = set()
    for r in sorted(raws, key=lambda x: x["price"] or 0, reverse=True):
        if r["reg_num"] and r["reg_num"] not in seen:
            seen.add(r["reg_num"])
            regs.append(r["reg_num"])
        if len(regs) >= max_details:
            break
    details = await asyncio.gather(*[
        _get(client, f"/fz44/contracts/{rn}") for rn in regs
    ])
    by_reg = {rn: d for rn, d in zip(regs, details) if d}
    for r in raws:
        d = by_reg.get(r["reg_num"])
        if not d:
            continue
        for doc in d.get("docs") or []:
            src = (doc or {}).get("source") or {}
            info = (src.get("suppliersInfo") or {}).get("supplierInfo") or {}
            egr = ((info.get("legalEntityRF") or {}).get("EGRULInfo") or {})
            if egr.get("INN") == r["inn"] and egr.get("shortName"):
                r["supplier_name"] = egr["shortName"]
                r["address"] = egr.get("address")
                break


async def search_gov_suppliers(query: str, *, region_code: int | None,
                               classifiers: list[str] | None = None,
                               limit: int = 20, max_details: int = 10) -> list[GovSupplier]:
    """Поставщики с агрегированными контрактами по теме запроса.

    Поток: classifier-поиск (ОКПД2) -> subject-поиск -> объединение ->
    группировка по ИНН -> имена из карточек -> только распознанные.
    """
    raws: list[dict] = []
    async with _client() as client:
        # --- 1. классификаторный поиск (главный) ---
        if classifiers:
            for code in classifiers[:3]:
                params: dict = {"classifier": code, "limit": limit,
                                "sort": "price_desc", "stage": "EC"}
                if region_code:
                    params["region"] = region_code
                data = await _get(client, "/fz44/contracts", params)
                raws += _index_to_raw(data, "EC")
                if len(raws) < 5:  # дополнительно исполняющиеся
                    params["stage"] = "E"
                    data = await _get(client, "/fz44/contracts", params)
                    raws += _index_to_raw(data, "E")

        # --- 2. полнотекстовый поиск (уточнение) ---
        simplified = " ".join(query.split()[:2]) or query
        for subject_q in (f'"{query}"', simplified):
            for stage in ("EC", "E"):
                params = {"subject": subject_q, "stage": stage, "limit": limit,
                          "sort": "price_desc"}
                if region_code:
                    params["region"] = region_code
                data = await _get(client, "/fz44/contracts", params)
                raws += _index_to_raw(data, stage)
            if len(raws) > 5:
                break

        # дедуп по reg_num
        seen_regs: set[str] = set()
        uniq: list[dict] = []
        for r in raws:
            key = r["reg_num"] or (r["subject"] or "") + str(r["price"])
            if key not in seen_regs:
                seen_regs.add(key)
                uniq.append(r)
        raws = uniq

        # --- 3. имена поставщиков ---
        await _fill_supplier_names(client, raws, max_details)

    # --- 4. агрегация по поставщику ---
    suppliers: dict[str, GovSupplier] = {}
    for r in raws:
        if not r.get("supplier_name") or not r["inn"]:
            continue  # без имени карточка бесполезна
        g = suppliers.get(r["inn"])
        if g is None:
            g = GovSupplier(inn=r["inn"], name=r["supplier_name"], address=r.get("address"))
            suppliers[r["inn"]] = g
        g.contracts.append(ContractExample(
            reg_num=r["reg_num"], price=r["price"], subject=r["subject"]))
    result = list(suppliers.values())
    result.sort(key=lambda g: g.total_value, reverse=True)
    return result


async def contracts_by_inn(inn: str, *, limit: int = 5) -> dict:
    """Краткая стата контрактов поставщика по ИНН: 44-ФЗ + 223-ФЗ.

    223-ФЗ: в индексе Госплана поле suppliers пустое (проверено живыми
    запросами — атрибуция не отдаётся), но параметр supplier=<ИНН>
    фильтрует корректно. Поэтому 223-опыт проверяем по ИНН на стадии
    юрлица, а в предметном поиске — только 44-ФЗ.
    """
    total_count = 0
    total_value = 0.0
    example = None
    async with _client() as client:
        for source, params in (("fz44", {"supplier": inn, "stage": "EC",
                                          "limit": limit, "sort": "price_desc"}),
                                ("fz223", {"supplier": inn, "limit": limit,
                                           "sort": "price_desc"})):
            data = await _get(client, f"/{source}/contracts", params)
            if data is None:
                continue
            items = data if isinstance(data, list) else []
            total_count += len(items)
            total_value += sum(float(i.get("price") or 0) for i in items)
            if not example and items:
                i = items[0]
                example = {"reg_num": i.get("reg_num"),
                           "price": float(i.get("price") or 0),
                           "subject": i.get("subject"),
                           "source": source}
    return {"count": total_count, "value": total_value, "example": example}
