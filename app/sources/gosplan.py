"""Источник: Госплан API (v2test.gosplan.info — тестовый сервер без ключа).

Что дает пользователю:
  * поставщики, которые РЕАЛЬНО поставляли искомый товар госзаказчикам;
  * ФАКТИЧЕСКИЕ ЦЕНЫ исполнения контрактов (не прайс с сайта, а деньги);
  * ИНН + полное название + адрес поставщика из карточки контракта.

Факты API (проверено по Swagger + живыми запросами):
  * GET /fz44/contracts?subject=<полнотекст>&stage=E|EC&region=<KLADR>&limit=
    -> индекс: subject, price, region, suppliers=[ИНН], customer, reg_num
  * GET /fz44/contracts/{reg_num} -> docs[].source.suppliersInfo.supplierInfo
     .legalEntityRF.EGRULInfo = {fullName, shortName, INN, KPP, address}
  * Регионы: KLADR-коды (77=Москва, 78=СПб...), stage: E=исполнение,
    EC=исполнение завершено, ET=прекращено, IN=аннулировано.

Graceful degradation: недоступность API не роняет поиск — этап пропускается."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

from ..config import get_settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ContractHit:
    supplier_name: str            # shortName из карточки контракта
    inn: str
    kpp: str | None
    address: str | None
    customer_inn: str | None
    reg_num: str | None
    price: float | None            # цена исполненного контракта, руб
    subject: str | None
    region: int | None
    stage: str | None

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def _client() -> httpx.AsyncClient:
    # Госплан «думает» до 14с на сложных запросах (см. prod-опыт tender/nmck)
    return httpx.AsyncClient(timeout=httpx.Timeout(connect=10, read=45, write=30, pool=60))


async def _get(client: httpx.AsyncClient, path: str, params: dict | None = None,
               retries: int = 2) -> list | dict | None:
    """GET с бэкоффом. rate limit / сетевые ошибки -> None (вызывающий деградирует)."""
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


async def search_food_contracts(query: str, *, region_code: int | None,
                                limit: int = 15, max_details: int = 8) -> list[ContractHit]:
    """Исполненные контракты по предмету + детали с именами поставщиков.

    Ищем по stage=E (исполняется) и EC (завершено) — это «живые» сделки.
    Детали (имена поставщиков) тянем только по top-N — беречь rate limit.

    Стратегия запроса: точная фраза -> фолбэк на упрощённый (первые 2 слова):
    узкие формулировки («в/с», ГОСТ-номера) в предметах контрактов почти не
    встречаются дословно, а поставщик того же товара — тот же.
    """
    simplified = " ".join(query.split()[:2]) or query
    attempts = [f'"{query}"', simplified] if simplified != query else [query, f'"{query}"']

    hits: list[ContractHit] = []
    async with _client() as client:
        for subject_q in attempts:
            for stage in ("EC", "E"):
                params: dict = {"subject": subject_q, "stage": stage, "limit": limit,
                                "sort": "price_desc"}
                if region_code:
                    params["region"] = region_code
                data = await _get(client, "/fz44/contracts", params)
                if data is None:
                    continue
                for it in data if isinstance(data, list) else []:
                    try:
                        hits.append(ContractHit(
                            supplier_name="", inn=(it.get("suppliers") or [""])[0],
                            kpp=None, address=None,
                            customer_inn=it.get("customer"),
                            reg_num=it.get("reg_num"),
                            price=float(it.get("price") or 0) or None,
                            subject=it.get("subject"),
                            region=it.get("region"), stage=stage,
                        ))
                    except (TypeError, ValueError):
                        continue
                if len(hits) >= limit // 2:
                    break  # завершённых (EC) обычно достаточно
            if hits:
                break  # фолбэк не нужен, если точная фраза нашла контракты

        # детали топ-N контрактов: имена/адреса поставщиков
        details_tasks = [h.reg_num for h in hits[:max_details] if h.reg_num]
        detail_results = await asyncio.gather(*[
            _get(client, f"/fz44/contracts/{rn}") for rn in details_tasks
        ])
        by_reg = {rn: d for rn, d in zip(details_tasks, detail_results) if d}
        for h in hits:
            d = by_reg.get(h.reg_num)
            if not d:
                continue
            for doc in d.get("docs") or []:
                src = (doc or {}).get("source") or {}
                info = (src.get("suppliersInfo") or {}).get("supplierInfo") or {}
                egr = ((info.get("legalEntityRF") or {}).get("EGRULInfo") or {})
                if egr.get("INN") == h.inn and egr.get("shortName"):
                    h.supplier_name = egr["shortName"]
                    h.address = egr.get("address")
                    h.kpp = egr.get("KPP")
                    break

    # без имени поставщика карточка малополезна — оставляем только распознанные
    named = [h for h in hits if h.supplier_name and h.inn]
    # стабильный порядок: сначала завершённые (реальные деньги), потом исполняемые
    return named
