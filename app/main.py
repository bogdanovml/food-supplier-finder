"""FastAPI-приложение: SSE-поиск, REST для карточек/заметок/RFQ/экспорта, static UI.

Прозрачность процесса: /api/search — Server-Sent Events поток событий пайплайна
(этапы/карточки/итог). Фронт рисует чек-лист этапов и живую таблицу."""
from __future__ import annotations

import asyncio
import io
import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse, Response
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from fastapi.staticfiles import StaticFiles

from . import db, export, regions
from .config import get_settings
from .llm import ask, LLMError
from .pipeline import run_search
from . import rfq as rfq_mod

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("app")

# Фейл-фаст: провайдеры/ключи — только в .env; без них сервис не стартует
_problems = get_settings().validate_secrets()
if _problems:
    raise RuntimeError("Отсутствуют обязательные переменные .env: " + "; ".join(_problems))

app = FastAPI(title="Food Supplier Finder", version="0.1.0")
STATIC = Path(__file__).parent / "static"


# ------------------------------ модели ------------------------------

class SearchQuery(BaseModel):
    query: str = Field(min_length=2, max_length=200)
    region: str = "Москва"


class NoteIn(BaseModel):
    text: str = Field(min_length=1, max_length=2000)


class RFQIn(BaseModel):
    search_id: int
    supplier_ids: list[int]
    extra_requirements: str = ""


# ------------------------------ SSE поиск ------------------------------

@app.post("/api/search")
async def api_search(body: SearchQuery, request: Request):
    """SSE-стрим событий пайплайна. Внутри — 8 этапов + карточки по мере готовности."""
    async def gen():
        queue: asyncio.Queue[dict | None] = asyncio.Queue()
        loop = asyncio.get_running_loop()

        async def emit(ev: dict) -> None:
            await queue.put(ev)

        task = loop.create_task(run_search(body.query, body.region, emit))

        try:
            while True:
                if await request.is_disconnected():
                    task.cancel()
                    break
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": "{}"}   # keepalive
                    continue
                if ev is None:
                    break
                yield {"event": ev["type"], "data": json.dumps(ev, ensure_ascii=False)}
                if ev["type"] == "done":
                    await task
                    break
        finally:
            if not task.done():
                task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    return EventSourceResponse(gen(), ping=15)


# ------------------------------ REST ------------------------------

@app.get("/api/regions")
async def api_regions():
    return regions.region_options()


@app.get("/api/searches")
async def api_searches():
    return db.list_searches()


@app.get("/api/searches/{search_id}/suppliers")
async def api_suppliers(search_id: int):
    return db.get_suppliers(search_id)


@app.get("/api/suppliers/{supplier_id}")
async def api_supplier(supplier_id: int):
    s = db.get_supplier(supplier_id)
    if not s:
        raise HTTPException(404, "поставщик не найден")
    s["notes"] = db.get_notes(supplier_id)
    return s


@app.post("/api/suppliers/{supplier_id}/notes")
async def api_add_note(supplier_id: int, body: NoteIn):
    if not db.get_supplier(supplier_id):
        raise HTTPException(404, "поставщик не найден")
    note_id = db.add_note(supplier_id, body.text)
    return {"id": note_id, "notes": db.get_notes(supplier_id)}


# ------------------------------ RFQ ------------------------------

@app.post("/api/rfq/preview")
async def api_rfq_preview(body: RFQIn):
    """Генерация RFQ-письма (LLM) без отправки — пользователь видит текст."""
    search = db.get_search(body.search_id)
    if not search:
        raise HTTPException(404, "поиск не найден")
    suppliers = [db.get_supplier(sid) for sid in body.supplier_ids]
    suppliers = [s for s in suppliers if s]
    if not suppliers:
        raise HTTPException(400, "нет поставщиков")
    try:
        subject, text = await rfq_mod.draft_letter(search["query"], search["region"],
                                                    suppliers, body.extra_requirements)
    except LLMError as exc:
        raise HTTPException(502, f"LLM недоступен: {exc}")
    return {"subject": subject, "body": text,
            "recipients": [e for s in suppliers for e in s["emails"]]}


@app.post("/api/rfq/send")
async def api_rfq_send(body: RFQIn):
    """Генерация + отправка RFQ всем поставщикам с email. SMTP-конфиг в .env."""
    s = get_settings()
    if not s.smtp_configured:
        raise HTTPException(400, "SMTP не настроен: заполните SMTP_* в .env")
    search = db.get_search(body.search_id)
    if not search:
        raise HTTPException(404, "поиск не найден")
    suppliers = [db.get_supplier(sid) for sid in body.supplier_ids]
    suppliers = [x for x in suppliers if x]
    try:
        result = await rfq_mod.send_letter(search, suppliers, body.extra_requirements)
    except LLMError as exc:
        raise HTTPException(502, f"LLM недоступен: {exc}")
    return result


# ------------------------------ экспорт ------------------------------

@app.get("/api/searches/{search_id}/export/xlsx")
async def api_export_xlsx(search_id: int):
    search = db.get_search(search_id)
    if not search:
        raise HTTPException(404, "поиск не найден")
    buf = io.BytesIO()
    n = export.to_xlsx(search, db.get_suppliers(search_id), buf)
    if not n:
        raise HTTPException(404, "нет поставщиков для экспорта")
    buf.seek(0)
    fname = f"suppliers-{search_id}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/searches/{search_id}/export/csv")
async def api_export_csv(search_id: int):
    """CSV открывается напрямую в Google Sheets (Файл → Импорт)."""
    search = db.get_search(search_id)
    if not search:
        raise HTTPException(404, "поиск не найден")
    csv_text = export.to_csv(search, db.get_suppliers(search_id))
    return Response(
        content="\ufeff" + csv_text,  # BOM: Excel/Sheets понимают кириллицу
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="suppliers-{search_id}.csv"'},
    )


# ------------------------------ static UI ------------------------------

app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=get_settings().app_port)
