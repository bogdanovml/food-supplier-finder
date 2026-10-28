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


class StatusIn(BaseModel):
    status: str = Field(min_length=2, max_length=40)


class RFQIn(BaseModel):
    search_id: int
    supplier_ids: list[int]
    extra_requirements: str = ""
    subject: str | None = None
    body: str | None = None
    mode: str = "test"  # "test" | "combat"
    manual_emails: list[str] = []


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


@app.get("/api/registry")
async def api_registry():
    """Глобальная база поставщиков: накапливается по всем поискам,
    у одного поставщика может быть много товаров."""
    return {"stats": db.registry_stats(), "suppliers": db.list_registry()}


@app.get("/api/registry/stats")
async def api_registry_stats():
    return db.registry_stats()


@app.get("/api/searches/{search_id}/excluded")
async def api_excluded(search_id: int):
    """Отсеянные карточки с причинами (вкладка прозрачности)."""
    return db.get_excluded(search_id)


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


@app.patch("/api/suppliers/{supplier_id}/status")
async def api_update_status(supplier_id: int, body: StatusIn):
    if not db.get_supplier(supplier_id):
        raise HTTPException(404, "поставщик не найден")
    ok = db.update_supplier_status(supplier_id, body.status)
    return {"ok": ok, "status": body.status}


# ------------------------------ RFQ ------------------------------

@app.post("/api/rfq/preview")
async def api_rfq_preview(body: RFQIn):
    """Генерация RFQ-письма (LLM) без отправки — пользователь видит текст."""
    search = db.get_search(body.search_id)
    if not search:
        raise HTTPException(404, "поиск не найден")
    suppliers = [db.get_supplier(sid) for sid in body.supplier_ids]
    suppliers = [s for s in suppliers if s]

    # Если текст письма уже есть и не является плейсхолдером — сохраняем правки пользователя
    if body.subject and body.body and not body.subject.startswith("Генерация"):
        subject, text = body.subject, body.body
    else:
        try:
            subject, text = await rfq_mod.draft_letter(search["query"], search["region"],
                                                        suppliers, body.extra_requirements)
        except LLMError as exc:
            raise HTTPException(502, f"LLM недоступен: {exc}")

    if body.mode == "test":
        recipients = [
            {"email": acc["email"], "name": acc["name"], "role": "Тестовый поставщик"}
            for acc in rfq_mod.get_test_suppliers()
        ]
    else:
        recipients = []
        seen = set()
        for s in suppliers:
            sup_name = s.get("legal_name") or s.get("name") or "Поставщик"
            for em in (s.get("emails") or []):
                val_e = rfq_mod._clean_email(em)
                if val_e and val_e not in seen:
                    seen.add(val_e)
                    recipients.append({"email": val_e, "name": sup_name, "role": "Поставщик из выдачи"})
        for em in body.manual_emails:
            val_e = rfq_mod._clean_email(em)
            if val_e and val_e not in seen:
                seen.add(val_e)
                recipients.append({"email": val_e, "name": "Ручной ввод", "role": "Вручную добавленный email"})

    now_day = db._now()[:10]
    daily_used = db.get_daily_rfq_count(now_day)
    s = get_settings()

    return {
        "subject": subject,
        "body": text,
        "mode": body.mode,
        "recipients": recipients,
        "daily_used": daily_used,
        "daily_limit": s.smtp_daily_limit,
    }


@app.post("/api/rfq/send")
async def api_rfq_send(body: RFQIn):
    """Генерация + отправка RFQ. Индивидуальные письма, лимит 50/сутки."""
    s = get_settings()
    if not s.smtp_configured:
        raise HTTPException(400, "SMTP не настроен: заполните SMTP_* в .env")
    search = db.get_search(body.search_id)
    if not search:
        raise HTTPException(404, "поиск не найден")
    suppliers = [db.get_supplier(sid) for sid in body.supplier_ids]
    suppliers = [x for x in suppliers if x]
    try:
        result = await rfq_mod.send_letter(
            search, suppliers, body.extra_requirements,
            custom_subject=body.subject, custom_body=body.body,
            mode=body.mode, manual_emails=body.manual_emails
        )
        for sup in suppliers:
            db.update_supplier_status(sup["id"], "rfq_sent")
        return result
    except ValueError as ve:
        raise HTTPException(429, str(ve))
    except LLMError as exc:
        raise HTTPException(502, f"LLM недоступен: {exc}")
    except Exception as exc:
        raise HTTPException(500, f"Ошибка отправки: {exc}")


@app.get("/api/rfq/inbox")
async def api_rfq_inbox():
    """Получение входящих ответов от поставщиков (синхронизация IMAP)."""
    messages = await asyncio.to_thread(rfq_mod.sync_inbox_from_imap)
    now_day = db._now()[:10]
    return {
        "messages": messages,
        "unread_count": db.get_inbox_unread_count(),
        "daily_used": db.get_daily_rfq_count(now_day),
        "daily_limit": get_settings().smtp_daily_limit,
    }


@app.post("/api/rfq/inbox/{msg_id}/read")
async def api_rfq_inbox_read(msg_id: str):
    """Пометить сообщение прочитанным."""
    db.mark_inbox_read(msg_id)
    return {"status": "ok"}


@app.get("/api/rfq/stats")
async def api_rfq_stats():
    """Текущая статистика рассылки и входящих ответов."""
    now_day = db._now()[:10]
    return {
        "unread_count": db.get_inbox_unread_count(),
        "daily_used": db.get_daily_rfq_count(now_day),
        "daily_limit": get_settings().smtp_daily_limit,
    }


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
