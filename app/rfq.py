"""RFQ (Request For Quotation): генерация письма-запроса КП и рассылка поставщикам.

Поток: LLM пишет грамотное письмо (умная модель) → пользователь подтверждает
(превью на фронте) → отправка через SMTP всем поставщикам с email (aiosmtplib).
Отправка только с явно настроенного пользователем SMTP-аккаунта (.env)."""
from __future__ import annotations

import asyncio
import logging
from email.message import EmailMessage

import aiosmtplib
from email_validator import validate_email, EmailNotValidError

from . import db
from .config import get_settings
from .llm import ask, LLMError

logger = logging.getLogger(__name__)


async def draft_letter(query: str, region: str, suppliers: list[dict],
                       extra: str = "") -> tuple[str, str]:
    """(subject, body) без отправки."""
    prompt = f"""Составь деловое письмо-запрос коммерческого предложения (RFQ) на русском.

Контекст:
- Ищем поставщика: "{query}", регион: {region}
- Письмо отправляет закупщик малого пищевого бизнеса
- Дополнительные требования от закупщика: {extra or "нет"}
- Приемлемый тон: вежливый, конкретный, без канцелярита
- Что запросить: цена за единицу с объёмной шкалой (от/до), минимальный заказ,
  условия доставки, сроки, документы (декларации/ХАССП), варианты оплаты
- Подпись: "С уважением, [Имя], отдел закупок"

Ответь строго в формате:
SUBJECT: <тема письма>
BODY:
<текст письма>"""
    text = await ask(prompt, smart=True, temperature=0.4, max_tokens=900)
    if "BODY:" in text:
        subject, _, body = text.partition("BODY:")
        subject = subject.replace("SUBJECT:", "").strip()
        return subject, body.strip()
    # деградация: единый текст
    return f"Запрос КП: {query}", text


def _valid_emails(supplier: dict) -> list[str]:
    out = []
    for e in supplier.get("emails") or []:
        try:
            validate_email(e, check_deliverability=False).email
            out.append(e)
        except EmailNotValidError:
            logger.debug("skip invalid email %s", e)
    return out


async def send_letter(search: dict, suppliers: list[dict], extra: str = "",
                      custom_subject: str | None = None, custom_body: str | None = None) -> dict:
    """Генерация + отправка. Возвращает отчёт по каждому адресату."""
    s = get_settings()
    recipients_map: list[tuple[dict, list[str]]] = []
    for sup in suppliers:
        emails = _valid_emails(sup)
        if emails:
            recipients_map.append((sup, emails))
    if not recipients_map:
        db.log_rfq(search["id"], [x["id"] for x in suppliers], "", "",
                   "no_recipients", "ни у одного поставщика нет валидного email")
        return {"sent": 0, "failed": 0, "detail": [], "status": "no_recipients"}

    if custom_subject and custom_body:
        subject, body = custom_subject.strip(), custom_body.strip()
    else:
        subject, body = await draft_letter(search["query"], search["region"], suppliers, extra)

    async def send_one(to: str) -> tuple[str, str]:
        msg = EmailMessage()
        msg["From"] = s.smtp_from or s.smtp_user
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)
        try:
            await aiosmtplib.send(msg, hostname=s.smtp_host, port=s.smtp_port,
                                  username=s.smtp_user, password=s.smtp_password,
                                  start_tls=True, timeout=30)
            return to, "ok"
        except Exception as exc:  # SMTP-ошибки не должны валить остальные письма
            logger.warning("rfq send to %s failed: %s", to, exc)
            return to, f"error: {type(exc).__name__}"

    results = await asyncio.gather(*[send_one(to)
                                     for _, emails in recipients_map for to in emails])
    sent = sum(1 for _, st in results if st == "ok")
    db.log_rfq(search["id"], [x["id"] for x in suppliers], subject, body,
               "sent" if sent else "failed", f"{sent}/{len(results)} писем")
    return {"sent": sent, "failed": len(results) - sent,
            "detail": [{"to": t, "status": st} for t, st in results],
            "subject": subject, "body": body}
