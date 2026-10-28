"""RFQ (Request For Quotation): генерация письма-запроса КП, рассылка и автоответы.

Поток:
1. LLM пишет персонализированное деловое письмо-запрос по товару и региону.
2. По умолчанию включен ТЕСТОВЫЙ РЕЖИМ: рассылка идет на 3 внутренних тестовых ящика
   (supplier1@maxim.org.ru, supplier2@..., supplier3@...).
3. Пользователь может переключить режим на БОЕВОЙ для реальной отправки поставщикам.
4. Отправка строго ИНДИВИДУАЛЬНЫМ письмом каждому поставщику (без общих копий!).
5. Лимит отправки: не более 50 писем в день (с ящика zapros@maxim.org.ru).
6. Тестовые ящики автоматически генерируют реалистичные коммерческие ответы в ящик zapros.
7. Входящие ответы доступны для просмотра в интерфейсе через IMAP (imap.beget.com).
"""
from __future__ import annotations

import asyncio
import email
from email.header import decode_header
from email.message import EmailMessage
import imaplib
import logging
import smtplib
from typing import Any

import aiosmtplib
from email_validator import validate_email, EmailNotValidError

from . import db
from .config import get_settings
from .llm import ask, LLMError

logger = logging.getLogger(__name__)

def get_test_suppliers() -> list[dict]:
    """Возвращает список тестовых контрагентов с паролями из .env."""
    s = get_settings()
    return [
        {
            "id": "test-sup-1",
            "email": "supplier1@maxim.org.ru",
            "name": "Поставщик 1",
            "password": s.test_supplier_1_pass,
            "reply_text": "Здравствуйте. Получили Ваш запрос. Вернёмся с ответом в кратчайшие сроки.",
        },
        {
            "id": "test-sup-2",
            "email": "supplier2@maxim.org.ru",
            "name": "Поставщик 2",
            "password": s.test_supplier_2_pass,
            "reply_text": "Здравствуйте. Получили Ваш запрос. Приняли в работу, вернёмся с расчётом условий в кратчайшие сроки.",
        },
        {
            "id": "test-sup-3",
            "email": "supplier3@maxim.org.ru",
            "name": "Поставщик 3",
            "password": s.test_supplier_3_pass,
            "reply_text": "Здравствуйте. Ваш запрос принят отделом продаж. Подготовим коммерческое предложение и свяжемся с Вами в ближайшее время.",
        },
    ]


async def draft_letter(query: str, region: str, suppliers: list[dict],
                       extra: str = "") -> tuple[str, str]:
    """Генерация текста письма-запроса КП без отправки."""
    prompt = f"""Составь профессиональное деловое письмо-запрос коммерческого предложения (RFQ) на русском языке.

Контекст:
- Ищем надежного оптового поставщика: "{query}"
- Регион закупки и доставки: {region}
- Дополнительные требования закупщика: {extra or "стандартные оптовые условия, регулярные поставки"}
- Тон: вежливый, конструктивный B2B-стиль без лишнего канцелярита
- Что запросить пунктами:
  1. Стоимость за единицу продукции с объёмной шкалой скидок (от/до).
  2. Минимальный объем заказа (MOQ).
  3. Условия и ориентировочные сроки доставки в регион.
  4. Варианты и условия оплаты (размер предоплаты, возможность отсрочки).
  5. Наличие сопроводительных документов (декларации соответствия, сертификаты ХАССП, регистрация в «Меркурии»).
- Подпись: "С уважением, отдел закупок"

Ответь СТРОГО в формате:
SUBJECT: <тема письма>
BODY:
<текст письма>"""
    try:
        text = await ask(prompt, smart=True, temperature=0.3, max_tokens=800)
    except Exception:
        text = ""

    if "BODY:" in text:
        subject, _, body = text.partition("BODY:")
        subject = subject.replace("SUBJECT:", "").strip()
        return subject, body.strip()

    # Фолбэк на детерминированный шаблон
    subject = f"Запрос коммерческого предложения: {query} ({region})"
    body = (
        f"Добрый день!\n\n"
        f"Наша организация ищет надежного оптового поставщика продукции «{query}» "
        f"в регионе «{region}» для регулярного долгосрочного сотрудничества.\n\n"
        f"Дополнительные условия: {extra or 'регулярные оптовые поставки'}.\n\n"
        f"Просим направить коммерческое предложение со следующей информацией:\n"
        f"1. Стоимость за единицу с шкалой скидок в зависимости от объема партии.\n"
        f"2. Минимальный объем разовой партии (MOQ).\n"
        f"3. Условия, график и ориентировочные сроки доставки.\n"
        f"4. Варианты оплаты (предоплата, возможность отсрочки платежа).\n"
        f"5. Наличие сертификатов, деклараций соответствия и документов качества (ХАССП/Меркурий).\n\n"
        f"Будем признательны за ответ с приложенным прайс-листом или каталогом.\n\n"
        f"С уважением,\nОтдел закупок"
    )
    return subject, body


def _clean_email(raw: str) -> str | None:
    try:
        return validate_email(raw.strip(), check_deliverability=False).email
    except (EmailNotValidError, AttributeError):
        return None


def _decode_mime_words(raw: str | None) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out = []
    for text, enc in parts:
        if isinstance(text, bytes):
            out.append(text.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(text))
    return "".join(out)


async def send_letter(search: dict, suppliers: list[dict], extra: str = "",
                      custom_subject: str | None = None, custom_body: str | None = None,
                      mode: str = "test", manual_emails: list[str] | None = None) -> dict:
    """Индивидуальная отправка писем-запросов поставщикам.

    - mode == "test": отправляет на 3 тестовых ящика (supplier1..3@maxim.org.ru)
    - mode == "combat": отправляет реальным поставщикам и на ручные email
    - Лимит: не более 50 писем в день.
    """
    s = get_settings()
    now_day = db._now()[:10]  # YYYY-MM-DD
    current_daily = db.get_daily_rfq_count(now_day)

    # 1. Формирование списка получателей
    recipients: list[dict] = []  # list of {"email": ..., "name": ...}

    if mode == "test":
        # В тестовом режиме шлем на 3 наших тестовых ящика
        recipients = [
            {"email": acc["email"], "name": acc["name"]}
            for acc in get_test_suppliers()
        ]
    else:
        # В боевом режиме: собираем из карточек
        seen_emails = set()
        for sup in suppliers:
            sup_name = sup.get("legal_name") or sup.get("name") or "Поставщик"
            for raw_e in (sup.get("emails") or []):
                val_e = _clean_email(raw_e)
                if val_e and val_e not in seen_emails:
                    seen_emails.add(val_e)
                    recipients.append({"email": val_e, "name": sup_name})

        # Добавляем вручную введенные email
        for raw_e in (manual_emails or []):
            val_e = _clean_email(raw_e)
            if val_e and val_e not in seen_emails:
                seen_emails.add(val_e)
                recipients.append({"email": val_e, "name": "Контрагент (ручной ввод)"})

    if not recipients:
        return {
            "sent": 0, "failed": 0, "detail": [],
            "status": "no_recipients",
            "message": "Нет доступных email-адресов для отправки. Введите email вручную или выберите поставщиков с почтой."
        }

    # 2. Проверка дневного лимита (50 писем в день)
    requested_count = len(recipients)
    if current_daily + requested_count > s.smtp_daily_limit:
        remaining = max(0, s.smtp_daily_limit - current_daily)
        raise ValueError(
            f"Превышен суточный лимит рассылки ({s.smtp_daily_limit} писем/сутки). "
            f"Сегодня отправлено: {current_daily}. Доступно еще: {remaining} писем."
        )

    # 3. Текст письма
    if custom_subject and custom_body:
        subject, body = custom_subject.strip(), custom_body.strip()
    else:
        subject, body = await draft_letter(search.get("query", ""), search.get("region", ""), suppliers, extra)

    # 4. Отправка КАЖДОМУ ПОСТАВЩИКУ ОТДЕЛЬНЫМ ПИСЬМОМ (без копий!)
    results: list[dict] = []

    async def _send_single(rec: dict) -> dict:
        to_email = rec["email"]
        to_name = rec["name"]
        msg = EmailMessage()
        msg["From"] = f"Отдел закупок <{s.smtp_from or s.smtp_user}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.set_content(body)

        try:
            await aiosmtplib.send(
                msg,
                hostname=s.smtp_host,
                port=s.smtp_port,
                use_tls=(s.smtp_port == 465),
                start_tls=(s.smtp_port != 465),
                username=s.smtp_user,
                password=s.smtp_password,
                timeout=20,
            )
            return {"to": to_email, "name": to_name, "status": "ok"}
        except Exception as exc:
            logger.warning("SMTP send to %s failed: %s", to_email, exc)
            return {"to": to_email, "name": to_name, "status": f"error: {exc}"}

    send_tasks = [_send_single(r) for r in recipients]
    results = await asyncio.gather(*send_tasks)

    sent_count = sum(1 for r in results if r["status"] == "ok")
    failed_count = len(results) - sent_count

    # Обновляем дневной счетчик в БД
    if sent_count > 0:
        db.increment_daily_rfq_count(sent_count, now_day)

    # Логируем рассылку
    sup_ids = [x["id"] for x in suppliers if "id" in x and isinstance(x["id"], int)]
    db.log_rfq(
        search.get("id"), sup_ids, subject, body,
        "sent" if sent_count else "failed",
        f"Режим: {mode}. Успешно: {sent_count}, ошибок: {failed_count}"
    )

    # 5. Если это тестовый режим — запускаем автоответчики тестовых ящиков!
    if mode == "test" and sent_count > 0:
        asyncio.create_task(
            trigger_test_auto_replies(
                query=search.get("query", "продукты питания"),
                region=search.get("region", ""),
                inquiry_subject=subject,
                inquiry_body=body
            )
        )

    return {
        "sent": sent_count,
        "failed": failed_count,
        "mode": mode,
        "daily_used": current_daily + sent_count,
        "daily_limit": s.smtp_daily_limit,
        "detail": results,
        "subject": subject,
        "body": body,
    }


async def trigger_test_auto_replies(query: str, region: str, inquiry_subject: str, inquiry_body: str) -> None:
    """Фоновая генерация и отправка реалистичных автоответов с 3 тестовых ящиков."""
    await asyncio.sleep(2.0)  # короткая пауза для реалистичности

    s = get_settings()
    target_inbox = s.smtp_user  # zapros@maxim.org.ru

    for acc in get_test_suppliers():
        try:
            # 1. Проверяем наличие входящих писем на тестовом ящике
            def _check_and_reply_sync():
                with imaplib.IMAP4_SSL("imap.beget.com", 993, timeout=10) as m:
                    m.login(acc["email"], acc["password"])
                    m.select("INBOX")
                    typ, data = m.search(None, "UNSEEN")
                    msg_ids = data[0].split()
                    if msg_ids:
                        for mid in msg_ids:
                            m.store(mid, "+FLAGS", "\\Seen")

                # Формируем нейтральный деловой автоответ
                reply_subject = f"Re: {inquiry_subject}"
                reply_body = (
                    f"{acc['reply_text']}\n\n"
                    f"С уважением,\n"
                    f"{acc['name']}"
                )

                # Отправляем ответ на zapros@maxim.org.ru
                msg = EmailMessage()
                msg["From"] = f"{acc['name']} <{acc['email']}>"
                msg["To"] = target_inbox
                msg["Subject"] = reply_subject
                msg.set_content(reply_body)

                with smtplib.SMTP_SSL("smtp.beget.com", 465, timeout=10) as s_client:
                    s_client.login(acc["email"], acc["password"])
                    s_client.send_message(msg)

            await asyncio.to_thread(_check_and_reply_sync)
            logger.info("Auto-reply sent from %s to %s", acc["email"], target_inbox)
        except Exception as exc:
            logger.warning("Auto-reply failed for %s: %s", acc["email"], exc)


def sync_inbox_from_imap() -> list[dict]:
    """Синхронизирует входящие письма из ящика zapros@maxim.org.ru через IMAP."""
    s = get_settings()
    messages: list[dict] = []

    try:
        with imaplib.IMAP4_SSL(s.imap_host, s.imap_port, timeout=15) as m:
            m.login(s.imap_user, s.imap_password)
            m.select("INBOX")
            typ, data = m.search(None, "ALL")
            msg_ids = data[0].split()

            # Берем последние 30 сообщений
            for mid in reversed(msg_ids[-30:]):
                try:
                    typ, msg_data = m.fetch(mid, "(RFC822)")
                    raw = msg_data[0][1]
                    parsed = email.message_from_bytes(raw)

                    raw_from = _decode_mime_words(parsed.get("From"))
                    raw_subj = _decode_mime_words(parsed.get("Subject"))
                    raw_date = parsed.get("Date") or ""
                    msg_id_header = parsed.get("Message-ID") or f"imap-{mid.decode()}"

                    from_name = raw_from
                    from_email = raw_from
                    if "<" in raw_from and ">" in raw_from:
                        from_name = raw_from.split("<")[0].strip().strip('"')
                        from_email = raw_from.split("<")[1].split(">")[0].strip()

                    # Извлечение тела
                    body = ""
                    if parsed.is_multipart():
                        for part in parsed.walk():
                            ctype = part.get_content_type()
                            if ctype == "text/plain":
                                body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                                break
                            elif ctype == "text/html" and not body:
                                body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                    else:
                        body = parsed.get_payload(decode=True).decode("utf-8", errors="replace")

                    msg_dict = {
                        "id": msg_id_header,
                        "from_name": from_name or from_email,
                        "from_email": from_email,
                        "to_email": s.imap_user,
                        "subject": raw_subj or "Без темы",
                        "body": body.strip(),
                        "received_at": raw_date,
                        "is_read": 0,
                    }
                    db.save_inbox_message(msg_dict)
                except Exception as inner_exc:
                    logger.debug("Failed to parse imap message %s: %s", mid, inner_exc)
    except Exception as exc:
        logger.warning("sync_inbox_from_imap failed: %s", exc)

    return db.get_inbox_messages(limit=50)
