"""LLM-клиент: OpenAI-compatible endpoint (провайдер задаётся в .env: LLM_BASE_URL).

Маршрутизация по сложности:
  ask(simple=...) -> LLM_MODEL_SIMPLE   — дешёвые задачи (классификация, план запросов)
  ask(smart=...)  -> LLM_MODEL_SMART  — структурирование, RFQ-письма

JSON-вызовы: модель просим отвечать строго JSON; парсим с фолбэком на
```\njson ... ```-блок (модели любят оборачивать)."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import httpx

from .config import get_settings

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(connect=10, read=90, write=15, pool=30)


class LLMError(RuntimeError):
    pass


async def _post_chat(payload: dict, api_key: str, base_url: str, retries: int = 2) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    f"{base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json=payload,
                )
                if resp.status_code == 429:
                    raise httpx.HTTPStatusError("429", request=resp.request, response=resp)
                resp.raise_for_status()
            data = resp.json()
            content = (data.get("choices") or [{}])[0].get("message", {}).get("content")
            if not isinstance(content, str) or not content.strip():
                raise LLMError("пустой ответ LLM")
            return content.strip()
        except (httpx.HTTPError, LLMError) as exc:
            last_exc = exc
            if attempt < retries:
                await asyncio.sleep(1.5 * (attempt + 1))
    raise LLMError(f"LLM request failed: {last_exc}")


async def ask(prompt: str, *, system: str = "", smart: bool = False, temperature: float = 0.3,
             max_tokens: int = 2000) -> str:
    """Текстовый вызов. smart=False — дешёвая модель."""
    s = get_settings()
    model = s.llm_model_smart if smart else s.llm_model_simple
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}
    ]
    return await _post_chat(
        {"model": model, "temperature": temperature, "max_tokens": max_tokens, "messages": messages},
        s.llm_api_key, s.llm_base_url,
    )


def _parse_json_loose(text: str) -> Any:
    """LLM иногда оборачивает JSON в ```json ... ``` или добавляет текст."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except ValueError:
        # попробуем вытащить первый {...} или [...]
        m = re.search(r"(\{.*\}|\[.*\])", text, re.DOTALL)
        if m:
            return json.loads(m.group(1))
        raise


async def ask_json(prompt: str, *, system: str = "", smart: bool = False, temperature: float = 0.2,
                   max_tokens: int = 2000) -> Any:
    """Вызов с ожиданием JSON-ответа."""
    sys_prompt = (system + " " if system else "") + "Отвечай СТРОГО валидным JSON без пояснений."
    text = await ask(prompt, system=sys_prompt, smart=smart, temperature=temperature, max_tokens=max_tokens)
    try:
        return _parse_json_loose(text)
    except ValueError as exc:
        logger.warning("LLM вернул не-JSON: %.200s", text)
        raise LLMError(f"не удалось распарсить JSON: {exc}") from exc
