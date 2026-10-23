"""Конфигурация приложения.

Политика: НИКАКИХ провайдеров, ключей и эндпоинтов в коде — всё читается
из .env (в .gitignore; в репозитории только .env.example с плейсхолдерами).
Обязательные переменные валидируются при старте — сервис падает сразу
с понятным сообщением, а не загадочно в середине поиска."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(BASE_DIR / ".env"), env_file_encoding="utf-8", extra="ignore")

    # LLM (OpenAI-compatible endpoint)
    llm_base_url: str = ""
    llm_api_key: str = ""
    llm_model_simple: str = ""
    llm_model_smart: str = ""

    # Поиск XML (SERP)
    xmlstock_user: str = ""
    xmlstock_key: str = ""
    xmlstock_yandex_url: str = ""
    xmlstock_google_url: str = ""

    # Госконтракты (Госплан API)
    gosplan_base_url: str = ""

    # SMTP (RFQ-рассылка)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = ""

    # Приложение
    app_port: int = 8000
    db_path: str = str(BASE_DIR / "data" / "suppliers.db")
    enrich_concurrency: int = 4
    enrich_max_sites: int = 12       # максимум сайтов для глубокого обхода за поиск
    serp_top: int = 20               # сколько позиций SERP берем на запрос

    def validate_secrets(self) -> list[str]:
        """Возвращает список проблем с обязательными переменными (провайдеры — в .env)."""
        missing = []
        if not (self.llm_base_url and self.llm_api_key and self.llm_model_simple and self.llm_model_smart):
            missing.append("LLM_BASE_URL, LLM_API_KEY, LLM_MODEL_SIMPLE, LLM_MODEL_SMART")
        if not (self.xmlstock_user and self.xmlstock_key
                and self.xmlstock_yandex_url and self.xmlstock_google_url):
            missing.append("XMLSTOCK_USER, XMLSTOCK_KEY, XMLSTOCK_YANDEX_URL, XMLSTOCK_GOOGLE_URL")
        if not self.gosplan_base_url:
            missing.append("GOSPLAN_BASE_URL")
        return missing

    @property
    def smtp_configured(self) -> bool:
        return bool(self.smtp_host and self.smtp_user and self.smtp_password)


@lru_cache
def get_settings() -> Settings:
    return Settings()
