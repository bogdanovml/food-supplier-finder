"""Конфигурация приложения. Все секреты — только из .env (в .gitignore)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(BASE_DIR / ".env"), env_file_encoding="utf-8", extra="ignore")

    # LLM (OpenAI-compatible)
    llm_base_url: str = "https://gpt.ai-tools.su/v1"
    llm_api_key: str = ""
    llm_model_simple: str = "gemma4:31b"
    llm_model_smart: str = "gemini-3.8-flash-high"

    # Поиск XML
    xmlstock_user: str = ""
    xmlstock_key: str = ""
    xmlstock_yandex_url: str = "https://xmlstock.com/yandex/xml/"
    xmlstock_google_url: str = "https://xmlstock.com/google/xml/"

    # Госконтракты
    gosplan_base_url: str = "https://v2.gosplan.info"

    # SMTP (RFQ)
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

    @property
    def smtp_configured(self) -> bool:
        return bool(self.smtp_host and self.smtp_user and self.smtp_password)


@lru_cache
def get_settings() -> Settings:
    return Settings()
