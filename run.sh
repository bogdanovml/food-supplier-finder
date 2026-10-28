#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

if [ ! -d ".venv" ]; then
  echo "Создание виртуального окружения Python..."
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  if [ -f "requirements.txt" ]; then
    .venv/bin/pip install -r requirements.txt
  else
    .venv/bin/pip install "fastapi>=0.115" "uvicorn[standard]>=0.32" "httpx[http2]>=0.28" "h2>=4.1.0" \
      "pydantic-settings>=2.6" "sse-starlette>=2.1" "jinja2>=3.1" "trafilatura>=1.12" \
      "price-parser>=0.4" "rapidfuzz>=3.0" "tldextract>=5.1" "playwright>=1.40" \
      "xlsxwriter>=3.2" "aiosmtplib>=3.0" "email-validator>=2.2"
  fi
  .venv/bin/python -m playwright install chromium
fi

if [ ! -f ".env" ]; then
  if [ -f ".env.example" ]; then
    echo "Создание .env из .env.example..."
    cp .env.example .env
    echo "ВНИМАНИЕ: Заполните переменные окружения в .env перед запуском."
  else
    echo "ОШИБКА: Файл .env не найден."
    exit 1
  fi
fi

PORT="${APP_PORT:-8000}"
echo "Запуск Food Supplier Finder на http://127.0.0.1:$PORT ..."
exec .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port "$PORT" --reload
