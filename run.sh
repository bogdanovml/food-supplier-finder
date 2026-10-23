#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

if [ ! -d ".venv" ]; then
  echo "Создание виртуального окружения Python..."
  python3 -m venv .venv
  .venv/bin/pip install --upgrade pip
  .venv/bin/pip install -r requirements.txt
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
