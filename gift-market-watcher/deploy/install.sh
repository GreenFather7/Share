#!/usr/bin/env bash
# Установка gift-market-watcher рядом с другими проектами.
#
# Что делает:   кладёт код в одну папку ($GMW_DIR, по умолчанию /opt/gift-market-watcher)
#               и поднимает Docker-проект «gmw»: Postgres, Redis, нормализатор, API.
# Чего НЕ делает: не ставит пакеты, не трогает другие контейнеры, nginx, firewall, cron, systemd.
#               Наружу ничего не открывает: API только на 127.0.0.1:$GMW_HOST_PORT.
# Удалить всё:  cd $GMW_DIR && docker compose --profile '*' down -v && cd / && rm -rf $GMW_DIR
#
# Запуск:  curl -fsSL https://raw.githubusercontent.com/GreenFather7/Share/claude/parser-from-chat-k5im4d/gift-market-watcher/deploy/install.sh | bash
set -euo pipefail

REPO="https://github.com/GreenFather7/Share.git"
BRANCH="${GMW_BRANCH:-claude/parser-from-chat-k5im4d}"
DIR="${GMW_DIR:-/opt/gift-market-watcher}"
PORT="${GMW_HOST_PORT:-8040}"

command -v docker >/dev/null || { echo "❌ Нет Docker. Сначала запусти preflight.sh и реши, ставить ли Docker."; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "❌ Нет docker compose v2."; exit 1; }
command -v git >/dev/null || { echo "❌ Нет git."; exit 1; }

SRC="$DIR/src"
if [ -d "$SRC/.git" ]; then
  echo "== Обновляю код в $SRC"
  git -C "$SRC" fetch --depth 1 origin "$BRANCH"
  git -C "$SRC" checkout -q -B "$BRANCH" FETCH_HEAD
else
  echo "== Скачиваю код в $SRC"
  mkdir -p "$DIR"
  git clone -q --depth 1 -b "$BRANCH" "$REPO" "$SRC"
fi

APP="$SRC/gift-market-watcher"
cd "$APP"
mkdir -p sessions && chmod 700 sessions
if [ ! -f .env ]; then
  cp .env.example .env
  TOKEN=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
  printf '\nGMW_HOST_PORT=%s\nGMW_API_TOKEN=%s\n' "$PORT" "$TOKEN" >> .env
  echo "== Создал .env (с токеном для API)"
fi
chmod 600 .env

echo "== Поднимаю проект gmw (Postgres, Redis, нормализатор, API)"
docker compose up -d --build postgres redis worker api

for _ in $(seq 1 30); do
  curl -fs "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1 && break
  sleep 2
done
if curl -fs "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
  echo "✅ API живо: http://127.0.0.1:${PORT}/health"
else
  echo "⚠️  API пока не ответило — посмотри: cd $APP && docker compose logs api"
fi

cat <<EOF

Дальше:
  1. Впиши TG_API_ID и TG_API_HASH:   nano $APP/.env
  2. Войди запасным аккаунтом:          cd $APP && docker compose run --rm login
  3. Запусти сборщик:                   docker compose --profile telegram up -d
  4. Проверь:                           curl -H "Authorization: Bearer \$(grep GMW_API_TOKEN .env | cut -d= -f2)" http://127.0.0.1:${PORT}/stats

Логи:      docker compose logs -f collector-telegram worker
Стоп:      docker compose --profile '*' stop
EOF
