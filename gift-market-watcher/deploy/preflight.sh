#!/usr/bin/env bash
# Разведка сервера перед установкой. ТОЛЬКО ЧИТАЕТ — ничего не ставит, не меняет и не запускает.
# Запуск:  curl -fsSL https://raw.githubusercontent.com/GreenFather7/Share/claude/parser-from-chat-k5im4d/gift-market-watcher/deploy/preflight.sh | bash
# Вывод можно целиком прислать в чат: паролей и ключей в нём нет.

PORT="${GMW_HOST_PORT:-8040}"
DIR="${GMW_DIR:-/opt/gift-market-watcher}"
ok()   { printf '  ✅ %s\n' "$*"; }
warn() { printf '  ⚠️  %s\n' "$*"; }
bad()  { printf '  ❌ %s\n' "$*"; }

echo "== Система"
. /etc/os-release 2>/dev/null && echo "  ОС: ${PRETTY_NAME:-?}"
echo "  CPU: $(nproc 2>/dev/null || echo ?) ядер, архитектура $(uname -m)"
free -h 2>/dev/null | awk '/^Mem:/ {print "  RAM: всего " $2 ", свободно " $7}'
df -h / 2>/dev/null | awk 'NR==2 {print "  Диск /: свободно " $4 " из " $2}'
MEM_AVAIL_MB=$(free -m 2>/dev/null | awk '/^Mem:/ {print $7}')
[ -n "$MEM_AVAIL_MB" ] && { [ "$MEM_AVAIL_MB" -ge 1500 ] && ok "памяти хватает (нужно ~1.3 ГБ с запасом)" || warn "свободной памяти мало: ${MEM_AVAIL_MB} МБ, проекту нужно ~1.3 ГБ"; }
DISK_AVAIL_GB=$(df -BG / 2>/dev/null | awk 'NR==2 {gsub("G","",$4); print $4}')
[ -n "$DISK_AVAIL_GB" ] && { [ "$DISK_AVAIL_GB" -ge 5 ] && ok "диска хватает" || warn "на диске меньше 5 ГБ"; }

echo "== Docker"
if command -v docker >/dev/null 2>&1; then
  ok "$(docker --version)"
  if docker compose version >/dev/null 2>&1; then ok "$(docker compose version | head -1)"; else bad "нет docker compose v2 (плагин)"; fi
  if docker info >/dev/null 2>&1; then
    ok "доступ к Docker есть у пользователя $(whoami)"
    echo "  Уже запущено контейнеров: $(docker ps -q | wc -l) (их не трогаем)"
    if docker ps -a --format '{{.Names}}' | grep -q '^gmw-'; then warn "уже есть контейнеры gmw-* — видимо, ставили раньше"; else ok "имя проекта gmw свободно"; fi
  else
    warn "Docker есть, но у $(whoami) нет к нему доступа — запускай установку через sudo или от пользователя из группы docker"
  fi
else
  bad "Docker не установлен. Ставить его на сервер с другими проектами — твоё решение; скажи, дам безопасную инструкцию"
fi

echo "== Порт и папка"
if command -v ss >/dev/null 2>&1 && ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${PORT}\$"; then
  bad "порт ${PORT} занят — выберем другой (GMW_HOST_PORT=...)"
else
  ok "порт ${PORT} свободен (API будет слушать только 127.0.0.1:${PORT})"
fi
[ -e "$DIR" ] && warn "папка $DIR уже существует" || ok "папка $DIR свободна"

echo "== Веб-сервер (для справки, трогать не будем)"
for s in nginx caddy apache2 httpd traefik; do
  pgrep -x "$s" >/dev/null 2>&1 && echo "  работает: $s"
done
docker ps --format '{{.Image}}' 2>/dev/null | grep -Eio 'nginx|traefik|caddy' | sort -u | sed 's/^/  в Docker: /'

echo "== Сеть до Telegram"
if command -v timeout >/dev/null 2>&1 && timeout 5 bash -c 'exec 3<>/dev/tcp/149.154.167.51/443' 2>/dev/null; then
  ok "149.154.167.51:443 (Telegram DC2) доступен"
else
  warn "не удалось подключиться к Telegram DC2"
fi

echo "== Готово. Ничего не изменено."
