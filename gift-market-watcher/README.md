# gift-market-watcher

Сбор событий NFT-подарков Telegram (листинги, смены цен, продажи, снятия) в нашу базу и выдача из неё.
Пользователи читают только из базы, в Telegram на их запросы мы не ходим.

## Как устроено

```
 сборщики ──▶ Redis Streams ──▶ нормализатор ──▶ Postgres ──▶ API (REST + WebSocket /live)
 (telegram,      (шина, ничего       (дедуп,          (events — лента,
  fake, дальше    не теряется         проекции)        listings — что на продаже,
  Portals/MRKT)   при падениях)                        gifts — владельцы)
```

- **Сборщик маркета** (`gmw/collectors/market.py`) работает поверх любого источника:
  - *горячий скан* — первая страница коллекции, где сервер отдаёт свежие изменения цены сверху. Интервал адаптивный: есть изменения — смотрим чаще, тишина — реже;
  - *полный скан* — все страницы. Пропавшие лоты проверяем точечно: сменился владелец → `sold`, нет → `delisted`;
  - *первый полный скан* — посев текущего состояния без событий, чтобы не завалить ленту ложными `listed`.
- **Пул аккаунтов** (`gmw/accounts.py`) раздаёт запросы по кругу и при `FLOOD_WAIT` отправляет аккаунт отдыхать.
- **События идемпотентны.** У события детерминированный id, а проекции учитывают время, так что повторная доставка и рестарты не дают дублей и ложных событий.
- **Фейковый маркет** (`gmw/collectors/fake.py`) живёт сам по себе. С ним весь конвейер гоняется без Telegram.

## Шаг 0 — замер (`bench.py`)

Перед боевым запуском узнаём реальные цифры: сколько лотов, сколько запросов на цикл, где флуд-лимит.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # впиши TG_API_ID и TG_API_HASH
python bench.py             # первый запуск спросит номер и код — это вход в аккаунт
```

Флаги: `--delay 0` (без пауз, вторым прогоном), `--full-budget 0` (без полного обхода), `--limit 100`.
Результат: `bench_out/report.json` с цифрами и `bench_out/snapshot.json` с собранными лотами.

## Запуск в Docker

```bash
cp .env.example .env
docker compose --profile fake up -d          # всё на фейковом маркете
curl localhost:8000/stats
```

С настоящим Telegram:
```bash
docker compose run --rm login                # один раз: номер, код, 2FA → sessions/*.session
docker compose --profile telegram up -d
```

## Запуск без Docker

Нужны Postgres и Redis (адреса — в `.env`).
```bash
python -m gmw initdb
python -m gmw worker &
python -m gmw collect fake &      # или: python -m gmw login && python -m gmw collect telegram
python -m gmw api                 # http://localhost:8000/docs
```

## API

| Запрос | Что отдаёт |
|---|---|
| `GET /events?type=sold&collection_id=…&source=…&slug=…&before=…&limit=50` | лента событий, новые сверху |
| `GET /gifts/{slug}` | владелец, текущие лоты, история гифта |
| `GET /floors` | флор и число лотов по коллекциям |
| `GET /stats` | всего событий, событий в секунду, лотов на продаже |
| `WS /live?type=…&collection_id=…` | события в реальном времени |

Типы событий: `listed`, `price_changed`, `delisted`, `sold`, `transfer`, `minted`, `burned`.

## Тесты

```bash
pip install -r requirements-dev.txt
pytest                                         # юнит-тесты
GMW_TEST_DATABASE_URL=postgresql://… GMW_TEST_REDIS_URL=redis://… pytest   # + весь конвейер на живых Postgres/Redis
```

## Дальше

1. Прогнать `bench.py` и по цифрам выставить число аккаунтов и интервалы.
2. Сборщики Portals / MRKT / Tonnel / Getgems — тот же `MarketAPI`, только другой источник.
3. Полный индекс: обход владельцев (`getSavedStarGifts`), передачи, выпуск, лидерборды.
4. Атрибуты (модель / фон / узор) и флоры по комбинациям, бот-отслеживания, Mini App.

⚠️ Используй отдельный аккаунт, не основной. Файлы `.env` и `*.session` никому не отправляй: это доступ к аккаунту.
