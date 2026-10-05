"""
Замер маркета NFT-подарков Telegram. Только чтение.

Отвечает на вопросы:
  1. Сколько коллекций и лотов на TG-маркете?
  2. Сколько длится «горячий» цикл (первая страница каждой коллекции)?
  3. Сколько запросов нужно на полный обход, проходит ли глубокая пагинация (sort_by_num) без сбоев курсоров?
  4. Когда прилетает FLOOD_WAIT?

Правила безопасности:
  * Ровно одна отправка на запрос (request_retries=0), Telethon не «досыпает» флуды сам (flood_sleep_threshold=0).
  * Первый FLOOD_WAIT или любая непонятная ошибка/таймаут → стоп (новых запросов нет), отчёт всё равно пишется.
  * Общий дедлайн --max-seconds: перед каждым запросом проверяем, что на ответ остаётся --reserve секунд.
  * В отчёт и на экран не попадают имя/UID аккаунта и владельцы лотов.

Запуск:
  python bench.py --login                       # один раз: вход (номер, код, 2FA); замер не выполняется
  python bench.py                               # каталог + горячий скан + полный обход (бюджет 300 запросов)
  python bench.py --collection 5170145012310081615 --full-budget 400 --hot 0   # полный обход одной коллекции
  python bench.py --max-seconds 600 --delay 2   # долгий прогон с паузой 2 с
"""

import argparse
import asyncio
import json
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, errors, functions

load_dotenv()

OUT_DIR = Path("bench_out")


class Stop(Exception):
    """Дальнейшие запросы запрещены: причина в stats.stopped."""


# ---------- утилиты ----------

def price_of(gift):
    """Цена лота в звёздах и TON. Новая схема — resell_amount, старая — resell_stars."""
    stars, ton = None, None
    for a in getattr(gift, "resell_amount", None) or []:
        if "Ton" in type(a).__name__:
            ton = a.amount / 1e9  # TON приходит в нано-единицах
        else:
            stars = a.amount + (getattr(a, "nanos", 0) or 0) / 1e9
    if stars is None and ton is None and getattr(gift, "resell_stars", None):
        stars = gift.resell_stars
    return stars, ton


def p95(values):
    """95-й перцентиль методом ближайшего ранга."""
    if not values:
        return 0
    s = sorted(values)
    return s[max(0, math.ceil(0.95 * len(s)) - 1)]


@dataclass
class Stats:
    deadline: float
    reserve: float
    wait_on_flood: bool = False
    requests: int = 0
    latencies: list = field(default_factory=list)
    flood_waits: list = field(default_factory=list)  # (номер запроса, метод, секунды)
    errors: list = field(default_factory=list)       # (номер запроса, метод, класс ошибки, RPC-код)
    stopped: str | None = None

    def summary(self):
        return {
            "requests": self.requests,
            "latency_avg_ms": round(sum(self.latencies) / len(self.latencies) * 1000) if self.latencies else 0,
            "latency_p95_ms": round(p95(self.latencies) * 1000),
            "flood_waits": self.flood_waits,
            "errors": self.errors[:50],
            "stopped": self.stopped,
        }


async def call(client, req, stats: Stats, delay: float):
    """Один запрос: проверка дедлайна, замер, остановка на флуде и непонятных ошибках."""
    method = type(req).__name__
    while True:
        if stats.stopped:
            raise Stop
        left = stats.deadline - time.monotonic()
        if left < stats.reserve:
            stats.stopped = f"дедлайн: на ответ осталось {left:.1f} с < резерва {stats.reserve} с"
            raise Stop
        stats.requests += 1
        n = stats.requests
        t0 = time.perf_counter()
        try:
            res = await asyncio.wait_for(client(req), timeout=stats.reserve)
        except errors.FloodWaitError as e:
            stats.flood_waits.append((n, method, e.seconds))
            print(f"  ⚠️  FLOOD_WAIT {e.seconds} с на запросе #{n} ({method})")
            if not stats.wait_on_flood:
                stats.stopped = f"FLOOD_WAIT {e.seconds} с на запросе #{n} ({method})"
                raise Stop
            await asyncio.sleep(e.seconds + 1)
            continue
        except Exception as e:  # noqa: BLE001 — исход неизвестен: дальше не идём
            code = getattr(e, "message", None) or getattr(e, "code", None)
            stats.errors.append((n, method, type(e).__name__, code))
            detail = f"{type(e).__name__} {code}" if code else type(e).__name__
            stats.stopped = f"запрос #{n} ({method}): {detail} — исход неизвестен"
            print(f"  ❌ {stats.stopped}")
            raise Stop
        stats.latencies.append(time.perf_counter() - t0)
        if delay:
            await asyncio.sleep(delay)
        return res


# ---------- шаги замера ----------

async def load_catalog(client, stats, delay):
    res = await call(client, functions.payments.GetStarGiftsRequest(hash=0), stats, delay)
    collections = []
    for g in res.gifts:
        on_resale = getattr(g, "availability_resale", None) or 0
        if on_resale:
            collections.append({"id": g.id, "title": getattr(g, "title", None) or str(g.id),
                                "on_resale": on_resale, "floor_stars": getattr(g, "resell_min_stars", None)})
    collections.sort(key=lambda c: c["on_resale"], reverse=True)
    return collections


def resale_request(gift_id, offset, limit, by_num=False):
    # Без sort_by_* сервер сортирует по времени последнего изменения цены (новые сверху) — «горячий» поток.
    # Для полного обхода — sort_by_num: порядок по номеру не плывёт, пока листаем.
    return functions.payments.GetResaleStarGiftsRequest(
        gift_id=gift_id, offset=offset, limit=limit, sort_by_num=by_num or None)


async def hot_scan(client, collections, limit, stats, delay, out):
    t0 = time.perf_counter()
    for c in collections:
        res = await call(client, resale_request(c["id"], "", limit), stats, delay)
        out.append({"collection": c["id"], "rows": len(res.gifts), "count": getattr(res, "count", None)})
    return time.perf_counter() - t0


async def full_scan_one(client, c, limit, budget_left, stats, delay):
    """Полный обход одной коллекции с проверками. Возвращает (итог, лоты, потраченные запросы)."""
    lots, seen, dupes, last_num, counts = {}, {""}, 0, None, []
    offset, used, status, order_breaks = "", 0, "complete", 0
    while True:
        if used >= budget_left:
            status = "budget"
            break
        try:
            res = await call(client, resale_request(c["id"], offset, limit, by_num=True), stats, delay)
        except Stop:
            status = "stopped"
            break
        used += 1
        if getattr(res, "count", None) is not None:
            counts.append(res.count)
        for g in res.gifts:
            dupes += g.slug in lots
            num = getattr(g, "num", None)
            if num is not None and last_num is not None and num < last_num:
                order_breaks += 1
            last_num = num if num is not None else last_num
            stars, ton = price_of(g)
            lots[g.slug] = {"num": num, "stars": stars, "ton": ton,
                            "ton_only": getattr(g, "resale_ton_only", None),
                            "has_owner": getattr(g, "owner_id", None) is not None}
        nxt = getattr(res, "next_offset", None)
        if not nxt:
            break
        if not res.gifts:
            status = "empty_page_with_cursor"
            break
        if nxt in seen:
            status = "repeated_cursor"
            break
        seen.add(nxt)
        offset = nxt
    if status == "complete":
        ref = counts[-1] if counts else c["on_resale"]
        if dupes:
            status = "duplicates"
        elif order_breaks:
            status = "order"
        elif counts and max(counts) - min(counts) > math.ceil(max(counts) * 0.02):
            status = "count_drift"
        elif len(lots) < ref - math.ceil(ref * 0.02):
            status = "short"
    result = {"collection": c["id"], "title": c["title"], "catalog_on_resale": c["on_resale"],
              "fetched_unique": len(lots), "pages": used, "duplicates": dupes, "order_breaks": order_breaks,
              "server_counts": [min(counts), max(counts)] if counts else None, "status": status}
    return result, lots, used


# ---------- main ----------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--login", action="store_true", help="только войти в аккаунт (номер, код, 2FA) и выйти")
    ap.add_argument("--limit", type=int, default=100, help="размер страницы (сервер может отдать меньше)")
    ap.add_argument("--delay", type=float, default=1.5,
                    help="пауза между запросами, с (замер 04.10: 0.1 → флуд на 41-м запросе, 1.5 → без флуда)")
    ap.add_argument("--hot", type=int, default=1, help="1 — горячий скан всех коллекций, 0 — пропустить")
    ap.add_argument("--full-budget", type=int, default=300, help="макс. запросов на полный обход (0 — без него)")
    ap.add_argument("--collection", type=int, help="полный обход только этой коллекции (id)")
    ap.add_argument("--min-lots", type=int, default=0,
                    help="без --collection: полный обход начинать с коллекций, где лотов не меньше")
    ap.add_argument("--max-seconds", type=float, default=600, help="общий дедлайн прогона, с")
    ap.add_argument("--reserve", type=float, default=30, help="сколько секунд оставлять на ответ запроса")
    ap.add_argument("--wait-on-flood", action="store_true",
                    help="на FLOOD_WAIT ждать и продолжать (по умолчанию — остановиться)")
    args = ap.parse_args()

    if not hasattr(functions.payments, "GetResaleStarGiftsRequest"):
        raise SystemExit("Telethon слишком старый: нет GetResaleStarGiftsRequest. Обнови: pip install -U telethon")

    client = TelegramClient(
        os.getenv("TG_SESSION", "bench"), int(os.environ["TG_API_ID"]), os.environ["TG_API_HASH"],
        flood_sleep_threshold=0,  # любой FLOOD_WAIT виден и попадает в отчёт
        request_retries=0,        # ровно одна отправка (в Telethon 1 = отправка + один повтор)
    )
    if args.login:
        await client.start()
        print("Вход выполнен, сессия сохранена. Теперь запускай замер без --login.")
        await client.disconnect()
        return

    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise SystemExit("Сессия не авторизована. Сначала: python bench.py --login")
    print("Аккаунт авторизован.\n")

    stats = Stats(deadline=time.monotonic() + args.max_seconds, reserve=args.reserve,
                  wait_on_flood=args.wait_on_flood)
    OUT_DIR.mkdir(exist_ok=True)
    collections, hot_pages, full_results, listings = [], [], [], {}
    hot_time = full_time = 0.0
    started = time.time()
    try:
        print("1/3 Каталог коллекций…")
        collections = await load_catalog(client, stats, args.delay)
        print(f"  Коллекций на маркете: {len(collections)}, лотов всего: {sum(c['on_resale'] for c in collections):,}")
        for c in collections[:10]:
            print(f"    {c['title']:<24} {c['on_resale']:>7,} лотов, флор {c['floor_stars']}⭐  (id {c['id']})")

        if args.hot:
            print(f"\n2/3 Горячий скан (1 страница × {len(collections)} коллекций)…")
            hot_time = await hot_scan(client, collections, args.limit, stats, args.delay, hot_pages)
            print(f"  Цикл: {hot_time:.1f} c")

        if args.full_budget:
            if args.collection:
                targets = [c for c in collections if c["id"] == args.collection]
                if not targets:
                    raise SystemExit(f"Коллекции {args.collection} нет среди выставленных на продажу")
            else:
                targets = sorted((c for c in collections if c["on_resale"] >= args.min_lots),
                                 key=lambda c: c["on_resale"])
            print(f"\n3/3 Полный обход по номеру (бюджет {args.full_budget} запросов)…")
            t0, left = time.perf_counter(), args.full_budget
            for c in targets:
                if left <= 0 or stats.stopped:
                    break
                result, lots, used = await full_scan_one(client, c, args.limit, left, stats, args.delay)
                left -= used
                full_results.append(result)
                listings.update(lots)
                print(f"  {c['title']:<24} {result['fetched_unique']:>7,} из {c['on_resale']:,} — {result['status']}")
            full_time = time.perf_counter() - t0
    except Stop:
        pass
    finally:
        s = stats.summary()
        per_page = max((p["rows"] for p in hot_pages), default=0) or args.limit
        total = sum(c["on_resale"] for c in collections)
        estimate = {
            "total_listings": total, "collections": len(collections), "page_size": per_page,
            "hot_pages_done": len(hot_pages), "hot_cycle_sec": round(hot_time, 1),
            "full_cycle_requests": sum(-(-c["on_resale"] // per_page) for c in collections),
        }
        report = {"started_at": started, "args": vars(args), "estimate": estimate, "stats": s,
                  "top_collections": collections[:30], "hot_pages": hot_pages, "full_scan": full_results,
                  "full_scan_sec": round(full_time, 1)}
        (OUT_DIR / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        (OUT_DIR / "snapshot.json").write_text(json.dumps(listings, ensure_ascii=False))
        await client.disconnect()

        print("\n===== ИТОГ =====")
        print(f"Лотов на маркете:            {total:,} в {len(collections)} коллекциях")
        print(f"Горячий скан:                {len(hot_pages)} страниц за {hot_time:.1f} c")
        print(f"Полный цикл, запросов:       {estimate['full_cycle_requests']:,}")
        print(f"Полный обход:                {[(r['title'], r['status']) for r in full_results] or '—'}")
        print(f"Задержка запроса:            {s['latency_avg_ms']} мс (p95 {s['latency_p95_ms']} мс)")
        print(f"FLOOD_WAIT:                  {s['flood_waits'] or 'не было'}")
        print(f"Запросов отправлено:         {s['requests']}")
        if stats.stopped:
            print(f"Остановлен:                  {stats.stopped}")
        print(f"\nОтчёт: {OUT_DIR / 'report.json'}")


if __name__ == "__main__":
    asyncio.run(main())
