"""
Замер маркета NFT-подарков Telegram.

Отвечает на три вопроса:
  1. Сколько всего лотов на TG-маркете (по всем коллекциям)?
  2. Сколько времени занимает «горячий» цикл (первая страница каждой коллекции)?
  3. Сколько запросов нужно на полный обход и когда прилетает FLOOD_WAIT?

Запуск:
  python bench.py                 # каталог + горячий скан + ограниченный полный обход
  python bench.py --delay 0       # без пауз между запросами (жёстче проверяем лимиты)
  python bench.py --full-budget 0 # без полного обхода, только каталог и горячий скан
"""

import argparse
import asyncio
import json
import os
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, errors, functions

load_dotenv()

OUT_DIR = Path("bench_out")


# ---------- утилиты ----------

def price_of(gift):
    """Цена лота в звёздах и TON. Поддерживает старую (resell_stars) и новую (resell_amount) схемы."""
    stars, ton = None, None
    amounts = getattr(gift, "resell_amount", None)
    if amounts:
        for a in amounts:
            name = type(a).__name__
            value = a.amount + getattr(a, "nanos", 0) / 1e9
            if "Ton" in name:
                ton = value / 1e9  # TON приходит в нано-единицах
            else:
                stars = value
    elif getattr(gift, "resell_stars", None):
        stars = gift.resell_stars
    return stars, ton


def owner_of(gift):
    peer = getattr(gift, "owner_id", None)
    if peer is None:
        return getattr(gift, "owner_name", None) or getattr(gift, "owner_address", None)
    for attr in ("user_id", "channel_id", "chat_id"):
        if hasattr(peer, attr):
            return f"{attr[0]}{getattr(peer, attr)}"
    return str(peer)


@dataclass
class Stats:
    requests: int = 0
    latencies: list = field(default_factory=list)
    flood_waits: list = field(default_factory=list)  # (номер запроса, секунды)
    errors: list = field(default_factory=list)
    wait_on_flood: bool = False   # False — на первом FLOOD_WAIT замер останавливается (аккаунт не долбим)
    stopped: str | None = None    # причина остановки

    def summary(self):
        lat = self.latencies or [0]
        return {
            "requests": self.requests,
            "latency_avg_ms": round(statistics.mean(lat) * 1000),
            "latency_p95_ms": round(sorted(lat)[max(0, int(len(lat) * 0.95) - 1)] * 1000),
            "flood_waits": self.flood_waits,
            "errors": self.errors[:20],
            "stopped": self.stopped,
        }


async def call(client, req, stats: Stats, delay: float):
    """Один запрос с замером времени и обработкой FLOOD_WAIT."""
    while True:
        stats.requests += 1
        t0 = time.perf_counter()
        try:
            res = await client(req)
            stats.latencies.append(time.perf_counter() - t0)
            if delay:
                await asyncio.sleep(delay)
            return res
        except errors.FloodWaitError as e:
            stats.flood_waits.append((stats.requests, e.seconds))
            print(f"  ⚠️  FLOOD_WAIT {e.seconds}s на запросе #{stats.requests}")
            if not stats.wait_on_flood:
                stats.stopped = f"FLOOD_WAIT {e.seconds}s на запросе #{stats.requests}"
                return None
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:  # noqa: BLE001
            stats.errors.append(f"{type(e).__name__}: {e}")
            print(f"  ❌ {type(e).__name__}: {e}")
            return None


# ---------- шаги замера ----------

async def load_catalog(client, stats, delay):
    res = await call(client, functions.payments.GetStarGiftsRequest(hash=0), stats, delay)
    if res is None:
        raise SystemExit("Не удалось загрузить каталог коллекций — см. ошибку выше")
    collections = []
    for g in res.gifts:
        on_resale = getattr(g, "availability_resale", None) or 0
        if on_resale:
            collections.append({
                "id": g.id,
                "title": getattr(g, "title", None) or str(g.id),
                "on_resale": on_resale,
                "floor_stars": getattr(g, "resell_min_stars", None),
            })
    collections.sort(key=lambda c: c["on_resale"], reverse=True)
    return collections


def resale_request(gift_id, offset, limit, by_num=False):
    # Без sort_by_* сервер сортирует по времени последнего изменения цены (новые сверху) — «горячий» поток.
    # Для полного обхода — sort_by_num: порядок по номеру не плывёт, пока листаем.
    return functions.payments.GetResaleStarGiftsRequest(
        gift_id=gift_id, offset=offset, limit=limit, sort_by_num=by_num or None,
    )


async def hot_scan(client, collections, limit, stats, delay):
    t0 = time.perf_counter()
    page_sizes = []
    for c in collections:
        if stats.stopped:
            break
        res = await call(client, resale_request(c["id"], "", limit), stats, delay)
        if res is not None:
            page_sizes.append(len(res.gifts))
    return time.perf_counter() - t0, page_sizes


async def full_scan(client, collections, limit, budget, stats, delay):
    """Полный обход коллекций (от маленьких к большим), пока не кончится бюджет запросов."""
    listings = {}
    done = []
    t0 = time.perf_counter()
    start_requests = stats.requests
    for c in sorted(collections, key=lambda c: c["on_resale"]):
        if stats.stopped:
            break
        offset, got, seen, status = "", 0, {""}, "complete"
        while True:
            if stats.requests - start_requests >= budget:
                done.append({"title": c["title"], "expected": c["on_resale"], "fetched": got, "status": "budget"})
                return time.perf_counter() - t0, listings, done
            res = await call(client, resale_request(c["id"], offset, limit, by_num=True), stats, delay)
            if res is None:
                status = "stopped" if stats.stopped else "error"
                break
            for g in res.gifts:
                stars, ton = price_of(g)
                listings[g.slug] = {
                    "gift_id": c["id"], "num": g.num, "stars": stars, "ton": ton,
                    "owner": owner_of(g),
                }
            got += len(res.gifts)
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
        done.append({"title": c["title"], "expected": c["on_resale"], "fetched": got, "status": status,
                     "server_count": getattr(res, "count", None) if res is not None else None})
    return time.perf_counter() - t0, listings, done


# ---------- main ----------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100, help="размер страницы (сервер может отдать меньше)")
    ap.add_argument("--delay", type=float, default=1.5,
                    help="пауза между запросами, сек (замер 04.10: 0.1 → флуд на 41-м запросе, 1.5 → без флуда)")
    ap.add_argument("--full-budget", type=int, default=300, help="макс. запросов на полный обход")
    ap.add_argument("--wait-on-flood", action="store_true",
                    help="на FLOOD_WAIT ждать и продолжать (по умолчанию — остановиться и записать отчёт)")
    args = ap.parse_args()

    if not hasattr(functions.payments, "GetResaleStarGiftsRequest"):
        raise SystemExit("Telethon слишком старый: нет GetResaleStarGiftsRequest. Обнови: pip install -U telethon")

    client = TelegramClient(
        os.getenv("TG_SESSION", "bench"),
        int(os.environ["TG_API_ID"]),
        os.environ["TG_API_HASH"],
        flood_sleep_threshold=0,  # любой FLOOD_WAIT виден и попадает в отчёт, а не «досыпается» внутри Telethon
        request_retries=1,        # без скрытых повторов
    )
    await client.start()
    me = await client.get_me()
    print(f"Аккаунт: {me.first_name} (id {me.id})\n")

    stats = Stats(wait_on_flood=args.wait_on_flood)
    OUT_DIR.mkdir(exist_ok=True)

    print("1/3 Каталог коллекций…")
    collections = await load_catalog(client, stats, args.delay)
    total = sum(c["on_resale"] for c in collections)
    print(f"  Коллекций на маркете: {len(collections)}, лотов всего: {total:,}")
    for c in collections[:10]:
        print(f"    {c['title']:<24} {c['on_resale']:>7,} лотов, флор {c['floor_stars']}⭐")

    print(f"\n2/3 Горячий скан (1 страница × {len(collections)} коллекций)…")
    hot_time, page_sizes = await hot_scan(client, collections, args.limit, stats, args.delay)
    max_page = max(page_sizes) if page_sizes else 0
    print(f"  Цикл: {hot_time:.1f} c, реальный размер страницы: до {max_page}")

    full_done, listings, full_time = [], {}, 0.0
    if args.full_budget and not stats.stopped:
        print(f"\n3/3 Полный обход (бюджет {args.full_budget} запросов)…")
        req_before = stats.requests
        full_time, listings, full_done = await full_scan(
            client, collections, args.limit, args.full_budget, stats, args.delay)
        used = stats.requests - req_before
        print(f"  Собрано лотов: {len(listings):,} за {used} запросов, {full_time:.1f} c")

    # оценки
    per_page = max_page or args.limit
    full_requests_needed = sum(-(-c["on_resale"] // per_page) for c in collections)
    s = stats.summary()
    req_time = statistics.mean(stats.latencies) + args.delay if stats.latencies else 0
    estimate = {
        "total_listings": total,
        "collections": len(collections),
        "page_size": per_page,
        "hot_cycle_sec": round(hot_time, 1),
        "full_cycle_requests": full_requests_needed,
        "full_cycle_sec_one_account": round(full_requests_needed * req_time, 1),
    }

    report = {"args": vars(args), "estimate": estimate, "stats": s,
              "top_collections": collections[:30], "full_scan": full_done}
    (OUT_DIR / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    (OUT_DIR / "snapshot.json").write_text(json.dumps(listings, ensure_ascii=False))

    print("\n===== ИТОГ =====")
    print(f"Лотов на маркете:            {total:,} в {len(collections)} коллекциях")
    print(f"Горячий цикл (1 аккаунт):    {hot_time:.1f} c")
    print(f"Полный цикл, запросов:       {full_requests_needed:,}")
    print(f"Полный цикл, оценка времени: {estimate['full_cycle_sec_one_account']} c на 1 аккаунте")
    print(f"Средняя задержка запроса:    {s['latency_avg_ms']} мс (p95 {s['latency_p95_ms']} мс)")
    print(f"FLOOD_WAIT:                  {s['flood_waits'] or 'не было'}")
    if stats.stopped:
        print(f"Остановлен:                  {stats.stopped} (повторно с этого аккаунта — не раньше, чем через это время)")
    print(f"\nОтчёт: {OUT_DIR / 'report.json'}  — скинь его мне")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
