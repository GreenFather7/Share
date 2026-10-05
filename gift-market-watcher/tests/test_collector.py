import pytest

from gmw.bus import MemoryBus
from gmw.collectors.fake import FakeMarket
from gmw.collectors.market import MarketCollector
from gmw.models import EventType


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


async def drain(bus):
    out = []
    while msgs := await bus.read(block_ms=10):
        out.extend(m for _, m in msgs)
    return out


@pytest.mark.asyncio
async def test_seed_then_hot_then_full():
    market = FakeMarket(collections=2, lots=120, seed=1)
    bus, clock = MemoryBus(), Clock()
    col = MarketCollector(market, bus, page_limit=20, hot_min=5, hot_max=60, full_interval=100, clock=clock)

    await col.refresh_catalog()
    # Первые полные сканы — посев без событий.
    for cid in list(col.sched):
        assert await col.full(cid) == 0
    msgs = await drain(bus)
    assert {m["kind"] for m in msgs} == {"collection", "seed"}
    seeded = sum(len(m["data"]["listings"]) for m in msgs if m["kind"] == "seed")
    assert seeded == len(market.lots)

    market.mutate(30)
    hot_events = 0
    for cid in list(col.sched):
        hot_events += await col.hot(cid)
    hot = [m["data"] for m in await drain(bus) if m["kind"] == "event"]
    assert hot_events == len(hot) > 0
    assert {e["type"] for e in hot} <= {"listed", "price_changed", "sold"}

    for cid in list(col.sched):
        await col.full(cid)
    full = [m["data"] for m in await drain(bus) if m["kind"] == "event"]
    # После полного скана снимок совпадает с маркетом, а пропавшие лоты разобраны на sold/delisted.
    assert {e["type"] for e in full} <= {t.value for t in EventType}
    for cid in col.sched:
        assert set(col.snapshot[cid]) == {s for s, l in market.lots.items() if l.listing.collection_id == cid}


@pytest.mark.asyncio
async def test_adaptive_hot_interval():
    market = FakeMarket(collections=1, lots=10, seed=2)
    clock = Clock()
    col = MarketCollector(market, MemoryBus(), hot_min=5, hot_max=60, clock=clock)
    await col.refresh_catalog()
    cid = next(iter(col.sched))
    await col.full(cid)
    await col.hot(cid)  # тишина → интервал растёт
    assert col.sched[cid].hot_interval == 7.5
    market.mutate(5)
    await col.hot(cid)  # активность → интервал падает
    assert col.sched[cid].hot_interval == 5


@pytest.mark.asyncio
async def test_step_picks_due_task_and_survives_errors():
    market = FakeMarket(collections=1, lots=5, seed=3)
    clock = Clock()
    col = MarketCollector(market, MemoryBus(), clock=clock)
    assert (await col.step())[0] == "catalog"
    assert (await col.step())[0] == "full"

    async def broken(*a):
        raise RuntimeError("сеть упала")
    market.page = broken
    kind, cid, n = await col.step()
    assert (kind, n) == ("hot", 0)
    assert col.sched[cid].next_hot == clock.t + col.hot_max


@pytest.mark.asyncio
async def test_full_scan_is_stable_while_market_moves():
    """Маркет меняется прямо во время листания. По «recent» страницы съезжают и лоты теряются,
    по номеру — полный обход видит каждый лот, который жил всё время обхода."""
    def moving(market, sort_override=None):
        orig = market.page

        async def page(cid, offset, limit, sort="recent"):
            res = await orig(cid, offset, limit, sort_override or sort)
            for slug in list(market.lots)[:3]:  # трое меняют цену → всплывают наверх «recent»
                lot = market.lots[slug]
                market.clock += 1
                market.lots[slug] = type(lot)(lot.listing, market.clock)
            return res
        market.page = page

    def run(sort_override):
        market = FakeMarket(collections=1, lots=300, seed=5)
        stable = set(market.lots)
        moving(market, sort_override)
        col = MarketCollector(market, MemoryBus(), page_limit=20, clock=Clock())
        return market, col, stable

    market, col, stable = run(None)  # по умолчанию полный обход идёт по номеру
    await col.refresh_catalog()
    cid = next(iter(col.sched))
    await col.full(cid)
    assert stable <= set(col.snapshot[cid])

    market, col, stable = run("recent")  # контрольный: так было бы по времени изменения
    await col.refresh_catalog()
    await col.full(cid)
    assert not stable <= set(col.snapshot[cid])


# ---------- находки ревью GPT ----------

class ScriptedMarket:
    """Источник, страницы которого задаются вручную: для проверки защит полного обхода."""
    source = "scripted"

    def __init__(self, pages, count=None, on_resale=3):
        self.pages, self.count, self.on_resale, self.calls = pages, count, on_resale, 0

    async def catalog(self):
        from gmw.models import Collection
        return [Collection(1, "C", self.on_resale)]

    async def page(self, cid, offset, limit, sort="recent"):
        from gmw.collectors.market import Page
        self.calls += 1
        listings, nxt = self.pages(offset)
        return Page(listings, nxt, self.count)

    async def gift_state(self, slug):
        raise AssertionError("незавершённый обход не должен проверять пропавших")


def lot(n, price=100):
    from gmw.models import Listing
    return Listing(f"C-{n}", 1, n, price, None, "u1")


async def seeded(market, lots):
    col = MarketCollector(market, MemoryBus(), {1: {l.slug: l for l in lots}}, clock=Clock())
    await col.refresh_catalog()
    return col


@pytest.mark.asyncio
async def test_repeated_cursor_does_not_loop_and_does_not_delist():
    """Находка №4: один и тот же непустой курсор раньше зацикливал сборщик."""
    market = ScriptedMarket(lambda off: ([lot(1)], "same-cursor"))
    col = await seeded(market, [lot(1), lot(2), lot(3)])
    scan = await col.scan_all(1)
    assert not scan.complete and "курсор повторился" in scan.reason
    assert market.calls == 2
    assert await col.full(1) == 0  # C-2, C-3 не видны, но и не «сняты»
    assert set(col.snapshot[1]) == {"C-1", "C-2", "C-3"}
    assert col.stats["full_incomplete"] == 1


@pytest.mark.asyncio
async def test_empty_page_with_cursor_is_incomplete():
    market = ScriptedMarket(lambda off: ([lot(1)], "next") if off == "" else ([], "more"))
    col = await seeded(market, [lot(1), lot(2)])
    scan = await col.scan_all(1)
    assert not scan.complete and "пустая страница" in scan.reason


@pytest.mark.asyncio
async def test_scan_far_below_server_count_is_incomplete():
    market = ScriptedMarket(lambda off: ([lot(1)], None), count=500, on_resale=500)
    col = await seeded(market, [lot(1), lot(2)])
    scan = await col.scan_all(1)
    assert not scan.complete and "собрано 1 из ~500" in scan.reason


@pytest.mark.asyncio
async def test_page_budget_stops_endless_cursors():
    market = ScriptedMarket(lambda off: ([lot(int(off or 0) + 1)], str(int(off or 0) + 1)), on_resale=3)
    col = await seeded(market, [lot(1)])
    scan = await col.scan_all(1)
    assert not scan.complete and "страниц" in scan.reason
    assert market.calls == col._max_pages(1)


@pytest.mark.asyncio
async def test_publish_failure_does_not_lose_the_change():
    """Находка №6: раньше снимок обновлялся до отправки — после сбоя шины событие терялось навсегда."""
    market = FakeMarket(collections=1, lots=20, seed=11)
    bus = MemoryBus()
    col = MarketCollector(market, bus, clock=Clock())
    await col.refresh_catalog()
    cid = next(iter(col.sched))
    await col.full(cid)
    await drain(bus)
    market.mutate(5)

    real = bus.publish_batch

    async def broken(messages):
        raise ConnectionError("Redis упал")
    bus.publish_batch = broken
    assert await col._execute("hot", cid) == 0  # ошибка поймана
    assert await drain(bus) == []

    bus.publish_batch = real
    n = await col.hot(cid)
    assert n > 0 and len([m for m in await drain(bus) if m["kind"] == "event"]) == n


@pytest.mark.asyncio
async def test_fx_drift_produces_no_events():
    market = FakeMarket(collections=1, lots=50, seed=12)
    col = MarketCollector(market, MemoryBus(), clock=Clock())
    await col.refresh_catalog()
    cid = next(iter(col.sched))
    await col.full(cid)
    market.fx_drift()
    assert await col.hot(cid) == 0
    assert await col.full(cid) == 0


@pytest.mark.asyncio
async def test_workers_run_in_parallel_but_never_twice_per_collection():
    """Находка №1: раньше сборщик выполнял задачи строго по одной."""
    import asyncio

    market = FakeMarket(collections=6, lots=120, seed=13)
    active: dict[int, int] = {}
    peak = {"total": 0}
    orig = market.page

    async def slow_page(cid, offset, limit, sort="recent"):
        active[cid] = active.get(cid, 0) + 1
        assert active[cid] == 1, "две задачи одной коллекции одновременно"
        peak["total"] = max(peak["total"], sum(active.values()))
        await asyncio.sleep(0.02)
        active[cid] -= 1
        return await orig(cid, offset, limit, sort)
    market.page = slow_page

    col = MarketCollector(market, MemoryBus(), page_limit=20, hot_min=0.01, hot_max=0.05, full_interval=0.3)
    runner = asyncio.create_task(col.run(workers=4, max_full=2))
    await asyncio.sleep(1.0)
    runner.cancel()
    assert peak["total"] >= 3
    assert col.stats["hot"] > 10 and col.stats["full_complete"] >= 6
