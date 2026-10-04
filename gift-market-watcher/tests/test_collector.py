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
