"""Весь конвейер на настоящих Postgres и Redis: фейковый маркет → шина → нормализатор → база → API.

Запускается, если заданы GMW_TEST_DATABASE_URL и GMW_TEST_REDIS_URL (иначе пропускается).
"""

import asyncio
import os

import httpx
import pytest
import pytest_asyncio

from gmw.api import create_app
from gmw.bus import STREAM, RedisBus, RedisLive
from gmw.collectors.fake import FakeMarket
from gmw.collectors.market import MarketCollector
from gmw.storage import Storage
from gmw.worker import handle_batch

DB = os.getenv("GMW_TEST_DATABASE_URL")
REDIS = os.getenv("GMW_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not (DB and REDIS), reason="нужны GMW_TEST_DATABASE_URL и GMW_TEST_REDIS_URL")


@pytest_asyncio.fixture
async def env():
    import redis.asyncio as aioredis
    r = aioredis.from_url(REDIS, decode_responses=True)
    await r.flushdb()
    st = await Storage.connect(DB)
    await st.pool.execute("DROP TABLE IF EXISTS events, listings, gifts, collections")
    await st.init()
    bus = RedisBus(r)
    await bus.ensure_group()
    yield st, bus, r
    await st.close()
    await r.aclose()


async def pump(bus, st, live):
    n = 0
    while msgs := await bus.read(block_ms=50):
        n += await handle_batch(msgs, st, live)
        await bus.ack([m for m, _ in msgs])
    return n


@pytest.mark.asyncio
async def test_end_to_end(env):
    st, bus, r = env
    live = RedisLive(r)
    market = FakeMarket(collections=2, lots=80, seed=7)
    col = MarketCollector(market, bus, page_limit=25)
    await col.refresh_catalog()
    for cid in list(col.sched):
        await col.full(cid)
    assert await pump(bus, st, live) == 0  # посев — без событий
    assert (await st.stats())["listings"] == len(market.lots)

    received = []

    async def listen():
        async for e in live.subscribe():
            received.append(e)

    listener = asyncio.create_task(listen())
    await asyncio.sleep(0.1)

    market.mutate(40)
    for cid in list(col.sched):
        await col.hot(cid)
        await col.full(cid)
    new = await pump(bus, st, live)
    assert new > 0
    await asyncio.sleep(0.2)
    listener.cancel()
    assert len(received) == new

    # Проекция совпадает с маркетом.
    stats = await st.stats()
    assert stats["listings"] == len(market.lots)
    assert stats["events_total"] == new

    # Повторная доставка тех же сообщений не задваивает события.
    raw = await r.xrange(STREAM)
    import json
    again = [(mid, json.loads(f["m"])) for mid, f in raw]
    assert await handle_batch(again, st, live) == 0

    # Рестарт сборщика: снимок из базы, без ложных событий.
    snap = await st.load_listings("fake")
    col2 = MarketCollector(market, bus, snap, page_limit=25)
    await col2.refresh_catalog()
    for cid in list(col2.sched):
        await col2.full(cid)
    assert await pump(bus, st, live) == 0

    # API читает из базы.
    app = create_app(st, live)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as http:
        events = (await http.get("/events", params={"limit": 500})).json()
        assert len(events) == new
        assert all(e["collection_title"] for e in events)
        sold = (await http.get("/events", params={"type": "sold"})).json()
        assert all(e["type"] == "sold" and e["to_owner"] for e in sold)
        floors = (await http.get("/floors")).json()
        assert {f["collection_id"] for f in floors} == {c.id for c in market.cols}
        slug = events[0]["slug"]
        gift = (await http.get(f"/gifts/{slug}")).json()
        assert gift["events"][0]["slug"] == slug
        assert (await http.get("/gifts/nope-1")).status_code == 404
        assert (await http.get("/events", params={"type": "bogus"})).status_code == 422

    # С токеном: без него — 401, /health открыт.
    app = create_app(st, live, token="s3cret")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as http:
        assert (await http.get("/stats")).status_code == 401
        assert (await http.get("/stats", headers={"Authorization": "Bearer nope"})).status_code == 401
        assert (await http.get("/stats", headers={"Authorization": "Bearer s3cret"})).status_code == 200
        assert (await http.get("/floors", params={"token": "s3cret"})).status_code == 200
        assert (await http.get("/health")).status_code == 200
