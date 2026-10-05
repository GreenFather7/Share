"""Шина между сборщиками и нормализатором + живая лента для API.

Шина — Redis Streams с consumer group: если нормализатор упал, непрочитанное и
неподтверждённое никуда не денется. Живая лента — Redis pub/sub (пропустил — не страшно,
всё есть в базе). Для тестов есть in-memory версии с тем же интерфейсом.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Protocol

from .models import Collection, Event

STREAM = "gmw:bus"
GROUP = "normalizer"
LIVE_CHANNEL = "gmw:live"

Message = tuple[str, dict]  # (id, {"kind": ..., "data": ...})


class Bus(Protocol):
    async def publish(self, kind: str, data: dict) -> None: ...
    async def publish_batch(self, messages: list[tuple[str, dict]]) -> None:
        """Атомарно: либо все сообщения попали в шину, либо ни одно."""
    async def read(self, count: int = 500, block_ms: int = 1000) -> list[Message]: ...
    async def ack(self, ids: list[str]) -> None: ...


async def publish_events(bus: Bus, events: list[Event]) -> None:
    if events:
        await bus.publish_batch([("event", e.to_dict()) for e in events])


async def publish_collection(bus: Bus, c: Collection) -> None:
    await bus.publish("collection", c.__dict__)


class RedisBus:
    # maxlen — защита Redis от бесконечного роста. Если нормализатор лежит так долго, что в шине накопился
    # миллион сообщений, самые старые будут обрезаны: сборщик их уже не повторит (outbox считает их доставленными).
    def __init__(self, redis, consumer: str = "worker-1", maxlen: int = 1_000_000):
        self.r = redis
        self.consumer = consumer
        self.maxlen = maxlen
        self._pending_drained = False

    async def ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except Exception as e:  # noqa: BLE001
            if "BUSYGROUP" not in str(e):
                raise

    async def publish(self, kind: str, data: dict) -> None:
        await self.publish_batch([(kind, data)])

    async def publish_batch(self, messages: list[tuple[str, dict]]) -> None:
        async with self.r.pipeline(transaction=True) as pipe:  # MULTI/EXEC
            for kind, data in messages:
                payload = json.dumps({"kind": kind, "data": data}, ensure_ascii=False)
                pipe.xadd(STREAM, {"m": payload}, maxlen=self.maxlen, approximate=True)
            await pipe.execute()

    async def read(self, count: int = 500, block_ms: int = 1000) -> list[Message]:
        # После рестарта сначала дочитываем то, что взяли, но не подтвердили.
        if not self._pending_drained:
            # Забираем себе зависшее у других (упавших) обработчиков дольше минуты, потом дочитываем своё.
            try:
                await self.r.xautoclaim(STREAM, GROUP, self.consumer, min_idle_time=60_000, start_id="0-0",
                                        count=count)
            except Exception as e:  # noqa: BLE001
                if "NOGROUP" in str(e):
                    raise
            msgs = await self._read("0", count, None)
            if msgs:
                return msgs
            self._pending_drained = True
        return await self._read(">", count, block_ms)

    async def _read(self, start: str, count: int, block_ms: int | None) -> list[Message]:
        res = await self.r.xreadgroup(GROUP, self.consumer, {STREAM: start}, count=count, block=block_ms)
        return [(_s(mid), json.loads(fields[b"m" if b"m" in fields else "m"]))
                for _, entries in (res or []) for mid, fields in entries]

    async def ack(self, ids: list[str]) -> None:
        if ids:
            await self.r.xack(STREAM, GROUP, *ids)


class MemoryBus:
    def __init__(self):
        self._q: asyncio.Queue[Message] = asyncio.Queue()
        self._n = 0
        self.acked: list[str] = []

    async def publish(self, kind: str, data: dict) -> None:
        await self.publish_batch([(kind, data)])

    async def publish_batch(self, messages: list[tuple[str, dict]]) -> None:
        encoded = [json.loads(json.dumps({"kind": k, "data": d})) for k, d in messages]  # сначала всё, потом кладём
        for m in encoded:
            self._n += 1
            self._q.put_nowait((str(self._n), m))

    async def read(self, count: int = 500, block_ms: int = 1000) -> list[Message]:
        msgs = []
        try:
            msgs.append(await asyncio.wait_for(self._q.get(), block_ms / 1000))
            while len(msgs) < count and not self._q.empty():
                msgs.append(self._q.get_nowait())
        except asyncio.TimeoutError:
            pass
        return msgs

    async def ack(self, ids: list[str]) -> None:
        self.acked.extend(ids)


# ---------- живая лента ----------

class Live(Protocol):
    async def publish(self, events: list[dict]) -> None: ...
    def subscribe(self) -> AsyncIterator[dict]: ...


class RedisLive:
    def __init__(self, redis):
        self.r = redis

    async def publish(self, events: list[dict]) -> None:
        for e in events:
            await self.r.publish(LIVE_CHANNEL, json.dumps(e, ensure_ascii=False, default=str))

    async def subscribe(self) -> AsyncIterator[dict]:
        pubsub = self.r.pubsub()
        await pubsub.subscribe(LIVE_CHANNEL)
        try:
            async for msg in pubsub.listen():
                if msg.get("type") == "message":
                    yield json.loads(msg["data"])
        finally:
            await pubsub.unsubscribe(LIVE_CHANNEL)
            await pubsub.aclose()


class MemoryLive:
    def __init__(self):
        self._subs: set[asyncio.Queue] = set()

    async def publish(self, events: list[dict]) -> None:
        for q in self._subs:
            for e in events:
                q.put_nowait(e)

    async def subscribe(self) -> AsyncIterator[dict]:
        q: asyncio.Queue = asyncio.Queue()
        self._subs.add(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._subs.discard(q)


def _s(x) -> str:
    return x.decode() if isinstance(x, bytes) else x
