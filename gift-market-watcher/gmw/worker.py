"""Нормализатор: читает шину, пишет в базу, раздаёт новые события в живую ленту."""

import asyncio
import logging
from datetime import datetime

from .bus import Bus, Live
from .models import Collection, Event, Listing
from .storage import Storage

log = logging.getLogger(__name__)


async def handle_batch(msgs: list[tuple[str, dict]], storage: Storage, live: Live) -> int:
    """Обработать пачку сообщений с сохранением порядка. Возвращает число новых событий."""
    total = 0
    pending: list[Event] = []

    async def flush():
        nonlocal total
        if pending:
            new = await storage.process(pending)
            total += len(new)
            await live.publish([e.to_dict() for e in new])
            pending.clear()

    for _, m in msgs:
        kind, data = m["kind"], m["data"]
        if kind == "event":
            pending.append(Event.from_dict(data))
            continue
        await flush()
        if kind == "collection":
            await storage.upsert_collection(Collection(**data))
        elif kind == "quotes":
            await storage.update_quotes(data["source"], datetime.fromisoformat(data["ts"]), data["listings"])
        elif kind == "seed":
            await storage.seed_listings(data["source"], data["collection_id"],
                                        [Listing(**l) for l in data["listings"]], datetime.fromisoformat(data["ts"]))
        else:
            log.warning("неизвестное сообщение: %s", kind)
    await flush()
    return total


async def run_worker(bus: Bus, storage: Storage, live: Live, stop: asyncio.Event | None = None) -> None:
    while not (stop and stop.is_set()):
        msgs = await bus.read()
        if not msgs:
            continue
        n = await handle_batch(msgs, storage, live)
        await bus.ack([mid for mid, _ in msgs])
        if n:
            log.info("записано %d новых событий", n)
