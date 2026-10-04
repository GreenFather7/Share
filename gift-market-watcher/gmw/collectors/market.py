"""Сборщик маркета: планировщик горячих и полных сканов поверх любого источника (MarketAPI).

* Горячий скан — первая страница коллекции (сервер сортирует по времени изменения цены, новые сверху).
  Частота адаптивная: нашли изменения — смотрим чаще, тишина — реже.
* Полный скан — все страницы коллекции, реже. Только он ловит продажи и снятия.
* Первый полный скан коллекции не порождает событий: это «посев» текущего состояния.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Protocol

from ..bus import Bus, publish_collection, publish_events
from ..differ import GiftState, diff_changes, diff_full
from ..models import Collection, Listing, utcnow

log = logging.getLogger(__name__)


class MarketAPI(Protocol):
    source: str

    async def catalog(self) -> list[Collection]: ...
    async def page(self, collection_id: int, offset: str, limit: int) -> tuple[list[Listing], str | None]: ...
    async def gift_state(self, slug: str) -> GiftState | None: ...


@dataclass
class _Sched:
    hot_interval: float
    next_hot: float
    next_full: float


class MarketCollector:
    def __init__(self, api: MarketAPI, bus: Bus, snapshot: dict[int, dict[str, Listing]] | None = None, *,
                 page_limit: int = 100, hot_min: float = 5, hot_max: float = 120,
                 full_interval: float = 600, catalog_interval: float = 300, clock=time.monotonic):
        self.api, self.bus = api, bus
        self.snapshot = snapshot if snapshot is not None else {}
        self.page_limit = page_limit
        self.hot_min, self.hot_max = hot_min, hot_max
        self.full_interval, self.catalog_interval = full_interval, catalog_interval
        self.clock = clock
        self.collections: dict[int, Collection] = {}
        self.sched: dict[int, _Sched] = {}
        self.next_catalog = 0.0

    # ---------- задачи ----------

    async def refresh_catalog(self) -> None:
        now = self.clock()
        cols = {c.id: c for c in await self.api.catalog()}
        for c in cols.values():
            await publish_collection(self.bus, c)
        # Следим за коллекциями с лотами и за теми, где у нас ещё что-то числится (чтобы поймать уход в ноль).
        watched = {cid for cid, c in cols.items() if c.on_resale} | {cid for cid, s in self.snapshot.items() if s}
        for cid in watched:
            if cid not in self.sched:
                seeded = cid in self.snapshot
                self.sched[cid] = _Sched(self.hot_min, now if seeded else float("inf"),
                                         now + self.full_interval if seeded else now)
        for cid in set(self.sched) - watched:
            del self.sched[cid]
        self.collections = cols
        self.next_catalog = now + self.catalog_interval

    async def hot(self, cid: int) -> int:
        listings, _ = await self.api.page(cid, "", self.page_limit)
        cur = {l.slug: l for l in listings}
        prev = self.snapshot.setdefault(cid, {})
        events = diff_changes(prev, cur, self.api.source, utcnow())
        prev.update(cur)
        await publish_events(self.bus, events)
        s = self.sched[cid]
        s.hot_interval = max(self.hot_min, s.hot_interval / 2) if events else min(self.hot_max, s.hot_interval * 1.5)
        s.next_hot = self.clock() + s.hot_interval
        return len(events)

    async def full(self, cid: int) -> int:
        cur: dict[str, Listing] = {}
        seed_ts = utcnow()  # момент начала обхода: всё, что изменилось позже, новее снимка
        offset = ""
        while True:
            listings, offset = await self.api.page(cid, offset, self.page_limit)
            cur.update((l.slug, l) for l in listings)
            if not offset or not listings:
                break
        s = self.sched[cid]
        now = self.clock()
        s.next_full = now + self.full_interval
        if cid not in self.snapshot:
            self.snapshot[cid] = cur
            await self.bus.publish("seed", {"source": self.api.source, "collection_id": cid, "ts": seed_ts.isoformat(),
                                            "listings": [l.__dict__ for l in cur.values()]})
            s.next_hot = now
            return 0
        events, self.snapshot[cid] = await diff_full(self.snapshot[cid], cur, self.api.gift_state,
                                                     self.api.source, utcnow())
        await publish_events(self.bus, events)
        return len(events)

    # ---------- планировщик ----------

    def _due(self) -> tuple[float, str, int | None]:
        best = (self.next_catalog, "catalog", None)
        for cid, s in self.sched.items():
            best = min(best, (s.next_full, "full", cid), (s.next_hot, "hot", cid))
        return best

    async def step(self) -> tuple[str, int | None, int]:
        """Выполнить ближайшую задачу. Возвращает (вид, коллекция, число событий)."""
        due, kind, cid = self._due()
        wait = due - self.clock()
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            if kind == "catalog":
                await self.refresh_catalog()
                return kind, None, 0
            n = await (self.hot(cid) if kind == "hot" else self.full(cid))
            return kind, cid, n
        except Exception as e:  # noqa: BLE001
            # Снимок не тронут — просто повторим позже.
            log.exception("%s %s: %s", kind, cid, e)
            retry = self.clock() + self.hot_max
            if kind == "catalog":
                self.next_catalog = retry
            elif cid in self.sched:
                s = self.sched[cid]
                if kind == "hot":
                    s.next_hot = retry
                else:
                    s.next_full = retry
            return kind, cid, 0

    async def run(self) -> None:
        while True:
            kind, cid, n = await self.step()
            if n:
                title = self.collections.get(cid).title if cid in self.collections else cid
                log.info("%s %s: %d событий", kind, title, n)
