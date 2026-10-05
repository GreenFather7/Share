"""Сборщик маркета: параллельный планировщик горячих и полных сканов поверх любого источника (MarketAPI).

* Горячий скан — первая страница коллекции (сервер сортирует по времени изменения цены, новые сверху).
  Частота адаптивная: нашли изменения — смотрим чаще, тишина — реже. Это снимок «сейчас», а не журнал:
  промежуточные изменения между двумя снимками не видны.
* Полный скан — все страницы в стабильном порядке (по номеру). Обход сверяется: повтор курсора, пустая страница
  с курсором, лимит страниц или сильное расхождение с количеством лотов → обход НЕ завершён, и пропавшие лоты
  мы не трогаем (обрабатываем как частичный снимок). Только завершённый обход проверяет пропавших.
* Первый завершённый полный скан коллекции не порождает событий: это «посев» текущего состояния.
* Сначала события уходят в шину (атомарно), и только потом обновляется снимок: если шина упала,
  то же изменение будет найдено и отправлено в следующий раз.
* Параллельность: `run(workers=N)` — N задач одновременно (по смыслу — сколько аккаунтов), но по одной коллекции
  одновременно идёт не больше одной задачи, а полных сканов — не больше `max_full`, чтобы они не съели горячие.
"""

import asyncio
import logging
import math
import time
from collections import Counter
from dataclasses import dataclass
from typing import Protocol

from ..bus import Bus, publish_collection, publish_events
from ..differ import GiftState, diff_changes, diff_full
from ..models import Collection, Listing, utcnow

log = logging.getLogger(__name__)


@dataclass
class Page:
    listings: list[Listing]
    next_offset: str | None
    count: int | None = None  # сколько лотов всего в выборке по словам сервера (если отдаёт)


class MarketAPI(Protocol):
    source: str

    async def catalog(self) -> list[Collection]: ...
    async def page(self, collection_id: int, offset: str, limit: int, sort: str = "recent") -> Page:
        """sort="recent" — по времени изменения цены (новые сверху), для горячего скана;
        sort="num" — по номеру гифта: порядок не плывёт, пока листаем, — для полного обхода."""
    async def gift_state(self, slug: str) -> GiftState | None: ...


@dataclass
class _Sched:
    hot_interval: float
    next_hot: float
    next_full: float


@dataclass
class ScanResult:
    listings: dict[str, Listing]
    complete: bool
    reason: str = ""
    pages: int = 0
    duplicates: int = 0


class MarketCollector:
    def __init__(self, api: MarketAPI, bus: Bus, snapshot: dict[int, dict[str, Listing]] | None = None, *,
                 page_limit: int = 100, hot_min: float = 5, hot_max: float = 120,
                 full_interval: float = 600, catalog_interval: float = 300, count_tolerance: float = 0.02,
                 clock=time.monotonic):
        self.api, self.bus = api, bus
        self.snapshot = snapshot if snapshot is not None else {}
        self.page_limit = page_limit
        self.hot_min, self.hot_max = hot_min, hot_max
        self.full_interval, self.catalog_interval = full_interval, catalog_interval
        self.count_tolerance = count_tolerance
        self.clock = clock
        self.collections: dict[int, Collection] = {}
        self.sched: dict[int, _Sched] = {}
        self.next_catalog = 0.0
        self.stats: Counter = Counter()
        self._busy: set = set()
        self._full_running = 0
        self._wake: asyncio.Event | None = None  # будит свободные потоки, когда расписание поменялось

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
                self.sched[cid] = _Sched(self.hot_min, now if seeded else math.inf,
                                         now + self.full_interval if seeded else now)
        for cid in set(self.sched) - watched:
            del self.sched[cid]
        self.collections = cols
        self.next_catalog = now + self.catalog_interval

    async def hot(self, cid: int) -> int:
        page = await self.api.page(cid, "", self.page_limit, "recent")
        cur = {l.slug: l for l in page.listings}
        prev = self.snapshot.setdefault(cid, {})
        events = diff_changes(prev, cur, self.api.source, utcnow())
        await publish_events(self.bus, events)  # упадёт — снимок не тронут
        prev.update(cur)
        s = self.sched.get(cid)
        if s:
            s.hot_interval = (max(self.hot_min, s.hot_interval / 2) if events
                              else min(self.hot_max, s.hot_interval * 1.5))
            s.next_hot = self.clock() + s.hot_interval
        self.stats["hot"] += 1
        return len(events)

    def _max_pages(self, cid: int) -> int:
        expected = self.collections[cid].on_resale if cid in self.collections else 0
        return math.ceil(max(expected, 1) * 1.5 / self.page_limit) + 10

    async def scan_all(self, cid: int) -> ScanResult:
        """Полный обход по номеру с проверками. Никогда не зацикливается."""
        cur: dict[str, Listing] = {}
        seen = {""}
        offset, pages, dupes, count = "", 0, 0, None
        max_pages = self._max_pages(cid)
        while True:
            page = await self.api.page(cid, offset, self.page_limit, "num")
            pages += 1
            if count is None and page.count is not None:
                count = page.count
            for l in page.listings:
                dupes += l.slug in cur
                cur[l.slug] = l
            nxt = page.next_offset
            if not nxt:
                break
            if not page.listings:
                return ScanResult(cur, False, "пустая страница, а курсор есть", pages, dupes)
            if nxt in seen:
                return ScanResult(cur, False, f"курсор повторился на странице {pages}", pages, dupes)
            if pages >= max_pages:
                return ScanResult(cur, False, f"больше {max_pages} страниц", pages, dupes)
            seen.add(nxt)
            offset = nxt
        ref = count if count is not None else (self.collections[cid].on_resale if cid in self.collections else None)
        if ref and len(cur) < ref * (1 - self.count_tolerance) - 5:
            return ScanResult(cur, False, f"собрано {len(cur)} из ~{ref}", pages, dupes)
        return ScanResult(cur, True, "", pages, dupes)

    async def full(self, cid: int) -> int:
        seed_ts = utcnow()  # момент начала обхода: всё, что изменилось позже, новее снимка
        scan = await self.scan_all(cid)
        s = self.sched.get(cid)
        now = self.clock()
        if not scan.complete:
            self.stats["full_incomplete"] += 1
            log.warning("полный обход %s не завершён (%s) — пропавшие лоты не трогаем", cid, scan.reason)
            if s:
                s.next_full = now + self.hot_max  # попробуем скоро ещё раз
            if cid not in self.snapshot:
                return 0  # посев только по завершённому обходу
            prev = self.snapshot[cid]
            events = diff_changes(prev, scan.listings, self.api.source, utcnow())
            await publish_events(self.bus, events)
            prev.update(scan.listings)
            return len(events)

        self.stats["full_complete"] += 1
        if s:
            s.next_full = now + self.full_interval
        if cid not in self.snapshot:
            await self.bus.publish("seed", {"source": self.api.source, "collection_id": cid, "ts": seed_ts.isoformat(),
                                            "listings": [l.__dict__ for l in scan.listings.values()]})
            self.snapshot[cid] = scan.listings
            if s:
                s.next_hot = now
            return 0
        events, snapshot = await diff_full(self.snapshot[cid], scan.listings, self.api.gift_state,
                                           self.api.source, utcnow())
        await publish_events(self.bus, events)  # сначала шина…
        self.snapshot[cid] = snapshot           # …потом снимок
        return len(events)

    # ---------- планировщик ----------

    def _tasks(self) -> list[tuple[float, str, int | None]]:
        tasks = [(self.next_catalog, "catalog", None)]
        for cid, s in self.sched.items():
            tasks += [(s.next_full, "full", cid), (s.next_hot, "hot", cid)]
        return sorted(tasks, key=lambda t: (t[0], t[1] != "hot"))  # при равенстве — горячий первым

    async def _execute(self, kind: str, cid: int | None) -> int:
        try:
            if kind == "catalog":
                await self.refresh_catalog()
                return 0
            return await (self.hot(cid) if kind == "hot" else self.full(cid))
        except Exception as e:  # noqa: BLE001
            # Снимок не тронут (он обновляется только после успешной отправки) — повторим позже.
            self.stats[f"{kind}_error"] += 1
            log.exception("%s %s: %s", kind, cid, e)
            retry = self.clock() + self.hot_max
            if kind == "catalog":
                self.next_catalog = retry
            elif (s := self.sched.get(cid)) is not None:
                if kind == "hot":
                    s.next_hot = retry
                else:
                    s.next_full = retry
            return 0

    async def step(self) -> tuple[str, int | None, int]:
        """Выполнить ближайшую задачу (последовательный режим, удобно для тестов)."""
        due, kind, cid = self._tasks()[0]
        wait = due - self.clock()
        if wait > 0:
            await asyncio.sleep(wait)
        return kind, cid, await self._execute(kind, cid)

    def _claim(self, max_full: int) -> tuple[str, int | None] | float:
        """Взять ближайшую готовую задачу, которую можно запустить. Иначе — сколько ждать до следующей."""
        now = self.clock()
        for due, kind, cid in self._tasks():
            key = "catalog" if kind == "catalog" else cid
            if key in self._busy or (kind == "full" and self._full_running >= max_full):
                continue
            if due > now:
                return due - now
            self._busy.add(key)
            self._full_running += kind == "full"
            return kind, cid
        return 1.0

    async def _worker(self, max_full: int) -> None:
        while True:
            claimed = self._claim(max_full)
            if not isinstance(claimed, tuple):
                self._wake.clear()  # между _claim и clear нет await — пробуждение не потеряется
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=min(max(claimed, 0.01), 1.0))
                except asyncio.TimeoutError:
                    pass
                continue
            kind, cid = claimed
            try:
                n = await self._execute(kind, cid)
            finally:
                self._busy.discard("catalog" if kind == "catalog" else cid)
                self._full_running -= kind == "full"
                self._wake.set()
            if n:
                title = self.collections[cid].title if cid in self.collections else cid
                log.info("%s %s: %d событий", kind, title, n)

    async def run(self, workers: int = 1, max_full: int | None = None) -> None:
        max_full = max_full if max_full is not None else max(1, workers // 4)
        self._wake = asyncio.Event()
        log.info("сборщик: %d параллельных задач, полных сканов одновременно до %d", workers, max_full)
        await asyncio.gather(*(self._worker(max_full) for _ in range(workers)))
