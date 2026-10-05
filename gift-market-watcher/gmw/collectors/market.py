"""Сборщик маркета: параллельный планировщик горячих и полных сканов поверх любого источника (MarketAPI).

* Горячий скан — первая страница коллекции (сервер сортирует по времени изменения цены, новые сверху).
  Частота адаптивная: нашли изменения — смотрим чаще, тишина — реже. Это снимок «сейчас», а не журнал:
  промежуточные изменения между двумя снимками не видны.
* Полный скан — все страницы в стабильном порядке (по номеру). Обход сверяется: повтор курсора, пустая страница
  с курсором, лимит страниц или сильное расхождение с количеством лотов → обход НЕ завершён, и пропавшие лоты
  мы не трогаем (обрабатываем как частичный снимок). Только завершённый обход проверяет пропавших.
* Первый завершённый полный скан коллекции не порождает событий: это «посев» текущего состояния.
* Доставка: новый снимок и сообщения скана фиксируются одной транзакцией в локальном состоянии (gmw/state.py),
  потом отправляются из outbox до подтверждения. Сбой шины, потерянный ответ шины или рестарт сборщика
  не теряют и не размножают уже найденные события (повтор несёт тот же id).
* Изменение только котировок (пересчёт TON по курсу) — не событие, а сообщение "quotes": база обновляет цены лота.
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

from ..bus import Bus
from ..differ import GiftState, diff_changes, diff_full, quote_updates
from ..models import Collection, Listing, utcnow
from ..state import CollectorState

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
    status: str = "complete"  # complete | repeated_cursor | empty_page_with_cursor | page_budget | duplicates |
                              # order | count_drift | short
    reason: str = ""
    pages: int = 0
    duplicates: int = 0
    counts: tuple[int, ...] = ()


def _tolerance(n: int, share: float) -> int:
    return math.ceil(n * share)


class MarketCollector:
    def __init__(self, api: MarketAPI, bus: Bus, snapshot: dict[int, dict[str, Listing]] | None = None, *,
                 state: CollectorState | None = None, page_limit: int = 100, hot_min: float = 5,
                 hot_max: float = 120, full_interval: float = 600, catalog_interval: float = 300,
                 count_tolerance: float = 0.02, max_duplicates: int = 0, clock=time.monotonic):
        """`snapshot` — снимок из Postgres; используется, только если локальное состояние пустое (первый запуск)."""
        self.api, self.bus = api, bus
        self.state = state or CollectorState()
        if self.state.is_empty() and snapshot:
            self.state.import_snapshot(snapshot)
        self.snapshot = self.state.load_snapshot()
        self.page_limit = page_limit
        self.hot_min, self.hot_max = hot_min, hot_max
        self.full_interval, self.catalog_interval = full_interval, catalog_interval
        self.count_tolerance, self.max_duplicates = count_tolerance, max_duplicates
        self.clock = clock
        self.collections: dict[int, Collection] = {}
        self.sched: dict[int, _Sched] = {}
        self.next_catalog = 0.0
        self.stats: Counter = Counter()
        self._busy: set = set()
        self._full_running = 0
        self._wake: asyncio.Event | None = None  # будит свободные потоки, когда расписание поменялось
        self._flush_lock = asyncio.Lock()

    # ---------- фиксация и доставка ----------

    def _commit(self, cid: int | None, messages: list[tuple[str, dict]], *, upserts: dict[str, Listing] | None = None,
                replace: bool = False) -> None:
        """Снимок + сообщения — одной транзакцией в локальное состояние, затем то же в память."""
        self.state.commit(cid, upserts, (), messages, replace=replace)
        if cid is not None:
            if replace:
                self.snapshot[cid] = dict(upserts or {})
            else:
                self.snapshot.setdefault(cid, {}).update(upserts or {})

    async def flush(self) -> int:
        """Отправить накопленное в outbox. Сбой шины — не ошибка задачи: сообщения ждут следующей попытки."""
        sent = 0
        async with self._flush_lock:
            while batch := self.state.pending():
                try:
                    await self.bus.publish_batch([(kind, data) for _, kind, data in batch])
                except Exception as e:  # noqa: BLE001
                    self.stats["flush_error"] += 1
                    log.warning("шина недоступна (%s): в очереди %d сообщений, повторим", e, self.state.outbox_size())
                    break
                self.state.delivered([seq for seq, _, _ in batch])
                sent += len(batch)
        return sent

    @staticmethod
    def _messages(events, quotes: list[Listing], source: str) -> list[tuple[str, dict]]:
        msgs = [("event", e.to_dict()) for e in events]
        if quotes:
            msgs.append(("quotes", {"source": source, "ts": utcnow().isoformat(),
                                    "listings": [{"slug": l.slug, "price_stars": l.price_stars,
                                                  "price_ton": l.price_ton} for l in quotes]}))
        return msgs

    # ---------- задачи ----------

    async def refresh_catalog(self) -> None:
        now = self.clock()
        cols = {c.id: c for c in await self.api.catalog()}
        self._commit(None, [("collection", c.__dict__) for c in cols.values()])
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
        await self.flush()

    def _partial(self, cid: int, cur: dict[str, Listing]) -> int:
        """Частичный снимок (горячий скан или незавершённый обход): только видимые лоты, пропавших не трогаем."""
        prev = self.snapshot.get(cid, {})
        events = diff_changes(prev, cur, self.api.source, utcnow())
        self._commit(cid, self._messages(events, quote_updates(prev, cur), self.api.source), upserts=cur)
        return len(events)

    async def hot(self, cid: int) -> int:
        page = await self.api.page(cid, "", self.page_limit, "recent")
        n = self._partial(cid, {l.slug: l for l in page.listings})
        s = self.sched.get(cid)
        if s:
            s.hot_interval = (max(self.hot_min, s.hot_interval / 2) if n
                              else min(self.hot_max, s.hot_interval * 1.5))
            s.next_hot = self.clock() + s.hot_interval
        self.stats["hot"] += 1
        await self.flush()
        return n

    def _max_pages(self, cid: int) -> int:
        expected = self.collections[cid].on_resale if cid in self.collections else 0
        return math.ceil(max(expected, 1) * 1.5 / self.page_limit) + 10

    async def scan_all(self, cid: int) -> ScanResult:
        """Полный обход по номеру с проверками согласованности. Никогда не зацикливается.

        complete=True только если: цепочка курсоров без повторов, нет пустых страниц с курсором, дублей не больше
        `max_duplicates`, номера идут по возрастанию, количество от сервера не «плыло» сильнее допуска
        и собрано не меньше (последнее количество − допуск). Даже тогда это не атомарный снимок живого рынка.
        """
        cur: dict[str, Listing] = {}
        seen = {""}
        offset, pages, dupes, last_num = "", 0, 0, None
        counts: list[int] = []
        max_pages = self._max_pages(cid)

        def result(complete, status, reason=""):
            return ScanResult(cur, complete, status, reason, pages, dupes, tuple(counts))

        while True:
            page = await self.api.page(cid, offset, self.page_limit, "num")
            pages += 1
            if page.count is not None:
                counts.append(page.count)
            for l in page.listings:
                if l.slug in cur:
                    dupes += 1
                if l.num is not None and last_num is not None and l.num < last_num:
                    return result(False, "order", f"номер {l.num} после {last_num} на странице {pages}")
                if l.num is not None:
                    last_num = l.num
                cur[l.slug] = l
            nxt = page.next_offset
            if not nxt:
                break
            if not page.listings:
                return result(False, "empty_page_with_cursor", f"пустая страница {pages}, а курсор есть")
            if nxt in seen:
                return result(False, "repeated_cursor", f"курсор повторился на странице {pages}")
            if pages >= max_pages:
                return result(False, "page_budget", f"больше {max_pages} страниц")
            seen.add(nxt)
            offset = nxt

        if dupes > self.max_duplicates:
            return result(False, "duplicates", f"{dupes} повторов лотов")
        if counts:
            tol = _tolerance(max(counts), self.count_tolerance)
            if max(counts) - min(counts) > tol:
                return result(False, "count_drift", f"количество плыло {min(counts)}…{max(counts)}")
            ref = counts[-1]
        else:
            ref = self.collections[cid].on_resale if cid in self.collections else 0
            tol = _tolerance(ref, self.count_tolerance)
        if len(cur) < ref - tol:
            return result(False, "short", f"собрано {len(cur)} из {ref}")
        return result(True, "complete")

    async def full(self, cid: int) -> int:
        seed_ts = utcnow()  # момент начала обхода: всё, что изменилось позже, новее снимка
        scan = await self.scan_all(cid)
        s = self.sched.get(cid)
        now = self.clock()
        self.stats[f"full_{scan.status}"] += 1
        if not scan.complete:
            self.stats["full_incomplete"] += 1
            log.warning("полный обход %s не завершён: %s (%s) — пропавшие лоты не трогаем", cid, scan.status, scan.reason)
            if s:
                s.next_full = now + self.hot_max  # попробуем скоро ещё раз
            if cid not in self.snapshot:
                return 0  # посев только по завершённому обходу
            n = self._partial(cid, scan.listings)
            await self.flush()
            return n

        if s:
            s.next_full = now + self.full_interval
        if cid not in self.snapshot:
            self._commit(cid, [("seed", {"source": self.api.source, "collection_id": cid, "ts": seed_ts.isoformat(),
                                         "listings": [l.__dict__ for l in scan.listings.values()]})],
                         upserts=scan.listings, replace=True)
            if s:
                s.next_hot = now
            await self.flush()
            return 0
        prev = self.snapshot[cid]
        events, snapshot = await diff_full(prev, scan.listings, self.api.gift_state, self.api.source, utcnow())
        self._commit(cid, self._messages(events, quote_updates(prev, snapshot), self.api.source),
                     upserts=snapshot, replace=True)
        await self.flush()
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
            # Ошибка источника до фиксации: снимок и outbox не тронуты — повторим позже.
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
