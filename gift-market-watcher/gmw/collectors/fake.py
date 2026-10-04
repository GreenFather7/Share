"""Фейковый маркет: живёт сам по себе (листинги, смены цен, продажи, снятия).

Нужен, чтобы гонять весь конвейер локально и в тестах без Telegram-аккаунтов.
"""

import asyncio
import random
from dataclasses import dataclass, replace

from ..differ import GiftState
from ..models import Collection, Listing

NAMES = ["Plush Pepe", "Durov's Cap", "Timeless Book", "Chill Flame", "Happy Brownie", "Swiss Watch"]


@dataclass
class _Lot:
    listing: Listing
    changed: int  # «время» последнего изменения цены


class FakeMarket:
    source = "fake"

    def __init__(self, collections: int = 3, lots: int = 150, seed: int | None = None):
        self.rng = random.Random(seed)
        self.cols = [Collection(1000 + i, NAMES[i % len(NAMES)]) for i in range(collections)]
        self.owners: dict[str, str] = {}
        self.lots: dict[str, _Lot] = {}
        self.clock = 0
        self.next_num = {c.id: 1 for c in self.cols}
        for _ in range(lots):
            self._list_new(self.rng.choice(self.cols).id)

    # ---------- жизнь маркета ----------

    def _list_new(self, cid: int) -> None:
        self.clock += 1
        num = self.next_num[cid]
        self.next_num[cid] += 1
        slug = f"{self._title(cid).replace(' ', '').replace(chr(39), '')}-{num}"
        owner = self.owners.setdefault(slug, f"u{self.rng.randint(1, 10_000)}")
        self.lots[slug] = _Lot(Listing(slug, cid, num, self.rng.randint(100, 5000), None, owner), self.clock)

    def _title(self, cid: int) -> str:
        return next(c.title for c in self.cols if c.id == cid)

    def mutate(self, n: int = 1) -> None:
        for _ in range(n):
            self.clock += 1
            r = self.rng.random()
            if r < 0.4 or not self.lots:
                self._list_new(self.rng.choice(self.cols).id)
                continue
            slug = self.rng.choice(list(self.lots))
            lot = self.lots[slug]
            if r < 0.75:
                price = max(1, int(lot.listing.price_stars * self.rng.uniform(0.8, 1.2)))
                self.lots[slug] = _Lot(replace(lot.listing, price_stars=price), self.clock)
            elif r < 0.9:
                del self.lots[slug]
                self.owners[slug] = f"u{self.rng.randint(1, 10_000)}"  # купили
            else:
                del self.lots[slug]  # сняли

    async def run_chaos(self, per_second: float = 5) -> None:
        while True:
            await asyncio.sleep(1 / per_second)
            self.mutate()

    # ---------- MarketAPI ----------

    async def catalog(self) -> list[Collection]:
        out = []
        for c in self.cols:
            prices = [l.listing.price_stars for l in self.lots.values() if l.listing.collection_id == c.id]
            out.append(replace(c, on_resale=len(prices), floor_stars=min(prices) if prices else None))
        return out

    async def page(self, collection_id: int, offset: str, limit: int) -> tuple[list[Listing], str | None]:
        lots = sorted((l for l in self.lots.values() if l.listing.collection_id == collection_id),
                      key=lambda l: l.changed, reverse=True)
        start = int(offset or 0)
        chunk = lots[start:start + limit]
        nxt = str(start + limit) if start + limit < len(lots) else None
        return [l.listing for l in chunk], nxt

    async def gift_state(self, slug: str) -> GiftState | None:
        if slug not in self.owners:
            return None
        return GiftState(self.owners[slug], slug in self.lots)
