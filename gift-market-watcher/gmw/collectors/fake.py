"""Фейковый маркет: живёт сам по себе (листинги, смены цен, продажи, снятия).

Нужен, чтобы гонять весь конвейер локально и в тестах без Telegram-аккаунтов.
"""

import asyncio
import random
from dataclasses import dataclass, replace

from ..differ import GiftState
from ..models import Collection, Listing
from .market import Page

NAMES = ["Plush Pepe", "Durov's Cap", "Timeless Book", "Chill Flame", "Happy Brownie", "Swiss Watch"]
MODELS = ["Unfinished", "Dragon Age", "Mystique", "Ice Nine", "Bull Run"]
BACKDROPS = ["Electric Indigo", "Turquoise", "Onyx Black", "Sapphire", "Roman Silver", "Malachite"]
PATTERNS = ["Stars", "Hearts", "Spiral", "Waves"]


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
        stars = self.rng.randint(100, 5000)
        ton_only = self.rng.random() < 0.1
        self.lots[slug] = _Lot(Listing(slug, cid, num, None if ton_only else stars, round(stars * 0.004, 2), owner,
                                       self.rng.choice(MODELS), self.rng.choice(BACKDROPS),
                                       self.rng.choice(PATTERNS), ton_only), self.clock)

    def _title(self, cid: int) -> str:
        return next(c.title for c in self.cols if c.id == cid)

    def mutate(self, n: int = 1) -> None:
        for _ in range(n):
            self.clock += 1
            r = self.rng.random()
            if r < 0.35 or not self.lots:
                self._list_new(self.rng.choice(self.cols).id)
                continue
            if r < 0.42:
                self.fx_drift()
                continue
            slug = self.rng.choice(list(self.lots))
            lot = self.lots[slug]
            if r < 0.72:  # продавец сменил цену
                l = lot.listing
                if l.ton_only:
                    new = replace(l, price_ton=round(l.price_ton * self.rng.uniform(0.8, 1.2), 2))
                else:
                    stars = max(1, int(l.price_stars * self.rng.uniform(0.8, 1.2)))
                    new = replace(l, price_stars=stars, price_ton=round(stars * 0.004, 2))
                self.lots[slug] = _Lot(new, self.clock)
            elif r < 0.9:
                del self.lots[slug]
                self.owners[slug] = f"u{self.rng.randint(1, 10_000)}"  # купили
            else:
                del self.lots[slug]  # сняли

    def fx_drift(self) -> None:
        """Сменился курс: TON-котировки звёздных лотов пересчитались. Продавцы цены не меняли."""
        k = self.rng.uniform(0.97, 1.03)
        for slug, lot in self.lots.items():
            l = lot.listing
            if not l.ton_only:
                self.lots[slug] = _Lot(replace(l, price_ton=round(l.price_ton * k, 2)), lot.changed)

    async def run_chaos(self, per_second: float = 5) -> None:
        while True:
            await asyncio.sleep(1 / per_second)
            self.mutate()

    # ---------- MarketAPI ----------

    async def catalog(self) -> list[Collection]:
        out = []
        for c in self.cols:
            lots = [l.listing for l in self.lots.values() if l.listing.collection_id == c.id]
            stars = [l.price_stars for l in lots if l.price_stars is not None]
            out.append(replace(c, on_resale=len(lots), floor_stars=min(stars) if stars else None))
        return out

    async def page(self, collection_id: int, offset: str, limit: int, sort: str = "recent") -> Page:
        lots = [l for l in self.lots.values() if l.listing.collection_id == collection_id]
        if sort == "num":
            lots.sort(key=lambda l: l.listing.num)
        else:
            lots.sort(key=lambda l: l.changed, reverse=True)
        start = int(offset or 0)
        chunk = lots[start:start + limit]
        nxt = str(start + limit) if start + limit < len(lots) else None
        return Page([l.listing for l in chunk], nxt, len(lots))

    async def gift_state(self, slug: str) -> GiftState | None:
        if slug not in self.owners:
            return None
        lot = self.lots.get(slug)
        return GiftState(self.owners[slug], lot is not None, lot.listing if lot else None)
