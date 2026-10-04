"""Общие модели: лот на маркете и событие. Всё, что ходит по шине и лежит в базе, — это они."""

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum

_EVENT_NS = uuid.UUID("5b0c1c1e-6a8e-4f53-9d55-6f2f7b1e2a10")


class EventType(str, Enum):
    LISTED = "listed"                # выставили на продажу
    PRICE_CHANGED = "price_changed"  # сменили цену
    DELISTED = "delisted"            # сняли с продажи (владелец тот же)
    SOLD = "sold"                    # купили (сменился владелец, цена = последняя выставленная)
    TRANSFER = "transfer"            # передали вне маркета
    MINTED = "minted"                # новый NFT (апгрейд)
    BURNED = "burned"                # сожгли / скрафтили


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Listing:
    """Лот на маркете в момент снимка."""
    slug: str
    collection_id: int
    num: int | None = None
    price_stars: int | None = None
    price_ton: float | None = None
    owner: str | None = None

    def same_price(self, other: "Listing") -> bool:
        return (self.price_stars, self.price_ton) == (other.price_stars, other.price_ton)


@dataclass(frozen=True)
class Event:
    type: EventType
    source: str
    slug: str
    collection_id: int | None = None
    num: int | None = None
    price_stars: int | None = None
    price_ton: float | None = None
    prev_price_stars: int | None = None
    prev_price_ton: float | None = None
    from_owner: str | None = None  # продавец / тот, от кого ушёл гифт
    to_owner: str | None = None    # покупатель / получатель
    ts: datetime = field(default_factory=utcnow)

    @property
    def id(self) -> uuid.UUID:
        """Детерминированный id: одно и то же событие, пришедшее дважды, не задвоится в базе."""
        key = "|".join(str(x) for x in (
            self.source, self.type.value, self.slug, self.price_stars, self.price_ton,
            self.from_owner, self.to_owner, self.ts.isoformat()))
        return uuid.uuid5(_EVENT_NS, key)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["type"] = self.type.value
        d["ts"] = self.ts.isoformat()
        d["id"] = str(self.id)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        d = {k: v for k, v in d.items() if k != "id"}
        d["type"] = EventType(d["type"])
        d["ts"] = datetime.fromisoformat(d["ts"])
        return cls(**d)


@dataclass(frozen=True)
class Collection:
    id: int
    title: str
    on_resale: int = 0
    floor_stars: int | None = None
