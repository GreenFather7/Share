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
    OWNER_CHANGED = "owner_changed"  # лот ушёл к другому владельцу: продажа ИЛИ передача — без подтверждения не знаем
    SOLD = "sold"                    # продажа с подтверждением сделки (пока не выдаётся: нужен источник подтверждения)
    GONE = "gone"                    # лот пропал, а гифт по slug не находится (сожгли / скрафтили / иное)
    TRANSFER = "transfer"            # передали вне маркета
    MINTED = "minted"                # новый NFT (апгрейд)
    BURNED = "burned"                # сожгли / скрафтили


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Listing:
    """Лот на маркете в момент снимка.

    Цена продавца — «родная»: TON, если лот продаётся только за TON (`ton_only`), иначе звёзды.
    Вторая котировка — пересчёт по курсу; её изменение не считается сменой цены продавцом.
    """
    slug: str
    collection_id: int
    num: int | None = None
    price_stars: float | None = None  # звёзды (с дробной частью из StarsAmount.nanos, если есть)
    price_ton: float | None = None
    owner: str | None = None
    model: str | None = None     # атрибуты NFT: модель, фон, узор
    backdrop: str | None = None
    pattern: str | None = None
    ton_only: bool | None = None

    def attrs(self) -> dict:
        return {"model": self.model, "backdrop": self.backdrop, "pattern": self.pattern}

    def native_price(self) -> tuple[str, float | None]:
        """("TON" | "XTR" | "?", сумма). "?" — валюту продавца из ответа не определить (нет признака и звёзд)."""
        if self.ton_only:
            return "TON", self.price_ton
        if self.price_stars is not None:
            return "XTR", self.price_stars
        return "?", self.price_ton

    def same_price(self, other: "Listing") -> bool:
        return self.native_price() == other.native_price()


@dataclass(frozen=True)
class Event:
    type: EventType
    source: str
    slug: str
    collection_id: int | None = None
    num: int | None = None
    price_stars: float | None = None
    price_ton: float | None = None
    prev_price_stars: float | None = None
    prev_price_ton: float | None = None
    from_owner: str | None = None  # продавец / тот, от кого ушёл гифт
    to_owner: str | None = None    # покупатель / получатель
    ts: datetime = field(default_factory=utcnow)
    model: str | None = None
    backdrop: str | None = None
    pattern: str | None = None
    ton_only: bool | None = None

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
