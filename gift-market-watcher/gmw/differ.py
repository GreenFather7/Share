"""Сравнение снимков маркета → события.

Два режима:
  * частичный (горячий скан, только первая страница): ловим listed / price_changed,
    пропавшие лоты НЕ трогаем — они могли просто уехать на следующую страницу;
  * полный (вся коллекция): пропавшие лоты проверяем точечно и решаем — sold или delisted.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from .models import Event, EventType, Listing


@dataclass(frozen=True)
class GiftState:
    """Что сейчас известно о гифте по точечному запросу."""
    owner: str | None
    on_sale: bool


Resolver = Callable[[str], Awaitable[GiftState | None]]


def _listed(cur: Listing, source: str, ts: datetime) -> Event:
    return Event(EventType.LISTED, source, cur.slug, cur.collection_id, cur.num,
                 cur.price_stars, cur.price_ton, from_owner=cur.owner, ts=ts, **cur.attrs())


def diff_changes(prev: Mapping[str, Listing], cur: Mapping[str, Listing],
                 source: str, ts: datetime) -> list[Event]:
    """Новые лоты и смены цены. Безопасно для частичного снимка."""
    events = []
    for slug, c in cur.items():
        p = prev.get(slug)
        if p is None:
            events.append(_listed(c, source, ts))
        elif p.owner and c.owner and p.owner != c.owner:
            # Купили и тут же перевыставили, пока мы не смотрели: продажа + новый листинг.
            events.append(Event(EventType.SOLD, source, slug, c.collection_id, c.num,
                                p.price_stars, p.price_ton, from_owner=p.owner, to_owner=c.owner, ts=ts,
                                **c.attrs()))
            events.append(_listed(c, source, ts))
        elif not p.same_price(c):
            events.append(Event(EventType.PRICE_CHANGED, source, slug, c.collection_id, c.num,
                                c.price_stars, c.price_ton, p.price_stars, p.price_ton,
                                from_owner=c.owner, ts=ts, **c.attrs()))
    return events


async def diff_full(prev: Mapping[str, Listing], cur: Mapping[str, Listing], resolve: Resolver,
                    source: str, ts: datetime) -> tuple[list[Event], dict[str, Listing]]:
    """Полный снимок коллекции. Возвращает события и новый снимок."""
    events = diff_changes(prev, cur, source, ts)
    snapshot = dict(cur)
    for slug in prev.keys() - cur.keys():
        p = prev[slug]
        state = await resolve(slug)
        if state is not None and state.on_sale:
            snapshot[slug] = p  # всё ещё на продаже — проскочил между страницами
            continue
        if state is not None and state.owner and p.owner and state.owner != p.owner:
            events.append(Event(EventType.SOLD, source, slug, p.collection_id, p.num,
                                p.price_stars, p.price_ton, from_owner=p.owner, to_owner=state.owner, ts=ts,
                                **p.attrs()))
        else:
            events.append(Event(EventType.DELISTED, source, slug, p.collection_id, p.num,
                                p.price_stars, p.price_ton, from_owner=p.owner, ts=ts, **p.attrs()))
    return events, snapshot
