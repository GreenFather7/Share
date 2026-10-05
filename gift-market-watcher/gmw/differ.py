"""Сравнение снимков маркета → события.

Два режима:
  * частичный (горячий скан или незавершённый обход): ловим listed / price_changed / owner_changed
    по тем лотам, что видим; пропавшие НЕ трогаем — они могли просто уехать на другую страницу;
  * полный (обход коллекции завершён и сверен): пропавшие лоты проверяем точечно.

Чего мы НЕ утверждаем: смена владельца — это не обязательно продажа (бывает передача или подарок),
поэтому событие называется owner_changed, а его цена — последняя цена продавца, а не цена сделки.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime

from .models import Event, EventType, Listing


@dataclass(frozen=True)
class GiftState:
    """Что сейчас известно о гифте по точечному запросу.

    `listing` — текущий лот (если гифт выставлен): с ним сразу видна и свежая цена.
    """
    owner: str | None
    on_sale: bool
    listing: Listing | None = None


# None — гифт по slug не найден. Любая другая неизвестность — исключение (снимок не трогаем, повторим позже).
Resolver = Callable[[str], Awaitable[GiftState | None]]


def _ev(type_: EventType, l: Listing, source: str, ts: datetime, **kw) -> Event:
    base = dict(collection_id=l.collection_id, num=l.num, price_stars=l.price_stars, price_ton=l.price_ton,
                from_owner=l.owner, ts=ts, ton_only=l.ton_only, **l.attrs())
    base.update(kw)
    return Event(type_, source, l.slug, **base)


def _owner_changed(p: Listing, new_owner: str, source: str, ts: datetime) -> Event:
    return _ev(EventType.OWNER_CHANGED, p, source, ts, from_owner=p.owner, to_owner=new_owner)


def diff_changes(prev: Mapping[str, Listing], cur: Mapping[str, Listing],
                 source: str, ts: datetime) -> list[Event]:
    """Новые лоты, смены цены продавцом и смены владельца у видимых лотов. Безопасно для частичного снимка."""
    events = []
    for slug, c in cur.items():
        p = prev.get(slug)
        if p is None:
            events.append(_ev(EventType.LISTED, c, source, ts))
        elif p.owner and c.owner and p.owner != c.owner:
            # Пока мы не смотрели, гифт ушёл к другому владельцу и снова выставлен.
            events.append(_owner_changed(p, c.owner, source, ts))
            events.append(_ev(EventType.LISTED, c, source, ts))
        elif not p.same_price(c):
            events.append(_ev(EventType.PRICE_CHANGED, c, source, ts,
                              prev_price_stars=p.price_stars, prev_price_ton=p.price_ton))
    return events


async def diff_full(prev: Mapping[str, Listing], cur: Mapping[str, Listing], resolve: Resolver,
                    source: str, ts: datetime) -> tuple[list[Event], dict[str, Listing]]:
    """Завершённый полный обход коллекции. Возвращает события и новый снимок."""
    events = diff_changes(prev, cur, source, ts)
    snapshot = dict(cur)
    for slug in sorted(prev.keys() - cur.keys()):
        p = prev[slug]
        state = await resolve(slug)  # исключение = неизвестно → пробрасываем, снимок не меняется
        if state is None:
            events.append(_ev(EventType.GONE, p, source, ts))
        elif state.on_sale:
            # Всё ещё на продаже — проскочил между страницами. Берём свежие факты точечной проверки.
            fresh = state.listing or p
            fresh = replace(fresh, collection_id=p.collection_id, num=fresh.num or p.num,
                            model=fresh.model or p.model, backdrop=fresh.backdrop or p.backdrop,
                            pattern=fresh.pattern or p.pattern)
            events.extend(diff_changes({slug: p}, {slug: fresh}, source, ts))
            snapshot[slug] = fresh
        elif state.owner and p.owner and state.owner != p.owner:
            events.append(_owner_changed(p, state.owner, source, ts))
        else:
            events.append(_ev(EventType.DELISTED, p, source, ts))
    return events, snapshot


def quote_updates(prev: Mapping[str, Listing], cur: Mapping[str, Listing]) -> list[Listing]:
    """Лоты, у которых цена продавца та же, а котировки другие (пересчёт по курсу).

    Это не событие, но база и API должны видеть актуальные котировки.
    """
    out = []
    for slug, c in cur.items():
        p = prev.get(slug)
        if (p is not None and p.same_price(c) and not (p.owner and c.owner and p.owner != c.owner)
                and (p.price_stars, p.price_ton) != (c.price_stars, c.price_ton)):
            out.append(c)
    return out
