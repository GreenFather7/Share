from datetime import datetime, timezone

import pytest

from gmw.differ import GiftState, diff_changes, diff_full
from gmw.models import EventType, Listing

TS = datetime(2026, 10, 4, tzinfo=timezone.utc)


def L(slug, price=100, owner="u1", ton=None):
    return Listing(slug, 1, int(slug.split("-")[1]), price, ton, owner)


def types(events):
    return [(e.type, e.slug) for e in events]


def test_new_listing_and_price_change():
    prev = {"A-1": L("A-1", 100)}
    cur = {"A-1": L("A-1", 90), "A-2": L("A-2", 50)}
    ev = diff_changes(prev, cur, "tg", TS)
    assert types(ev) == [(EventType.PRICE_CHANGED, "A-1"), (EventType.LISTED, "A-2")]
    assert (ev[0].prev_price_stars, ev[0].price_stars) == (100, 90)


def test_ton_price_change_is_detected():
    ev = diff_changes({"A-1": L("A-1", None, ton=1.5)}, {"A-1": L("A-1", None, ton=1.6)}, "tg", TS)
    assert types(ev) == [(EventType.PRICE_CHANGED, "A-1")]


def test_partial_scan_ignores_disappeared():
    assert diff_changes({"A-1": L("A-1")}, {}, "tg", TS) == []


def test_sold_and_relisted_between_scans():
    ev = diff_changes({"A-1": L("A-1", 100, "u1")}, {"A-1": L("A-1", 150, "u2")}, "tg", TS)
    assert types(ev) == [(EventType.SOLD, "A-1"), (EventType.LISTED, "A-1")]
    assert (ev[0].from_owner, ev[0].to_owner, ev[0].price_stars) == ("u1", "u2", 100)


@pytest.mark.asyncio
async def test_full_scan_resolves_disappeared():
    prev = {"A-1": L("A-1", 100, "u1"), "A-2": L("A-2", 200, "u1"), "A-3": L("A-3"), "A-4": L("A-4")}
    states = {"A-1": GiftState("u9", False), "A-2": GiftState("u1", False),
              "A-3": GiftState("u1", True), "A-4": None}

    async def resolve(slug):
        return states[slug]

    ev, snap = await diff_full(prev, {}, resolve, "tg", TS)
    by_slug = {e.slug: e for e in ev}
    assert by_slug["A-1"].type == EventType.SOLD and by_slug["A-1"].to_owner == "u9"
    assert by_slug["A-2"].type == EventType.DELISTED
    assert "A-3" not in by_slug and "A-3" in snap  # всё ещё на продаже
    assert by_slug["A-4"].type == EventType.DELISTED  # гифт пропал совсем
    assert set(snap) == {"A-3"}


def test_event_id_is_deterministic():
    a = diff_changes({}, {"A-1": L("A-1")}, "tg", TS)[0]
    b = diff_changes({}, {"A-1": L("A-1")}, "tg", TS)[0]
    assert a.id == b.id
    assert a.id != diff_changes({}, {"A-1": L("A-1", 101)}, "tg", TS)[0].id
