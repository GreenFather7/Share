from datetime import datetime, timezone
from types import SimpleNamespace as NS

import pytest

from gmw.collectors.telegram import owner_of, price_of
from gmw.differ import GiftState, diff_changes, diff_full
from gmw.models import EventType, Listing

TS = datetime(2026, 10, 4, tzinfo=timezone.utc)


def L(slug, price=100, owner="u1", ton=None, ton_only=None):
    return Listing(slug, 1, int(slug.split("-")[1]), price, ton, owner, ton_only=ton_only)


def types(events):
    return [(e.type, e.slug) for e in events]


def test_new_listing_and_price_change():
    prev = {"A-1": L("A-1", 100)}
    cur = {"A-1": L("A-1", 90), "A-2": L("A-2", 50)}
    ev = diff_changes(prev, cur, "tg", TS)
    assert types(ev) == [(EventType.PRICE_CHANGED, "A-1"), (EventType.LISTED, "A-2")]
    assert (ev[0].prev_price_stars, ev[0].price_stars) == (100, 90)


def test_fx_only_change_is_not_a_price_change():
    """Находка GPT №3: звёзды те же, TON-котировка пересчиталась по курсу → это НЕ смена цены продавцом."""
    ev = diff_changes({"A-1": L("A-1", 100, ton=1.0)}, {"A-1": L("A-1", 100, ton=1.1)}, "tg", TS)
    assert ev == []


def test_ton_only_price_change_is_detected():
    prev = {"A-1": L("A-1", None, ton=1.5, ton_only=True)}
    assert types(diff_changes(prev, {"A-1": L("A-1", None, ton=1.6, ton_only=True)}, "tg", TS)) == \
        [(EventType.PRICE_CHANGED, "A-1")]
    # Звёздная котировка у TON-only лота — вторичная: её дрейф не событие.
    prev = {"A-1": L("A-1", 375, ton=1.5, ton_only=True)}
    assert diff_changes(prev, {"A-1": L("A-1", 380, ton=1.5, ton_only=True)}, "tg", TS) == []


def test_switch_to_ton_only_is_a_price_change():
    ev = diff_changes({"A-1": L("A-1", 100, ton=0.4)}, {"A-1": L("A-1", 100, ton=0.4, ton_only=True)}, "tg", TS)
    assert types(ev) == [(EventType.PRICE_CHANGED, "A-1")]


def test_stars_nanos_are_kept():
    """Находка GPT №3: StarsAmount(amount=100, nanos=500000000) → 100.5, а не 100."""
    StarsAmount = type("StarsAmount", (), {})
    StarsTonAmount = type("StarsTonAmount", (), {})
    a = StarsAmount(); a.amount, a.nanos = 100, 500_000_000
    t = StarsTonAmount(); t.amount = 1_250_000_000
    assert price_of(NS(resell_amount=[a, t])) == (100.5, 1.25)
    b = StarsAmount(); b.amount, b.nanos = 100, 0
    assert price_of(NS(resell_amount=[b])) == (100, None)


def test_owner_name_is_not_an_identity():
    """Находка GPT №2: отображаемое имя не идентификатор — не подставляем его вместо владельца."""
    assert owner_of(NS(owner_id=None, owner_name="Dima", owner_address=None)) is None
    assert owner_of(NS(owner_id=None, owner_name="Dima", owner_address="EQabc")) == "ton:EQabc"
    assert owner_of(NS(owner_id=NS(user_id=42))) == "u42"


def test_partial_scan_ignores_disappeared():
    assert diff_changes({"A-1": L("A-1")}, {}, "tg", TS) == []


def test_owner_change_on_listed_lot_is_not_called_sold():
    """Находка GPT №2: смена владельца — это не доказанная продажа (может быть передача)."""
    ev = diff_changes({"A-1": L("A-1", 100, "u1")}, {"A-1": L("A-1", 150, "u2")}, "tg", TS)
    assert types(ev) == [(EventType.OWNER_CHANGED, "A-1"), (EventType.LISTED, "A-1")]
    assert (ev[0].from_owner, ev[0].to_owner, ev[0].price_stars) == ("u1", "u2", 100)


@pytest.mark.asyncio
async def test_full_scan_resolves_disappeared():
    prev = {"A-1": L("A-1", 100, "u1"), "A-2": L("A-2", 200, "u1"), "A-3": L("A-3", 300),
            "A-4": L("A-4"), "A-5": L("A-5", 500)}
    states = {"A-1": GiftState("u9", False), "A-2": GiftState("u1", False),
              "A-3": GiftState("u1", True, L("A-3", 300)),
              "A-4": None,
              "A-5": GiftState("u1", True, L("A-5", 450))}

    async def resolve(slug):
        return states[slug]

    ev, snap = await diff_full(prev, {}, resolve, "tg", TS)
    by_slug = {e.slug: e for e in ev}
    assert by_slug["A-1"].type == EventType.OWNER_CHANGED and by_slug["A-1"].to_owner == "u9"
    assert by_slug["A-2"].type == EventType.DELISTED
    assert "A-3" not in by_slug and snap["A-3"].price_stars == 300  # всё ещё на продаже, цена та же
    assert by_slug["A-4"].type == EventType.GONE  # гифт по slug не найден — так и говорим
    # Находка GPT №4: проверка видела новую цену — берём её, а не старый лот.
    assert by_slug["A-5"].type == EventType.PRICE_CHANGED and snap["A-5"].price_stars == 450
    assert set(snap) == {"A-3", "A-5"}


@pytest.mark.asyncio
async def test_unknown_resolution_propagates():
    """Ошибка точечной проверки — это «неизвестно»: исключение, а не delisted."""
    async def resolve(slug):
        raise TimeoutError

    with pytest.raises(TimeoutError):
        await diff_full({"A-1": L("A-1")}, {}, resolve, "tg", TS)


def test_event_id_is_deterministic():
    a = diff_changes({}, {"A-1": L("A-1")}, "tg", TS)[0]
    b = diff_changes({}, {"A-1": L("A-1")}, "tg", TS)[0]
    assert a.id == b.id
    assert a.id != diff_changes({}, {"A-1": L("A-1", 101)}, "tg", TS)[0].id
