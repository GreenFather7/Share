"""Источник «маркет Telegram» через MTProto (Telethon) и пул аккаунтов."""

from telethon import TelegramClient, errors, functions

from ..accounts import AccountPool
from ..differ import GiftState
from ..models import Collection, Listing
from .market import Page


def _num(x: float) -> float | int:
    return int(x) if float(x).is_integer() else x


def price_of(gift) -> tuple[float | None, float | None]:
    """Цена лота: звёзды и TON. Новая схема — resell_amount (StarsAmount / StarsTonAmount), старая — resell_stars.

    StarsAmount = amount + nanos/1e9 (дробь не теряем). StarsTonAmount.amount — в нанотонах.
    """
    stars, ton = None, None
    for a in getattr(gift, "resell_amount", None) or []:
        if "Ton" in type(a).__name__:
            ton = _num(a.amount / 1e9)
        else:
            stars = _num(a.amount + (getattr(a, "nanos", 0) or 0) / 1e9)
    if stars is None and ton is None and getattr(gift, "resell_stars", None):
        stars = int(gift.resell_stars)
    return stars, ton


def owner_of(gift) -> str | None:
    """Устойчивый идентификатор владельца. Отображаемое имя (owner_name) — НЕ идентификатор: не используем."""
    peer = getattr(gift, "owner_id", None)
    if peer is not None:
        for attr, prefix in (("user_id", "u"), ("channel_id", "c"), ("chat_id", "g")):
            if hasattr(peer, attr):
                return f"{prefix}{getattr(peer, attr)}"
        return str(peer)
    address = getattr(gift, "owner_address", None)  # гифт выведен в TON — адрес и есть владелец
    return f"ton:{address}" if address else None


def listing_of(gift, collection_id: int) -> Listing:
    stars, ton = price_of(gift)
    return Listing(gift.slug, collection_id, getattr(gift, "num", None), stars, ton, owner_of(gift),
                   **attrs_of(gift), ton_only=bool(getattr(gift, "resale_ton_only", False)))


def attrs_of(gift) -> dict:
    """Модель, фон и узор из gift.attributes (StarGiftAttributeModel / Backdrop / Pattern)."""
    out = {"model": None, "backdrop": None, "pattern": None}
    for a in getattr(gift, "attributes", None) or []:
        kind = type(a).__name__
        for key, marker in (("model", "Model"), ("backdrop", "Backdrop"), ("pattern", "Pattern")):
            if marker in kind:
                out[key] = getattr(a, "name", None)
    return out


def flood_seconds(e: Exception) -> int | None:
    return e.seconds if isinstance(e, errors.FloodWaitError) else None


class TelegramMarket:
    source = "telegram"

    def __init__(self, pool: AccountPool):
        if not hasattr(functions.payments, "GetResaleStarGiftsRequest"):
            raise SystemExit("Telethon слишком старый: нет GetResaleStarGiftsRequest. Обнови: pip install -U telethon")
        self.pool = pool

    @classmethod
    async def connect(cls, sessions: list[str], api_id: int, api_hash: str,
                      min_interval: float = 2.0, state_path: str | None = "flood_state.json") -> "TelegramMarket":
        clients = []
        for s in sessions:
            # flood_sleep_threshold=0: любой FLOOD_WAIT поднимается к нам, а не «досыпается» внутри Telethon
            # (по умолчанию он молча спит на ожиданиях до 60 с). request_retries=1: без скрытых повторов.
            c = TelegramClient(s, api_id, api_hash, flood_sleep_threshold=0, request_retries=1)
            await c.connect()
            if not await c.is_user_authorized():
                raise SystemExit(f"Сессия {s} не авторизована — сначала: python -m gmw login")
            clients.append(c)
        return cls(AccountPool(clients, min_interval, flood_seconds, names=sessions, state_path=state_path))

    async def catalog(self) -> list[Collection]:
        res = await self.pool.call(functions.payments.GetStarGiftsRequest(hash=0))
        return [Collection(g.id, getattr(g, "title", None) or str(g.id),
                           getattr(g, "availability_resale", None) or 0, getattr(g, "resell_min_stars", None))
                for g in res.gifts]

    async def page(self, collection_id: int, offset: str, limit: int, sort: str = "recent") -> Page:
        # Без sort_by_* — по времени последнего изменения цены, новые сверху (горячий скан).
        # sort_by_num — стабильный порядок для полного обхода.
        res = await self.pool.call(functions.payments.GetResaleStarGiftsRequest(
            gift_id=collection_id, offset=offset, limit=limit, sort_by_num=(sort == "num") or None))
        return Page([listing_of(g, collection_id) for g in res.gifts],
                    getattr(res, "next_offset", None) or None, getattr(res, "count", None))

    async def gift_state(self, slug: str) -> GiftState | None:
        try:
            res = await self.pool.call(functions.payments.GetUniqueStarGiftRequest(slug=slug))
        except errors.RPCError as e:
            msg = e.message or ""
            if "SLUG_INVALID" in msg or "NOT_FOUND" in msg:
                return None  # гифт по slug не находится
            raise  # любая другая ошибка — «неизвестно», решать не будем
        g = res.gift
        stars, ton = price_of(g)
        on_sale = stars is not None or ton is not None
        return GiftState(owner_of(g), on_sale, listing_of(g, getattr(g, "gift_id", 0)) if on_sale else None)
