"""Источник «маркет Telegram» через MTProto (Telethon) и пул аккаунтов."""

from telethon import TelegramClient, errors, functions

from ..accounts import AccountPool
from ..differ import GiftState
from ..models import Collection, Listing


def price_of(gift) -> tuple[int | None, float | None]:
    """Цена лота: звёзды и TON. Новая схема — resell_amount (StarsAmount / StarsTonAmount), старая — resell_stars."""
    stars, ton = None, None
    for a in getattr(gift, "resell_amount", None) or []:
        if "Ton" in type(a).__name__:
            ton = a.amount / 1e9  # TON приходит в нано-единицах
        else:
            stars = int(a.amount)
    if stars is None and ton is None and getattr(gift, "resell_stars", None):
        stars = int(gift.resell_stars)
    return stars, ton


def owner_of(gift) -> str | None:
    peer = getattr(gift, "owner_id", None)
    if peer is None:
        return getattr(gift, "owner_name", None) or getattr(gift, "owner_address", None)
    for attr, prefix in (("user_id", "u"), ("channel_id", "c"), ("chat_id", "g")):
        if hasattr(peer, attr):
            return f"{prefix}{getattr(peer, attr)}"
    return str(peer)


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
                      min_interval: float = 0.1) -> "TelegramMarket":
        clients = []
        for s in sessions:
            c = TelegramClient(s, api_id, api_hash)
            await c.connect()
            if not await c.is_user_authorized():
                raise SystemExit(f"Сессия {s} не авторизована — сначала: python -m gmw login")
            clients.append(c)
        return cls(AccountPool(clients, min_interval, flood_seconds))

    async def catalog(self) -> list[Collection]:
        res = await self.pool.call(functions.payments.GetStarGiftsRequest(hash=0))
        return [Collection(g.id, getattr(g, "title", None) or str(g.id),
                           getattr(g, "availability_resale", None) or 0, getattr(g, "resell_min_stars", None))
                for g in res.gifts]

    async def page(self, collection_id: int, offset: str, limit: int) -> tuple[list[Listing], str | None]:
        # Без sort_by_price / sort_by_num — сортировка по времени последнего изменения цены, новые сверху.
        res = await self.pool.call(functions.payments.GetResaleStarGiftsRequest(
            gift_id=collection_id, offset=offset, limit=limit))
        listings = []
        for g in res.gifts:
            stars, ton = price_of(g)
            listings.append(Listing(g.slug, collection_id, getattr(g, "num", None), stars, ton, owner_of(g)))
        return listings, getattr(res, "next_offset", None)

    async def gift_state(self, slug: str) -> GiftState | None:
        try:
            res = await self.pool.call(functions.payments.GetUniqueStarGiftRequest(slug=slug))
        except errors.RPCError as e:
            if "INVALID" in e.message or "NOT_FOUND" in e.message:
                return None  # гифта больше нет (сожгли / скрафтили)
            raise
        stars, ton = price_of(res.gift)
        return GiftState(owner_of(res.gift), stars is not None or ton is not None)
