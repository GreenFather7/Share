"""Postgres: запись событий (с дедупликацией), проекции и запросы для API."""

import uuid
from datetime import datetime
from pathlib import Path

import asyncpg

from .models import Collection, Event, EventType, Listing

SCHEMA = Path(__file__).with_name("schema.sql")

_EVENT_COLS = ("id", "ts", "type", "source", "slug", "collection_id", "num", "price_stars", "price_ton",
               "prev_price_stars", "prev_price_ton", "from_owner", "to_owner")


class Storage:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> "Storage":
        return cls(await asyncpg.create_pool(dsn, min_size=1, max_size=10))

    async def close(self) -> None:
        await self.pool.close()

    async def init(self) -> None:
        async with self.pool.acquire() as con:
            await con.execute(SCHEMA.read_text())

    # ---------- запись (нормализатор) ----------

    async def process(self, events: list[Event]) -> list[Event]:
        """Пишет события и обновляет проекции в одной транзакции. Возвращает только новые."""
        events = list({e.id: e for e in events}.values())
        if not events:
            return []
        rows = [(e.id, e.ts, e.type.value, e.source, e.slug, e.collection_id, e.num, e.price_stars,
                 e.price_ton, e.prev_price_stars, e.prev_price_ton, e.from_owner, e.to_owner) for e in events]
        async with self.pool.acquire() as con, con.transaction():
            inserted = await con.fetch(f"""
                INSERT INTO events ({", ".join(_EVENT_COLS)})
                SELECT * FROM unnest($1::uuid[], $2::timestamptz[], $3::text[], $4::text[], $5::text[],
                                     $6::bigint[], $7::int[], $8::bigint[], $9::numeric[], $10::bigint[],
                                     $11::numeric[], $12::text[], $13::text[])
                ON CONFLICT (id) DO NOTHING
                RETURNING id""", *zip(*rows))
            new_ids = {r["id"] for r in inserted}
            new = [e for e in events if e.id in new_ids]
            for e in sorted(new, key=lambda e: e.ts):
                await self._apply(con, e)
        return new

    async def _apply(self, con, e: Event) -> None:
        if e.type in (EventType.LISTED, EventType.PRICE_CHANGED):
            await con.execute("""
                INSERT INTO listings (source, slug, collection_id, num, price_stars, price_ton, owner, active, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8)
                ON CONFLICT (source, slug) DO UPDATE SET price_stars = EXCLUDED.price_stars,
                    price_ton = EXCLUDED.price_ton, owner = COALESCE(EXCLUDED.owner, listings.owner),
                    active = true, updated_at = EXCLUDED.updated_at
                WHERE listings.updated_at <= EXCLUDED.updated_at""",
                e.source, e.slug, e.collection_id, e.num, e.price_stars, e.price_ton, e.from_owner, e.ts)
            owner = e.from_owner
        elif e.type in (EventType.SOLD, EventType.DELISTED, EventType.BURNED):
            # Не удаляем, а гасим с меткой времени: иначе запоздалый посев «воскресит» проданный лот.
            await con.execute("""
                INSERT INTO listings (source, slug, collection_id, num, price_stars, price_ton, owner, active, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, false, $8)
                ON CONFLICT (source, slug) DO UPDATE SET active = false, updated_at = EXCLUDED.updated_at
                WHERE listings.updated_at <= EXCLUDED.updated_at""",
                e.source, e.slug, e.collection_id or 0, e.num, e.price_stars, e.price_ton, e.from_owner, e.ts)
            owner = e.to_owner if e.type == EventType.SOLD else e.from_owner
        else:  # transfer / minted
            owner = e.to_owner
        if owner:
            await con.execute("""
                INSERT INTO gifts (slug, collection_id, num, owner, updated_at) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (slug) DO UPDATE SET owner = EXCLUDED.owner, updated_at = EXCLUDED.updated_at,
                    collection_id = COALESCE(EXCLUDED.collection_id, gifts.collection_id),
                    num = COALESCE(EXCLUDED.num, gifts.num)
                WHERE gifts.updated_at <= EXCLUDED.updated_at""",
                e.slug, e.collection_id, e.num, owner, e.ts)

    async def seed_listings(self, source: str, collection_id: int, listings: list[Listing], ts: datetime) -> None:
        """Посев: заменить лоты коллекции снимком (первый полный скан, без событий).

        Всё, что новее снимка (`ts`), не трогаем — повторная доставка старого посева ничего не сломает.
        """
        async with self.pool.acquire() as con, con.transaction():
            await con.execute("""
                UPDATE listings SET active = false, updated_at = $4
                WHERE source = $1 AND collection_id = $2 AND updated_at <= $4 AND active AND NOT slug = ANY($3::text[])""",
                source, collection_id, [l.slug for l in listings], ts)
            await con.executemany("""
                INSERT INTO listings (source, slug, collection_id, num, price_stars, price_ton, owner, active, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8)
                ON CONFLICT (source, slug) DO UPDATE SET price_stars = EXCLUDED.price_stars,
                    price_ton = EXCLUDED.price_ton, owner = EXCLUDED.owner, active = true,
                    updated_at = EXCLUDED.updated_at
                WHERE listings.updated_at <= EXCLUDED.updated_at""",
                [(source, l.slug, collection_id, l.num, l.price_stars, l.price_ton, l.owner, ts) for l in listings])
            await con.executemany("""
                INSERT INTO gifts (slug, collection_id, num, owner, updated_at) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (slug) DO UPDATE SET owner = EXCLUDED.owner, updated_at = EXCLUDED.updated_at
                WHERE gifts.updated_at <= EXCLUDED.updated_at""",
                [(l.slug, collection_id, l.num, l.owner, ts) for l in listings if l.owner])

    async def upsert_collection(self, c: Collection) -> None:
        await self.pool.execute("""
            INSERT INTO collections (id, title, on_resale, floor_stars, updated_at) VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, on_resale = EXCLUDED.on_resale,
                floor_stars = EXCLUDED.floor_stars, updated_at = now()""",
            c.id, c.title, c.on_resale, c.floor_stars)

    # ---------- чтение (сборщики и API) ----------

    async def load_listings(self, source: str) -> dict[int, dict[str, Listing]]:
        """Снимок маркета для сборщика после рестарта: {collection_id: {slug: Listing}}."""
        rows = await self.pool.fetch("SELECT * FROM listings WHERE source = $1 AND active", source)
        out: dict[int, dict[str, Listing]] = {}
        for r in rows:
            out.setdefault(r["collection_id"], {})[r["slug"]] = Listing(
                r["slug"], r["collection_id"], r["num"], r["price_stars"],
                float(r["price_ton"]) if r["price_ton"] is not None else None, r["owner"])
        return out

    async def events(self, *, type: str | None = None, source: str | None = None,
                     collection_id: int | None = None, slug: str | None = None,
                     before: datetime | None = None, limit: int = 50) -> list[dict]:
        where, args = [], []
        for col, val in (("type", type), ("source", source), ("collection_id", collection_id), ("slug", slug)):
            if val is not None:
                args.append(val)
                where.append(f"e.{col} = ${len(args)}")
        if before is not None:
            args.append(before)
            where.append(f"e.ts < ${len(args)}")
        args.append(limit)
        rows = await self.pool.fetch(f"""
            SELECT e.*, c.title AS collection_title FROM events e
            LEFT JOIN collections c ON c.id = e.collection_id
            {"WHERE " + " AND ".join(where) if where else ""}
            ORDER BY e.ts DESC LIMIT ${len(args)}""", *args)
        return [_row(r) for r in rows]

    async def gift(self, slug: str) -> dict | None:
        g = await self.pool.fetchrow("SELECT * FROM gifts WHERE slug = $1", slug)
        listings = await self.pool.fetch("SELECT * FROM listings WHERE slug = $1 AND active ORDER BY source", slug)
        if g is None and not listings:
            return None
        return {"slug": slug, "owner": g["owner"] if g else None,
                "listings": [_row(r) for r in listings],
                "events": await self.events(slug=slug, limit=50)}

    async def floors(self) -> list[dict]:
        rows = await self.pool.fetch("""
            SELECT c.id AS collection_id, c.title, c.on_resale,
                   MIN(l.price_stars) AS floor_stars, MIN(l.price_ton) AS floor_ton, COUNT(l.slug) AS tracked
            FROM collections c LEFT JOIN listings l ON l.collection_id = c.id AND l.active
            GROUP BY c.id ORDER BY c.on_resale DESC""")
        return [_row(r) for r in rows]

    async def stats(self) -> dict:
        r = await self.pool.fetchrow("""
            SELECT (SELECT COUNT(*) FROM events) AS events_total,
                   (SELECT COUNT(*) FROM events WHERE received_at > now() - interval '60 seconds') AS last_minute,
                   (SELECT COUNT(*) FROM listings WHERE active) AS listings""")
        return {"events_total": r["events_total"], "events_per_sec": round(r["last_minute"] / 60, 1),
                "listings": r["listings"]}


def _row(r) -> dict:
    d = dict(r)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
        elif v is not None and k.endswith("_ton"):
            d[k] = float(v)
    return d
