-- Лента событий: всё, что когда-либо случилось с гифтами. Только дописываем.
-- (Когда событий станет сотни миллионов — переносим эту таблицу в ClickHouse, остальное остаётся тут.)
CREATE TABLE IF NOT EXISTS events (
    id               uuid PRIMARY KEY,
    ts               timestamptz NOT NULL,
    type             text NOT NULL,
    source           text NOT NULL,
    slug             text NOT NULL,
    collection_id    bigint,
    num              integer,
    price_stars      bigint,
    price_ton        numeric(20, 9),
    prev_price_stars bigint,
    prev_price_ton   numeric(20, 9),
    from_owner       text,
    to_owner         text,
    received_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_ts_idx         ON events (ts DESC);
CREATE INDEX IF NOT EXISTS events_type_ts_idx    ON events (type, ts DESC);
CREATE INDEX IF NOT EXISTS events_coll_ts_idx    ON events (collection_id, ts DESC);
CREATE INDEX IF NOT EXISTS events_slug_ts_idx    ON events (slug, ts DESC);
CREATE INDEX IF NOT EXISTS events_received_idx   ON events (received_at DESC);

-- Текущее состояние маркета (проекция событий): что выставлено прямо сейчас (active) и за сколько.
CREATE TABLE IF NOT EXISTS listings (
    source        text NOT NULL,
    slug          text NOT NULL,
    collection_id bigint NOT NULL,
    num           integer,
    price_stars   bigint,
    price_ton     numeric(20, 9),
    owner         text,
    active        boolean NOT NULL DEFAULT true,  -- false = продан / снят (храним, чтобы знать «когда»)
    updated_at    timestamptz NOT NULL,
    PRIMARY KEY (source, slug)
);
CREATE INDEX IF NOT EXISTS listings_coll_price_idx ON listings (collection_id, price_stars) WHERE active;

-- Последний известный владелец каждого гифта.
CREATE TABLE IF NOT EXISTS gifts (
    slug          text PRIMARY KEY,
    collection_id bigint,
    num           integer,
    owner         text,
    updated_at    timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS gifts_owner_idx ON gifts (owner);

-- Каталог коллекций (из payments.getStarGifts).
CREATE TABLE IF NOT EXISTS collections (
    id          bigint PRIMARY KEY,
    title       text NOT NULL,
    on_resale   integer NOT NULL DEFAULT 0,
    floor_stars bigint,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Атрибуты NFT (добавлены позже — ALTER, чтобы обновлялись и существующие базы).
ALTER TABLE events   ADD COLUMN IF NOT EXISTS model text;
ALTER TABLE events   ADD COLUMN IF NOT EXISTS backdrop text;
ALTER TABLE events   ADD COLUMN IF NOT EXISTS pattern text;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS model text;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS backdrop text;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS pattern text;
CREATE INDEX IF NOT EXISTS listings_attr_idx ON listings (collection_id, model, backdrop) WHERE active;

-- Звёзды с дробной частью (StarsAmount.nanos) и признак «продаётся только за TON».
ALTER TABLE events   ALTER COLUMN price_stars      TYPE numeric(24, 9);
ALTER TABLE events   ALTER COLUMN prev_price_stars TYPE numeric(24, 9);
ALTER TABLE listings ALTER COLUMN price_stars      TYPE numeric(24, 9);
ALTER TABLE collections ALTER COLUMN floor_stars   TYPE numeric(24, 9);
ALTER TABLE events   ADD COLUMN IF NOT EXISTS ton_only boolean;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS ton_only boolean;
