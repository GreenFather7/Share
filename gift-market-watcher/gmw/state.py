"""Локальное состояние сборщика (SQLite): снимок маркета + исходящие сообщения (outbox).

Каждый скан фиксируется одной транзакцией: новый снимок коллекции и сообщения для шины вместе.
Отправка идёт уже из outbox и повторяется, пока шина не подтвердит. Поэтому:
  * наблюдение, найденное при упавшей шине, не теряется, даже если рынок успел вернуться назад;
  * если шина записала, а ответ потерялся, повторная отправка несёт ТО ЖЕ сообщение (тот же id события) —
    в Postgres оно дедуплицируется;
  * после рестарта сборщика снимок берётся отсюда, и уже найденный переход не порождается заново.
"""

import json
import sqlite3
from collections.abc import Iterable

from .models import Listing

_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    collection_id INTEGER NOT NULL,
    slug          TEXT NOT NULL,
    data          TEXT NOT NULL,
    PRIMARY KEY (collection_id, slug)
);
CREATE TABLE IF NOT EXISTS seeded (collection_id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS outbox (
    seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    data TEXT NOT NULL
);
"""


class CollectorState:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.db = sqlite3.connect(path, isolation_level=None)  # транзакции открываем сами
        if path != ":memory:":
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(_SCHEMA)

    def close(self) -> None:
        self.db.close()

    # ---------- снимок ----------

    def is_empty(self) -> bool:
        return self.db.execute("SELECT NOT EXISTS (SELECT 1 FROM seeded)").fetchone()[0] == 1

    def load_snapshot(self) -> dict[int, dict[str, Listing]]:
        out: dict[int, dict[str, Listing]] = {cid: {} for (cid,) in self.db.execute("SELECT collection_id FROM seeded")}
        for cid, slug, data in self.db.execute("SELECT collection_id, slug, data FROM snapshot"):
            out.setdefault(cid, {})[slug] = Listing(**json.loads(data))
        return out

    def import_snapshot(self, snapshot: dict[int, dict[str, Listing]]) -> None:
        """Первый запуск с локальным состоянием: перенести снимок, восстановленный из Postgres."""
        for cid, listings in snapshot.items():
            self.commit(cid, listings, replace=True)

    # ---------- одна транзакция на скан ----------

    def commit(self, collection_id: int | None, upserts: dict[str, Listing] | None = None,
               removals: Iterable[str] = (), messages: Iterable[tuple[str, dict]] = (), *,
               replace: bool = False) -> None:
        """Атомарно: снимок коллекции (заменить целиком или дописать/удалить) + сообщения в outbox."""
        cur = self.db.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            if collection_id is not None:
                if replace:
                    cur.execute("DELETE FROM snapshot WHERE collection_id = ?", (collection_id,))
                cur.execute("INSERT OR IGNORE INTO seeded VALUES (?)", (collection_id,))
                cur.executemany("INSERT OR REPLACE INTO snapshot VALUES (?, ?, ?)",
                                [(collection_id, slug, json.dumps(l.__dict__)) for slug, l in (upserts or {}).items()])
                cur.executemany("DELETE FROM snapshot WHERE collection_id = ? AND slug = ?",
                                [(collection_id, slug) for slug in removals])
            cur.executemany("INSERT INTO outbox (kind, data) VALUES (?, ?)",
                            [(kind, json.dumps(data, ensure_ascii=False)) for kind, data in messages])
            cur.execute("COMMIT")
        except BaseException:
            cur.execute("ROLLBACK")
            raise

    # ---------- outbox ----------

    def pending(self, limit: int = 500) -> list[tuple[int, str, dict]]:
        rows = self.db.execute("SELECT seq, kind, data FROM outbox ORDER BY seq LIMIT ?", (limit,)).fetchall()
        return [(seq, kind, json.loads(data)) for seq, kind, data in rows]

    def delivered(self, seqs: list[int]) -> None:
        self.db.executemany("DELETE FROM outbox WHERE seq = ?", [(s,) for s in seqs])

    def outbox_size(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
