"""Публичное API: всё отдаётся из нашей базы, в Telegram на запрос пользователя мы не ходим."""

from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

from .bus import Live
from .models import EventType
from .storage import Storage


def create_app(storage: Storage, live: Live) -> FastAPI:
    app = FastAPI(title="gift-market-watcher")

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/stats")
    async def stats():
        return await storage.stats()

    @app.get("/events")
    async def events(type: EventType | None = None, source: str | None = None,
                     collection_id: int | None = None, slug: str | None = None,
                     before: datetime | None = None, limit: int = Query(50, ge=1, le=500)):
        return await storage.events(type=type.value if type else None, source=source,
                                    collection_id=collection_id, slug=slug, before=before, limit=limit)

    @app.get("/gifts/{slug}")
    async def gift(slug: str):
        g = await storage.gift(slug)
        if g is None:
            raise HTTPException(404, "гифт не найден")
        return g

    @app.get("/floors")
    async def floors():
        return await storage.floors()

    @app.websocket("/live")
    async def live_feed(ws: WebSocket, type: str | None = None, collection_id: int | None = None,
                        source: str | None = None):
        """Живая лента. Фильтры — как у /events: ?type=sold&collection_id=..."""
        await ws.accept()
        try:
            async for e in live.subscribe():
                if type and e["type"] != type:
                    continue
                if collection_id and e["collection_id"] != collection_id:
                    continue
                if source and e["source"] != source:
                    continue
                await ws.send_json(e)
        except WebSocketDisconnect:
            pass

    return app
