"""Публичное API: всё отдаётся из нашей базы, в Telegram на запрос пользователя мы не ходим."""

import secrets
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect

from .bus import Live
from .models import EventType
from .storage import Storage


def create_app(storage: Storage, live: Live, token: str | None = None) -> FastAPI:
    """`token` — если задан, все запросы (кроме /health) требуют `Authorization: Bearer <token>` или `?token=`."""

    def authorized(header: str | None, query: str | None) -> bool:
        if not token:
            return True
        given = query or (header[7:] if header and header.startswith("Bearer ") else None)
        return given is not None and secrets.compare_digest(given, token)

    async def require_token(request: Request):
        if not authorized(request.headers.get("authorization"), request.query_params.get("token")):
            raise HTTPException(401, "нужен токен: Authorization: Bearer <GMW_API_TOKEN>")

    app = FastAPI(title="gift-market-watcher")
    guarded = [Depends(require_token)]

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/stats", dependencies=guarded)
    async def stats():
        return await storage.stats()

    @app.get("/events", dependencies=guarded)
    async def events(type: EventType | None = None, source: str | None = None,
                     collection_id: int | None = None, slug: str | None = None,
                     before: datetime | None = None, limit: int = Query(50, ge=1, le=500)):
        return await storage.events(type=type.value if type else None, source=source,
                                    collection_id=collection_id, slug=slug, before=before, limit=limit)

    @app.get("/gifts/{slug}", dependencies=guarded)
    async def gift(slug: str):
        g = await storage.gift(slug)
        if g is None:
            raise HTTPException(404, "гифт не найден")
        return g

    @app.get("/floors", dependencies=guarded)
    async def floors():
        return await storage.floors()

    @app.get("/floors/{collection_id}", dependencies=guarded)
    async def attribute_floors(collection_id: int, by: str = "model"):
        """Флоры по комбинациям: ?by=model,backdrop — самый дешёвый лот каждой комбинации («Где купить»)."""
        try:
            return await storage.attribute_floors(collection_id, by.split(","))
        except ValueError as e:
            raise HTTPException(422, str(e))

    @app.websocket("/live")
    async def live_feed(ws: WebSocket, type: str | None = None, collection_id: int | None = None,
                        source: str | None = None):
        """Живая лента. Фильтры — как у /events: ?type=sold&collection_id=...&token=..."""
        if not authorized(ws.headers.get("authorization"), ws.query_params.get("token")):
            await ws.close(code=4401)
            return
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
