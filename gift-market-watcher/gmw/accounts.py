"""Пул аккаунтов: раздаёт запросы по кругу, держит паузу между запросами и уважает FLOOD_WAIT."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger(__name__)


class FloodWait(Exception):
    def __init__(self, seconds: int):
        super().__init__(f"FLOOD_WAIT {seconds}s")
        self.seconds = seconds


class AccountPool:
    """`clients` — любые awaitable-вызываемые объекты (TelegramClient или фейк в тестах).

    `flood_error` — функция, которая по исключению говорит, сколько ждать (или None, если это не флуд).
    """

    def __init__(self, clients: list[Callable[[Any], Awaitable[Any]]], min_interval: float = 0.1,
                 flood_error: Callable[[Exception], int | None] | None = None):
        if not clients:
            raise ValueError("нужен хотя бы один аккаунт")
        self._clients = clients
        self._ready_at = [0.0] * len(clients)
        self._next = 0
        self._min_interval = min_interval
        self._flood_error = flood_error or (lambda e: e.seconds if isinstance(e, FloodWait) else None)
        self._lock = asyncio.Lock()
        self.requests = 0
        self.flood_waits: list[tuple[int, int]] = []  # (индекс аккаунта, секунды)

    async def _acquire(self) -> int:
        async with self._lock:
            now = time.monotonic()
            # Самый «свободный» аккаунт, при равенстве — по кругу.
            order = [(self._next + i) % len(self._clients) for i in range(len(self._clients))]
            idx = min(order, key=lambda i: max(self._ready_at[i], now))
            wait = self._ready_at[idx] - now
            self._ready_at[idx] = max(self._ready_at[idx], now) + self._min_interval
            self._next = (idx + 1) % len(self._clients)
        if wait > 0:
            await asyncio.sleep(wait)
        return idx

    async def call(self, request: Any) -> Any:
        while True:
            idx = await self._acquire()
            self.requests += 1
            try:
                return await self._clients[idx](request)
            except Exception as e:  # noqa: BLE001
                seconds = self._flood_error(e)
                if seconds is None:
                    raise
                self.flood_waits.append((idx, seconds))
                log.warning("аккаунт #%d: FLOOD_WAIT %ss", idx, seconds)
                async with self._lock:
                    self._ready_at[idx] = time.monotonic() + seconds + 1
