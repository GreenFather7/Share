"""Пул аккаунтов для параллельных запросов.

* На каждом аккаунте в моменте не больше одного запроса.
* Между запросами одного аккаунта — пауза `min_interval`, отсчитывается от конца предыдущего запроса.
* FLOOD_WAIT: аккаунт отдыхает до срока, срок пишется в `state_path` и переживает рестарт.
  Запрос, получивший FLOOD_WAIT, сервером не выполнен — его безопасно отдать другому свободному аккаунту.
* Любая другая ошибка (в т. ч. таймаут с неизвестным исходом) пробрасывается без повтора.
"""

import asyncio
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger(__name__)


class FloodWait(Exception):
    def __init__(self, seconds: int):
        super().__init__(f"FLOOD_WAIT {seconds}s")
        self.seconds = seconds


class AccountPool:
    """`clients` — awaitable-вызываемые объекты (TelegramClient или фейк в тестах).

    `flood_error(e)` → сколько секунд ждать, либо None, если это не флуд.
    `names` — устойчивые имена аккаунтов (для файла состояния), по умолчанию индексы.
    """

    def __init__(self, clients: list[Callable[[Any], Awaitable[Any]]], min_interval: float = 2.0,
                 flood_error: Callable[[Exception], int | None] | None = None, *,
                 names: list[str] | None = None, state_path: str | None = None, clock=time.monotonic):
        if not clients:
            raise ValueError("нужен хотя бы один аккаунт")
        self._clients = clients
        self.names = names or [str(i) for i in range(len(clients))]
        self._min_interval = min_interval
        self._flood_error = flood_error or (lambda e: e.seconds if isinstance(e, FloodWait) else None)
        self._clock = clock
        self._ready_at = [0.0] * len(clients)
        self._busy = [False] * len(clients)
        self._next = 0
        self._cond = asyncio.Condition()
        self._state_path = state_path
        self.requests = 0
        self.flood_waits: list[tuple[str, int]] = []  # (аккаунт, секунды)
        self._load_state()

    @property
    def size(self) -> int:
        return len(self._clients)

    # ---------- состояние флудов на диске ----------

    def _load_state(self) -> None:
        if not self._state_path or not os.path.exists(self._state_path):
            return
        try:
            with open(self._state_path) as f:
                deadlines = json.load(f)
        except (OSError, ValueError):
            log.warning("не удалось прочитать %s — считаю, что ожиданий нет", self._state_path)
            return
        now_wall, now = time.time(), self._clock()
        for i, name in enumerate(self.names):
            left = deadlines.get(name, 0) - now_wall
            if left > 0:
                self._ready_at[i] = now + left
                log.info("аккаунт %s ещё отдыхает %.0f с (FLOOD_WAIT до рестарта)", name, left)

    def _save_state(self) -> None:
        if not self._state_path:
            return
        now_wall, now = time.time(), self._clock()
        deadlines = {name: now_wall + (self._ready_at[i] - now)
                     for i, name in enumerate(self.names) if self._ready_at[i] - now > self._min_interval}
        tmp = self._state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(deadlines, f)
        os.replace(tmp, self._state_path)

    # ---------- выдача аккаунтов ----------

    async def _acquire(self) -> int:
        async with self._cond:
            while True:
                now = self._clock()
                order = [(self._next + k) % self.size for k in range(self.size)]
                free = [i for i in order if not self._busy[i]]
                if free:
                    idx = min(free, key=lambda i: self._ready_at[i])
                    wait = self._ready_at[idx] - now
                    if wait <= 0:  # срок проверяется прямо перед выдачей — флуд, пришедший во время ожидания, учтён
                        self._busy[idx] = True
                        self._next = (idx + 1) % self.size
                        return idx
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=wait)
                    except asyncio.TimeoutError:
                        pass
                else:
                    await self._cond.wait()

    async def _release(self, idx: int, flood: int | None = None) -> None:
        async with self._cond:
            self._busy[idx] = False
            now = self._clock()
            self._ready_at[idx] = max(self._ready_at[idx], now + (flood + 1 if flood is not None else self._min_interval))
            if flood is not None:
                self._save_state()
            self._cond.notify_all()

    async def call(self, request: Any) -> Any:
        while True:
            idx = await self._acquire()
            self.requests += 1
            try:
                result = await self._clients[idx](request)
            except Exception as e:  # noqa: BLE001
                seconds = self._flood_error(e)
                await self._release(idx, seconds)
                if seconds is None:
                    raise
                self.flood_waits.append((self.names[idx], seconds))
                log.warning("аккаунт %s: FLOOD_WAIT %s с — отдыхает, запрос уйдёт другому", self.names[idx], seconds)
                continue
            await self._release(idx)
            return result
