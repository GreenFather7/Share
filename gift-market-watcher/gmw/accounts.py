"""Пул аккаунтов для параллельных запросов.

* На каждом аккаунте в моменте не больше одного запроса.
* Между запросами одного аккаунта — пауза `min_interval`, отсчитывается от конца предыдущего запроса.
* FLOOD_WAIT хранится отдельно от паузы, с привязкой к аккаунту и методу: (аккаунт, метод) → срок.
  Сроки пишутся в `state_path` и переживают рестарт. Повреждённый файл — отказ стартовать
  (молча забыть ожидания нельзя).
* После FLOOD_WAIT: при `reroute_on_flood=True` запрос отдаётся другому аккаунту, свободному для этого метода
  (FLOOD_WAIT означает, что сервер запрос не выполнил), при False — FloodWait пробрасывается вызывающему.
* Любая другая ошибка, таймаут или отмена — исход неизвестен: без повтора, аккаунт освобождается с обычной паузой.
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


class FloodStateError(RuntimeError):
    """Файл со сроками FLOOD_WAIT не читается — без разбора стартовать нельзя."""


def method_of(request: Any) -> str:
    return type(request).__name__


class AccountPool:
    """`clients` — awaitable-вызываемые объекты (TelegramClient или фейк в тестах).

    `flood_error(e)` → сколько секунд ждать, либо None, если это не флуд.
    `names` — устойчивые имена аккаунтов (для файла состояния), по умолчанию индексы.
    """

    def __init__(self, clients: list[Callable[[Any], Awaitable[Any]]], min_interval: float = 2.0,
                 flood_error: Callable[[Exception], int | None] | None = None, *,
                 names: list[str] | None = None, state_path: str | None = None,
                 reroute_on_flood: bool = True, clock=time.monotonic):
        if not clients:
            raise ValueError("нужен хотя бы один аккаунт")
        self._clients = clients
        self.names = names or [str(i) for i in range(len(clients))]
        self._min_interval = min_interval
        self._flood_error = flood_error or (lambda e: e.seconds if isinstance(e, FloodWait) else None)
        self._clock = clock
        self._ready_at = [0.0] * len(clients)             # обычная пауза
        self._flood: dict[tuple[int, str], float] = {}   # (аккаунт, метод) → срок, monotonic
        self._busy = [False] * len(clients)
        self._next = 0
        self._cond = asyncio.Condition()
        self._state_path = state_path
        self.reroute_on_flood = reroute_on_flood
        self.requests = 0
        self.flood_waits: list[tuple[str, str, int]] = []  # (аккаунт, метод, секунды)
        self.unknown: list[tuple[str, str, str]] = []      # (аккаунт, метод, ошибка) — исход неизвестен
        self._load_state()

    @property
    def size(self) -> int:
        return len(self._clients)

    # ---------- сроки FLOOD_WAIT на диске ----------

    def _load_state(self) -> None:
        if not self._state_path or not os.path.exists(self._state_path):
            return
        try:
            with open(self._state_path) as f:
                saved = json.load(f)
            items = [(r["account"], r["method"], float(r["until"])) for r in saved["floods"]]
        except (OSError, ValueError, KeyError, TypeError) as e:
            raise FloodStateError(f"{self._state_path} повреждён ({e}); разберитесь вручную или удалите файл, "
                                  f"если уверены, что ожиданий нет") from e
        index = {n: i for i, n in enumerate(self.names)}
        now_wall, now = time.time(), self._clock()
        for account, method, until in items:
            if account in index and until > now_wall:
                self._flood[(index[account], method)] = now + (until - now_wall)
                log.info("аккаунт %s, %s: FLOOD_WAIT ещё %.0f с (с прошлого запуска)", account, method, until - now_wall)

    def _save_state(self) -> None:
        if not self._state_path:
            return
        now_wall, now = time.time(), self._clock()
        floods = [{"account": self.names[i], "method": m, "until": now_wall + (t - now)}
                  for (i, m), t in self._flood.items() if t > now]
        tmp = self._state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"floods": floods}, f)
        os.replace(tmp, self._state_path)

    # ---------- выдача аккаунтов ----------

    def _ready(self, i: int, method: str) -> float:
        return max(self._ready_at[i], self._flood.get((i, method), 0.0))

    async def _acquire(self, method: str) -> int:
        async with self._cond:
            while True:
                now = self._clock()
                order = [(self._next + k) % self.size for k in range(self.size)]
                free = [i for i in order if not self._busy[i]]
                if free:
                    idx = min(free, key=lambda i: self._ready(i, method))
                    wait = self._ready(idx, method) - now
                    if wait <= 0:  # сроки проверяются прямо перед выдачей — флуд, пришедший во время ожидания, учтён
                        self._busy[idx] = True
                        self._next = (idx + 1) % self.size
                        return idx
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=wait)
                    except asyncio.TimeoutError:
                        pass
                else:
                    await self._cond.wait()

    def _release_now(self, idx: int, method: str, flood: int | None) -> None:
        """Вызывать под self._cond."""
        self._busy[idx] = False
        now = self._clock()
        self._ready_at[idx] = max(self._ready_at[idx], now + self._min_interval)
        if flood is not None:
            self._flood[(idx, method)] = max(self._flood.get((idx, method), 0.0), now + flood + 1)
            self._save_state()
        self._cond.notify_all()

    async def _release(self, idx: int, method: str, flood: int | None = None) -> None:
        async with self._cond:
            self._release_now(idx, method, flood)

    async def call(self, request: Any) -> Any:
        method = method_of(request)
        while True:
            idx = await self._acquire(method)
            self.requests += 1
            try:
                result = await self._clients[idx](request)
            except asyncio.CancelledError:
                # Отменили посреди запроса: ушёл ли он на сервер — неизвестно. Не повторяем, аккаунт освобождаем.
                self.unknown.append((self.names[idx], method, "cancelled"))
                await asyncio.shield(self._release(idx, method))
                raise
            except Exception as e:  # noqa: BLE001
                seconds = self._flood_error(e)
                await self._release(idx, method, seconds)
                if seconds is None:
                    self.unknown.append((self.names[idx], method, type(e).__name__))
                    raise
                self.flood_waits.append((self.names[idx], method, seconds))
                if not self.reroute_on_flood:
                    raise
                log.warning("аккаунт %s, %s: FLOOD_WAIT %s с — запрос уйдёт другому аккаунту",
                            self.names[idx], method, seconds)
                continue
            await self._release(idx, method)
            return result
