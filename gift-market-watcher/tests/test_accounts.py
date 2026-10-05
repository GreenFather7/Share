import asyncio
import json
import time

import pytest

from gmw.accounts import AccountPool, FloodStateError, FloodWait


def account(i, log, flood_once=False, delay=0.0):
    state = {"flooded": False}

    async def client(req):
        log.append(("start", i, req))
        if flood_once and not state["flooded"]:
            state["flooded"] = True
            raise FloodWait(30)
        await asyncio.sleep(delay)
        log.append(("end", i, req))
        return i
    return client


@pytest.mark.asyncio
async def test_flood_moves_request_to_free_account():
    log = []
    pool = AccountPool([account(0, log, flood_once=True), account(1, log)], min_interval=0)
    results = [await pool.call(n) for n in range(3)]
    assert pool.flood_waits == [("0", "int", 30)]
    assert results == [1, 1, 1]  # 0 отдыхает 30 с — всё идёт через 1
    assert log[0] == ("start", 0, 0)


@pytest.mark.asyncio
async def test_one_request_in_flight_per_account():
    log = []
    pool = AccountPool([account(i, log, delay=0.05) for i in range(3)], min_interval=0)
    await asyncio.gather(*(pool.call(n) for n in range(9)))
    in_flight = {i: 0 for i in range(3)}
    peak_total = 0
    for kind, i, _ in log:
        in_flight[i] += 1 if kind == "start" else -1
        assert in_flight[i] <= 1, "у аккаунта два запроса одновременно"
        peak_total = max(peak_total, sum(in_flight.values()))
    assert peak_total == 3  # но аккаунты работают параллельно


@pytest.mark.asyncio
async def test_min_interval_counts_from_request_end():
    log = []
    pool = AccountPool([account(0, log, delay=0.05)], min_interval=0.1)
    t0 = time.monotonic()
    await pool.call(1)
    await pool.call(2)
    assert time.monotonic() - t0 >= 0.05 + 0.1 + 0.05 - 0.01


@pytest.mark.asyncio
async def test_flood_deadline_survives_restart(tmp_path):
    path = str(tmp_path / "flood.json")
    log = []
    pool = AccountPool([account(0, log, flood_once=True), account(1, log)], min_interval=0,
                       names=["acc_a", "acc_b"], state_path=path)
    await pool.call(1)
    saved = json.load(open(path))["floods"]
    assert [(f["account"], f["method"]) for f in saved] == [("acc_a", "int")] and saved[0]["until"] > time.time() + 25

    # «Рестарт»: новый пул с тем же файлом — acc_a всё ещё отдыхает, работает только acc_b.
    log2 = []
    pool2 = AccountPool([account(0, log2), account(1, log2)], min_interval=0,
                        names=["acc_a", "acc_b"], state_path=path)
    assert [await pool2.call(n) for n in range(3)] == [1, 1, 1]


@pytest.mark.asyncio
async def test_waiting_request_rechecks_flood_before_sending():
    """Запрос ждёт свободный аккаунт; пока ждал, этот аккаунт словил флуд — отправлять на него нельзя."""
    log = []
    gate = asyncio.Event()

    async def slow_then_flood(req):
        log.append(("start", 0, req))
        if req == "first":
            await gate.wait()
            raise FloodWait(30)
        return 0
    # оба запроса — строки, т. е. один «метод» str: флуд первого касается и второго

    pool = AccountPool([slow_then_flood, account(1, log)], min_interval=0)
    pool._busy[1] = True  # второй аккаунт занят — второй запрос вынужден ждать первый
    first = asyncio.create_task(pool.call("first"))
    await asyncio.sleep(0.01)
    second = asyncio.create_task(pool.call("second"))
    await asyncio.sleep(0.01)
    gate.set()  # первый получает FLOOD_WAIT → аккаунт 0 уходит отдыхать
    await asyncio.sleep(0.01)
    assert ("start", 0, "second") not in log
    await pool._release(1, "str")  # освобождаем второй аккаунт — оба запроса уходят на него
    assert await asyncio.wait_for(asyncio.gather(first, second), 1) == [1, 1]


@pytest.mark.asyncio
async def test_unknown_errors_are_not_retried():
    calls = []

    async def broken(req):
        calls.append(req)
        raise TimeoutError("исход неизвестен")

    pool = AccountPool([broken, broken], min_interval=0)
    with pytest.raises(TimeoutError):
        await pool.call(1)
    assert calls == [1]


@pytest.mark.asyncio
async def test_short_flood_is_kept_even_when_pause_is_longer(tmp_path):
    """Повторное ревью №1: при паузе 12 с флуд 5 с раньше не попадал в файл и забывался после рестарта."""
    path = str(tmp_path / "flood.json")
    log = []
    pool = AccountPool([account(0, log, flood_once=True), account(1, log)], min_interval=12,
                       names=["a", "b"], state_path=path)
    pool._flood_error = lambda e: 5 if isinstance(e, FloodWait) else None
    await pool.call(1)
    saved = json.load(open(path))["floods"]
    assert [(f["account"], f["method"]) for f in saved] == [("a", "int")]
    restarted = AccountPool([account(0, []), account(1, [])], min_interval=12, names=["a", "b"], state_path=path)
    assert (0, "int") in restarted._flood


@pytest.mark.asyncio
async def test_corrupted_flood_state_refuses_to_start(tmp_path):
    """Повторное ревью №1: повреждённый файл раньше означал «ожиданий нет»."""
    path = tmp_path / "flood.json"
    path.write_text("{oops")
    with pytest.raises(FloodStateError):
        AccountPool([account(0, [])], state_path=str(path))


@pytest.mark.asyncio
async def test_flood_is_scoped_by_method():
    """Флуд на одном методе не блокирует аккаунт для другого метода."""
    log = []

    async def client(req):
        log.append(req)
        if req == "page" and log.count("page") == 1:
            raise FloodWait(30)
        return req

    pool = AccountPool([client], min_interval=0)
    pool.reroute_on_flood = False
    with pytest.raises(FloodWait):
        await pool.call("page")            # метод str — отдыхает
    assert await asyncio.wait_for(pool.call(b"catalog"), 1) == b"catalog"  # метод bytes — свободен


@pytest.mark.asyncio
async def test_no_reroute_mode_raises_flood_to_caller():
    log = []
    pool = AccountPool([account(0, log, flood_once=True), account(1, log)], min_interval=0, reroute_on_flood=False)
    with pytest.raises(FloodWait):
        await pool.call(1)
    assert [e for e in log if e[0] == "start"] == [("start", 0, 1)]  # второму аккаунту не передавали


@pytest.mark.asyncio
async def test_cancel_mid_request_frees_the_account():
    """Повторное ревью, P2: отмена посреди запроса оставляла аккаунт «занятым» навсегда."""
    started = asyncio.Event()

    async def hang(req):
        started.set()
        await asyncio.sleep(10)

    async def quick(req):
        return "ok"

    pool = AccountPool([hang], min_interval=0)
    task = asyncio.create_task(pool.call(1))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert pool.unknown == [("0", "int", "cancelled")] and pool._busy == [False]
    pool._clients[0] = quick
    assert await asyncio.wait_for(pool.call(2), 1) == "ok"


def test_telethon_sends_exactly_once_with_request_retries_0():
    """Повторное ревью №2: в Telethon request_retries=1 — это отправка + повтор. Проверяем на настоящем _call."""
    from telethon import TelegramClient, errors, functions
    from telethon.sessions import MemorySession

    async def run(retries):
        client = TelegramClient(MemorySession(), 1, "0" * 32, request_retries=retries, flood_sleep_threshold=0)
        sends = []

        class Sender:
            def send(self, request, ordered=False):
                sends.append(request)
                fut = asyncio.get_running_loop().create_future()
                fut.set_exception(errors.ServerError(request=request, message="boom", code=500))
                return fut
        client._sender = Sender()
        try:
            await client._call(client._sender, functions.help.GetConfigRequest())
        except Exception:  # noqa: BLE001
            pass
        return len(sends)

    assert asyncio.run(run(1)) == 2
    assert asyncio.run(run(0)) == 1
