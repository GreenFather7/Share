import asyncio
import json
import time

import pytest

from gmw.accounts import AccountPool, FloodWait


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
    assert pool.flood_waits == [("0", 30)]
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
    saved = json.load(open(path))
    assert set(saved) == {"acc_a"} and saved["acc_a"] > time.time() + 25

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

    pool = AccountPool([slow_then_flood, account(1, log)], min_interval=0)
    pool._busy[1] = True  # второй аккаунт занят — второй запрос вынужден ждать первый
    first = asyncio.create_task(pool.call("first"))
    await asyncio.sleep(0.01)
    second = asyncio.create_task(pool.call("second"))
    await asyncio.sleep(0.01)
    gate.set()  # первый получает FLOOD_WAIT → аккаунт 0 уходит отдыхать
    await asyncio.sleep(0.01)
    assert ("start", 0, "second") not in log
    await pool._release(1)  # освобождаем второй аккаунт — оба запроса уходят на него
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
