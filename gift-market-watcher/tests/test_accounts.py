import pytest

from gmw.accounts import AccountPool, FloodWait


@pytest.mark.asyncio
async def test_round_robin_and_flood_rotation():
    calls = []

    def make(i, flood_once=False):
        state = {"flooded": False}

        async def client(req):
            calls.append(i)
            if flood_once and not state["flooded"]:
                state["flooded"] = True
                raise FloodWait(30)
            return (i, req)
        return client

    pool = AccountPool([make(0, flood_once=True), make(1)], min_interval=0)
    results = [await pool.call(n) for n in range(4)]
    # Аккаунт 0 словил флуд → запрос ушёл на 1, дальше 0 остыл и всё идёт через 1.
    assert pool.flood_waits == [(0, 30)]
    assert [r[0] for r in results] == [1, 1, 1, 1]
    assert [r[1] for r in results] == [0, 1, 2, 3]
    assert calls[0] == 0


@pytest.mark.asyncio
async def test_other_errors_propagate():
    async def broken(req):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await AccountPool([broken], min_interval=0).call(1)
