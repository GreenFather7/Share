"""bench.py против фейкового Telegram: без сети, без аккаунтов."""
import asyncio
import json
import sys
from types import SimpleNamespace as NS

from telethon import errors, functions

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import bench  # noqa: E402


class StarsAmount:
    def __init__(self, a):
        self.amount, self.nanos = a, 0


def fake_client(mode):
    class FakeClient:
        sent = []

        def __init__(self, *a, **kw):
            assert kw["flood_sleep_threshold"] == 0 and kw["request_retries"] == 0
            self.n = 0

        async def connect(self): pass
        async def is_user_authorized(self): return True
        async def disconnect(self): pass

        async def __call__(self, req):
            self.n += 1
            FakeClient.sent.append(type(req).__name__)
            if mode == "flood" and self.n == 3:
                raise errors.FloodWaitError(request=None, capture=37)
            if mode == "timeout" and self.n == 3:
                await asyncio.sleep(5)
            if isinstance(req, functions.payments.GetStarGiftsRequest):
                return NS(gifts=[NS(id=i, title=f"G{i}", availability_resale=250 * i, resell_min_stars=100)
                                 for i in (1, 5)])
            gid, off = req.gift_id, int(req.offset or 0)
            total = 250 * gid
            end = min(off + req.limit, total)
            gifts = [NS(slug=f"G{gid}-{k}", num=k, resell_amount=[StarsAmount(100 + k)], owner_id=NS(user_id=k))
                     for k in range(off, end)]
            if mode == "dupes" and off > 0:
                gifts = gifts[:1] * 2
            nxt = "" if end >= total else (str(off) if mode == "repeat" and off > 0 else str(off + req.limit))
            return NS(gifts=gifts, next_offset=nxt, count=total)
    return FakeClient


def run(monkeypatch, tmp_path, mode, *args):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(bench, "TelegramClient", fake_client(mode))
    monkeypatch.setenv("TG_API_ID", "1")
    monkeypatch.setenv("TG_API_HASH", "x")
    monkeypatch.setattr(sys, "argv", ["bench.py", "--delay", "0", "--full-budget", "60", "--reserve", "1", *args])
    asyncio.run(bench.main())
    return json.loads((tmp_path / "bench_out" / "report.json").read_text())


def statuses(r):
    return [f["status"] for f in r["full_scan"]]


def test_normal_run(monkeypatch, tmp_path):
    r = run(monkeypatch, tmp_path, "ok")
    assert r["stats"]["stopped"] is None and len(r["hot_pages"]) == 2
    assert statuses(r) == ["complete", "complete"]


def test_first_flood_stops_and_still_writes_report(monkeypatch, tmp_path):
    r = run(monkeypatch, tmp_path, "flood")
    assert r["stats"]["stopped"].startswith("FLOOD_WAIT 37")
    assert r["stats"]["requests"] == 3 and r["full_scan"] == []


def test_unknown_timeout_stops_without_more_requests(monkeypatch, tmp_path):
    r = run(monkeypatch, tmp_path, "timeout")
    assert "исход неизвестен" in r["stats"]["stopped"] and r["stats"]["requests"] == 3


def test_repeated_cursor_and_duplicates_are_not_complete(monkeypatch, tmp_path):
    assert set(statuses(run(monkeypatch, tmp_path, "repeat"))) == {"repeated_cursor"}
    assert set(statuses(run(monkeypatch, tmp_path, "dupes"))) == {"duplicates"}


def test_single_collection_and_deadline(monkeypatch, tmp_path):
    r = run(monkeypatch, tmp_path, "ok", "--collection", "5", "--hot", "0", "--full-budget", "30")
    assert [(f["collection"], f["status"]) for f in r["full_scan"]] == [(5, "complete")]
    r = run(monkeypatch, tmp_path, "ok", "--max-seconds", "0.5")
    assert r["stats"]["stopped"].startswith("дедлайн") and r["stats"]["requests"] == 0


def test_report_has_no_account_or_owner_identity(monkeypatch, tmp_path):
    run(monkeypatch, tmp_path, "ok")
    snapshot = (tmp_path / "bench_out" / "snapshot.json").read_text()
    assert "user_id" not in snapshot and '"owner"' not in snapshot


def test_p95_nearest_rank():
    assert bench.p95([0.001, 1.0]) == 1.0
    assert bench.p95([]) == 0
