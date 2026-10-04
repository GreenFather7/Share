"""Точка входа: python -m gmw <команда>.

  initdb              создать таблицы
  worker              нормализатор: шина → база → живая лента
  api                 HTTP/WebSocket API
  collect telegram    сборщик маркета Telegram (нужны TG_API_ID, TG_API_HASH и залогиненные сессии)
  collect fake        сборщик фейкового маркета — для разработки без аккаунтов
  login               залогинить сессии из TG_SESSIONS (спросит номер и код)
"""

import argparse
import asyncio
import logging

import redis.asyncio as aioredis

from . import config
from .bus import RedisBus, RedisLive
from .storage import Storage


async def cmd_initdb(s: config.Settings) -> None:
    st = await Storage.connect(s.database_url)
    await st.init()
    await st.close()
    print("таблицы готовы")


async def cmd_worker(s: config.Settings) -> None:
    from .worker import run_worker
    r = aioredis.from_url(s.redis_url, decode_responses=True)
    bus = RedisBus(r)
    await bus.ensure_group()
    st = await Storage.connect(s.database_url)
    await st.init()
    await run_worker(bus, st, RedisLive(r))


async def cmd_collect(s: config.Settings, source: str) -> None:
    from .collectors.market import MarketCollector
    r = aioredis.from_url(s.redis_url, decode_responses=True)
    bus = RedisBus(r)
    await bus.ensure_group()
    st = await Storage.connect(s.database_url)
    await st.init()
    tasks = []
    if source == "fake":
        from .collectors.fake import FakeMarket
        api = FakeMarket()
        tasks.append(asyncio.create_task(api.run_chaos()))
    else:
        from .collectors.telegram import TelegramMarket
        if not (s.tg_api_id and s.tg_api_hash):
            raise SystemExit("Нужны TG_API_ID и TG_API_HASH в .env")
        api = await TelegramMarket.connect(s.tg_sessions, s.tg_api_id, s.tg_api_hash, s.request_interval)
    snapshot = await st.load_listings(api.source)
    await st.close()
    collector = MarketCollector(api, bus, snapshot, hot_min=s.hot_min, hot_max=s.hot_max,
                                full_interval=s.full_interval)
    await collector.run()


async def cmd_login(s: config.Settings) -> None:
    from telethon import TelegramClient
    if not (s.tg_api_id and s.tg_api_hash):
        raise SystemExit("Нужны TG_API_ID и TG_API_HASH в .env")
    for name in s.tg_sessions:
        print(f"== сессия {name}")
        c = TelegramClient(name, s.tg_api_id, s.tg_api_hash)
        await c.start()
        me = await c.get_me()
        print(f"   ок: {me.first_name} (id {me.id})")
        await c.disconnect()


def main() -> None:
    ap = argparse.ArgumentParser(prog="gmw", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("initdb")
    sub.add_parser("worker")
    sub.add_parser("api")
    sub.add_parser("login")
    p = sub.add_parser("collect")
    p.add_argument("source", choices=["telegram", "fake"])
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    s = config.load()
    if args.cmd == "api":
        import uvicorn
        from .api import create_app

        async def serve():
            st = await Storage.connect(s.database_url)
            app = create_app(st, RedisLive(aioredis.from_url(s.redis_url, decode_responses=True)), s.api_token)
            await uvicorn.Server(uvicorn.Config(app, host=s.api_host, port=s.api_port)).serve()
        asyncio.run(serve())
    elif args.cmd == "initdb":
        asyncio.run(cmd_initdb(s))
    elif args.cmd == "worker":
        asyncio.run(cmd_worker(s))
    elif args.cmd == "login":
        asyncio.run(cmd_login(s))
    else:
        asyncio.run(cmd_collect(s, args.source))


if __name__ == "__main__":
    main()
