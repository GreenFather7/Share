"""Настройки из окружения / .env."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    tg_api_id: int | None
    tg_api_hash: str | None
    tg_sessions: list[str]
    request_interval: float
    hot_min: float
    hot_max: float
    full_interval: float
    api_host: str
    api_port: int


def load() -> Settings:
    load_dotenv()
    env = os.getenv
    sessions = env("TG_SESSIONS") or env("TG_SESSION") or "bench"
    return Settings(
        database_url=env("GMW_DATABASE_URL", "postgresql://gmw:gmw@localhost:5432/gmw"),
        redis_url=env("GMW_REDIS_URL", "redis://localhost:6379/0"),
        tg_api_id=int(env("TG_API_ID")) if env("TG_API_ID") else None,
        tg_api_hash=env("TG_API_HASH") or None,
        tg_sessions=[s.strip() for s in sessions.split(",") if s.strip()],
        request_interval=float(env("GMW_REQUEST_INTERVAL", "0.1")),
        hot_min=float(env("GMW_HOT_MIN", "5")),
        hot_max=float(env("GMW_HOT_MAX", "120")),
        full_interval=float(env("GMW_FULL_INTERVAL", "600")),
        api_host=env("GMW_API_HOST", "0.0.0.0"),
        api_port=int(env("GMW_API_PORT", "8000")),
    )
