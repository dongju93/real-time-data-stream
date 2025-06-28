from contextlib import asynccontextmanager
from typing import AsyncGenerator

from asyncpg import Connection, Pool, create_pool

from utils.config_loader import get_config

HOST: str = get_config("database", "connection", "host")
PORT: int = get_config("database", "connection", "port")
DATABASE: str = get_config("database", "connection", "database")
USER: str = get_config("database", "credentials", "user")
PASSWORD: str = get_config("database", "credentials", "password")


class DatabasePool:
    def __init__(self) -> None:
        self.pool: Pool | None = None

    async def create(self) -> Pool:
        if not self.pool:
            self.pool = await create_pool(
                user=USER,
                password=PASSWORD,
                database=DATABASE,
                host=HOST,
                port=PORT,
                min_size=5,
                max_size=20,
                max_queries=10000,
                max_inactive_connection_lifetime=300.0,
                command_timeout=60,
                server_settings={
                    "jit": "on",
                    "application_name": "real-time-data-stream",
                    "tcp_keepalives_idle": "600",
                    "tcp_keepalives_interval": "30",
                    "tcp_keepalives_count": "3",
                },
            )
        return self.pool

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None


db_pool = DatabasePool()


@asynccontextmanager
async def get_connection() -> AsyncGenerator[Connection]:
    if not db_pool.pool:
        raise RuntimeError("Database pool not initialized")

    async with db_pool.pool.acquire() as conn:
        yield conn
