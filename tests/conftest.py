from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

import asyncpg
import pytest

from app.config import Settings


@pytest.fixture(scope="session")
def database_url() -> str | None:
    return os.getenv("TEST_DATABASE_URL")


@pytest.fixture
def require_database(database_url: str | None) -> str:
    runtime_database_url = os.getenv("DATABASE_URL")

    if not database_url:
        if runtime_database_url:
            pytest.skip(
                "TEST_DATABASE_URL is required for destructive DB-backed tests; "
                "DATABASE_URL is intentionally ignored"
            )
        pytest.skip("TEST_DATABASE_URL is required for this test")

    if runtime_database_url and _same_database_target(database_url, runtime_database_url):
        pytest.skip(
            "TEST_DATABASE_URL resolves to the same database target as DATABASE_URL; "
            "destructive DB-backed tests are blocked to protect recorded data"
        )
    return database_url


@pytest.fixture
def settings_factory(require_database: str) -> Callable[..., Settings]:
    def _factory(**overrides) -> Settings:
        base = {
            "database_url": require_database,
            "upstream_base_url": "https://upstream.test",
            "enable_raw_http_recording": True,
            "enable_qwen_trace_recording": True,
            "anonymization_hash_salt": "integration-test-salt-long-enough-to-pass",
            "upstream_discount_header_name": "X-Chutes-Research-OptIn",
            "upstream_discount_header_value": "true",
        }
        base.update(overrides)
        return Settings(**base)

    return _factory


async def _truncate_tables(database_url: str) -> None:
    conn = await _connect_with_retry(database_url)
    try:
        await conn.execute(
            """
            TRUNCATE TABLE
                anon_usage_traces,
                anon_hash_domain_map,
                anon_trace_sessions,
                raw_http_records
            RESTART IDENTITY;
            """
        )
    except asyncpg.UndefinedTableError:
        pass
    finally:
        await conn.close()


async def _fetch_one(database_url: str, query: str, *args):
    conn = await _connect_with_retry(database_url)
    try:
        return await conn.fetchrow(query, *args)
    finally:
        await conn.close()


async def _fetch_value(database_url: str, query: str, *args):
    conn = await _connect_with_retry(database_url)
    try:
        return await conn.fetchval(query, *args)
    finally:
        await conn.close()


@pytest.fixture
def db_truncate(
    require_database: str,
) -> Callable[[], Awaitable[None]]:
    async def _run() -> None:
        await _truncate_tables(require_database)

    return _run


@pytest.fixture
def db_fetch_one(
    require_database: str,
) -> Callable[..., Awaitable[asyncpg.Record | None]]:
    async def _run(query: str, *args):
        return await _fetch_one(require_database, query, *args)

    return _run


@pytest.fixture
def db_fetch_value(
    require_database: str,
) -> Callable[..., Awaitable[object]]:
    async def _run(query: str, *args):
        return await _fetch_value(require_database, query, *args)

    return _run


async def _connect_with_retry(database_url: str, attempts: int = 3) -> asyncpg.Connection:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.connect(database_url)
        except Exception as exc:  # pragma: no cover - transient infra path
            last_error = exc
            if attempt == attempts:
                raise
            await asyncio.sleep(0.6 * attempt)
    assert last_error is not None
    raise last_error


def _same_database_target(left: str, right: str) -> bool:
    left_parts = _database_target_parts(left)
    right_parts = _database_target_parts(right)
    return bool(left_parts and right_parts and left_parts == right_parts)


def _database_target_parts(url: str) -> tuple[str, str | None, int | None, str] | None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return None

    hostname = parsed.hostname.lower() if parsed.hostname else None
    database_name = parsed.path.lstrip("/").strip()
    if not hostname or not database_name:
        return None

    return (
        parsed.scheme,
        hostname,
        parsed.port,
        database_name,
    )
