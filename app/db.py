from __future__ import annotations

import asyncpg


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_http_records (
    request_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    query_string TEXT NOT NULL,
    upstream_url TEXT NOT NULL,
    request_headers JSONB NOT NULL,
    request_body BYTEA NOT NULL,
    response_status INTEGER NOT NULL,
    response_headers JSONB NOT NULL,
    response_body BYTEA NOT NULL,
    duration_ms INTEGER NOT NULL,
    client_ip TEXT,
    is_stream BOOLEAN NOT NULL,
    error TEXT
);

CREATE INDEX IF NOT EXISTS raw_http_records_created_at_idx ON raw_http_records (created_at DESC);
CREATE INDEX IF NOT EXISTS raw_http_records_path_idx ON raw_http_records (path);

CREATE TABLE IF NOT EXISTS anon_trace_sessions (
    chat_id BIGSERIAL PRIMARY KEY,
    context_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anon_hash_domain_map (
    hash_value BIGINT PRIMARY KEY,
    mapped_id BIGSERIAL UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anon_trace_clock (
    clock_id SMALLINT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL
);

INSERT INTO anon_trace_clock (clock_id, started_at)
VALUES (1, NOW())
ON CONFLICT (clock_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS anon_usage_traces (
    id BIGSERIAL PRIMARY KEY,
    request_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    chat_id BIGINT NOT NULL,
    parent_chat_id BIGINT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    input_length INTEGER NOT NULL,
    output_length INTEGER NOT NULL,
    type TEXT NOT NULL,
    turn INTEGER NOT NULL,
    hash_ids BIGINT[] NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS anon_usage_traces_request_id_idx ON anon_usage_traces (request_id);
CREATE INDEX IF NOT EXISTS anon_usage_traces_created_at_idx ON anon_usage_traces (created_at DESC);
CREATE INDEX IF NOT EXISTS anon_usage_traces_chat_id_idx ON anon_usage_traces (chat_id);
"""


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=10,
        command_timeout=60,
    )


async def init_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)


async def cleanup_old_records(pool: asyncpg.Pool, retention_days: int) -> dict[str, int]:
    """Delete records older than *retention_days*.

    Returns a dict mapping table name to number of rows deleted.
    """
    if retention_days <= 0:
        return {}

    results: dict[str, int] = {}
    async with pool.acquire() as conn:
        async with conn.transaction():
            for table, col in [
                ("raw_http_records", "created_at"),
                ("anon_usage_traces", "created_at"),
            ]:
                tag = await conn.execute(
                    f"DELETE FROM {table} WHERE {col} < NOW() - INTERVAL '{int(retention_days)} days'"
                )
                results[table] = int(tag.split()[-1])
    return results
