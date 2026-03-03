from __future__ import annotations

from datetime import timezone

import asyncpg
import orjson

from app.config import Settings
from app.models import RawHTTPRecord, UsageTraceCandidate


class PostgresRecorder:
    def __init__(self, pool: asyncpg.Pool, settings: Settings):
        self.pool = pool
        self.settings = settings

    async def record(
        self,
        raw_record: RawHTTPRecord,
        trace_candidate: UsageTraceCandidate | None,
    ) -> None:
        if self.settings.enable_raw_http_recording:
            await self._record_raw_http(raw_record)

        if self.settings.enable_qwen_trace_recording and trace_candidate is not None:
            await self._record_trace(trace_candidate)

    async def _record_raw_http(self, record: RawHTTPRecord) -> None:
        request_body = _truncate_bytes(record.request_body, self.settings.max_recorded_body_bytes)
        response_body = _truncate_bytes(record.response_body, self.settings.max_recorded_body_bytes)

        await self.pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                duration_ms,
                client_ip,
                is_stream,
                error
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10::jsonb,$11,$12,$13,$14,$15
            )
            """,
            record.request_id,
            record.created_at,
            record.method,
            record.path,
            record.query_string,
            record.upstream_url,
            orjson.dumps(record.request_headers).decode("utf-8"),
            request_body,
            record.response_status,
            orjson.dumps(record.response_headers).decode("utf-8"),
            response_body,
            record.duration_ms,
            record.client_ip,
            record.is_stream,
            record.error,
        )

    async def _record_trace(self, trace: UsageTraceCandidate) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                chat_id = await _get_or_create_chat_id(conn, trace.context_hash)
                parent_chat_id = (
                    await _get_or_create_chat_id(conn, trace.parent_context_hash)
                    if trace.parent_context_hash
                    else -1
                )

                mapped_hash_ids = []
                for hash_value in trace.raw_hash_values:
                    mapped_hash_ids.append(await _get_or_create_hash_mapped_id(conn, hash_value))

                started_at = await _get_trace_clock_start(conn)
                observed = trace.observed_at
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=timezone.utc)
                timestamp_seconds = (observed - started_at).total_seconds()

                metadata = {}
                if trace.model:
                    metadata["model"] = trace.model

                await conn.execute(
                    """
                    INSERT INTO anon_usage_traces (
                        request_id,
                        created_at,
                        chat_id,
                        parent_chat_id,
                        timestamp,
                        input_length,
                        output_length,
                        type,
                        turn,
                        hash_ids,
                        metadata
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::bigint[],$11::jsonb
                    )
                    """,
                    trace.request_id,
                    observed,
                    chat_id,
                    parent_chat_id,
                    timestamp_seconds,
                    trace.input_length,
                    trace.output_length,
                    trace.trace_type,
                    trace.turn,
                    mapped_hash_ids,
                    orjson.dumps(metadata).decode("utf-8"),
                )


async def _get_or_create_chat_id(conn: asyncpg.Connection, context_hash: str | None) -> int:
    if context_hash is None:
        return -1

    row = await conn.fetchrow(
        """
        INSERT INTO anon_trace_sessions (context_hash)
        VALUES ($1)
        ON CONFLICT (context_hash)
        DO UPDATE SET context_hash = EXCLUDED.context_hash
        RETURNING chat_id
        """,
        context_hash,
    )
    return int(row["chat_id"])


async def _get_or_create_hash_mapped_id(conn: asyncpg.Connection, hash_value: int) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO anon_hash_domain_map (hash_value)
        VALUES ($1)
        ON CONFLICT (hash_value)
        DO UPDATE SET hash_value = EXCLUDED.hash_value
        RETURNING mapped_id
        """,
        hash_value,
    )
    return int(row["mapped_id"])


async def _get_trace_clock_start(conn: asyncpg.Connection):
    row = await conn.fetchrow(
        """
        SELECT started_at
        FROM anon_trace_clock
        WHERE clock_id = 1
        """
    )
    return row["started_at"]


def _truncate_bytes(payload: bytes, max_bytes: int) -> bytes:
    if max_bytes and max_bytes > 0:
        return payload[:max_bytes]
    return payload


class NoopRecorder:
    async def record(self, raw_record: RawHTTPRecord, trace_candidate: UsageTraceCandidate | None):
        return None
