from __future__ import annotations

from datetime import timezone
import hashlib
from datetime import datetime
import asyncio

import asyncpg
import orjson

from app.config import Settings
from app.models import RawHTTPRecord, UsageTraceCandidate
from app.object_storage import ObjectStorage


class PostgresRecorder:
    def __init__(
        self,
        pool: asyncpg.Pool,
        settings: Settings,
        object_storage: ObjectStorage | None = None,
    ):
        self.pool = pool
        self.settings = settings
        self.object_storage = object_storage

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
        original_request_body = bytes(record.request_body)
        original_response_body = bytes(record.response_body)

        record.request_body_size_bytes = (
            record.request_body_size_bytes
            if record.request_body_size_bytes is not None
            else len(original_request_body)
        )
        record.response_body_size_bytes = (
            record.response_body_size_bytes
            if record.response_body_size_bytes is not None
            else len(original_response_body)
        )
        record.request_body_sha256 = (
            record.request_body_sha256
            or hashlib.sha256(original_request_body).hexdigest()
        )
        record.response_body_sha256 = (
            record.response_body_sha256
            or hashlib.sha256(original_response_body).hexdigest()
        )

        record = await self._archive_inline_if_enabled(record)
        record = self._compact_json_payloads(record)

        request_body = _truncate_bytes(record.request_body, self.settings.max_recorded_body_bytes)
        response_body = _truncate_bytes(record.response_body, self.settings.max_recorded_body_bytes)

        await self.pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                correlation_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                request_json,
                request_body_format,
                stored_request_content_type,
                request_body_size_bytes,
                request_body_sha256,
                request_blob_key,
                request_blob_url,
                response_status,
                response_headers,
                response_body,
                response_json,
                response_body_format,
                stored_response_content_type,
                response_body_size_bytes,
                response_body_sha256,
                response_blob_key,
                response_blob_url,
                archived_at,
                archive_error,
                duration_ms,
                client_ip,
                is_stream,
                upstream_invocation_id,
                chutes_trace,
                error
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19,$20::jsonb,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33::jsonb,$34
            )
            """,
            record.request_id,
            record.correlation_id,
            record.created_at,
            record.method,
            record.path,
            record.query_string,
            record.upstream_url,
            orjson.dumps(record.request_headers).decode("utf-8"),
            request_body,
            orjson.dumps(record.request_json).decode("utf-8")
            if record.request_json is not None
            else None,
            record.request_body_format,
            record.stored_request_content_type,
            record.request_body_size_bytes,
            record.request_body_sha256,
            record.request_blob_key,
            record.request_blob_url,
            record.response_status,
            orjson.dumps(record.response_headers).decode("utf-8"),
            response_body,
            orjson.dumps(record.response_json).decode("utf-8")
            if record.response_json is not None
            else None,
            record.response_body_format,
            record.stored_response_content_type,
            record.response_body_size_bytes,
            record.response_body_sha256,
            record.response_blob_key,
            record.response_blob_url,
            record.archived_at,
            record.archive_error,
            record.duration_ms,
            record.client_ip,
            record.is_stream,
            record.upstream_invocation_id,
            orjson.dumps(record.chutes_trace).decode("utf-8"),
            record.error,
        )

    def _compact_json_payloads(self, record: RawHTTPRecord) -> RawHTTPRecord:
        if record.request_json is not None:
            record.request_body = b""
        if record.response_json is not None:
            record.response_body = b""
        return record

    async def _archive_inline_if_enabled(self, record: RawHTTPRecord) -> RawHTTPRecord:
        if not self.settings.archive_on_ingest or self.object_storage is None:
            return record

        if not record.request_body and not record.response_body:
            return record

        created_at = record.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        date_prefix = created_at.strftime("%Y/%m/%d")
        base_key = (
            f"{self.settings.archive_object_prefix.strip('/')}/"
            f"{date_prefix}/{record.request_id}"
        )

        async def _store_request():
            if not record.request_body:
                return None
            return await self.object_storage.put_bytes(
                key=f"{base_key}/request.bin",
                payload=record.request_body,
                content_type="application/octet-stream",
            )

        async def _store_response():
            if not record.response_body:
                return None
            return await self.object_storage.put_bytes(
                key=f"{base_key}/response.bin",
                payload=record.response_body,
                content_type="application/octet-stream",
            )

        try:
            request_obj, response_obj = await asyncio.gather(
                _store_request(),
                _store_response(),
            )
        except Exception as exc:
            record.archive_error = str(exc)[:1200]
            return record

        record.request_body_size_bytes = len(record.request_body)
        record.request_body_sha256 = hashlib.sha256(record.request_body).hexdigest()
        record.response_body_size_bytes = len(record.response_body)
        record.response_body_sha256 = hashlib.sha256(record.response_body).hexdigest()

        if request_obj is not None:
            record.request_blob_key = request_obj.key
            record.request_blob_url = request_obj.url
            record.request_body_sha256 = request_obj.sha256
            record.request_body_size_bytes = request_obj.size_bytes
        if response_obj is not None:
            record.response_blob_key = response_obj.key
            record.response_blob_url = response_obj.url
            record.response_body_sha256 = response_obj.sha256
            record.response_body_size_bytes = response_obj.size_bytes

        record.request_body = b""
        record.response_body = b""
        record.archived_at = datetime.now(timezone.utc)
        record.archive_error = None
        return record

    async def _record_trace(self, trace: UsageTraceCandidate) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                chat_id = await _get_or_create_chat_id(conn, trace.context_hash)
                parent_chat_id = (
                    await _get_or_create_chat_id(conn, trace.parent_context_hash)
                    if trace.parent_context_hash
                    else -1
                )

                mapped_hash_ids = await _bulk_get_or_create_hash_mapped_ids(
                    conn, trace.raw_hash_values
                )

                started_at = await _get_trace_clock_start(conn)
                observed = trace.observed_at
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=timezone.utc)
                timestamp_seconds = (observed - started_at).total_seconds()

                metadata = {}
                if trace.model:
                    metadata["model"] = trace.model
                if trace.correlation_id:
                    metadata["correlation_id"] = str(trace.correlation_id)
                if trace.upstream_invocation_id:
                    metadata["upstream_invocation_id"] = trace.upstream_invocation_id
                if trace.trace_invocation_id:
                    metadata["trace_invocation_id"] = trace.trace_invocation_id
                if trace.target_instance_id:
                    metadata["target_instance_id"] = trace.target_instance_id
                if trace.target_uid is not None:
                    metadata["target_uid"] = trace.target_uid
                if trace.target_hotkey:
                    metadata["target_hotkey"] = trace.target_hotkey
                if trace.target_coldkey:
                    metadata["target_coldkey"] = trace.target_coldkey
                if trace.target_child_id:
                    metadata["target_child_id"] = trace.target_child_id
                if trace.trace_event_count is not None:
                    metadata["trace_event_count"] = trace.trace_event_count

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


async def _bulk_get_or_create_hash_mapped_ids(
    conn: asyncpg.Connection,
    hash_values: list[int],
) -> list[int]:
    if not hash_values:
        return []

    unique_values = list(set(hash_values))

    rows = await conn.fetch(
        """
        INSERT INTO anon_hash_domain_map (hash_value)
        SELECT unnest($1::bigint[])
        ON CONFLICT (hash_value)
        DO UPDATE SET hash_value = EXCLUDED.hash_value
        RETURNING hash_value, mapped_id
        """,
        unique_values,
    )

    lookup = {int(row["hash_value"]): int(row["mapped_id"]) for row in rows}
    return [lookup[hv] for hv in hash_values]


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
