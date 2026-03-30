"""Export helpers for raw HTTP trace data."""

from __future__ import annotations

import base64
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import UUID

import asyncpg
import orjson

from app.object_storage import ObjectStorage

logger = logging.getLogger(__name__)


RAW_EXPORT_BATCH_QUERY = """\
SELECT
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
FROM raw_http_records
WHERE ($1::timestamptz IS NULL OR created_at >= $1)
  AND ($2::timestamptz IS NULL OR created_at < $2)
  AND (
        $3::timestamptz IS NULL
        OR created_at > $3
        OR (created_at = $3 AND request_id > $4::uuid)
      )
ORDER BY created_at ASC, request_id ASC
LIMIT $5
"""


async def raw_row_to_jsonl(
    row: asyncpg.Record,
    *,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
) -> bytes:
    request_json = row["request_json"]
    response_json = row["response_json"]
    request_payload = bytes(row["request_body"])
    response_payload = bytes(row["response_body"])

    if request_json is None and resolve_archived_bodies and object_storage is not None:
        if not request_payload and row["request_blob_url"]:
            request_payload = await object_storage.get_bytes(
                key=row["request_blob_key"],
                url=row["request_blob_url"],
            )
    if response_json is None and resolve_archived_bodies and object_storage is not None:
        if not response_payload and row["response_blob_url"]:
            response_payload = await object_storage.get_bytes(
                key=row["response_blob_key"],
                url=row["response_blob_url"],
            )

    request_body_text, request_body_base64 = _decode_body(
        request_payload,
        json_value=request_json,
    )
    response_body_text, response_body_base64 = _decode_body(
        response_payload,
        json_value=response_json,
    )

    record = {
        "request_id": str(row["request_id"]),
        "correlation_id": str(row["correlation_id"]) if row["correlation_id"] else None,
        "created_at": _to_iso(row["created_at"]),
        "method": row["method"],
        "path": row["path"],
        "query_string": row["query_string"],
        "upstream_url": row["upstream_url"],
        "request_headers": _json_field(row["request_headers"]),
        "request_body_text": request_body_text,
        "request_body_base64": request_body_base64,
        "request_body_format": row["request_body_format"],
        "stored_request_content_type": row["stored_request_content_type"],
        "request_body_size_bytes": row["request_body_size_bytes"],
        "request_body_sha256": row["request_body_sha256"],
        "request_blob_key": row["request_blob_key"],
        "request_blob_url": row["request_blob_url"],
        "response_status": row["response_status"],
        "response_headers": _json_field(row["response_headers"]),
        "response_body_text": response_body_text,
        "response_body_base64": response_body_base64,
        "response_body_format": row["response_body_format"],
        "stored_response_content_type": row["stored_response_content_type"],
        "response_body_size_bytes": row["response_body_size_bytes"],
        "response_body_sha256": row["response_body_sha256"],
        "response_blob_key": row["response_blob_key"],
        "response_blob_url": row["response_blob_url"],
        "archived_at": _to_iso(row["archived_at"]),
        "archive_error": row["archive_error"],
        "duration_ms": row["duration_ms"],
        "client_ip": row["client_ip"],
        "is_stream": row["is_stream"],
        "upstream_invocation_id": row["upstream_invocation_id"],
        "chutes_trace": _json_field(row["chutes_trace"]),
        "error": row["error"],
    }
    return orjson.dumps(record)


async def export_raw_http_jsonl(
    pool: asyncpg.Pool,
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
) -> list[bytes]:
    """Export raw HTTP records to JSONL rows."""
    lines: list[bytes] = []
    async for line in iter_raw_http_jsonl(
        pool,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        object_storage=object_storage,
        resolve_archived_bodies=resolve_archived_bodies,
    ):
        lines.append(line)
    return lines


async def iter_raw_http_jsonl(
    pool: asyncpg.Pool,
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
    batch_size: int = 10,
) -> AsyncIterator[bytes]:
    remaining = limit if limit and limit > 0 else None
    cursor_created_at: datetime | None = None
    cursor_request_id: UUID | None = None

    async with pool.acquire() as conn:
        while True:
            fetch_size = min(batch_size, remaining) if remaining is not None else batch_size
            rows = await conn.fetch(
                RAW_EXPORT_BATCH_QUERY,
                start_time,
                end_time,
                cursor_created_at,
                cursor_request_id,
                fetch_size,
            )
            if not rows:
                break

            for row in rows:
                try:
                    yield await raw_row_to_jsonl(
                        row,
                        object_storage=object_storage,
                        resolve_archived_bodies=resolve_archived_bodies,
                    )
                except Exception:
                    rid = row["request_id"] if row else "unknown"
                    logger.exception("Failed to serialize row %s, skipping", rid)
                    error_record = {"_export_error": True, "request_id": str(rid)}
                    yield orjson.dumps(error_record)

            last_row = rows[-1]
            cursor_created_at = last_row["created_at"]
            cursor_request_id = last_row["request_id"]

            if remaining is not None:
                remaining -= len(rows)
                if remaining <= 0:
                    break


async def export_raw_http_to_file(
    pool: asyncpg.Pool,
    output_path: str | Path,
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
) -> int:
    """Write raw HTTP records to a JSONL file and return row count."""
    row_count = 0
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        async for line in iter_raw_http_jsonl(
            pool,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            object_storage=object_storage,
            resolve_archived_bodies=resolve_archived_bodies,
        ):
            f.write(line)
            f.write(b"\n")
            row_count += 1
    return row_count


def _to_iso(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return orjson.loads(value)
        except orjson.JSONDecodeError:
            return value
    return value


def _decode_body(
    body: bytes | bytearray | memoryview,
    *,
    json_value: Any | None = None,
) -> tuple[str | None, str | None]:
    if json_value is not None:
        return orjson.dumps(json_value).decode("utf-8"), None

    payload = bytes(body)
    if not payload:
        return "", None
    try:
        return payload.decode("utf-8"), None
    except UnicodeDecodeError:
        encoded = base64.b64encode(payload).decode("ascii")
        return None, encoded
