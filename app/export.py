"""Export helpers for raw HTTP trace data."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import orjson

from app.object_storage import ObjectStorage


RAW_EXPORT_QUERY = """\
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
    request_body_size_bytes,
    request_body_sha256,
    request_blob_key,
    request_blob_url,
    response_status,
    response_headers,
    response_body,
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
ORDER BY created_at ASC
"""

RAW_EXPORT_QUERY_WITH_DATE_RANGE = """\
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
    request_body_size_bytes,
    request_body_sha256,
    request_blob_key,
    request_blob_url,
    response_status,
    response_headers,
    response_body,
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
WHERE created_at >= $1 AND created_at < $2
ORDER BY created_at ASC
"""


async def raw_row_to_jsonl(
    row: asyncpg.Record,
    *,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
) -> bytes:
    request_payload = bytes(row["request_body"])
    response_payload = bytes(row["response_body"])

    if resolve_archived_bodies and object_storage is not None:
        if not request_payload and row.get("request_blob_url"):
            request_payload = await object_storage.get_bytes(
                key=row.get("request_blob_key"),
                url=row.get("request_blob_url"),
            )
        if not response_payload and row.get("response_blob_url"):
            response_payload = await object_storage.get_bytes(
                key=row.get("response_blob_key"),
                url=row.get("response_blob_url"),
            )

    request_body_text, request_body_base64 = _decode_body(request_payload)
    response_body_text, response_body_base64 = _decode_body(response_payload)

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
        "request_body_size_bytes": row["request_body_size_bytes"],
        "request_body_sha256": row["request_body_sha256"],
        "request_blob_key": row["request_blob_key"],
        "request_blob_url": row["request_blob_url"],
        "response_status": row["response_status"],
        "response_headers": _json_field(row["response_headers"]),
        "response_body_text": response_body_text,
        "response_body_base64": response_body_base64,
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
    query = RAW_EXPORT_QUERY
    args: list[Any] = []
    if start_time and end_time:
        query = RAW_EXPORT_QUERY_WITH_DATE_RANGE
        args.extend([start_time, end_time])
    if limit is not None and limit > 0:
        query += f"\nLIMIT {int(limit)}"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    lines: list[bytes] = []
    for row in rows:
        lines.append(
            await raw_row_to_jsonl(
                row,
                object_storage=object_storage,
                resolve_archived_bodies=resolve_archived_bodies,
            )
        )
    return lines


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
    lines = await export_raw_http_jsonl(
        pool,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        object_storage=object_storage,
        resolve_archived_bodies=resolve_archived_bodies,
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        for line in lines:
            f.write(line)
            f.write(b"\n")
    return len(lines)


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


def _decode_body(body: bytes | bytearray | memoryview) -> tuple[str | None, str | None]:
    payload = bytes(body)
    if not payload:
        return "", None
    try:
        return payload.decode("utf-8"), None
    except UnicodeDecodeError:
        encoded = base64.b64encode(payload).decode("ascii")
        return None, encoded
