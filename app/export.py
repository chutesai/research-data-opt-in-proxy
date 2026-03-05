"""Manual full-text export helpers for raw HTTP trace data.

This module intentionally does not expose any HTTP routes. Exports are meant
to be run manually by trusted operators from a secure environment.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import orjson


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
    response_status,
    response_headers,
    response_body,
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
    response_status,
    response_headers,
    response_body,
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


def raw_row_to_jsonl(row: asyncpg.Record) -> bytes:
    request_body_text, request_body_base64 = _decode_body(row["request_body"])
    response_body_text, response_body_base64 = _decode_body(row["response_body"])

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
        "response_status": row["response_status"],
        "response_headers": _json_field(row["response_headers"]),
        "response_body_text": response_body_text,
        "response_body_base64": response_body_base64,
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
) -> list[bytes]:
    """Export raw HTTP records to full-text JSONL rows."""
    async with pool.acquire() as conn:
        if start_time and end_time:
            rows = await conn.fetch(RAW_EXPORT_QUERY_WITH_DATE_RANGE, start_time, end_time)
        else:
            rows = await conn.fetch(RAW_EXPORT_QUERY)
    return [raw_row_to_jsonl(row) for row in rows]


async def export_raw_http_to_file(
    pool: asyncpg.Pool,
    output_path: str | Path,
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> int:
    """Write full-text raw HTTP records to a JSONL file and return row count."""
    lines = await export_raw_http_jsonl(pool, start_time=start_time, end_time=end_time)
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
