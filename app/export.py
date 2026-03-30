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


# ---------------------------------------------------------------------------
# Streaming cursor query.  Fetches all columns needed for a JSONL row but
# lets Python assemble the final JSON.  The key optimisation is using an
# asyncpg portal-based cursor (prefetch N) so Postgres streams rows
# incrementally instead of materialising the full result set.
#
# We deliberately exclude request_body / response_body (bytea, always
# empty after compact-json migration) and read request_json / response_json
# as text so we can splice them directly into the output without a
# deserialize–reserialize round trip.
# ---------------------------------------------------------------------------
CURSOR_QUERY = """\
SELECT
    request_id,
    correlation_id,
    created_at,
    method,
    path,
    query_string,
    upstream_url,
    request_headers,
    request_json::text  AS request_json_text,
    request_body_format,
    stored_request_content_type,
    request_body_size_bytes,
    request_body_sha256,
    request_blob_key,
    request_blob_url,
    response_status,
    response_headers,
    response_json::text AS response_json_text,
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
ORDER BY created_at ASC, request_id ASC
"""


def _cursor_row_to_jsonl(row: asyncpg.Record) -> bytes:
    """Build a JSONL line from a cursor row.

    ``request_json_text`` and ``response_json_text`` arrive as pre-serialised
    text strings from Postgres, so we avoid a full deserialize + reserialize
    cycle.
    """
    record: dict[str, Any] = {
        "request_id": str(row["request_id"]),
        "correlation_id": (
            str(row["correlation_id"]) if row["correlation_id"] else None
        ),
        "created_at": _to_iso(row["created_at"]),
        "method": row["method"],
        "path": row["path"],
        "query_string": row["query_string"],
        "upstream_url": row["upstream_url"],
        "request_headers": _json_field(row["request_headers"]),
        "request_body_text": row["request_json_text"] or "",
        "request_body_base64": None,
        "request_body_format": row["request_body_format"],
        "stored_request_content_type": row["stored_request_content_type"],
        "request_body_size_bytes": row["request_body_size_bytes"],
        "request_body_sha256": row["request_body_sha256"],
        "request_blob_key": row["request_blob_key"],
        "request_blob_url": row["request_blob_url"],
        "response_status": row["response_status"],
        "response_headers": _json_field(row["response_headers"]),
        "response_body_text": row["response_json_text"] or "",
        "response_body_base64": None,
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


async def iter_raw_http_jsonl(
    pool: asyncpg.Pool,
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
    cursor_prefetch: int = 50,
) -> AsyncIterator[bytes]:
    """Stream JSONL rows using an asyncpg portal-based cursor.

    A single SQL query selects all rows in order; asyncpg fetches them in
    batches of ``cursor_prefetch`` via a server-side portal so memory stays
    constant regardless of total row count.  Each row is serialised to JSON
    in Python using ``orjson``.
    """
    emitted = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            stmt = await conn.prepare(CURSOR_QUERY)
            async for row in stmt.cursor(
                start_time,
                end_time,
                prefetch=cursor_prefetch,
            ):
                if limit is not None and emitted >= limit:
                    break
                try:
                    yield _cursor_row_to_jsonl(row)
                    emitted += 1
                except Exception:
                    rid = row["request_id"] if row else "unknown"
                    logger.exception(
                        "Failed to serialize row %s, skipping", rid,
                    )
                    yield orjson.dumps(
                        {"_export_error": True, "request_id": str(rid)},
                    )
                    emitted += 1


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
