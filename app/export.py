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
# Scout query: lightweight cursor-based pagination over IDs and metadata.
# Avoids decompressing large TOAST columns (request_json, response_json,
# request_body, response_body) so the DB can serve thousands of rows per
# second even when the table holds 100k+ rows with multi-KB JSON payloads.
# ---------------------------------------------------------------------------
SCOUT_BATCH_QUERY = """\
SELECT
    request_id,
    created_at
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

# ---------------------------------------------------------------------------
# Hydrate query: builds a complete JSONL line server-side in Postgres via
# json_build_object so the TOAST data is serialised once inside the DB and
# arrives as a single text column.  Falls back to the Python serialiser for
# rows that need S3 blob resolution or have binary (non-JSON) bodies.
# ---------------------------------------------------------------------------
HYDRATE_JSONL_QUERY = """\
SELECT
    request_id,
    (
      json_build_object(
        'request_id',                   request_id::text,
        'correlation_id',               correlation_id::text,
        'created_at',                   to_char(created_at AT TIME ZONE 'UTC',
                                                'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),
        'method',                       method,
        'path',                         path,
        'query_string',                 query_string,
        'upstream_url',                 upstream_url,
        'request_headers',              request_headers::json,
        'request_body_text',            CASE
                                          WHEN request_json IS NOT NULL THEN request_json::text
                                          ELSE ''
                                        END,
        'request_body_base64',          NULL::text,
        'request_body_format',          request_body_format,
        'stored_request_content_type',  stored_request_content_type,
        'request_body_size_bytes',      request_body_size_bytes,
        'request_body_sha256',          request_body_sha256,
        'request_blob_key',             request_blob_key,
        'request_blob_url',             request_blob_url,
        'response_status',              response_status,
        'response_headers',             response_headers::json,
        'response_body_text',           CASE
                                          WHEN response_json IS NOT NULL THEN response_json::text
                                          ELSE ''
                                        END,
        'response_body_base64',         NULL::text,
        'response_body_format',         response_body_format,
        'stored_response_content_type', stored_response_content_type,
        'response_body_size_bytes',     response_body_size_bytes,
        'response_body_sha256',         response_body_sha256,
        'response_blob_key',            response_blob_key,
        'response_blob_url',            response_blob_url,
        'archived_at',                  CASE
                                          WHEN archived_at IS NULL THEN NULL::text
                                          ELSE to_char(archived_at AT TIME ZONE 'UTC',
                                                       'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"')
                                        END,
        'archive_error',                archive_error,
        'duration_ms',                  duration_ms,
        'client_ip',                    client_ip,
        'is_stream',                    is_stream,
        'upstream_invocation_id',       upstream_invocation_id,
        'chutes_trace',                 chutes_trace::json,
        'error',                        error
      )::text
    ) AS jsonl_line
FROM raw_http_records
WHERE request_id = ANY($1::uuid[])
ORDER BY created_at ASC, request_id ASC
"""

# Legacy full-row query kept for rows that need Python-side body handling
# (S3 blob resolution, binary payloads).
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
WHERE request_id = ANY($1::uuid[])
ORDER BY created_at ASC, request_id ASC
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


async def iter_raw_http_jsonl(
    pool: asyncpg.Pool,
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    object_storage: ObjectStorage | None = None,
    resolve_archived_bodies: bool = False,
    scout_batch_size: int = 200,
    hydrate_batch_size: int = 20,
) -> AsyncIterator[bytes]:
    """Stream JSONL rows using a two-phase scout/hydrate pattern.

    Phase 1 (scout): lightweight cursor query fetches only ``request_id``
    and ``created_at`` — no TOAST decompression.

    Phase 2 (hydrate): for each batch of IDs, build the JSONL line
    server-side in Postgres via ``json_build_object`` so only a single
    text column traverses the wire.  Rows needing S3 blob resolution
    fall back to Python-side serialisation.
    """
    remaining = limit if limit and limit > 0 else None
    cursor_created_at: datetime | None = None
    cursor_request_id: UUID | None = None

    async with pool.acquire() as conn:
        while True:
            scout_size = (
                min(scout_batch_size, remaining)
                if remaining is not None
                else scout_batch_size
            )
            scout_rows = await conn.fetch(
                SCOUT_BATCH_QUERY,
                start_time,
                end_time,
                cursor_created_at,
                cursor_request_id,
                scout_size,
            )
            if not scout_rows:
                break

            # Process scouted IDs in hydration sub-batches
            all_ids = [row["request_id"] for row in scout_rows]
            for i in range(0, len(all_ids), hydrate_batch_size):
                batch_ids = all_ids[i : i + hydrate_batch_size]

                if resolve_archived_bodies and object_storage is not None:
                    # Fall back to Python-side serialisation for blob resolution
                    rows = await conn.fetch(RAW_EXPORT_BATCH_QUERY, batch_ids)
                    for row in rows:
                        try:
                            yield await raw_row_to_jsonl(
                                row,
                                object_storage=object_storage,
                                resolve_archived_bodies=True,
                            )
                        except Exception:
                            rid = row["request_id"] if row else "unknown"
                            logger.exception(
                                "Failed to serialize row %s, skipping", rid,
                            )
                            yield orjson.dumps(
                                {"_export_error": True, "request_id": str(rid)},
                            )
                else:
                    # Fast path: Postgres builds the JSONL line server-side
                    try:
                        jsonl_rows = await conn.fetch(
                            HYDRATE_JSONL_QUERY, batch_ids,
                        )
                        for jrow in jsonl_rows:
                            yield jrow["jsonl_line"].encode("utf-8")
                    except Exception:
                        logger.exception(
                            "Server-side JSONL failed for batch starting %s, "
                            "falling back to row-by-row",
                            batch_ids[0],
                        )
                        # Fallback: fetch full rows and serialise in Python
                        rows = await conn.fetch(
                            RAW_EXPORT_BATCH_QUERY, batch_ids,
                        )
                        for row in rows:
                            try:
                                yield await raw_row_to_jsonl(row)
                            except Exception:
                                rid = row["request_id"] if row else "unknown"
                                logger.exception(
                                    "Failed to serialize row %s, skipping", rid,
                                )
                                yield orjson.dumps(
                                    {"_export_error": True, "request_id": str(rid)},
                                )

            last = scout_rows[-1]
            cursor_created_at = last["created_at"]
            cursor_request_id = last["request_id"]

            if remaining is not None:
                remaining -= len(scout_rows)
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
