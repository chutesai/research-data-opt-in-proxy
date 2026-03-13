from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timezone

import asyncpg
import orjson

from app.config import Settings
from app.object_storage import ObjectStorage
from app.response_storage import normalize_request_for_storage, normalize_response_for_storage


@dataclass(slots=True)
class CompactMigrationResult:
    scanned: int = 0
    migrated: int = 0
    compacted: int = 0
    skipped: int = 0

    def add(self, other: "CompactMigrationResult") -> None:
        self.scanned += other.scanned
        self.migrated += other.migrated
        self.compacted += other.compacted
        self.skipped += other.skipped


async def migrate_raw_http_records_to_compact_json(
    *,
    pool: asyncpg.Pool,
    settings: Settings,
    limit: int,
    object_storage: ObjectStorage | None = None,
    concurrency: int = 1,
) -> CompactMigrationResult:
    if limit <= 0:
        return CompactMigrationResult()

    concurrency = max(1, concurrency)
    result = CompactMigrationResult()
    async with pool.acquire() as conn:
        candidate_rows = await conn.fetch(
            """
            SELECT
                request_id,
                created_at
            FROM raw_http_records
            WHERE (
                    request_body_format = 'bytes'
                 OR response_body_format = 'bytes'
                 OR (request_body_format = 'json' AND request_json IS NULL)
                 OR (response_body_format = 'json' AND response_json IS NULL)
              )
              AND NOT (
                  (COALESCE(request_blob_url, '') LIKE 'mem://%' AND COALESCE(octet_length(request_body), 0) = 0)
               OR (COALESCE(response_blob_url, '') LIKE 'mem://%' AND COALESCE(octet_length(response_body), 0) = 0)
              )
            ORDER BY created_at ASC, request_id ASC
            LIMIT $1
            """,
            limit,
        )

    if concurrency == 1:
        for candidate in candidate_rows:
            result.add(await _migrate_request_id(
                pool=pool,
                settings=settings,
                request_id=candidate["request_id"],
                object_storage=object_storage,
            ))
        return result

    semaphore = asyncio.Semaphore(concurrency)

    async def _run_candidate(request_id):
        async with semaphore:
            return await _migrate_request_id(
                pool=pool,
                settings=settings,
                request_id=request_id,
                object_storage=object_storage,
            )

    batch_results = await asyncio.gather(
        *(_run_candidate(candidate["request_id"]) for candidate in candidate_rows)
    )
    for item in batch_results:
        result.add(item)

    return result


async def _migrate_request_id(
    *,
    pool: asyncpg.Pool,
    settings: Settings,
    request_id,
    object_storage: ObjectStorage | None,
) -> CompactMigrationResult:
    result = CompactMigrationResult(scanned=1)
    try:
        async with asyncio.timeout(120):
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        request_id,
                        created_at,
                        request_body_format,
                        COALESCE(octet_length(request_body), 0) AS request_body_inline_size,
                        request_json IS NOT NULL AS has_request_json,
                        request_blob_key,
                        request_blob_url,
                        response_headers,
                        response_body_format,
                        stored_response_content_type,
                        COALESCE(octet_length(response_body), 0) AS response_body_inline_size,
                        response_json IS NOT NULL AS has_response_json,
                        response_blob_key,
                        response_blob_url,
                        archived_at
                    FROM raw_http_records
                    WHERE request_id = $1
                    """,
                    request_id,
                )
                if row is None:
                    result.skipped = 1
                    return result

                request_format = row["request_body_format"] or "bytes"
                response_format = row["response_body_format"] or "bytes"
                request_blob_key = row["request_blob_key"]
                request_blob_url = row["request_blob_url"]
                response_blob_key = row["response_blob_key"]
                response_blob_url = row["response_blob_url"]
                archived_at = row["archived_at"]
                request_inline_size = int(row["request_body_inline_size"] or 0)
                response_inline_size = int(row["response_body_inline_size"] or 0)
                request_json_exists = bool(row["has_request_json"])
                response_json_exists = bool(row["has_response_json"])
                request_json = None
                response_json = None
                request_body = b""
                response_body = b""

                request_needs_body = (
                    not request_json_exists
                    or (
                        request_json_exists
                        and request_inline_size > 0
                        and request_blob_url is None
                        and object_storage is not None
                    )
                )
                response_needs_body = (
                    not response_json_exists
                    or (
                        response_json_exists
                        and response_inline_size > 0
                        and response_blob_url is None
                        and object_storage is not None
                    )
                )

                if request_needs_body and request_inline_size > 0:
                    request_body = await _load_inline_body_bytes(
                        conn=conn,
                        request_id=row["request_id"],
                        column="request_body",
                    )

                if response_needs_body and response_inline_size > 0:
                    response_body = await _load_inline_body_bytes(
                        conn=conn,
                        request_id=row["request_id"],
                        column="response_body",
                    )

                if not request_json_exists:
                    if not request_body and row["request_blob_url"] and object_storage is not None:
                        request_body = await object_storage.get_bytes(
                            key=row["request_blob_key"],
                            url=row["request_blob_url"],
                        )
                    request_json, request_format = normalize_request_for_storage(request_body)
                elif request_format == "bytes" and not request_body and row["request_blob_url"] is None:
                    request_format = "empty"

                if not response_json_exists:
                    if not response_body and row["response_blob_url"] and object_storage is not None:
                        response_body = await object_storage.get_bytes(
                            key=row["response_blob_key"],
                            url=row["response_blob_url"],
                        )
                    response_headers = row["response_headers"]
                    if isinstance(response_headers, str):
                        response_headers = orjson.loads(response_headers)
                    response_content_type = (
                        row["stored_response_content_type"]
                        or _first_header_value(response_headers, "content-type")
                        or ""
                    )
                    normalized_response = normalize_response_for_storage(
                        response_body,
                        response_content_type,
                        observed_at=row["created_at"],
                    )
                    if normalized_response is not None:
                        response_json = normalized_response.json_payload
                        response_format = normalized_response.storage_format
                    elif not response_body and row["response_blob_url"] is None:
                        response_format = "empty"
                elif response_format == "bytes" and not response_body and row["response_blob_url"] is None:
                    response_format = "empty"

                request_has_json = request_json_exists or request_json is not None
                response_has_json = response_json_exists or response_json is not None

                if (
                    not request_has_json
                    and not response_has_json
                    and request_format == "bytes"
                    and response_format == "bytes"
                ):
                    result.skipped = 1
                    return result

                compacted = False

                if request_has_json and request_body and request_blob_url is None and object_storage is not None:
                    request_blob_key, request_blob_url = await _archive_body(
                        object_storage=object_storage,
                        settings=settings,
                        created_at=row["created_at"],
                        request_id=str(row["request_id"]),
                        suffix="request.bin",
                        payload=request_body,
                    )
                    compacted = True

                if response_has_json and response_body and response_blob_url is None and object_storage is not None:
                    response_blob_key, response_blob_url = await _archive_body(
                        object_storage=object_storage,
                        settings=settings,
                        created_at=row["created_at"],
                        request_id=str(row["request_id"]),
                        suffix="response.bin",
                        payload=response_body,
                    )
                    compacted = True

                request_body_present = bool(request_body) or request_inline_size > 0
                response_body_present = bool(response_body) or response_inline_size > 0
                clear_request_body = bool(
                    request_has_json and (request_blob_url is not None or not request_body_present)
                )
                clear_response_body = bool(
                    response_has_json and (response_blob_url is not None or not response_body_present)
                )
                request_format_changed = request_format != (row["request_body_format"] or "bytes")
                response_format_changed = response_format != (row["response_body_format"] or "bytes")

                if clear_request_body or clear_response_body:
                    compacted = True
                if request_format_changed or response_format_changed:
                    compacted = True

                await conn.execute(
                    """
                    UPDATE raw_http_records
                    SET request_json = COALESCE($2::jsonb, request_json),
                        request_body_format = CASE
                            WHEN $3::text IS NULL THEN request_body_format
                            ELSE $3
                        END,
                        stored_request_content_type = COALESCE($4, stored_request_content_type, 'application/json'),
                        request_blob_key = COALESCE($5, request_blob_key),
                        request_blob_url = COALESCE($6, request_blob_url),
                        response_json = COALESCE($7::jsonb, response_json),
                        response_body_format = CASE
                            WHEN $8::text IS NULL THEN response_body_format
                            ELSE $8
                        END,
                        stored_response_content_type = COALESCE($9, stored_response_content_type, 'application/json'),
                        response_blob_key = COALESCE($10, response_blob_key),
                        response_blob_url = COALESCE($11, response_blob_url),
                        archived_at = CASE
                            WHEN $12::timestamptz IS NOT NULL THEN COALESCE(archived_at, $12::timestamptz)
                            ELSE archived_at
                        END,
                        request_body = CASE WHEN $13 THEN $14::bytea ELSE request_body END,
                        response_body = CASE WHEN $15 THEN $16::bytea ELSE response_body END
                    WHERE request_id = $1
                    """,
                    row["request_id"],
                    orjson.dumps(request_json).decode("utf-8") if request_json is not None else None,
                    request_format if (request_json is not None or request_format_changed) else None,
                    "application/json" if request_json is not None else None,
                    request_blob_key,
                    request_blob_url,
                    orjson.dumps(response_json).decode("utf-8") if response_json is not None else None,
                    response_format if (response_json is not None or response_format_changed) else None,
                    "application/json" if response_json is not None else None,
                    response_blob_key,
                    response_blob_url,
                    archived_at if compacted else None,
                    clear_request_body,
                    b"",
                    clear_response_body,
                    b"",
                )

                result.migrated = 1
                result.compacted = 1 if compacted else 0
                return result
    except Exception:
        result.skipped = 1
        return result


async def _archive_body(
    *,
    object_storage: ObjectStorage,
    settings: Settings,
    created_at,
    request_id: str,
    suffix: str,
    payload: bytes,
) -> tuple[str, str]:
    created = created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    base_key = (
        f"{settings.archive_object_prefix.strip('/')}/"
        f"{created.strftime('%Y/%m/%d')}/{request_id}"
    )
    stored = await object_storage.put_bytes(
        key=f"{base_key}/{suffix}",
        payload=payload,
        content_type="application/octet-stream",
    )
    return stored.key, stored.url


async def _load_inline_body_bytes(
    *,
    conn: asyncpg.Connection,
    request_id,
    column: str,
) -> bytes:
    if column not in {"request_body", "response_body"}:
        raise ValueError(f"Unsupported bytea column: {column}")

    value = await conn.fetchval(
        f"SELECT {column} FROM raw_http_records WHERE request_id = $1",
        request_id,
    )
    if value is None:
        return b""
    return bytes(value)


def _first_header_value(headers, key: str) -> str | None:
    if not isinstance(headers, dict):
        return None
    values = headers.get(key)
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, str):
            return first
    return None
