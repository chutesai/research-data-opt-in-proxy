from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timezone
from uuid import UUID

import asyncpg

from app.config import Settings
from app.object_storage import ObjectStorage


_ARCHIVE_LOCK_ID = 512874330


@dataclass(slots=True)
class ArchiveBatchResult:
    attempted: int
    archived: int
    failed: int
    skipped_due_to_lock: bool = False


async def archive_small_batch(
    *,
    pool: asyncpg.Pool,
    settings: Settings,
    storage: ObjectStorage,
    limit: int | None = None,
) -> ArchiveBatchResult:
    batch_limit = limit or settings.archive_batch_size
    if batch_limit <= 0:
        return ArchiveBatchResult(attempted=0, archived=0, failed=0)

    async with pool.acquire() as conn:
        lock_ok = await conn.fetchval("SELECT pg_try_advisory_lock($1)", _ARCHIVE_LOCK_ID)
        if not lock_ok:
            return ArchiveBatchResult(
                attempted=0,
                archived=0,
                failed=0,
                skipped_due_to_lock=True,
            )

        try:
            rows = await conn.fetch(
                """
                SELECT
                    request_id,
                    created_at,
                    request_body,
                    response_body,
                    request_body_sha256,
                    response_body_sha256,
                    request_body_size_bytes,
                    response_body_size_bytes
                FROM raw_http_records
                WHERE archived_at IS NULL
                  AND (octet_length(request_body) > 0 OR octet_length(response_body) > 0)
                ORDER BY created_at ASC
                LIMIT $1
                """,
                batch_limit,
            )

            archived = 0
            failed = 0

            for row in rows:
                request_id: UUID = row["request_id"]
                created_at = row["created_at"]
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)

                date_prefix = created_at.strftime("%Y/%m/%d")
                base_key = f"{settings.archive_object_prefix.strip('/')}/{date_prefix}/{request_id}"
                request_body = bytes(row["request_body"])
                response_body = bytes(row["response_body"])

                try:
                    req_key = None
                    req_url = None
                    req_sha = row["request_body_sha256"]
                    req_size = row["request_body_size_bytes"]

                    if request_body:
                        req_obj = await storage.put_bytes(
                            key=f"{base_key}/request.bin",
                            payload=request_body,
                            content_type="application/octet-stream",
                        )
                        req_key = req_obj.key
                        req_url = req_obj.url
                        req_sha = req_obj.sha256
                        req_size = req_obj.size_bytes

                    res_key = None
                    res_url = None
                    res_sha = row["response_body_sha256"]
                    res_size = row["response_body_size_bytes"]

                    if response_body:
                        res_obj = await storage.put_bytes(
                            key=f"{base_key}/response.bin",
                            payload=response_body,
                            content_type="application/octet-stream",
                        )
                        res_key = res_obj.key
                        res_url = res_obj.url
                        res_sha = res_obj.sha256
                        res_size = res_obj.size_bytes

                    # Fallback checksum/size if the body was already empty.
                    req_sha = req_sha or hashlib.sha256(request_body).hexdigest()
                    res_sha = res_sha or hashlib.sha256(response_body).hexdigest()
                    req_size = req_size if req_size is not None else len(request_body)
                    res_size = res_size if res_size is not None else len(response_body)

                    await conn.execute(
                        """
                        UPDATE raw_http_records
                        SET request_blob_key = COALESCE($2, request_blob_key),
                            request_blob_url = COALESCE($3, request_blob_url),
                            request_body_sha256 = COALESCE($4, request_body_sha256),
                            request_body_size_bytes = COALESCE($5, request_body_size_bytes),
                            response_blob_key = COALESCE($6, response_blob_key),
                            response_blob_url = COALESCE($7, response_blob_url),
                            response_body_sha256 = COALESCE($8, response_body_sha256),
                            response_body_size_bytes = COALESCE($9, response_body_size_bytes),
                            request_body = $10,
                            response_body = $11,
                            archived_at = NOW(),
                            archive_error = NULL
                        WHERE request_id = $1
                        """,
                        request_id,
                        req_key,
                        req_url,
                        req_sha,
                        req_size,
                        res_key,
                        res_url,
                        res_sha,
                        res_size,
                        b"",
                        b"",
                    )
                    archived += 1
                except Exception as exc:  # pragma: no cover - defensive path
                    failed += 1
                    await conn.execute(
                        """
                        UPDATE raw_http_records
                        SET archive_error = $2
                        WHERE request_id = $1
                        """,
                        request_id,
                        str(exc)[:1200],
                    )

            return ArchiveBatchResult(
                attempted=len(rows),
                archived=archived,
                failed=failed,
            )
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _ARCHIVE_LOCK_ID)
