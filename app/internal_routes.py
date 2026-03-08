from __future__ import annotations

import hmac
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.archive_worker import archive_small_batch
from app.db import cleanup_old_records
from app.export import iter_raw_http_jsonl


def create_internal_router() -> APIRouter:
    router = APIRouter(tags=["internal"])

    @router.api_route(
        "/internal/archive/run",
        methods=["GET", "POST"],
        include_in_schema=False,
    )
    async def run_archive(
        request: Request,
        limit: int | None = None,
    ):
        container = request.app.state.container
        settings = container.settings

        if not settings.archive_endpoint_secret:
            return JSONResponse(status_code=404, content={"error": "not_found"})

        if not _secret_ok(
            request=request,
            expected=settings.archive_endpoint_secret,
            header_name=settings.archive_endpoint_secret_header_name,
        ):
            return JSONResponse(status_code=401, content={"error": "unauthorized"})

        if container.db_pool is None:
            return JSONResponse(
                status_code=500,
                content={"error": "db_not_configured"},
            )
        if container.object_storage is None:
            return JSONResponse(
                status_code=500,
                content={"error": "object_storage_not_configured"},
            )

        batch_limit = limit if limit and limit > 0 else settings.archive_batch_size
        result = await archive_small_batch(
            pool=container.db_pool,
            settings=settings,
            storage=container.object_storage,
            limit=batch_limit,
        )

        cleanup = await cleanup_old_records(
            container.db_pool,
            retention_days=settings.retention_days,
        )

        return {
            "ok": True,
            "attempted": result.attempted,
            "archived": result.archived,
            "failed": result.failed,
            "skipped_due_to_lock": result.skipped_due_to_lock,
            "batch_limit": batch_limit,
            "cleanup": cleanup,
        }

    @router.get("/internal/export/raw-http.jsonl", include_in_schema=False)
    async def export_raw_http(
        request: Request,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        resolve_archived_bodies: bool = False,
    ):
        container = request.app.state.container
        settings = container.settings

        if not settings.export_endpoint_secret:
            return JSONResponse(status_code=404, content={"error": "not_found"})

        if not _secret_ok(
            request=request,
            expected=settings.export_endpoint_secret,
            header_name=settings.export_endpoint_secret_header_name,
        ):
            return JSONResponse(status_code=401, content={"error": "unauthorized"})

        if container.db_pool is None:
            return JSONResponse(
                status_code=500,
                content={"error": "db_not_configured"},
            )

        start_dt = _parse_iso(start) if start else None
        end_dt = _parse_iso(end) if end else None
        if (start_dt is None) ^ (end_dt is None):
            return JSONResponse(
                status_code=400,
                content={"error": "bad_request", "detail": "start and end must be provided together"},
            )

        object_storage = container.object_storage if resolve_archived_bodies else None
        async def _iter_lines():
            async for line in iter_raw_http_jsonl(
                container.db_pool,
                start_time=start_dt,
                end_time=end_dt,
                limit=limit,
                object_storage=object_storage,
                resolve_archived_bodies=resolve_archived_bodies,
            ):
                yield line + b"\n"

        return StreamingResponse(
            _iter_lines(),
            media_type="application/x-ndjson",
            headers={"cache-control": "no-store"},
        )

    return router


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _secret_ok(*, request: Request, expected: str, header_name: str) -> bool:
    direct = request.headers.get(header_name)
    if isinstance(direct, str) and hmac.compare_digest(direct, expected):
        return True

    auth_header = request.headers.get("authorization", "")
    prefix = "Bearer "
    if auth_header.startswith(prefix):
        token = auth_header[len(prefix):].strip()
        if token and hmac.compare_digest(token, expected):
            return True

    return False
