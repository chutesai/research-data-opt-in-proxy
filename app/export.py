"""Export anonymized traces in Qwen-Bailian JSONL format.

The Qwen-Bailian format (https://github.com/alibaba-edu/qwen-bailian-usagetraces-anon)
uses JSONL with these fields per line:
    chat_id, parent_chat_id, timestamp, input_length, output_length, type, turn, hash_ids

Our ``anon_usage_traces`` table stores exactly these fields (plus internal
metadata). This module provides a query, formatter, and authenticated HTTP
endpoint to export them.

Access is restricted to API keys listed in ``EXPORT_API_KEY_WHITELIST``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import orjson
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse


EXPORT_QUERY = """\
SELECT
    t.chat_id,
    t.parent_chat_id,
    t.timestamp,
    t.input_length,
    t.output_length,
    t.type,
    t.turn,
    t.hash_ids
FROM anon_usage_traces t
ORDER BY t.created_at ASC
"""

EXPORT_QUERY_WITH_DATE_RANGE = """\
SELECT
    t.chat_id,
    t.parent_chat_id,
    t.timestamp,
    t.input_length,
    t.output_length,
    t.type,
    t.turn,
    t.hash_ids
FROM anon_usage_traces t
WHERE t.created_at >= $1 AND t.created_at < $2
ORDER BY t.created_at ASC
"""


def row_to_jsonl(row: asyncpg.Record) -> bytes:
    """Convert a single query row to a Qwen-Bailian JSONL line."""
    record = {
        "chat_id": row["chat_id"],
        "parent_chat_id": row["parent_chat_id"],
        "timestamp": round(row["timestamp"], 3),
        "input_length": row["input_length"],
        "output_length": row["output_length"],
        "type": row["type"],
        "turn": row["turn"],
        "hash_ids": list(row["hash_ids"]),
    }
    return orjson.dumps(record)


def _extract_bearer_token(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def create_export_router() -> APIRouter:
    router = APIRouter(prefix="/internal", tags=["export"])

    @router.get("/export/traces.jsonl")
    async def export_traces(
        request: Request,
        start: str | None = None,
        end: str | None = None,
    ):
        container = request.app.state.container
        settings = container.settings

        # --- auth gate ---
        allowed_keys = settings.export_api_keys
        if not allowed_keys:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found", "detail": "Export endpoint is disabled"},
            )

        token = _extract_bearer_token(request)
        if not token or token not in allowed_keys:
            return JSONResponse(
                status_code=403,
                content={"error": "forbidden", "detail": "Invalid or missing API key"},
            )

        # --- resolve pool ---
        recorder = container.recorder
        pool = getattr(recorder, "pool", None)
        if pool is None:
            return JSONResponse(
                status_code=503,
                content={"error": "unavailable", "detail": "Database recording is not enabled"},
            )

        # --- parse optional date range ---
        start_time = _parse_iso(start) if start else None
        end_time = _parse_iso(end) if end else None

        async def _stream():
            async with pool.acquire() as conn:
                if start_time and end_time:
                    rows = await conn.fetch(EXPORT_QUERY_WITH_DATE_RANGE, start_time, end_time)
                else:
                    rows = await conn.fetch(EXPORT_QUERY)
                for row in rows:
                    yield row_to_jsonl(row) + b"\n"

        return StreamingResponse(
            _stream(),
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": "attachment; filename=traces.jsonl",
            },
        )

    return router


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# --- standalone helpers (for CLI / cron use) ---

async def export_traces_jsonl(
    pool: asyncpg.Pool,
    *,
    start_time=None,
    end_time=None,
) -> list[bytes]:
    """Export all anonymized traces as Qwen-Bailian JSONL lines."""
    async with pool.acquire() as conn:
        if start_time and end_time:
            rows = await conn.fetch(EXPORT_QUERY_WITH_DATE_RANGE, start_time, end_time)
        else:
            rows = await conn.fetch(EXPORT_QUERY)

    return [row_to_jsonl(row) for row in rows]


async def export_traces_to_file(
    pool: asyncpg.Pool,
    output_path: str,
    *,
    start_time=None,
    end_time=None,
) -> int:
    """Export traces to a JSONL file. Returns number of records written."""
    lines = await export_traces_jsonl(pool, start_time=start_time, end_time=end_time)
    with open(output_path, "wb") as f:
        for line in lines:
            f.write(line)
            f.write(b"\n")
    return len(lines)
