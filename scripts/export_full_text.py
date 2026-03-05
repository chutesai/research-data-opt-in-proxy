#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timezone

from app.db import create_pool
from app.export import export_raw_http_to_file


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def _run() -> int:
    parser = argparse.ArgumentParser(
        description="Manual full-text export of raw_http_records as JSONL.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSONL path.",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Postgres DSN (defaults to DATABASE_URL env var).",
    )
    parser.add_argument(
        "--start",
        help="Start timestamp (ISO-8601, inclusive).",
    )
    parser.add_argument(
        "--end",
        help="End timestamp (ISO-8601, exclusive).",
    )
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required (or pass --database-url).")

    start_time = _parse_iso(args.start) if args.start else None
    end_time = _parse_iso(args.end) if args.end else None

    pool = await create_pool(args.database_url)
    try:
        count = await export_raw_http_to_file(
            pool,
            args.output,
            start_time=start_time,
            end_time=end_time,
        )
    finally:
        await pool.close()

    print(f"Exported {count} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
