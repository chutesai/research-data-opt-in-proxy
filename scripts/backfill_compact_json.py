#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg

from app.config import Settings
from app.object_storage import create_object_storage
from app.storage_migration import migrate_raw_http_records_to_compact_json


async def _remaining_candidates(pool) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM raw_http_records
                WHERE request_body_format = 'bytes'
                   OR response_body_format = 'bytes'
                   OR (request_body_format = 'json' AND request_json IS NULL)
                   OR (response_body_format = 'json' AND response_json IS NULL)
                """
            )
        )


async def _run() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill legacy raw_http_records rows into compact JSON storage.",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Postgres DSN (defaults to DATABASE_URL env var).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Rows to attempt per batch.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Maximum batches to run. 0 means run until completion.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional pause between batches.",
    )
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required (or pass --database-url).")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")
    if args.max_batches < 0:
        raise SystemExit("--max-batches must be >= 0")
    if args.sleep_seconds < 0:
        raise SystemExit("--sleep-seconds must be >= 0")

    settings = Settings(
        database_url=args.database_url,
        enable_qwen_trace_recording=False,
    )
    pool = await asyncpg.create_pool(
        dsn=args.database_url,
        min_size=0,
        max_size=2,
        command_timeout=600,
    )
    storage = create_object_storage(settings)

    total_scanned = 0
    total_migrated = 0
    total_compacted = 0
    total_skipped = 0

    try:
        batch_number = 0
        while True:
            if args.max_batches and batch_number >= args.max_batches:
                break

            batch_number += 1
            result = await migrate_raw_http_records_to_compact_json(
                pool=pool,
                settings=settings,
                limit=args.batch_size,
                object_storage=storage,
            )
            total_scanned += result.scanned
            total_migrated += result.migrated
            total_compacted += result.compacted
            total_skipped += result.skipped
            remaining = await _remaining_candidates(pool)

            print(
                "batch="
                f"{batch_number} scanned={result.scanned} migrated={result.migrated} "
                f"compacted={result.compacted} skipped={result.skipped} remaining={remaining}",
                flush=True,
            )

            if result.scanned == 0:
                break
            if result.migrated == 0 and result.skipped == result.scanned:
                break
            if remaining == 0:
                break
            if args.sleep_seconds > 0:
                await asyncio.sleep(args.sleep_seconds)
    finally:
        await pool.close()

    print(
        "totals "
        f"scanned={total_scanned} migrated={total_migrated} compacted={total_compacted} "
        f"skipped={total_skipped}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
