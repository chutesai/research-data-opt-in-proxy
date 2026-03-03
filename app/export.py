"""Export anonymized traces in Qwen-Bailian JSONL format.

The Qwen-Bailian format (https://github.com/alibaba-edu/qwen-bailian-usagetraces-anon)
uses JSONL with these fields per line:
    chat_id, parent_chat_id, timestamp, input_length, output_length, type, turn, hash_ids

Our ``anon_usage_traces`` table stores exactly these fields (plus internal
metadata). This module provides a query and formatter to export them.
"""

from __future__ import annotations

import orjson
import asyncpg


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


async def export_traces_jsonl(
    pool: asyncpg.Pool,
    *,
    start_time=None,
    end_time=None,
) -> list[bytes]:
    """Export all anonymized traces as Qwen-Bailian JSONL lines.

    Returns a list of bytes, each a valid JSON line.
    For large datasets, consider streaming directly to a file instead.
    """
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
