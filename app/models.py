from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class RawHTTPRecord:
    request_id: UUID
    created_at: datetime
    method: str
    path: str
    query_string: str
    upstream_url: str
    request_headers: dict[str, list[str]]
    request_body: bytes
    response_status: int
    response_headers: dict[str, list[str]]
    response_body: bytes
    duration_ms: int
    client_ip: str | None
    is_stream: bool
    error: str | None = None


@dataclass(slots=True)
class UsageTraceCandidate:
    request_id: UUID
    observed_at: datetime
    context_hash: str
    parent_context_hash: str | None
    input_length: int
    output_length: int
    trace_type: str
    turn: int
    raw_hash_values: list[int]
    model: str | None = None
