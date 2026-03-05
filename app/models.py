from __future__ import annotations

from typing import Any
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class RawHTTPRecord:
    request_id: UUID
    correlation_id: UUID
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
    upstream_invocation_id: str | None
    chutes_trace: dict[str, Any]
    request_body_size_bytes: int | None = None
    request_body_sha256: str | None = None
    response_body_size_bytes: int | None = None
    response_body_sha256: str | None = None
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
    correlation_id: UUID | None = None
    upstream_invocation_id: str | None = None
    trace_invocation_id: str | None = None
    target_instance_id: str | None = None
    target_uid: int | None = None
    target_hotkey: str | None = None
    target_coldkey: str | None = None
    target_child_id: str | None = None
    trace_event_count: int | None = None
