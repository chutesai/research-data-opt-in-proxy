from __future__ import annotations

import re
from typing import Any, Mapping

import orjson


_MAX_STORED_TRACE_EVENTS = 128

_TRACE_TARGET_RE = re.compile(
    r"query target=([^ ]+) uid=([0-9]+) hotkey=([^ ]+)(?: coldkey=([^ ]+))?"
)
_TRACE_TARGET_ERROR_RE = re.compile(
    r"error encountered while querying target=([^ ]+) uid=([0-9]+) "
    r"hotkey=([^ ]+)(?: coldkey=([^ ]+))?: (.*)"
)


def extract_chutes_trace_metadata(
    response_body: bytes,
    response_content_type: str,
    response_headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Extract trace metadata from Chutes trace SSE envelopes.

    When `X-Chutes-Trace: true` is enabled upstream, streamed payloads are
    wrapped in `{"trace": ...}` and `{"result": ...}` SSE events.
    """
    metadata: dict[str, Any] = {}

    upstream_invocation_id = _get_header(response_headers, "x-chutes-invocationid")
    if upstream_invocation_id:
        metadata["upstream_invocation_id"] = upstream_invocation_id

    if "text/event-stream" not in response_content_type.lower():
        return metadata

    trace_events: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total_trace_events = 0
    trace_invocation_id: str | None = None

    decoded = response_body.decode("utf-8", errors="ignore")
    for payload in _iter_top_level_sse_payloads(decoded):
        trace = payload.get("trace")
        if not isinstance(trace, dict):
            continue

        total_trace_events += 1
        if len(trace_events) < _MAX_STORED_TRACE_EVENTS:
            trace_events.append(trace)

        invocation_id = _as_str(trace.get("invocation_id"))
        if invocation_id:
            trace_invocation_id = invocation_id

        child_id = _as_str(trace.get("child_id"))
        chute_id = _as_str(trace.get("chute_id"))
        function_name = _as_str(trace.get("function"))
        timestamp = _as_str(trace.get("timestamp"))
        message = _as_str(trace.get("message")) or ""

        if match := _TRACE_TARGET_RE.search(message):
            attempt = {
                "instance_id": match.group(1),
                "uid": _as_int(match.group(2)),
                "hotkey": match.group(3),
                "coldkey": match.group(4),
                "invocation_id": invocation_id,
                "child_id": child_id,
                "chute_id": chute_id,
                "function": function_name,
                "timestamp": timestamp,
                "message": message,
            }
            attempts.append(attempt)

        if match := _TRACE_TARGET_ERROR_RE.search(message):
            error = {
                "instance_id": match.group(1),
                "uid": _as_int(match.group(2)),
                "hotkey": match.group(3),
                "coldkey": match.group(4),
                "error": match.group(5),
                "invocation_id": invocation_id,
                "child_id": child_id,
                "chute_id": chute_id,
                "function": function_name,
                "timestamp": timestamp,
                "message": message,
            }
            errors.append(error)

    if trace_invocation_id:
        metadata["trace_invocation_id"] = trace_invocation_id

    if total_trace_events:
        metadata["trace_event_count"] = total_trace_events
    if trace_events:
        metadata["trace_events"] = trace_events
    if total_trace_events > len(trace_events):
        metadata["trace_event_truncated"] = True

    if attempts:
        errored_instances = {
            (item.get("instance_id"), item.get("child_id"))
            for item in errors
        }
        for attempt in attempts:
            attempt["has_error"] = (
                (attempt.get("instance_id"), attempt.get("child_id")) in errored_instances
            )
        metadata["attempts"] = attempts

    if errors:
        metadata["errors"] = errors

    selected_attempt = _select_target_attempt(attempts)
    if selected_attempt:
        metadata["target_instance_id"] = selected_attempt.get("instance_id")
        metadata["target_uid"] = selected_attempt.get("uid")
        metadata["target_hotkey"] = selected_attempt.get("hotkey")
        metadata["target_coldkey"] = selected_attempt.get("coldkey")
        metadata["target_child_id"] = selected_attempt.get("child_id")

    return metadata


def unwrap_chutes_non_stream_body(
    response_body: bytes,
) -> tuple[bytes, str] | None:
    """Unwrap trace-envelope payloads for non-streaming responses."""
    try:
        direct_payload = orjson.loads(response_body)
    except orjson.JSONDecodeError:
        direct_payload = None

    if isinstance(direct_payload, dict):
        if unwrapped := _unwrap_envelope_dict(direct_payload):
            return unwrapped

    decoded = response_body.decode("utf-8", errors="ignore")
    payloads = list(_iter_top_level_sse_payloads(decoded))
    if not payloads:
        return None

    non_trace_payloads = [
        payload
        for payload in payloads
        if not isinstance(payload.get("trace"), dict)
    ]
    if not non_trace_payloads:
        return None

    if len(non_trace_payloads) == 1:
        payload = non_trace_payloads[0]
        if unwrapped := _unwrap_envelope_dict(payload):
            return unwrapped
        return orjson.dumps(payload), "application/json"

    result_chunks: list[bytes] = []
    for payload in non_trace_payloads:
        if isinstance(payload.get("result"), str):
            result_chunks.append(payload["result"].encode("utf-8"))

    if result_chunks:
        return b"".join(result_chunks), "text/event-stream"

    return None


def _select_target_attempt(attempts: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not attempts:
        return None
    for attempt in reversed(attempts):
        if not attempt.get("has_error"):
            return attempt
    return attempts[-1]


def _iter_top_level_sse_payloads(decoded_sse: str):
    for line in decoded_sse.splitlines():
        payload = _parse_sse_data_line(line)
        if payload is not None:
            yield payload


def _get_header(headers: Mapping[str, str] | None, key_lower: str) -> str | None:
    if not headers:
        return None
    for key, value in headers.items():
        if key.lower() == key_lower:
            return value
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _as_str(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _looks_like_json_payload(payload: dict[str, Any]) -> bool:
    keys = {"id", "object", "choices", "usage"}
    return bool(keys.intersection(payload.keys()))


def _extract_first_inner_payload(result: str) -> dict[str, Any] | None:
    for line in result.splitlines():
        payload = _parse_sse_data_line(line)
        if payload is not None:
            return payload
    return None


def _parse_sse_data_line(line: str) -> dict[str, Any] | None:
    if not line.startswith("data:"):
        return None
    raw = line[5:].strip()
    if not raw or raw == "[DONE]":
        return None
    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _unwrap_envelope_dict(
    payload: dict[str, Any],
    default_content_type: str = "application/json",
) -> tuple[bytes, str] | None:
    content_type = _as_str(payload.get("content_type")) or default_content_type

    if isinstance(payload.get("json"), (dict, list)):
        return orjson.dumps(payload["json"]), content_type

    if isinstance(payload.get("result"), dict):
        return _unwrap_envelope_dict(payload["result"], content_type)

    if isinstance(payload.get("result"), str):
        result_payload = _extract_first_inner_payload(payload["result"])
        if result_payload is not None:
            nested = _unwrap_envelope_dict(result_payload, content_type)
            if nested is not None:
                return nested
            return orjson.dumps(result_payload), "application/json"
        return payload["result"].encode("utf-8"), content_type

    if _looks_like_json_payload(payload):
        return orjson.dumps(payload), "application/json"

    return None


class TraceSSEUnwrapper:
    """Unwrap `{"result": ...}` SSE envelopes and drop `{"trace": ...}` events."""

    def __init__(self):
        self._buffer = bytearray()
        self._saw_wrapped = False

    def feed(self, chunk: bytes) -> bytes:
        self._buffer.extend(chunk)
        output = bytearray()
        while True:
            newline_idx = self._buffer.find(b"\n")
            if newline_idx < 0:
                break
            line = bytes(self._buffer[: newline_idx + 1])
            del self._buffer[: newline_idx + 1]
            output.extend(self._transform_line(line))
        return bytes(output)

    def finalize(self) -> bytes:
        if not self._buffer:
            return b""
        line = bytes(self._buffer)
        self._buffer.clear()
        if not line.endswith(b"\n"):
            line = line + b"\n"
        return self._transform_line(line)

    def _transform_line(self, line_with_newline: bytes) -> bytes:
        stripped = line_with_newline.rstrip(b"\r\n")
        if not stripped:
            if self._saw_wrapped:
                return b""
            return line_with_newline

        if not stripped.startswith(b"data:"):
            return line_with_newline

        payload_raw = stripped[5:].strip()
        if not payload_raw:
            if self._saw_wrapped:
                return b""
            return line_with_newline

        try:
            payload = orjson.loads(payload_raw)
        except orjson.JSONDecodeError:
            return line_with_newline

        if not isinstance(payload, dict):
            return line_with_newline

        if "trace" in payload:
            self._saw_wrapped = True
            return b": trace\n\n"

        if "result" in payload:
            self._saw_wrapped = True
            return _result_to_bytes(payload["result"])

        return line_with_newline


def _result_to_bytes(result: Any) -> bytes:
    if isinstance(result, str):
        encoded = result.encode("utf-8")
        if encoded.endswith(b"\n"):
            return encoded
        return encoded + b"\n"
    if isinstance(result, bytes):
        return result
    if result is None:
        return b"\n"
    return b"data: " + orjson.dumps(result) + b"\n\n"


class StreamingTraceMetadataBuilder:
    def __init__(self, response_headers: Mapping[str, str] | None = None):
        self._buffer = bytearray()
        self._trace_events: list[dict[str, Any]] = []
        self._attempts: list[dict[str, Any]] = []
        self._errors: list[dict[str, Any]] = []
        self._total_trace_events = 0
        self._trace_invocation_id: str | None = None
        self._upstream_invocation_id = _get_header(
            response_headers,
            "x-chutes-invocationid",
        )

    def feed(self, chunk: bytes) -> None:
        self._buffer.extend(chunk)
        while True:
            newline_idx = self._buffer.find(b"\n")
            if newline_idx < 0:
                break
            line = bytes(self._buffer[: newline_idx + 1])
            del self._buffer[: newline_idx + 1]
            self._consume_line(line)

    def finalize(self) -> dict[str, Any]:
        if self._buffer:
            trailing = bytes(self._buffer)
            self._buffer.clear()
            if not trailing.endswith(b"\n"):
                trailing += b"\n"
            self._consume_line(trailing)

        metadata: dict[str, Any] = {}
        if self._upstream_invocation_id:
            metadata["upstream_invocation_id"] = self._upstream_invocation_id
        if self._trace_invocation_id:
            metadata["trace_invocation_id"] = self._trace_invocation_id
        if self._total_trace_events:
            metadata["trace_event_count"] = self._total_trace_events
        if self._trace_events:
            metadata["trace_events"] = self._trace_events
        if self._total_trace_events > len(self._trace_events):
            metadata["trace_event_truncated"] = True
        if self._attempts:
            errored_instances = {
                (item.get("instance_id"), item.get("child_id"))
                for item in self._errors
            }
            for attempt in self._attempts:
                attempt["has_error"] = (
                    (attempt.get("instance_id"), attempt.get("child_id")) in errored_instances
                )
            metadata["attempts"] = self._attempts
        if self._errors:
            metadata["errors"] = self._errors

        selected_attempt = _select_target_attempt(self._attempts)
        if selected_attempt:
            metadata["target_instance_id"] = selected_attempt.get("instance_id")
            metadata["target_uid"] = selected_attempt.get("uid")
            metadata["target_hotkey"] = selected_attempt.get("hotkey")
            metadata["target_coldkey"] = selected_attempt.get("coldkey")
            metadata["target_child_id"] = selected_attempt.get("child_id")

        return metadata

    def _consume_line(self, line_with_newline: bytes) -> None:
        stripped = line_with_newline.rstrip(b"\r\n")
        if not stripped.startswith(b"data:"):
            return
        payload_raw = stripped[5:].strip()
        if not payload_raw or payload_raw == b"[DONE]":
            return
        try:
            payload = orjson.loads(payload_raw)
        except orjson.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return

        trace = payload.get("trace")
        if not isinstance(trace, dict):
            return

        self._total_trace_events += 1
        if len(self._trace_events) < _MAX_STORED_TRACE_EVENTS:
            self._trace_events.append(trace)

        invocation_id = _as_str(trace.get("invocation_id"))
        if invocation_id:
            self._trace_invocation_id = invocation_id

        child_id = _as_str(trace.get("child_id"))
        chute_id = _as_str(trace.get("chute_id"))
        function_name = _as_str(trace.get("function"))
        timestamp = _as_str(trace.get("timestamp"))
        message = _as_str(trace.get("message")) or ""

        if match := _TRACE_TARGET_RE.search(message):
            self._attempts.append(
                {
                    "instance_id": match.group(1),
                    "uid": _as_int(match.group(2)),
                    "hotkey": match.group(3),
                    "coldkey": match.group(4),
                    "invocation_id": invocation_id,
                    "child_id": child_id,
                    "chute_id": chute_id,
                    "function": function_name,
                    "timestamp": timestamp,
                    "message": message,
                }
            )

        if match := _TRACE_TARGET_ERROR_RE.search(message):
            self._errors.append(
                {
                    "instance_id": match.group(1),
                    "uid": _as_int(match.group(2)),
                    "hotkey": match.group(3),
                    "coldkey": match.group(4),
                    "error": match.group(5),
                    "invocation_id": invocation_id,
                    "child_id": child_id,
                    "chute_id": chute_id,
                    "function": function_name,
                    "timestamp": timestamp,
                    "message": message,
                }
            )
