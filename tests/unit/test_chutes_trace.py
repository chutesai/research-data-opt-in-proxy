from __future__ import annotations

import pytest

import orjson

from app.chutes_trace import (
    StreamingTraceMetadataBuilder,
    TraceSSEUnwrapper,
    extract_chutes_trace_metadata,
    unwrap_chutes_non_stream_body,
)


@pytest.mark.unit
def test_extract_chutes_trace_metadata_parses_target_and_error():
    body = (
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:20.205691","invocation_id":"inv-1","message":"identified 2 available targets"}}\n\n'
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:20.210865","invocation_id":"inv-1","child_id":"child-1","chute_id":"chute-1","function":"chat_stream","message":"attempting to query target=instance-abc uid=207 hotkey=hotkey-123 coldkey=coldkey-456"}}\n\n'
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:21.210865","invocation_id":"inv-1","child_id":"child-1","chute_id":"chute-1","function":"chat_stream","message":"error encountered while querying target=instance-abc uid=207 hotkey=hotkey-123 coldkey=coldkey-456: timeout"}}\n\n'
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:22.210865","invocation_id":"inv-1","child_id":"child-2","chute_id":"chute-1","function":"chat_stream","message":"attempting to query target=instance-def uid=208 hotkey=hotkey-789 coldkey=coldkey-999"}}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"ok\\"}}],\\"usage\\":{\\"prompt_tokens\\":5,\\"completion_tokens\\":1}}\\n"}\n\n'
    )

    metadata = extract_chutes_trace_metadata(
        body,
        "text/event-stream",
        {"X-Chutes-InvocationID": "parent-123"},
    )

    assert metadata["upstream_invocation_id"] == "parent-123"
    assert metadata["trace_invocation_id"] == "inv-1"
    assert metadata["target_instance_id"] == "instance-def"
    assert metadata["target_uid"] == 208
    assert metadata["target_hotkey"] == "hotkey-789"
    assert metadata["target_coldkey"] == "coldkey-999"
    assert metadata["target_child_id"] == "child-2"
    assert metadata["trace_event_count"] == 4
    assert len(metadata["attempts"]) == 2
    assert len(metadata["errors"]) == 1
    assert metadata["attempts"][0]["has_error"] is True
    assert metadata["attempts"][1]["has_error"] is False


@pytest.mark.unit
def test_trace_sse_unwrapper_drops_trace_events():
    unwrapper = TraceSSEUnwrapper()
    chunk = (
        b'data: {"trace":{"invocation_id":"inv-1","message":"identified"}}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"hello\\"}}]}\\n"}\n\n'
        b'data: {"result":"\\n"}\n\n'
    )
    out = unwrapper.feed(chunk) + unwrapper.finalize()
    text = out.decode("utf-8")

    assert '"trace"' not in text
    assert ": trace" in text
    assert 'data: {"choices":[{"delta":{"content":"hello"}}]}' in text


@pytest.mark.unit
def test_streaming_trace_metadata_builder_matches_full_extractor():
    body = (
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:20.205691","invocation_id":"inv-1","message":"identified 2 available targets"}}\n\n'
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:20.210865","invocation_id":"inv-1","child_id":"child-1","chute_id":"chute-1","function":"chat_stream","message":"attempting to query target=instance-abc uid=207 hotkey=hotkey-123 coldkey=coldkey-456"}}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"ok\\"}}],\\"usage\\":{\\"prompt_tokens\\":5,\\"completion_tokens\\":1}}\\n"}\n\n'
    )
    builder = StreamingTraceMetadataBuilder({"X-Chutes-InvocationID": "parent-123"})

    midpoint = len(body) // 2
    builder.feed(body[:midpoint])
    builder.feed(body[midpoint:])

    assert builder.finalize() == extract_chutes_trace_metadata(
        body,
        "text/event-stream",
        {"X-Chutes-InvocationID": "parent-123"},
    )


@pytest.mark.unit
def test_unwrap_chutes_non_stream_body_json_envelope():
    body = (
        b'data: {"trace":{"invocation_id":"inv-1","message":"identified"}}\n\n'
        b'data: {"content_type":"application/json","json":{"id":"abc","choices":[{"message":{"content":"ok"}}]}}\n\n'
    )

    unwrapped = unwrap_chutes_non_stream_body(body)
    assert unwrapped is not None
    unwrapped_body, content_type = unwrapped
    assert content_type == "application/json"
    payload = orjson.loads(unwrapped_body)
    assert payload["id"] == "abc"
    assert payload["choices"][0]["message"]["content"] == "ok"


@pytest.mark.unit
def test_unwrap_chutes_non_stream_body_direct_json_wrapper():
    body = orjson.dumps(
        {
            "content_type": "application/json",
            "json": {
                "id": "chatcmpl-direct",
                "choices": [{"message": {"content": "ok"}}],
            },
        }
    )

    unwrapped = unwrap_chutes_non_stream_body(body)
    assert unwrapped is not None
    unwrapped_body, content_type = unwrapped
    assert content_type == "application/json"
    payload = orjson.loads(unwrapped_body)
    assert payload["id"] == "chatcmpl-direct"


@pytest.mark.unit
def test_unwrap_chutes_non_stream_body_nested_result_wrapper():
    body = orjson.dumps(
        {
            "result": {
                "content_type": "application/json",
                "json": {
                    "id": "nested",
                    "choices": [{"message": {"content": "ok"}}],
                },
            }
        }
    )

    unwrapped = unwrap_chutes_non_stream_body(body)
    assert unwrapped is not None
    unwrapped_body, content_type = unwrapped
    assert content_type == "application/json"
    payload = orjson.loads(unwrapped_body)
    assert payload["id"] == "nested"


@pytest.mark.unit
def test_unwrap_chutes_non_stream_body_direct_error_payload():
    body = (
        b'data: {"trace":{"invocation_id":"inv-1","message":"identified"}}\n\n'
        b'data: {"error":"infra_overload","detail":"Infrastructure is at maximum capacity, try again later"}\n\n'
    )

    unwrapped = unwrap_chutes_non_stream_body(body)
    assert unwrapped is not None
    unwrapped_body, content_type = unwrapped
    assert content_type == "application/json"
    payload = orjson.loads(unwrapped_body)
    assert payload["error"] == "infra_overload"
    assert "maximum capacity" in payload["detail"]
