from __future__ import annotations

from datetime import datetime, timezone

import orjson
import pytest

from app.response_storage import normalize_request_for_storage, normalize_response_for_storage


@pytest.mark.unit
def test_normalize_request_for_storage_compacts_json():
    payload, body_format = normalize_request_for_storage(
        b'{"model":"x","messages":[{"role":"user","content":"hello"}]}'
    )
    assert body_format == "json"
    assert payload["model"] == "x"


@pytest.mark.unit
def test_normalize_request_for_storage_marks_empty_body():
    payload, body_format = normalize_request_for_storage(b"")

    assert payload is None
    assert body_format == "empty"


@pytest.mark.unit
def test_normalize_response_for_storage_unwraps_trace_json_envelope():
    body = (
        b'data: {"trace":{"message":"identified"}}\n\n'
        b'data: {"content_type":"application/json","json":{"id":"abc","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}}\n\n'
    )

    normalized = normalize_response_for_storage(
        body,
        "text/event-stream",
        observed_at=datetime.now(timezone.utc),
    )

    assert normalized is not None
    assert normalized.storage_format == "json"
    assert normalized.json_payload["id"] == "abc"
    assert normalized.json_payload["choices"][0]["message"]["content"] == "ok"


@pytest.mark.unit
def test_normalize_response_for_storage_reconstructs_completed_stream():
    body = (
        b'data: {"result":"data: {\\"id\\":\\"chatcmpl-1\\",\\"object\\":\\"chat.completion.chunk\\",\\"created\\":1740000000,\\"model\\":\\"test-model\\",\\"choices\\":[{\\"index\\":0,\\"delta\\":{\\"role\\":\\"assistant\\",\\"content\\":\\"Hel\\"}}]}\\n"}\n\n'
        b'data: {"result":"data: {\\"id\\":\\"chatcmpl-1\\",\\"object\\":\\"chat.completion.chunk\\",\\"created\\":1740000000,\\"model\\":\\"test-model\\",\\"choices\\":[{\\"index\\":0,\\"delta\\":{\\"content\\":\\"lo\\"},\\"finish_reason\\":\\"stop\\"}],\\"usage\\":{\\"prompt_tokens\\":5,\\"completion_tokens\\":2}}\\n"}\n\n'
        b'data: {"result":"data: [DONE]\\n"}\n\n'
    )

    normalized = normalize_response_for_storage(
        body,
        "text/event-stream",
        observed_at=datetime.now(timezone.utc),
    )

    assert normalized is not None
    payload = normalized.json_payload
    assert payload["object"] == "chat.completion"
    assert payload["model"] == "test-model"
    assert payload["choices"][0]["message"]["content"] == "Hello"
    assert payload["choices"][0]["finish_reason"] == "stop"
    assert payload["usage"]["completion_tokens"] == 2


@pytest.mark.unit
def test_normalize_response_for_storage_reconstructs_stream_without_done_marker():
    body = (
        b'data: {"result":"data: {\\"id\\":\\"chatcmpl-3\\",\\"object\\":\\"chat.completion.chunk\\",\\"created\\":1740000001,\\"model\\":\\"test-model\\",\\"choices\\":[{\\"index\\":0,\\"delta\\":{\\"role\\":\\"assistant\\",\\"content\\":\\"Hel\\"}}]}\\n"}\n\n'
        b'data: {"result":"\\n"}\n\n'
        b'data: {"result":"data: {\\"id\\":\\"chatcmpl-3\\",\\"object\\":\\"chat.completion.chunk\\",\\"created\\":1740000001,\\"model\\":\\"test-model\\",\\"choices\\":[{\\"index\\":0,\\"delta\\":{\\"content\\":\\"lo\\"},\\"finish_reason\\":\\"stop\\"}],\\"usage\\":{\\"prompt_tokens\\":5,\\"completion_tokens\\":2}}\\n"}\n\n'
    )

    normalized = normalize_response_for_storage(
        body,
        "text/event-stream",
        observed_at=datetime.now(timezone.utc),
    )

    assert normalized is not None
    payload = normalized.json_payload
    assert payload["object"] == "chat.completion"
    assert payload["model"] == "test-model"
    assert payload["choices"][0]["message"]["content"] == "Hello"
    assert payload["choices"][0]["finish_reason"] == "stop"
    assert payload["usage"]["completion_tokens"] == 2


@pytest.mark.unit
def test_normalize_response_for_storage_skips_incomplete_stream():
    body = (
        b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'
    )

    normalized = normalize_response_for_storage(
        body,
        "text/event-stream",
        observed_at=datetime.now(timezone.utc),
    )

    assert normalized is None


@pytest.mark.unit
def test_normalize_response_for_storage_keeps_plain_json():
    body = orjson.dumps(
        {
            "id": "chatcmpl-2",
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }
    )
    normalized = normalize_response_for_storage(body, "application/json")
    assert normalized is not None
    assert normalized.json_payload["id"] == "chatcmpl-2"
