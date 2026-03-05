from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.anonymizer import build_usage_trace_candidate
from app.siphash import siphash24

_TEST_SALT = "unit-test-salt-long-enough-to-pass"


@pytest.mark.unit
def test_siphash_known_vector_empty_message():
    key = bytes(range(16))
    digest = siphash24(key, b"")
    assert digest == 0x726FDB47DD0E0E31


@pytest.mark.unit
def test_build_usage_trace_candidate_non_stream_response():
    request_payload = {
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "messages": [
            {"role": "system", "content": "You are concise."},
            {"role": "user", "content": "Say hello"},
        ],
    }
    response_payload = (
        b'{"choices":[{"message":{"content":"hello"}}],'
        b'"usage":{"prompt_tokens":13,"completion_tokens":2}}'
    )

    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload=request_payload,
        response_body=response_payload,
        response_content_type="application/json",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )

    assert trace is not None
    assert trace.input_length == 13
    assert trace.output_length == 2
    assert trace.turn == 2
    assert trace.trace_type == "text"
    assert trace.parent_context_hash is not None
    assert len(trace.raw_hash_values) >= 1


@pytest.mark.unit
def test_build_usage_trace_candidate_stream_response_uses_sse_usage():
    request_payload = {
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "messages": [{"role": "user", "content": "List numbers"}],
        "stream": True,
    }
    stream_body = (
        b'data: {"choices":[{"delta":{"content":"1"}}]}\n\n'
        b'data: {"choices":[{"delta":{"content":",2"}}],"usage":{"prompt_tokens":5,"completion_tokens":2}}\n\n'
        b"data: [DONE]\n\n"
    )

    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload=request_payload,
        response_body=stream_body,
        response_content_type="text/event-stream",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )

    assert trace is not None
    assert trace.input_length == 5
    assert trace.output_length == 2
    assert trace.turn == 1


@pytest.mark.unit
def test_build_usage_trace_candidate_stream_response_uses_wrapped_sse_usage():
    request_payload = {
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "messages": [{"role": "user", "content": "List numbers"}],
        "stream": True,
    }
    stream_body = (
        b'data: {"trace":{"invocation_id":"inv-1","message":"identified target"}}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"1\\"}}]}\\n"}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\",2\\"}}],\\"usage\\":{\\"prompt_tokens\\":6,\\"completion_tokens\\":2}}\\n"}\n\n'
        b'data: {"result":"data: [DONE]\\n"}\n\n'
    )

    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload=request_payload,
        response_body=stream_body,
        response_content_type="text/event-stream",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )

    assert trace is not None
    assert trace.input_length == 6
    assert trace.output_length == 2
    assert trace.turn == 1


@pytest.mark.unit
def test_build_usage_trace_candidate_detects_image_type():
    request_payload = {
        "model": "vision-model",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What is in this image?"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.com/pic.png"},
                    },
                ],
            }
        ],
    }

    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload=request_payload,
        response_body=b'{"choices":[{"message":{"content":"A cat."}}]}',
        response_content_type="application/json",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )

    assert trace is not None
    assert trace.trace_type == "image"


@pytest.mark.unit
def test_build_usage_trace_returns_none_for_no_messages():
    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload={"model": "test"},
        response_body=b'{}',
        response_content_type="application/json",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )
    assert trace is None


@pytest.mark.unit
def test_build_usage_trace_returns_none_for_none_payload():
    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload=None,
        response_body=b'{}',
        response_content_type="application/json",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )
    assert trace is None


@pytest.mark.unit
def test_build_usage_trace_returns_none_for_malformed_json():
    trace = build_usage_trace_candidate(
        request_id=uuid4(),
        request_payload={"messages": "not-a-list"},
        response_body=b'not json',
        response_content_type="application/json",
        observed_at=datetime.now(timezone.utc),
        anonymization_hash_salt=_TEST_SALT,
    )
    assert trace is None


@pytest.mark.unit
def test_siphash_rejects_short_key():
    with pytest.raises(ValueError, match="16 bytes"):
        siphash24(b"short", b"msg")
