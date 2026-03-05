from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import orjson
import pytest

from app.export import raw_row_to_jsonl


@pytest.mark.unit
def test_raw_row_to_jsonl_with_utf8_bodies():
    row = {
        "request_id": uuid4(),
        "correlation_id": uuid4(),
        "created_at": datetime.now(timezone.utc),
        "method": "POST",
        "path": "/v1/chat/completions",
        "query_string": "",
        "upstream_url": "https://llm.chutes.ai/v1/chat/completions",
        "request_headers": {"content-type": ["application/json"]},
        "request_body": b'{"messages":[{"role":"user","content":"hello"}]}',
        "response_status": 200,
        "response_headers": {"content-type": ["application/json"]},
        "response_body": b'{"choices":[{"message":{"content":"ok"}}]}',
        "duration_ms": 123,
        "client_ip": "1.2.3.4",
        "is_stream": False,
        "upstream_invocation_id": "inv-1",
        "chutes_trace": {"target_instance_id": "inst-1"},
        "error": None,
    }

    payload = orjson.loads(raw_row_to_jsonl(row))
    assert payload["request_body_text"].startswith("{\"messages\"")
    assert payload["request_body_base64"] is None
    assert payload["response_body_text"].startswith("{\"choices\"")
    assert payload["response_body_base64"] is None
    assert payload["chutes_trace"]["target_instance_id"] == "inst-1"


@pytest.mark.unit
def test_raw_row_to_jsonl_with_binary_body_uses_base64():
    row = {
        "request_id": uuid4(),
        "correlation_id": uuid4(),
        "created_at": datetime.now(timezone.utc),
        "method": "POST",
        "path": "/upload",
        "query_string": "",
        "upstream_url": "https://llm.chutes.ai/upload",
        "request_headers": {},
        "request_body": b"\xff\xfe\xfd",
        "response_status": 200,
        "response_headers": {},
        "response_body": b"\xff\x00",
        "duration_ms": 1,
        "client_ip": None,
        "is_stream": False,
        "upstream_invocation_id": None,
        "chutes_trace": {},
        "error": None,
    }

    payload = orjson.loads(raw_row_to_jsonl(row))
    assert payload["request_body_text"] is None
    assert isinstance(payload["request_body_base64"], str)
    assert payload["response_body_text"] is None
    assert isinstance(payload["response_body_base64"], str)
