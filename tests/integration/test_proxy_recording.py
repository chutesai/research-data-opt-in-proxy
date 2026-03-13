from __future__ import annotations

import httpx
import json
import orjson
import pytest
from uuid import UUID

from app.main import create_app


@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_stream_proxy_records_raw_and_trace(
    settings_factory,
    db_truncate,
    db_fetch_one,
    db_fetch_value,
):
    await db_truncate()

    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("X-Chutes-Research-OptIn") == "true"
        assert request.headers.get("X-Chutes-Trace") == "true"
        correlation = request.headers.get("X-Chutes-Correlation-Id")
        assert correlation is not None
        UUID(correlation)
        seen["forwarded_correlation_id"] = correlation
        payload = {
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "Qwen/Qwen2.5-7B-Instruct",
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "hello"}}
            ],
            "usage": {"prompt_tokens": 11, "completion_tokens": 1, "total_tokens": 12},
        }
        return httpx.Response(
            status_code=200,
            headers={
                "content-type": "application/json",
                "x-chutes-invocationid": "parent-invocation-123",
            },
            content=orjson.dumps(payload),
        )

    transport = httpx.MockTransport(handler)
    app = create_app(settings_factory(), upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer test"},
                json={
                    "model": "Qwen/Qwen2.5-7B-Instruct",
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )

    assert resp.status_code == 200
    body = resp.json()
    assert body["choices"][0]["message"]["content"] == "hello"
    assert "x-chutes-correlation-id" not in resp.headers
    assert "x-chutes-invocationid" not in resp.headers

    raw_count = await db_fetch_value("SELECT COUNT(*) FROM raw_http_records")
    trace_count = await db_fetch_value("SELECT COUNT(*) FROM anon_usage_traces")
    assert raw_count == 1
    assert trace_count == 1

    trace_row = await db_fetch_one(
        "SELECT input_length, output_length, type, metadata FROM anon_usage_traces LIMIT 1",
    )
    metadata = trace_row["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert trace_row["input_length"] == 11
    assert trace_row["output_length"] == 1
    assert trace_row["type"] == "text"
    assert metadata["upstream_invocation_id"] == "parent-invocation-123"
    assert UUID(metadata["correlation_id"])
    assert metadata["correlation_id"] == seen["forwarded_correlation_id"]

    raw_row = await db_fetch_one(
        """
        SELECT
            correlation_id,
            upstream_invocation_id,
            request_json,
            response_json,
            request_body_format,
            response_body_format,
            octet_length(request_body) AS request_len,
            octet_length(response_body) AS response_len
        FROM raw_http_records
        LIMIT 1
        """,
    )
    assert raw_row["upstream_invocation_id"] == "parent-invocation-123"
    assert raw_row["correlation_id"] is not None
    assert str(raw_row["correlation_id"]) == seen["forwarded_correlation_id"]
    assert raw_row["request_len"] == 0
    assert raw_row["response_len"] == 0
    assert raw_row["request_body_format"] == "json"
    assert raw_row["response_body_format"] == "json"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_empty_request_body_is_marked_empty_format(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=orjson.dumps({"data": [{"id": "model-1"}]}),
        )

    app = create_app(settings_factory(), upstream_transport=httpx.MockTransport(handler))

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get("/v1/models")

    assert resp.status_code == 200

    raw_row = await db_fetch_one(
        """
        SELECT request_json, request_body_format, octet_length(request_body) AS request_len
        FROM raw_http_records
        LIMIT 1
        """
    )
    assert raw_row["request_json"] is None
    assert raw_row["request_body_format"] == "empty"
    assert raw_row["request_len"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_proxy_records_sse_response(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    stream_body = (
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:20.205691","invocation_id":"inv-1","message":"identified 2 available targets"}}\n\n'
        b'data: {"trace":{"timestamp":"2026-03-05T08:17:20.210865","invocation_id":"inv-1","child_id":"child-1","chute_id":"chute-1","function":"chat_stream","message":"attempting to query target=instance-abc uid=207 hotkey=hotkey-123 coldkey=coldkey-456"}}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"A\\"}}]}\\n"}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"B\\"}}],\\"usage\\":{\\"prompt_tokens\\":7,\\"completion_tokens\\":2}}\\n"}\n\n'
        b'data: {"result":"data: [DONE]\\n"}\n\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={
                "content-type": "text/event-stream",
                "x-chutes-invocationid": "parent-stream-123",
            },
            content=stream_body,
        )

    transport = httpx.MockTransport(handler)
    app = create_app(settings_factory(), upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer test"},
                json={
                    "model": "Qwen/Qwen2.5-7B-Instruct",
                    "stream": True,
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )

    assert resp.status_code == 200
    assert "data:" in resp.text
    assert '"trace"' not in resp.text
    assert '"delta":{"content":"A"}' in resp.text
    assert "x-chutes-correlation-id" not in resp.headers
    assert "x-chutes-invocationid" not in resp.headers

    raw = await db_fetch_one(
        """
        SELECT
            is_stream,
            response_body,
            response_json,
            response_body_format,
            stored_response_content_type,
            chutes_trace,
            correlation_id,
            upstream_invocation_id
        FROM raw_http_records
        LIMIT 1
        """,
    )
    chutes_trace = raw["chutes_trace"]
    if isinstance(chutes_trace, str):
        chutes_trace = json.loads(chutes_trace)
    assert raw["is_stream"] is True
    assert raw["response_body"] == b""
    response_json = raw["response_json"]
    if isinstance(response_json, str):
        response_json = json.loads(response_json)
    assert raw["response_body_format"] == "json"
    assert raw["stored_response_content_type"] == "application/json"
    assert response_json["choices"][0]["message"]["content"] == "AB"
    assert response_json["usage"]["completion_tokens"] == 2
    assert chutes_trace["target_instance_id"] == "instance-abc"
    assert chutes_trace["target_uid"] == 207
    assert chutes_trace["target_hotkey"] == "hotkey-123"
    assert chutes_trace["target_coldkey"] == "coldkey-456"
    assert raw["correlation_id"] is not None
    assert raw["upstream_invocation_id"] == "parent-stream-123"

    trace_row = await db_fetch_one(
        "SELECT input_length, output_length, metadata FROM anon_usage_traces LIMIT 1",
    )
    metadata = trace_row["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert trace_row["input_length"] == 7
    assert trace_row["output_length"] == 2
    assert metadata["target_instance_id"] == "instance-abc"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_incomplete_stream_is_not_recorded(
    settings_factory,
    db_truncate,
    db_fetch_value,
):
    await db_truncate()

    stream_body = (
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"A\\"}}]}\\n"}\n\n'
        b'data: {"result":"data: {\\"choices\\":[{\\"delta\\":{\\"content\\":\\"B\\"}}]}\\n"}\n\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"content-type": "text/event-stream"},
            content=stream_body,
        )

    app = create_app(settings_factory(), upstream_transport=httpx.MockTransport(handler))

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                json={
                    "model": "Qwen/Qwen2.5-7B-Instruct",
                    "stream": True,
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )

    assert resp.status_code == 200
    assert '"delta":{"content":"A"}' in resp.text
    assert await db_fetch_value("SELECT COUNT(*) FROM raw_http_records") == 0
    assert await db_fetch_value("SELECT COUNT(*) FROM anon_usage_traces") == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_stream_trace_envelope_is_unwrapped(
    settings_factory,
    db_truncate,
):
    await db_truncate()

    wrapped_body = (
        b'data: {"trace":{"invocation_id":"inv-1","message":"identified"}}\n\n'
        b'data: {"content_type":"application/json","json":{"id":"chatcmpl-xyz","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}}\n\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={
                "content-type": "text/event-stream",
                "x-chutes-invocationid": "parent-env-1",
            },
            content=wrapped_body,
        )

    transport = httpx.MockTransport(handler)
    app = create_app(settings_factory(), upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer test"},
                json={
                    "model": "Qwen/Qwen2.5-7B-Instruct",
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    assert "x-chutes-correlation-id" not in resp.headers
    assert "x-chutes-invocationid" not in resp.headers
    assert resp.json()["choices"][0]["message"]["content"] == "ok"
