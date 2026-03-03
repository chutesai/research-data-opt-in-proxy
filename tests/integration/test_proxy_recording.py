from __future__ import annotations

import httpx
import orjson
import pytest

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

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("X-Chutes-Research-OptIn") == "true"
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
            headers={"content-type": "application/json"},
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

    raw_count = await db_fetch_value("SELECT COUNT(*) FROM raw_http_records")
    trace_count = await db_fetch_value("SELECT COUNT(*) FROM anon_usage_traces")
    assert raw_count == 1
    assert trace_count == 1

    trace_row = await db_fetch_one(
        "SELECT input_length, output_length, type FROM anon_usage_traces LIMIT 1",
    )
    assert trace_row["input_length"] == 11
    assert trace_row["output_length"] == 1
    assert trace_row["type"] == "text"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_proxy_records_sse_response(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    stream_body = (
        b'data: {"choices":[{"delta":{"content":"A"}}]}\n\n'
        b'data: {"choices":[{"delta":{"content":"B"}}],"usage":{"prompt_tokens":7,"completion_tokens":2}}\n\n'
        b"data: [DONE]\n\n"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"content-type": "text/event-stream"},
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

    raw = await db_fetch_one(
        "SELECT is_stream, response_body FROM raw_http_records LIMIT 1",
    )
    assert raw["is_stream"] is True
    assert b"[DONE]" in raw["response_body"]

    trace_row = await db_fetch_one(
        "SELECT input_length, output_length FROM anon_usage_traces LIMIT 1",
    )
    assert trace_row["input_length"] == 7
    assert trace_row["output_length"] == 2
