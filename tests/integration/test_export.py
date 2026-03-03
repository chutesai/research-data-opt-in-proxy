from __future__ import annotations

import httpx
import orjson
import pytest

from app.main import create_app


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_returns_jsonl_with_valid_key(
    settings_factory,
    db_truncate,
    db_fetch_value,
):
    """With a valid API key and data in DB, export returns JSONL."""
    await db_truncate()

    # First, insert a record via the proxy
    def handler(request: httpx.Request) -> httpx.Response:
        payload = {
            "choices": [{"message": {"content": "hello"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1},
        }
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=orjson.dumps(payload),
        )

    transport = httpx.MockTransport(handler)
    export_key = "test-export-key-42"
    settings = settings_factory(export_api_key_whitelist=export_key)
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            # Create a trace by proxying a request
            await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )

            # Now export with valid key
            resp = await client.get(
                "/internal/export/traces.jsonl",
                headers={"Authorization": f"Bearer {export_key}"},
            )

    assert resp.status_code == 200
    assert "application/x-ndjson" in resp.headers.get("content-type", "")

    lines = [l for l in resp.text.strip().split("\n") if l]
    assert len(lines) >= 1

    record = orjson.loads(lines[0])
    assert "chat_id" in record
    assert "parent_chat_id" in record
    assert "timestamp" in record
    assert "input_length" in record
    assert "output_length" in record
    assert "type" in record
    assert "turn" in record
    assert "hash_ids" in record
    # Must NOT have internal fields
    assert "request_id" not in record
    assert "metadata" not in record
    assert "created_at" not in record


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_rejected_with_wrong_key(
    settings_factory,
):
    """Wrong key on the export endpoint returns 403."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    settings = settings_factory(export_api_key_whitelist="correct-key")
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get(
                "/internal/export/traces.jsonl",
                headers={"Authorization": "Bearer wrong-key"},
            )

    assert resp.status_code == 403
