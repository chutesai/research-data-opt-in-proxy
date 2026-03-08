from __future__ import annotations

import json
import os

import httpx
import pytest

from app.main import create_app


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_live_llm_proxy_end_to_end(
    settings_factory,
    db_truncate,
    db_fetch_one,
    db_fetch_value,
):
    api_key = os.getenv("CHUTES_API_KEY")
    if not api_key:
        pytest.skip("CHUTES_API_KEY is required for live e2e")

    await db_truncate()

    settings = settings_factory(
        upstream_base_url="https://llm.chutes.ai",
    )
    app = create_app(settings=settings)

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://local-proxy",
            timeout=90.0,
        ) as client:
            models_resp = await client.get("/v1/models", headers=headers)
            assert models_resp.status_code == 200, models_resp.text
            models_payload = models_resp.json()

            data = models_payload.get("data") if isinstance(models_payload, dict) else None
            assert isinstance(data, list) and data, "No models returned from /v1/models"
            model_id = _choose_model_for_smoke_test(data)

            try:
                chat_resp = await client.post(
                    "/v1/chat/completions",
                    headers=headers,
                    json={
                        "model": model_id,
                        "messages": [{"role": "user", "content": "Reply with just: ok"}],
                        "max_tokens": 8,
                    },
                )
            except httpx.TimeoutException:
                pytest.skip("Live upstream inference timed out during e2e run")

    assert chat_resp.status_code == 200, chat_resp.text
    assert "x-chutes-correlation-id" not in chat_resp.headers
    chat_payload = chat_resp.json()
    assert isinstance(chat_payload.get("choices"), list)

    raw_count = await db_fetch_value("SELECT COUNT(*) FROM raw_http_records")
    trace_count = await db_fetch_value("SELECT COUNT(*) FROM anon_usage_traces")

    # We called /v1/models and /v1/chat/completions.
    assert raw_count >= 2
    # Only chat-completions carries messages; /v1/models should not generate trace.
    assert trace_count >= 1

    raw_row = await db_fetch_one(
        "SELECT correlation_id, chutes_trace FROM raw_http_records WHERE path = '/v1/chat/completions' ORDER BY created_at DESC LIMIT 1"
    )
    assert raw_row is not None
    assert raw_row["correlation_id"] is not None
    chutes_trace = raw_row["chutes_trace"]
    if isinstance(chutes_trace, str):
        chutes_trace = json.loads(chutes_trace)
    assert chutes_trace.get("upstream_invocation_id")


def _choose_model_for_smoke_test(model_list: list[dict]) -> str:
    cheapest: tuple[float, str] | None = None
    fallback: str | None = None

    for model in model_list:
        model_id = model.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        if fallback is None:
            fallback = model_id

        price = model.get("price")
        if not isinstance(price, dict):
            continue
        input_price = price.get("input")
        tao = None
        if isinstance(input_price, dict):
            tao = input_price.get("tao")
        if not isinstance(tao, (int, float)):
            continue
        if cheapest is None or float(tao) < cheapest[0]:
            cheapest = (float(tao), model_id)

    if cheapest is not None:
        return cheapest[1]
    if fallback is not None:
        return fallback
    raise AssertionError("Unable to find a valid model id in /v1/models response")
