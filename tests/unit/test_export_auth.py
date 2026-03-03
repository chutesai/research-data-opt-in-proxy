from __future__ import annotations

import httpx
import orjson
import pytest

from app.config import Settings
from app.main import create_app


def _make_app(*, whitelist: str = "", recording: bool = False) -> tuple:
    """Create app with export whitelist config. Returns (app, transport)."""
    settings = Settings(
        enable_raw_http_recording=recording,
        enable_qwen_trace_recording=False,
        anonymization_hash_salt="",
        export_api_key_whitelist=whitelist,
    )

    transport = httpx.MockTransport(lambda r: httpx.Response(200))
    app = create_app(settings, upstream_transport=transport)
    return app, transport


@pytest.mark.unit
@pytest.mark.asyncio
async def test_export_returns_404_when_disabled():
    """When EXPORT_API_KEY_WHITELIST is empty, endpoint returns 404."""
    app, _ = _make_app(whitelist="")

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get(
                "/internal/export/traces.jsonl",
                headers={"Authorization": "Bearer some-key"},
            )

    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_export_returns_403_without_key():
    """Missing Authorization header returns 403."""
    app, _ = _make_app(whitelist="valid-key-123")

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get("/internal/export/traces.jsonl")

    assert resp.status_code == 403
    assert resp.json()["error"] == "forbidden"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_export_returns_403_with_wrong_key():
    """Wrong API key returns 403."""
    app, _ = _make_app(whitelist="valid-key-123")

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
    assert resp.json()["error"] == "forbidden"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_export_returns_503_when_no_db():
    """Valid key but no DB recording returns 503."""
    app, _ = _make_app(whitelist="valid-key-123", recording=False)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get(
                "/internal/export/traces.jsonl",
                headers={"Authorization": "Bearer valid-key-123"},
            )

    assert resp.status_code == 503
    assert resp.json()["error"] == "unavailable"
