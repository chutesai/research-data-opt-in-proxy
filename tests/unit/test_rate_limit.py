from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx

from app.rate_limit import RateLimitMiddleware


def _make_app(max_requests: int, window_seconds: int = 60) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        max_requests=max_requests,
        window_seconds=window_seconds,
    )

    @app.get("/test")
    async def test_endpoint():
        return {"ok": True}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rate_limit_blocks_after_max():
    app = _make_app(max_requests=3)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        for _ in range(3):
            resp = await client.get("/test")
            assert resp.status_code == 200

        resp = await client.get("/test")
        assert resp.status_code == 429
        assert "rate_limit_exceeded" in resp.json()["error"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rate_limit_allows_healthz():
    app = _make_app(max_requests=1)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.get("/test")
        assert resp.status_code == 200

        resp = await client.get("/test")
        assert resp.status_code == 429

        # healthz is always allowed
        resp = await client.get("/healthz")
        assert resp.status_code == 200

        # health alias is always allowed
        resp = await client.get("/health")
        assert resp.status_code == 200


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rate_limit_disabled_when_zero():
    app = _make_app(max_requests=0)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        for _ in range(20):
            resp = await client.get("/test")
            assert resp.status_code == 200
