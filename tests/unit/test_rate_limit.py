from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx

from app.rate_limit import RateLimitMiddleware


def _make_app(
    max_requests: int,
    window_seconds: int = 60,
    max_tracked_clients: int = 50000,
) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        max_requests=max_requests,
        window_seconds=window_seconds,
        max_tracked_clients=max_tracked_clients,
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
        assert resp.headers["ratelimit-limit"] == "3"
        assert resp.headers["ratelimit-remaining"] == "0"
        assert resp.headers["retry-after"] == "60"


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

        resp = await client.options("/test")
        assert resp.status_code in {200, 405}

        # healthz is always allowed
        resp = await client.get("/healthz")
        assert resp.status_code == 200

        # health alias is always allowed
        resp = await client.get("/health")
        assert resp.status_code == 200

        # internal archive endpoint is exempt (for cron trigger)
        resp = await client.post("/internal/archive/run")
        assert resp.status_code in {404, 405}

        # internal export endpoint is exempt
        resp = await client.get("/internal/export/raw-http.jsonl")
        assert resp.status_code in {404, 405}


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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rate_limit_prefers_platform_ip_headers():
    app = _make_app(max_requests=1)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        resp = await client.get(
            "/test",
            headers={
                "x-forwarded-for": "198.51.100.9",
            },
        )
        assert resp.status_code == 200
        assert resp.headers["ratelimit-limit"] == "1"
        assert resp.headers["ratelimit-remaining"] == "0"

        # Different forwarded IP should be treated as a separate client key.
        resp = await client.get(
            "/test",
            headers={
                "x-forwarded-for": "198.51.100.8",
            },
        )
        assert resp.status_code == 200

        # Same forwarded IP should now be rate-limited even if x-real-ip changes.
        resp = await client.get(
            "/test",
            headers={
                "x-real-ip": "203.0.113.2",
                "x-forwarded-for": "198.51.100.9",
            },
        )
        assert resp.status_code == 429
        assert resp.headers["retry-after"] == "60"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rate_limit_evicts_old_client_keys_when_capacity_exceeded():
    app = _make_app(max_requests=1, max_tracked_clients=2)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        for ip in ("203.0.113.1", "203.0.113.2", "203.0.113.3"):
            resp = await client.get("/test", headers={"x-forwarded-for": ip})
            assert resp.status_code == 200

        # Oldest client state should have been evicted when IP3 arrived.
        resp = await client.get("/test", headers={"x-forwarded-for": "203.0.113.1"})
        assert resp.status_code == 200
