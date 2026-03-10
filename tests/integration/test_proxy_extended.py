from __future__ import annotations

import httpx
import orjson
import pytest
from uuid import UUID

from app.config import Settings
from app.main import create_app
from app.recorder import NoopRecorder


class _SpyRecorder:
    def __init__(self):
        self.calls: list[tuple[object, object]] = []

    async def record(self, raw_record, trace_candidate):
        self.calls.append((raw_record, trace_candidate))


def _recording_settings(**overrides) -> Settings:
    base = {
        "database_url": "",
        "enable_raw_http_recording": True,
        "enable_qwen_trace_recording": True,
        "anonymization_hash_salt": "proxy-error-test-salt-long-enough-to-pass",
        "upstream_base_url": "https://upstream.test",
        "upstream_discount_header_name": "X-Chutes-Research-OptIn",
        "upstream_discount_header_value": "true",
    }
    base.update(overrides)
    return Settings(**base)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upstream_transport_error_is_not_recorded():
    """When upstream is unreachable, the proxy returns 502 without calling the recorder."""
    recorder = _SpyRecorder()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    transport = httpx.MockTransport(handler)
    app = create_app(
        _recording_settings(),
        upstream_transport=transport,
        recorder_override=recorder,
    )

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer test"},
                json={
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )

    assert resp.status_code == 502
    assert resp.json()["error"] == "upstream_request_failed"
    assert recorder.calls == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upstream_http_error_response_is_not_recorded():
    """Upstream 4xx/5xx responses are passed through without calling the recorder."""
    recorder = _SpyRecorder()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=429,
            headers={"content-type": "application/json"},
            content=b'{"error":"rate_limit"}',
        )

    transport = httpx.MockTransport(handler)
    app = create_app(
        _recording_settings(),
        upstream_transport=transport,
        recorder_override=recorder,
    )

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer test"},
                json={
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "Hello"}],
                },
            )

    assert resp.status_code == 429
    assert resp.json()["error"] == "rate_limit"
    assert recorder.calls == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_body_truncation(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    """When max_recorded_body_bytes is set, stored bodies are truncated."""
    await db_truncate()

    large_content = "x" * 5000

    def handler(request: httpx.Request) -> httpx.Response:
        payload = {
            "choices": [{"message": {"content": large_content}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 5000},
        }
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=orjson.dumps(payload),
        )

    transport = httpx.MockTransport(handler)
    settings = settings_factory(max_recorded_body_bytes=100)
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )

    assert resp.status_code == 200

    raw = await db_fetch_one(
        "SELECT request_body, response_body FROM raw_http_records LIMIT 1",
    )
    assert len(raw["request_body"]) <= 100
    assert len(raw["response_body"]) <= 100


@pytest.mark.integration
@pytest.mark.asyncio
async def test_noop_recorder_no_database_needed():
    """When both recording modes disabled, no DB is needed."""
    settings = Settings(
        database_url="",
        enable_raw_http_recording=False,
        enable_qwen_trace_recording=False,
        anonymization_hash_salt="noop-test-salt-long-enough-to-pass",
        upstream_base_url="https://upstream.test",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"choices":[{"message":{"content":"hi"}}]}',
        )

    transport = httpx.MockTransport(handler)
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        assert isinstance(app.state.container.recorder, NoopRecorder)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )

    assert resp.status_code == 200
    assert resp.json()["choices"][0]["message"]["content"] == "hi"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_request_body_too_large():
    """Requests exceeding max_request_body_bytes get 413."""
    settings = Settings(
        database_url="",
        enable_raw_http_recording=False,
        enable_qwen_trace_recording=False,
        anonymization_hash_salt="body-limit-test-salt-long-enough",
        max_request_body_bytes=50,
    )

    transport = httpx.MockTransport(lambda r: httpx.Response(200))
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                content=b"x" * 200,
            )

    assert resp.status_code == 413
    assert "request_too_large" in resp.json()["error"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rate_limit_headers_exposed_on_proxy_responses():
    settings = Settings(
        database_url="",
        enable_raw_http_recording=False,
        enable_qwen_trace_recording=False,
        anonymization_hash_salt="rate-limit-header-test-salt-long-enough",
        upstream_base_url="https://upstream.test",
        rate_limit_requests=1,
        rate_limit_window_seconds=60,
        rate_limit_max_tracked_clients=10,
    )

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=b'{"choices":[{"message":{"content":"ok"}}]}',
        )
    )
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            ok = await client.post(
                "/v1/chat/completions",
                headers={"x-forwarded-for": "203.0.113.10"},
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )
            limited = await client.post(
                "/v1/chat/completions",
                headers={"x-forwarded-for": "203.0.113.10"},
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )

    assert ok.status_code == 200
    assert ok.headers["ratelimit-limit"] == "1"
    assert ok.headers["ratelimit-remaining"] == "0"
    assert int(ok.headers["ratelimit-reset"]) >= 1
    assert limited.status_code == 429
    assert limited.headers["ratelimit-limit"] == "1"
    assert limited.headers["ratelimit-remaining"] == "0"
    assert int(limited.headers["retry-after"]) >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_auth_and_discount_headers_stripped_from_recording(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    """Authorization and discount headers are completely absent from recorded headers."""
    await db_truncate()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={
                "content-type": "application/json",
                "x-chutes-invocationid": "inv-123",
            },
            content=b'{"choices":[{"message":{"content":"ok"}}]}',
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
                headers={
                    "Authorization": "Bearer sk-super-secret-key",
                    "Forwarded": "for=198.51.100.1;proto=https",
                    "X-Vercel-Oidc-Token": "oidc-secret-token",
                    "X-Vercel-Proxy-Signature": "proxy-signature-secret",
                },
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )

    assert resp.status_code == 200

    raw = await db_fetch_one(
        "SELECT request_headers, response_headers, correlation_id FROM raw_http_records LIMIT 1",
    )
    request_headers_dict = raw["request_headers"]
    if isinstance(request_headers_dict, str):
        import json
        request_headers_dict = json.loads(request_headers_dict)
    response_headers_dict = raw["response_headers"]
    if isinstance(response_headers_dict, str):
        import json
        response_headers_dict = json.loads(response_headers_dict)

    # Authorization must be completely absent, not redacted
    assert "authorization" not in request_headers_dict
    # Discount header must also be absent
    assert "x-chutes-research-optin" not in request_headers_dict
    assert "x-chutes-trace" not in request_headers_dict
    assert "x-chutes-correlation-id" not in request_headers_dict
    assert "x-chutes-realip" not in request_headers_dict
    assert "forwarded" not in request_headers_dict
    assert "x-vercel-oidc-token" not in request_headers_dict
    assert "x-vercel-proxy-signature" not in request_headers_dict
    assert "x-chutes-invocationid" not in response_headers_dict
    # Normal headers should still be present
    assert "content-type" in request_headers_dict
    assert raw["correlation_id"] is not None
    UUID(str(raw["correlation_id"]))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_ip_header_forwarded_from_proxy_extraction():
    captured_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_headers.update(dict(request.headers))
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"choices":[{"message":{"content":"ok"}}]}',
        )

    transport = httpx.MockTransport(handler)
    settings = Settings(
        database_url="",
        enable_raw_http_recording=False,
        enable_qwen_trace_recording=False,
        anonymization_hash_salt="real-ip-forwarding-test-salt-long-enough",
        upstream_base_url="https://upstream.test",
        upstream_discount_header_name="X-Chutes-Research-OptIn",
        upstream_discount_header_value="true",
    )
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                headers={
                    "X-Forwarded-For": "203.0.113.10, 10.0.0.5",
                    "X-Chutes-RealIP": "198.51.100.77",
                },
                json={"model": "m", "messages": [{"role": "user", "content": "Hi"}]},
            )

    assert resp.status_code == 200
    assert captured_headers["x-chutes-realip"] == "203.0.113.10"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_internal_export_path_blocked():
    settings = Settings(
        database_url="",
        enable_raw_http_recording=False,
        enable_qwen_trace_recording=False,
        anonymization_hash_salt="blocked-export-test-salt-long-enough",
        upstream_base_url="https://upstream.test",
    )

    transport = httpx.MockTransport(lambda r: httpx.Response(200, json={"unexpected": True}))
    app = create_app(settings, upstream_transport=transport)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get("/internal/export/traces.jsonl")

    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"
