from __future__ import annotations

import pytest
import httpx

from app.proxy import _build_forward_headers, _filter_response_headers, _headers_to_multimap
from app.client_ip import extract_client_ip


class _FakeHeaders:
    """Minimal stand-in for Starlette Headers with a .raw attribute."""

    def __init__(self, pairs: list[tuple[str, str]]):
        self.raw = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in pairs]


@pytest.mark.unit
def test_build_forward_headers_strips_hop_by_hop():
    """Hop-by-hop headers from client must NOT be forwarded upstream."""

    class FakeRequest:
        class headers:
            raw = [
                (b"authorization", b"Bearer secret"),
                (b"connection", b"keep-alive"),
                (b"transfer-encoding", b"chunked"),
                (b"host", b"proxy.example.com"),
                (b"upgrade", b"websocket"),
                (b"x-custom", b"value"),
                (b"content-length", b"123"),
                (b"x-chutes-trace", b"false"),
                (b"x-chutes-correlation-id", b"user-supplied"),
                (b"x-chutes-realip", b"spoofed"),
            ]

    result = _build_forward_headers(
        FakeRequest(),
        managed_headers=frozenset(
            {"x-chutes-trace", "x-chutes-correlation-id", "x-chutes-realip"}
        ),
    )
    keys = {k.lower() for k, _ in result}

    assert "authorization" in keys
    assert "x-custom" in keys
    assert "connection" not in keys
    assert "transfer-encoding" not in keys
    assert "host" not in keys
    assert "upgrade" not in keys
    assert "content-length" not in keys
    assert "x-chutes-trace" not in keys
    assert "x-chutes-correlation-id" not in keys
    assert "x-chutes-realip" not in keys


@pytest.mark.unit
def test_headers_to_multimap_strips_sensitive():
    """Sensitive headers are completely omitted (not redacted) from the multimap."""
    headers = _FakeHeaders([
        ("authorization", "Bearer sk-secret-key"),
        ("x-api-key", "my-api-key"),
        ("content-type", "application/json"),
        ("x-chutes-research-optin", "true"),
    ])

    strip = frozenset({"authorization", "x-api-key", "x-chutes-research-optin"})
    result = _headers_to_multimap(headers, strip_keys=strip)

    assert "authorization" not in result
    assert "x-api-key" not in result
    assert "x-chutes-research-optin" not in result
    assert result["content-type"] == ["application/json"]


@pytest.mark.unit
def test_headers_to_multimap_strips_x_chutes_prefixes():
    headers = _FakeHeaders([
        ("content-type", "application/json"),
        ("x-chutes-invocationid", "inv-123"),
        ("x-chutes-correlation-id", "corr-123"),
    ])

    result = _headers_to_multimap(headers, strip_prefixes=("x-chutes-",))

    assert "content-type" in result
    assert "x-chutes-invocationid" not in result
    assert "x-chutes-correlation-id" not in result


@pytest.mark.unit
def test_headers_to_multimap_no_strip():
    headers = _FakeHeaders([
        ("authorization", "Bearer sk-secret-key"),
        ("content-type", "application/json"),
    ])

    result = _headers_to_multimap(headers)
    assert result["authorization"] == ["Bearer sk-secret-key"]
    assert result["content-type"] == ["application/json"]


@pytest.mark.unit
def test_filter_response_headers_strips_x_chutes_prefixes():
    headers = httpx.Headers(
        {
            "content-type": "application/json",
            "x-chutes-invocationid": "inv-123",
            "x-chutes-correlation-id": "corr-123",
            "x-other": "ok",
        }
    )

    result = _filter_response_headers(
        headers,
        drop_content_type=False,
        strip_prefixes=("x-chutes-",),
    )

    assert "content-type" in {key.lower() for key in result}
    assert "x-other" in {key.lower() for key in result}
    assert "x-chutes-invocationid" not in {key.lower() for key in result}
    assert "x-chutes-correlation-id" not in {key.lower() for key in result}


@pytest.mark.unit
def test_extract_client_ip_prefers_vercel_managed_forwarded_for():
    class FakeRequest:
        headers = httpx.Headers(
            {
                "x-real-ip": "203.0.113.10",
                "x-vercel-forwarded-for": "198.51.100.8",
                "x-forwarded-for": "198.51.100.9, 10.0.0.1",
            }
        )
        client = type("Client", (), {"host": "127.0.0.1"})()

    assert extract_client_ip(FakeRequest()) == "198.51.100.9"


@pytest.mark.unit
def test_extract_client_ip_falls_back_to_socket_peer_when_no_trusted_forwarded_header():
    class FakeRequest:
        headers = httpx.Headers({"x-real-ip": "203.0.113.10"})
        client = type("Client", (), {"host": "127.0.0.1"})()

    assert extract_client_ip(FakeRequest()) == "127.0.0.1"
