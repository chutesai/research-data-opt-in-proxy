from __future__ import annotations

import pytest

from app.proxy import _build_forward_headers, _headers_to_multimap


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
            ]

    result = _build_forward_headers(
        FakeRequest(),
        managed_headers=frozenset({"x-chutes-trace", "x-chutes-correlation-id"}),
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
def test_headers_to_multimap_no_strip():
    headers = _FakeHeaders([
        ("authorization", "Bearer sk-secret-key"),
        ("content-type", "application/json"),
    ])

    result = _headers_to_multimap(headers)
    assert result["authorization"] == ["Bearer sk-secret-key"]
    assert result["content-type"] == ["application/json"]
