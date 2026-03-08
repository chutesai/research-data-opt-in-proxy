import httpx

from app.config import Settings
from app.main import _create_http_client


def test_create_http_client_uses_http1_and_custom_limits_by_default():
    client = _create_http_client(settings=Settings())
    try:
        transport = client._transport
        assert isinstance(transport, httpx.AsyncHTTPTransport)
        assert transport._pool._http2 is False
        assert transport._pool._max_connections == 200
        assert transport._pool._max_keepalive_connections == 100
    finally:
        import asyncio

        asyncio.run(client.aclose())


def test_create_http_client_can_enable_http2_explicitly():
    client = _create_http_client(settings=Settings(upstream_http2_enabled=True))
    try:
        transport = client._transport
        assert isinstance(transport, httpx.AsyncHTTPTransport)
        assert transport._pool._http2 is True
    finally:
        import asyncio

        asyncio.run(client.aclose())


def test_create_http_client_preserves_explicit_transport():
    transport = httpx.MockTransport(lambda request: httpx.Response(200))
    client = _create_http_client(
        settings=Settings(),
        upstream_transport=transport,
    )
    try:
        assert client._transport is transport
    finally:
        import asyncio

        asyncio.run(client.aclose())
