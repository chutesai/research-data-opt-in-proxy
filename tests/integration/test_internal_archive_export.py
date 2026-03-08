from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

import httpx
import pytest

from app.main import create_app


@dataclass(slots=True)
class _Stored:
    payload: bytes


class _FakeObjectStorage:
    def __init__(self):
        self.store: dict[str, _Stored] = {}

    async def put_bytes(self, *, key: str, payload: bytes, content_type: str):
        self.store[key] = _Stored(payload=payload)
        sha = hashlib.sha256(payload).hexdigest()
        return type(
            "StoredObject",
            (),
            {
                "key": key,
                "url": f"mem://{key}",
                "size_bytes": len(payload),
                "sha256": sha,
            },
        )()

    async def get_bytes(self, *, key: str | None = None, url: str | None = None) -> bytes:
        resolved = key or (url or "").replace("mem://", "")
        return self.store[resolved].payload


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archive_and_export_internal_endpoints(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    export_secret = "export-secret-test"
    archive_secret = "archive-secret-test"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"choices":[{"message":{"content":"ok"}}]}',
        )

    settings = settings_factory(
        export_endpoint_secret=export_secret,
        archive_endpoint_secret=archive_secret,
        enable_qwen_trace_recording=False,
        archive_storage_provider="auto",
        archive_s3_bucket="",
        vercel_blob_read_write_token="",
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(handler))
    fake_storage = _FakeObjectStorage()

    async with app.router.lifespan_context(app):
        app.state.container.object_storage = fake_storage

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            # Insert one row via normal proxy path.
            proxied = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "hello"}]},
            )
            assert proxied.status_code == 200
            response_correlation_id = proxied.headers["x-chutes-correlation-id"]
            assert UUID(response_correlation_id)

            unauthorized_archive = await client.post("/internal/archive/run")
            assert unauthorized_archive.status_code == 401

            archive_resp = await client.post(
                "/internal/archive/run?limit=10",
                headers={"X-Chutes-Archive-Secret": archive_secret},
            )
            assert archive_resp.status_code == 200
            payload = archive_resp.json()
            assert payload["archived"] == 1
            assert payload["failed"] == 0

            row = await db_fetch_one(
                """
                SELECT
                    correlation_id,
                    octet_length(request_body) AS req_len,
                    octet_length(response_body) AS resp_len,
                    request_blob_key,
                    request_blob_url,
                    response_blob_key,
                    response_blob_url,
                    archived_at,
                    request_body_sha256,
                    response_body_sha256,
                    request_body_size_bytes,
                    response_body_size_bytes
                FROM raw_http_records
                ORDER BY created_at DESC
                LIMIT 1
                """,
            )
            assert row["req_len"] == 0
            assert row["resp_len"] == 0
            assert str(row["correlation_id"]) == response_correlation_id
            assert row["request_blob_key"] is not None
            assert row["response_blob_key"] is not None
            assert row["request_blob_url"].startswith("mem://")
            assert row["response_blob_url"].startswith("mem://")
            assert row["archived_at"] is not None
            assert row["request_body_sha256"]
            assert row["response_body_sha256"]
            assert row["request_body_size_bytes"] > 0
            assert row["response_body_size_bytes"] > 0

            unauthorized_export = await client.get("/internal/export/raw-http.jsonl")
            assert unauthorized_export.status_code == 401

            export_resp = await client.get(
                "/internal/export/raw-http.jsonl?resolve_archived_bodies=true&limit=1",
                headers={"X-Chutes-Export-Secret": export_secret},
            )
            assert export_resp.status_code == 200
            lines = [line for line in export_resp.text.splitlines() if line.strip()]
            assert len(lines) == 1
            exported = json.loads(lines[0])
            assert exported["request_blob_key"]
            assert exported["response_blob_key"]
            assert exported["request_body_text"]
            assert exported["response_body_text"]
            assert exported["request_body_sha256"]
            assert exported["response_body_sha256"]
            assert exported["correlation_id"] == response_correlation_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_inline_archive_on_ingest_stores_bodies_in_object_storage(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"choices":[{"message":{"content":"ok"}}]}',
        )

    settings = settings_factory(
        enable_qwen_trace_recording=False,
        archive_on_ingest=True,
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(handler))
    fake_storage = _FakeObjectStorage()

    async with app.router.lifespan_context(app):
        app.state.container.object_storage = fake_storage
        app.state.container.recorder.object_storage = fake_storage

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/v1/chat/completions",
                json={"model": "m", "messages": [{"role": "user", "content": "hello"}]},
            )

    assert resp.status_code == 200

    row = await db_fetch_one(
        """
        SELECT
            octet_length(request_body) AS req_len,
            octet_length(response_body) AS resp_len,
            request_blob_key,
            request_blob_url,
            response_blob_key,
            response_blob_url,
            archived_at
        FROM raw_http_records
        ORDER BY created_at DESC
        LIMIT 1
        """,
    )
    assert row["req_len"] == 0
    assert row["resp_len"] == 0
    assert row["request_blob_key"] is not None
    assert row["request_blob_url"].startswith("mem://")
    assert row["response_blob_key"] is not None
    assert row["response_blob_url"].startswith("mem://")
    assert row["archived_at"] is not None
