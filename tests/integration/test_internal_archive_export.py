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
        await app.state.container.db_pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                correlation_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                duration_ms,
                client_ip,
                is_stream,
                upstream_invocation_id,
                chutes_trace,
                error
            ) VALUES (
                '00000000-0000-0000-0000-000000000010'::uuid,
                '00000000-0000-0000-0000-000000000110'::uuid,
                NOW(),
                'POST',
                '/v1/chat/completions',
                '',
                'https://llm.chutes.ai/v1/chat/completions',
                '{}'::jsonb,
                $1::bytea,
                200,
                '{"content-type":["application/json"]}'::jsonb,
                $2::bytea,
                123,
                '1.2.3.4',
                false,
                'inv-legacy',
                '{}'::jsonb,
                NULL
            )
            """,
            b'{"model":"m","messages":[{"role":"user","content":"hello"}]}',
            b'{"choices":[{"message":{"content":"ok"}}]}',
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
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
            assert row["correlation_id"] is not None
            response_correlation_id = str(row["correlation_id"])
            assert UUID(response_correlation_id)
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_resolve_archived_bodies_requires_object_storage(
    settings_factory,
    db_truncate,
):
    await db_truncate()

    settings = settings_factory(
        export_endpoint_secret="export-secret-test",
        enable_qwen_trace_recording=False,
        archive_storage_provider="auto",
        archive_s3_bucket="",
        vercel_blob_read_write_token="",
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(lambda request: httpx.Response(200)))

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get(
                "/internal/export/raw-http.jsonl?resolve_archived_bodies=true&limit=1",
                headers={"X-Chutes-Export-Secret": "export-secret-test"},
            )

    assert resp.status_code == 500
    assert resp.json() == {"error": "object_storage_not_configured"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_compact_json_endpoint_migrates_existing_wrapped_response(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    archive_secret = "archive-secret-test"
    settings = settings_factory(
        archive_endpoint_secret=archive_secret,
        enable_qwen_trace_recording=False,
        archive_storage_provider="auto",
        archive_s3_bucket="",
        vercel_blob_read_write_token="",
    )

    wrapped_body = (
        b'data: {"trace":{"invocation_id":"inv-1","message":"identified"}}\n\n'
        b'data: {"content_type":"application/json","json":{"id":"chatcmpl-1","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}}\n\n'
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    fake_storage = _FakeObjectStorage()

    async with app.router.lifespan_context(app):
        app.state.container.object_storage = fake_storage

        await app.state.container.db_pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                correlation_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                duration_ms,
                client_ip,
                is_stream,
                upstream_invocation_id,
                chutes_trace,
                error
            ) VALUES (
                '00000000-0000-0000-0000-000000000099'::uuid,
                '00000000-0000-0000-0000-000000000199'::uuid,
                NOW(),
                'POST',
                '/v1/chat/completions',
                '',
                'https://llm.chutes.ai/v1/chat/completions',
                '{}'::jsonb,
                $2::bytea,
                200,
                '{"content-type":["text/event-stream"]}'::jsonb,
                $1::bytea,
                123,
                '1.2.3.4',
                false,
                'inv-1',
                '{}'::jsonb,
                NULL
            )
            """,
            wrapped_body,
            b'{"model":"m","messages":[{"role":"user","content":"hello"}]}',
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/internal/storage/compact-json?limit=10",
                headers={"X-Chutes-Archive-Secret": archive_secret},
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["migrated"] == 1
    assert payload["compacted"] == 1

    row = await db_fetch_one(
        """
        SELECT
            request_json,
            response_json,
            request_body_format,
            response_body_format,
            octet_length(request_body) AS request_len,
            octet_length(response_body) AS response_len,
            request_blob_url,
            response_blob_url
        FROM raw_http_records
        LIMIT 1
        """
    )
    assert row["request_len"] == 0
    assert row["response_len"] == 0
    assert row["request_blob_url"].startswith("mem://")
    assert row["response_blob_url"].startswith("mem://")
    request_json = row["request_json"]
    response_json = row["response_json"]
    if isinstance(request_json, str):
        request_json = json.loads(request_json)
    if isinstance(response_json, str):
        response_json = json.loads(response_json)
    assert row["request_body_format"] == "json"
    assert row["response_body_format"] == "json"
    assert request_json["messages"][0]["content"] == "hello"
    assert response_json["choices"][0]["message"]["content"] == "ok"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_compact_json_endpoint_marks_empty_request_body(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    archive_secret = "archive-secret-test"
    settings = settings_factory(
        archive_endpoint_secret=archive_secret,
        enable_qwen_trace_recording=False,
        archive_storage_provider="auto",
        archive_s3_bucket="",
        vercel_blob_read_write_token="",
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(lambda request: httpx.Response(200)))

    async with app.router.lifespan_context(app):
        await app.state.container.db_pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                correlation_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                response_json,
                response_body_format,
                stored_response_content_type,
                duration_ms,
                client_ip,
                is_stream,
                upstream_invocation_id,
                chutes_trace,
                error
            ) VALUES (
                '00000000-0000-0000-0000-000000000109'::uuid,
                '00000000-0000-0000-0000-000000000209'::uuid,
                NOW(),
                'GET',
                '/v1/models',
                '',
                'https://llm.chutes.ai/v1/models',
                '{}'::jsonb,
                ''::bytea,
                200,
                '{"content-type":["application/json"]}'::jsonb,
                ''::bytea,
                '{"data":[{"id":"model-1"}]}'::jsonb,
                'json',
                'application/json',
                12,
                '1.2.3.4',
                false,
                NULL,
                '{}'::jsonb,
                NULL
            )
            """
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/internal/storage/compact-json?limit=10",
                headers={"X-Chutes-Archive-Secret": archive_secret},
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["migrated"] == 1
    assert payload["compacted"] == 1

    row = await db_fetch_one(
        """
        SELECT
            request_json,
            request_body_format,
            octet_length(request_body) AS request_len
        FROM raw_http_records
        LIMIT 1
        """
    )
    assert row["request_json"] is None
    assert row["request_body_format"] == "empty"
    assert row["request_len"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_compact_json_endpoint_skips_unrecoverable_mem_rows(
    settings_factory,
    db_truncate,
    db_fetch_one,
    db_fetch_value,
):
    await db_truncate()

    archive_secret = "archive-secret-test"
    settings = settings_factory(
        archive_endpoint_secret=archive_secret,
        enable_qwen_trace_recording=False,
        archive_storage_provider="auto",
        archive_s3_bucket="",
        vercel_blob_read_write_token="",
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    fake_storage = _FakeObjectStorage()

    async with app.router.lifespan_context(app):
        app.state.container.object_storage = fake_storage

        await app.state.container.db_pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                correlation_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                request_blob_url,
                response_status,
                response_headers,
                response_body,
                response_blob_url,
                duration_ms,
                client_ip,
                is_stream,
                upstream_invocation_id,
                chutes_trace,
                error
            ) VALUES (
                '00000000-0000-0000-0000-000000000120'::uuid,
                '00000000-0000-0000-0000-000000000220'::uuid,
                NOW() - INTERVAL '2 minutes',
                'POST',
                '/v1/chat/completions',
                '',
                'https://llm.chutes.ai/v1/chat/completions',
                '{}'::jsonb,
                ''::bytea,
                'mem://raw-http-archive/test/request.bin',
                200,
                '{"content-type":["application/json"]}'::jsonb,
                ''::bytea,
                'mem://raw-http-archive/test/response.bin',
                12,
                '1.2.3.4',
                false,
                'inv-mem',
                '{}'::jsonb,
                NULL
            ),
            (
                '00000000-0000-0000-0000-000000000121'::uuid,
                '00000000-0000-0000-0000-000000000221'::uuid,
                NOW() - INTERVAL '1 minute',
                'POST',
                '/v1/chat/completions',
                '',
                'https://llm.chutes.ai/v1/chat/completions',
                '{}'::jsonb,
                $1::bytea,
                NULL,
                200,
                '{"content-type":["application/json"]}'::jsonb,
                $2::bytea,
                NULL,
                12,
                '1.2.3.4',
                false,
                'inv-live',
                '{}'::jsonb,
                NULL
            )
            """,
            b'{"model":"m","messages":[{"role":"user","content":"hello"}]}',
            b'{"choices":[{"message":{"content":"ok"}}]}',
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/internal/storage/compact-json?limit=1",
                headers={"X-Chutes-Archive-Secret": archive_secret},
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["scanned"] == 1
    assert payload["migrated"] == 1
    assert payload["skipped"] == 0

    migrated_row = await db_fetch_one(
        """
        SELECT
            request_body_format,
            response_body_format,
            request_json,
            response_json
        FROM raw_http_records
        WHERE request_id = '00000000-0000-0000-0000-000000000121'::uuid
        """
    )
    assert migrated_row["request_body_format"] == "json"
    assert migrated_row["response_body_format"] == "json"
    assert migrated_row["request_json"] is not None
    assert migrated_row["response_json"] is not None

    skipped_row_format = await db_fetch_value(
        """
        SELECT request_body_format || '/' || response_body_format
        FROM raw_http_records
        WHERE request_id = '00000000-0000-0000-0000-000000000120'::uuid
        """
    )
    assert skipped_row_format == "bytes/bytes"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_compact_json_endpoint_marks_incomplete_stream_response_empty(
    settings_factory,
    db_truncate,
    db_fetch_one,
):
    await db_truncate()

    archive_secret = "archive-secret-test"
    settings = settings_factory(
        archive_endpoint_secret=archive_secret,
        enable_qwen_trace_recording=False,
        archive_storage_provider="auto",
        archive_s3_bucket="",
        vercel_blob_read_write_token="",
    )

    app = create_app(settings, upstream_transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    incomplete_stream = (
        b'data: {"trace":{"message":"identified 8 available targets"}}\n\n'
        b'data: {"trace":{"message":"attempting target"}}\n\n'
    )

    async with app.router.lifespan_context(app):
        await app.state.container.db_pool.execute(
            """
            INSERT INTO raw_http_records (
                request_id,
                correlation_id,
                created_at,
                method,
                path,
                query_string,
                upstream_url,
                request_headers,
                request_body,
                request_json,
                request_body_format,
                stored_request_content_type,
                response_status,
                response_headers,
                response_body,
                response_body_format,
                stored_response_content_type,
                duration_ms,
                client_ip,
                is_stream,
                upstream_invocation_id,
                chutes_trace,
                error
            ) VALUES (
                '00000000-0000-0000-0000-000000000122'::uuid,
                '00000000-0000-0000-0000-000000000222'::uuid,
                NOW(),
                'POST',
                '/v1/chat/completions',
                '',
                'https://llm.chutes.ai/v1/chat/completions',
                '{}'::jsonb,
                ''::bytea,
                '{"model":"m","messages":[{"role":"user","content":"hello"}]}'::jsonb,
                'json',
                'application/json',
                200,
                '{"content-type":["text/event-stream"]}'::jsonb,
                $1::bytea,
                'bytes',
                'text/event-stream',
                12,
                '1.2.3.4',
                true,
                'inv-incomplete',
                '{}'::jsonb,
                NULL
            )
            """,
            incomplete_stream,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/internal/storage/compact-json?limit=10",
                headers={"X-Chutes-Archive-Secret": archive_secret},
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["migrated"] == 1

    row = await db_fetch_one(
        """
        SELECT
            request_body_format,
            response_body_format,
            request_json,
            response_json
        FROM raw_http_records
        WHERE request_id = '00000000-0000-0000-0000-000000000122'::uuid
        """
    )
    assert row["request_body_format"] == "json"
    assert row["response_body_format"] == "empty"
    assert row["request_json"] is not None
    assert row["response_json"] is None
