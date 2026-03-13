from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from uuid import uuid4

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app.anonymizer import build_usage_trace_candidate, parse_json_bytes
from app.client_ip import extract_client_ip
from app.chutes_trace import (
    TraceSSEUnwrapper,
    extract_chutes_trace_metadata,
    unwrap_chutes_non_stream_body,
)
from app.models import RawHTTPRecord
from app.response_storage import (
    StreamingResponseJsonBuilder,
    normalize_request_for_storage,
    normalize_response_for_storage,
)


logger = logging.getLogger(__name__)

_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    # httpx transparently decompresses response content; forwarding this header
    # would make downstream clients attempt a second decompression.
    "content-encoding",
}

_BLOCKED_PROXY_PATH_PREFIXES = ("/internal/export", "/internal/archive")
_INTERNAL_HEADER_PREFIXES = ("x-chutes-",)



def create_proxy_router() -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    @router.get("/healthz")
    async def healthz(request: Request):
        container = request.app.state.container
        return {
            "status": "ok",
            "service": container.settings.service_name,
            "environment": container.settings.environment,
            "recording": {
                "raw_http": container.settings.enable_raw_http_recording,
                "qwen_trace": container.settings.enable_qwen_trace_recording,
            },
        }

    @router.api_route(
        "/",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
        include_in_schema=False,
    )
    @router.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
        include_in_schema=False,
    )
    async def proxy_request(request: Request, full_path: str = ""):
        container = request.app.state.container
        settings = container.settings

        if request.url.path.startswith(_BLOCKED_PROXY_PATH_PREFIXES):
            return JSONResponse(
                status_code=404,
                content={"error": "not_found", "detail": "Not found"},
            )

        started_at = datetime.now(timezone.utc)
        request_id = uuid4()
        correlation_id = uuid4()

        body = await request.body()

        max_body = settings.max_request_body_bytes
        if max_body > 0 and len(body) > max_body:
            return JSONResponse(
                status_code=413,
                content={
                    "error": "request_too_large",
                    "detail": f"Request body exceeds {max_body} bytes limit",
                },
            )

        request_payload = parse_json_bytes(body)
        request_json, request_body_format = normalize_request_for_storage(body)
        client_ip = extract_client_ip(request)

        query_string = request.url.query or ""
        path_segment = full_path.lstrip("/")
        upstream_url = f"{settings.normalized_upstream_base_url}/{path_segment}"

        forward_headers = _build_forward_headers(
            request=request,
            managed_headers=settings.managed_upstream_header_set,
        )
        if (
            settings.upstream_trace_header_name
            and settings.upstream_trace_header_value is not None
        ):
            forward_headers.append(
                (
                    settings.upstream_trace_header_name,
                    settings.upstream_trace_header_value,
                )
            )
        if settings.upstream_correlation_id_header_name:
            forward_headers.append(
                (
                    settings.upstream_correlation_id_header_name,
                    str(correlation_id),
                )
            )
        if settings.upstream_real_ip_header_name and client_ip:
            forward_headers.append(
                (
                    settings.upstream_real_ip_header_name,
                    client_ip,
                )
            )
        if (
            settings.upstream_discount_header_name
            and settings.upstream_discount_header_value is not None
        ):
            forward_headers.append(
                (
                    settings.upstream_discount_header_name,
                    settings.upstream_discount_header_value,
                )
            )

        try:
            upstream_request = container.http_client.build_request(
                method=request.method,
                url=upstream_url,
                headers=forward_headers,
                content=body,
                params=request.query_params.multi_items(),
            )
            upstream_response = await container.http_client.send(upstream_request, stream=True)
        except httpx.RequestError as exc:
            return JSONResponse(
                status_code=502,
                content={
                    "error": "upstream_request_failed",
                    "detail": str(exc),
                },
            )

        response_content_type = upstream_response.headers.get("content-type", "")
        stream_requested = bool(
            isinstance(request_payload, dict) and request_payload.get("stream") is True
        )
        is_stream = "text/event-stream" in response_content_type.lower() and stream_requested

        if is_stream:
            return _build_streaming_response(
                request=request,
                request_id=request_id,
                request_payload=request_payload,
                request_body=body,
                started_at=started_at,
                correlation_id=correlation_id,
                client_ip=client_ip,
                query_string=query_string,
                upstream_url=upstream_url,
                upstream_response=upstream_response,
            )

        response_body = await upstream_response.aread()
        completed_at = datetime.now(timezone.utc)
        await upstream_response.aclose()
        trace_metadata = extract_chutes_trace_metadata(
            response_body,
            response_content_type,
            upstream_response.headers,
        )
        client_response_body = response_body
        client_response_content_type = response_content_type
        if unwrapped := unwrap_chutes_non_stream_body(response_body):
            client_response_body, client_response_content_type = unwrapped

        normalized_response = normalize_response_for_storage(
            response_body,
            response_content_type,
            observed_at=completed_at,
        )

        strip_headers = settings.stripped_header_set
        raw_record = RawHTTPRecord(
            request_id=request_id,
            correlation_id=correlation_id,
            created_at=started_at,
            method=request.method,
            path=request.url.path,
            query_string=query_string,
            upstream_url=upstream_url,
            request_headers=_headers_to_multimap(
                request.headers,
                strip_keys=strip_headers,
                strip_prefixes=_INTERNAL_HEADER_PREFIXES,
            ),
            request_body=body,
            request_json=request_json,
            request_body_format=request_body_format,
            stored_request_content_type=request.headers.get("content-type"),
            response_status=upstream_response.status_code,
            response_headers=_headers_to_multimap(
                upstream_response.headers,
                strip_keys=strip_headers,
                strip_prefixes=_INTERNAL_HEADER_PREFIXES,
            ),
            response_body=response_body,
            response_json=(
                normalized_response.json_payload if normalized_response is not None else None
            ),
            response_body_format=(
                normalized_response.storage_format
                if normalized_response is not None
                else ("empty" if not response_body else "bytes")
            ),
            stored_response_content_type=(
                normalized_response.content_type if normalized_response is not None else None
            ),
            duration_ms=_duration_ms(started_at, completed_at),
            client_ip=client_ip,
            is_stream=False,
            upstream_invocation_id=_as_optional_str(trace_metadata.get("upstream_invocation_id")),
            chutes_trace=trace_metadata,
            error=None,
        )

        trace_candidate = build_usage_trace_candidate(
            request_id=request_id,
            request_payload=request_payload,
            response_body=(
                normalized_response.body_bytes
                if normalized_response is not None
                else client_response_body
            ),
            response_content_type=(
                normalized_response.content_type
                if normalized_response is not None
                else client_response_content_type
            ),
            observed_at=completed_at,
            anonymization_hash_salt=settings.anonymization_hash_salt,
            correlation_id=correlation_id,
            trace_metadata=trace_metadata,
        )
        if _should_record_upstream_result(upstream_response.status_code):
            _spawn_record_task(
                request=request,
                recorder=container.recorder,
                raw_record=raw_record,
                trace_candidate=trace_candidate,
            )

        response_headers = _filter_response_headers(
            upstream_response.headers,
            drop_content_type=True,
            strip_prefixes=_INTERNAL_HEADER_PREFIXES,
        )

        return Response(
            content=client_response_body,
            status_code=upstream_response.status_code,
            media_type=client_response_content_type or None,
            headers=response_headers,
        )

    return router


def _build_streaming_response(
    *,
    request: Request,
    request_id,
    request_payload,
    request_body: bytes,
    started_at: datetime,
    correlation_id,
    client_ip: str | None,
    query_string: str,
    upstream_url: str,
    upstream_response: httpx.Response,
):
    container = request.app.state.container
    settings = container.settings
    response_content_type = upstream_response.headers.get("content-type", "text/event-stream")
    max_buffer = settings.max_stream_buffer_bytes
    captured_chunks = bytearray()
    buffer_truncated = False
    unwrapper = TraceSSEUnwrapper()
    storage_builder = StreamingResponseJsonBuilder()
    request_json, request_body_format = normalize_request_for_storage(request_body)

    async def _iterator():
        nonlocal buffer_truncated
        stream_error: str | None = None
        try:
            async for chunk in upstream_response.aiter_bytes():
                storage_builder.feed(chunk)
                if not buffer_truncated:
                    if max_buffer > 0 and len(captured_chunks) + len(chunk) > max_buffer:
                        remaining = max_buffer - len(captured_chunks)
                        if remaining > 0:
                            captured_chunks.extend(chunk[:remaining])
                        buffer_truncated = True
                    else:
                        captured_chunks.extend(chunk)
                outgoing_chunk = unwrapper.feed(chunk)
                if outgoing_chunk:
                    yield outgoing_chunk

            remaining = unwrapper.finalize()
            if remaining:
                yield remaining
        except Exception as exc:  # pragma: no cover - network interruption path
            stream_error = str(exc)
            raise
        finally:
            completed_at = datetime.now(timezone.utc)
            await upstream_response.aclose()
            trace_metadata = extract_chutes_trace_metadata(
                bytes(captured_chunks),
                response_content_type,
                upstream_response.headers,
            )
            normalized_response = normalize_response_for_storage(
                bytes(captured_chunks),
                response_content_type,
                observed_at=completed_at,
            )
            if normalized_response is None:
                normalized_response = storage_builder.finalize(observed_at=completed_at)

            strip_headers = settings.stripped_header_set
            raw_record = RawHTTPRecord(
                request_id=request_id,
                correlation_id=correlation_id,
                created_at=started_at,
                method=request.method,
                path=request.url.path,
                query_string=query_string,
                upstream_url=upstream_url,
                request_headers=_headers_to_multimap(
                    request.headers,
                    strip_keys=strip_headers,
                    strip_prefixes=_INTERNAL_HEADER_PREFIXES,
                ),
                request_body=request_body,
                request_json=request_json,
                request_body_format=request_body_format,
                stored_request_content_type=request.headers.get("content-type"),
                response_status=upstream_response.status_code,
                response_headers=_headers_to_multimap(
                    upstream_response.headers,
                    strip_keys=strip_headers,
                    strip_prefixes=_INTERNAL_HEADER_PREFIXES,
                ),
                response_body=bytes(captured_chunks),
                response_json=(
                    normalized_response.json_payload if normalized_response is not None else None
                ),
                response_body_format=(
                    normalized_response.storage_format
                    if normalized_response is not None
                    else ("empty" if not captured_chunks else "bytes")
                ),
                stored_response_content_type=(
                    normalized_response.content_type if normalized_response is not None else None
                ),
                duration_ms=_duration_ms(started_at, completed_at),
                client_ip=client_ip,
                is_stream=True,
                upstream_invocation_id=_as_optional_str(trace_metadata.get("upstream_invocation_id")),
                chutes_trace=trace_metadata,
                error=stream_error,
            )

            trace_candidate = build_usage_trace_candidate(
                request_id=request_id,
                request_payload=request_payload,
                response_body=(
                    normalized_response.body_bytes
                    if normalized_response is not None
                    else bytes(captured_chunks)
                ),
                response_content_type=(
                    normalized_response.content_type
                    if normalized_response is not None
                    else response_content_type
                ),
                observed_at=completed_at,
                anonymization_hash_salt=settings.anonymization_hash_salt,
                correlation_id=correlation_id,
                trace_metadata=trace_metadata,
            )
            if (
                normalized_response is not None
                and _should_record_upstream_result(upstream_response.status_code)
            ):
                _spawn_record_task(
                    request=request,
                    recorder=container.recorder,
                    raw_record=raw_record,
                    trace_candidate=trace_candidate,
                )

    response_headers = _filter_response_headers(
        upstream_response.headers,
        drop_content_type=True,
        strip_prefixes=_INTERNAL_HEADER_PREFIXES,
    )

    return StreamingResponse(
        _iterator(),
        status_code=upstream_response.status_code,
        media_type=response_content_type,
        headers=response_headers,
    )


def _build_forward_headers(
    request: Request,
    *,
    managed_headers: frozenset[str],
) -> list[tuple[str, str]]:
    headers: list[tuple[str, str]] = []
    for key_bytes, value_bytes in request.headers.raw:
        key = key_bytes.decode("latin-1")
        value = value_bytes.decode("latin-1")
        key_lower = key.lower()
        if key_lower in _HOP_BY_HOP_HEADERS:
            continue
        if key_lower in {"host", "content-length"}:
            continue
        if key_lower in managed_headers:
            continue
        headers.append((key, value))
    return headers


def _filter_response_headers(
    headers: httpx.Headers,
    *,
    drop_content_type: bool,
    strip_prefixes: tuple[str, ...] = (),
) -> dict[str, str]:
    filtered: dict[str, str] = {}
    for key, value in headers.multi_items():
        key_lower = key.lower()
        if key_lower in _HOP_BY_HOP_HEADERS:
            continue
        if any(key_lower.startswith(prefix) for prefix in strip_prefixes):
            continue
        if key_lower == "content-length":
            continue
        if drop_content_type and key_lower == "content-type":
            continue
        filtered[key] = value
    return filtered


def _headers_to_multimap(
    headers,
    *,
    strip_keys: frozenset[str] | None = None,
    strip_prefixes: tuple[str, ...] = (),
) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}

    raw = getattr(headers, "raw", None)
    if raw is not None:
        for key_bytes, value_bytes in raw:
            key = key_bytes.decode("latin-1").lower()
            if strip_keys and key in strip_keys:
                continue
            if any(key.startswith(prefix) for prefix in strip_prefixes):
                continue
            value = value_bytes.decode("latin-1")
            output.setdefault(key, []).append(value)
        return output

    for key, value in headers.multi_items():
        key_lower = key.lower()
        if strip_keys and key_lower in strip_keys:
            continue
        if any(key_lower.startswith(prefix) for prefix in strip_prefixes):
            continue
        output.setdefault(key_lower, []).append(value)
    return output
def _duration_ms(started_at: datetime, completed_at: datetime) -> int:
    return max(0, int((completed_at - started_at).total_seconds() * 1000))


def _should_record_upstream_result(status_code: int) -> bool:
    return status_code < 400


async def _safe_record(recorder, raw_record, trace_candidate) -> None:
    try:
        await recorder.record(raw_record, trace_candidate)
    except Exception as exc:  # pragma: no cover - defensive path
        logger.exception("Failed to record usage data: %s", exc)


def _spawn_record_task(*, request: Request, recorder, raw_record, trace_candidate) -> None:
    task = asyncio.create_task(_safe_record(recorder, raw_record, trace_candidate))
    pending_tasks = request.app.state.container.pending_tasks
    pending_tasks.add(task)
    task.add_done_callback(pending_tasks.discard)


def _as_optional_str(value) -> str | None:
    if isinstance(value, str):
        return value
    return None
