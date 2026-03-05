from __future__ import annotations

import time
from collections import defaultdict

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple in-memory per-IP sliding window rate limiter."""

    def __init__(self, app, *, max_requests: int, window_seconds: int):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if self.max_requests <= 0:
            return await call_next(request)

        if request.url.path in {
            "/healthz",
            "/health",
            "/internal/archive/run",
            "/internal/export/raw-http.jsonl",
        }:
            return await call_next(request)

        client_ip = _get_client_ip(request)
        now = time.monotonic()
        window_start = now - self.window_seconds

        timestamps = self._hits[client_ip]
        timestamps[:] = [t for t in timestamps if t > window_start]

        if len(timestamps) >= self.max_requests:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limit_exceeded",
                    "detail": (
                        f"Max {self.max_requests} requests per "
                        f"{self.window_seconds}s exceeded"
                    ),
                },
            )

        timestamps.append(now)
        return await call_next(request)


def _get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"
