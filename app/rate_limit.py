from __future__ import annotations

import asyncio
import math
import time
from collections import deque

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app.client_ip import extract_client_ip


class RateLimitMiddleware(BaseHTTPMiddleware):
    """In-memory per-IP sliding window rate limiter with bounded state."""

    def __init__(
        self,
        app,
        *,
        max_requests: int,
        window_seconds: int,
        max_tracked_clients: int = 50000,
    ):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.max_tracked_clients = max(1, max_tracked_clients)
        self._hits: dict[str, deque[float]] = {}
        self._blocked_until: dict[str, float] = {}
        self._last_seen: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if self.max_requests <= 0:
            return await call_next(request)

        if request.method == "OPTIONS" or request.url.path in {
            "/healthz",
            "/health",
            "/internal/archive/run",
            "/internal/export/raw-http.jsonl",
        }:
            return await call_next(request)

        client_ip = extract_client_ip(request) or "unknown"
        now = time.monotonic()
        window_start = now - self.window_seconds

        async with self._lock:
            self._prune_stale_state(now=now, window_start=window_start)

            blocked_until = self._blocked_until.get(client_ip)
            if blocked_until and blocked_until > now:
                retry_after = _seconds_until(blocked_until, now)
                return _rate_limited_response(
                    max_requests=self.max_requests,
                    window_seconds=self.window_seconds,
                    retry_after=retry_after,
                )

            timestamps = self._hits.setdefault(client_ip, deque())
            while timestamps and timestamps[0] <= window_start:
                timestamps.popleft()

            if len(timestamps) >= self.max_requests:
                blocked_until = now + self.window_seconds
                self._blocked_until[client_ip] = blocked_until
                self._last_seen[client_ip] = now
                retry_after = _seconds_until(blocked_until, now)
                return _rate_limited_response(
                    max_requests=self.max_requests,
                    window_seconds=self.window_seconds,
                    retry_after=retry_after,
                )

            timestamps.append(now)
            self._last_seen[client_ip] = now
            reset_after = _seconds_until(timestamps[0] + self.window_seconds, now)
            remaining = max(0, self.max_requests - len(timestamps))
            self._evict_if_needed(now=now, window_start=window_start)

        response = await call_next(request)
        _apply_rate_limit_headers(
            response,
            max_requests=self.max_requests,
            remaining=remaining,
            reset_after=reset_after,
            window_seconds=self.window_seconds,
        )
        return response

    def _prune_stale_state(self, *, now: float, window_start: float) -> None:
        stale_clients: list[str] = []
        for client_ip, blocked_until in self._blocked_until.items():
            if blocked_until <= now:
                stale_clients.append(client_ip)
        for client_ip in stale_clients:
            self._blocked_until.pop(client_ip, None)

        empty_clients: list[str] = []
        for client_ip, timestamps in self._hits.items():
            while timestamps and timestamps[0] <= window_start:
                timestamps.popleft()
            if not timestamps and client_ip not in self._blocked_until:
                empty_clients.append(client_ip)
        for client_ip in empty_clients:
            self._hits.pop(client_ip, None)
            self._last_seen.pop(client_ip, None)

    def _evict_if_needed(self, *, now: float, window_start: float) -> None:
        self._prune_stale_state(now=now, window_start=window_start)
        if len(self._last_seen) <= self.max_tracked_clients:
            return

        overflow = len(self._last_seen) - self.max_tracked_clients
        for client_ip, _ in sorted(self._last_seen.items(), key=lambda item: item[1])[:overflow]:
            self._last_seen.pop(client_ip, None)
            self._hits.pop(client_ip, None)
            self._blocked_until.pop(client_ip, None)


def _rate_limited_response(
    *,
    max_requests: int,
    window_seconds: int,
    retry_after: int,
) -> JSONResponse:
    response = JSONResponse(
        status_code=429,
        content={
            "error": "rate_limit_exceeded",
            "detail": (
                f"Max {max_requests} requests per "
                f"{window_seconds}s exceeded"
            ),
        },
    )
    _apply_rate_limit_headers(
        response,
        max_requests=max_requests,
        remaining=0,
        reset_after=retry_after,
        window_seconds=window_seconds,
        retry_after=retry_after,
    )
    return response


def _apply_rate_limit_headers(
    response: Response,
    *,
    max_requests: int,
    remaining: int,
    reset_after: int,
    window_seconds: int,
    retry_after: int | None = None,
) -> None:
    policy = f"{max_requests};w={window_seconds}"
    response.headers["RateLimit-Policy"] = policy
    response.headers["RateLimit-Limit"] = str(max_requests)
    response.headers["RateLimit-Remaining"] = str(max(0, remaining))
    response.headers["RateLimit-Reset"] = str(max(0, reset_after))
    response.headers["X-RateLimit-Limit"] = str(max_requests)
    response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
    response.headers["X-RateLimit-Reset"] = str(max(0, reset_after))
    if retry_after is not None:
        response.headers["Retry-After"] = str(max(1, retry_after))
    response.headers.setdefault("Cache-Control", "no-store")


def _seconds_until(target_time: float, now: float) -> int:
    return max(1, math.ceil(target_time - now))
