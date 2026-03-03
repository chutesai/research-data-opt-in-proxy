from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import Settings, get_settings
from app.db import create_pool, init_schema
from app.export import create_export_router
from app.proxy import create_proxy_router
from app.rate_limit import RateLimitMiddleware
from app.recorder import NoopRecorder, PostgresRecorder


@dataclass(slots=True)
class AppContainer:
    settings: Settings
    http_client: httpx.AsyncClient
    recorder: object


def create_app(
    settings: Settings | None = None,
    *,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
    recorder_override: object | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = None
        timeout = httpx.Timeout(
            connect=resolved_settings.http_connect_timeout_seconds,
            read=resolved_settings.http_read_timeout_seconds,
            write=resolved_settings.http_write_timeout_seconds,
            pool=resolved_settings.http_pool_timeout_seconds,
        )
        http_client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            transport=upstream_transport,
        )

        if recorder_override is not None:
            recorder = recorder_override
        elif (
            resolved_settings.enable_raw_http_recording
            or resolved_settings.enable_qwen_trace_recording
        ):
            if not resolved_settings.database_url:
                raise RuntimeError(
                    "DATABASE_URL is required when recording is enabled"
                )
            pool = await create_pool(resolved_settings.database_url)
            await init_schema(pool)
            recorder = PostgresRecorder(pool=pool, settings=resolved_settings)
        else:
            recorder = NoopRecorder()

        app.state.container = AppContainer(
            settings=resolved_settings,
            http_client=http_client,
            recorder=recorder,
        )

        try:
            yield
        finally:
            await http_client.aclose()
            if pool is not None:
                await pool.close()

    app = FastAPI(
        title="Research Data Opt-In Proxy",
        version="0.1.0",
        lifespan=lifespan,
    )

    if resolved_settings.cors_allow_origins:
        origins = [
            o.strip()
            for o in resolved_settings.cors_allow_origins.split(",")
            if o.strip()
        ]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    if resolved_settings.rate_limit_requests > 0:
        app.add_middleware(
            RateLimitMiddleware,
            max_requests=resolved_settings.rate_limit_requests,
            window_seconds=resolved_settings.rate_limit_window_seconds,
        )

    app.include_router(create_export_router())
    app.include_router(create_proxy_router())
    return app


def _get_app() -> FastAPI:
    """Lazy app factory for the module-level ``app`` attribute.

    ``api/index.py`` (Vercel entrypoint) and ``uvicorn app.main:app`` both
    access the module-level ``app``.  Using ``__getattr__`` defers creation
    until first access so that importing this module during test collection
    does not require production env vars.
    """
    return create_app()


def __getattr__(name: str):
    if name == "app":
        global app  # noqa: PLW0603
        app = _get_app()
        return app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
