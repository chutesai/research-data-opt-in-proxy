from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI

from app.config import Settings, get_settings
from app.db import create_pool, init_schema
from app.proxy import create_proxy_router
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
    app.include_router(create_proxy_router())
    return app


app = create_app()
