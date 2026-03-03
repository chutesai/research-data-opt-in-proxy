from __future__ import annotations

from functools import lru_cache

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    service_name: str = "research-data-opt-in-proxy"
    environment: str = "development"

    upstream_base_url: AnyHttpUrl = "https://llm.chutes.ai"
    upstream_discount_header_name: str | None = None
    upstream_discount_header_value: str | None = None

    database_url: str = Field(default="", min_length=0)

    enable_raw_http_recording: bool = True
    enable_qwen_trace_recording: bool = True

    # A secret salt used for anonymized SipHash-2-4 token block hashes.
    anonymization_hash_salt: str = "change-me"

    http_connect_timeout_seconds: float = 15.0
    http_read_timeout_seconds: float = 600.0
    http_write_timeout_seconds: float = 30.0
    http_pool_timeout_seconds: float = 30.0

    # 0 means no truncation; values >0 cap bytes stored in DB.
    max_recorded_body_bytes: int = 0

    @property
    def normalized_upstream_base_url(self) -> str:
        return str(self.upstream_base_url).rstrip("/")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
