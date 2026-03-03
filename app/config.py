from __future__ import annotations

from functools import lru_cache

from pydantic import AnyHttpUrl, Field, model_validator
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
    # MUST be set explicitly; the default will cause a validation error when
    # Qwen trace recording is enabled.
    anonymization_hash_salt: str = ""

    http_connect_timeout_seconds: float = 15.0
    http_read_timeout_seconds: float = 600.0
    http_write_timeout_seconds: float = 30.0
    http_pool_timeout_seconds: float = 30.0

    # 0 means no truncation; values >0 cap bytes stored in DB.
    max_recorded_body_bytes: int = 0

    # Max request body the proxy will accept (bytes). 0 = unlimited.
    max_request_body_bytes: int = 10 * 1024 * 1024  # 10 MiB

    # Max bytes buffered from an SSE stream before truncating recording.
    # The response is still forwarded in full; only the recorded copy is capped.
    max_stream_buffer_bytes: int = 50 * 1024 * 1024  # 50 MiB

    # Per-client-IP rate limit: max requests per window.
    rate_limit_requests: int = 0  # 0 = disabled
    rate_limit_window_seconds: int = 60

    # Header names completely stripped from raw HTTP recording storage.
    stripped_header_names: str = "authorization,x-api-key,cookie,set-cookie"

    # CORS: comma-separated allowed origins. "*" allows all. Empty disables CORS.
    cors_allow_origins: str = "*"

    # Data retention: days to keep records. 0 = keep forever.
    retention_days: int = 0

    # Comma-separated API keys allowed to call the /export endpoint.
    # Empty string disables the export endpoint entirely.
    export_api_key_whitelist: str = ""

    @model_validator(mode="after")
    def _salt_must_be_set_when_tracing(self) -> "Settings":
        if self.enable_qwen_trace_recording:
            salt = self.anonymization_hash_salt
            if not salt or salt in {"change-me", "change-me-to-a-long-random-secret"}:
                raise ValueError(
                    "ANONYMIZATION_HASH_SALT must be set to a real secret value "
                    "when ENABLE_QWEN_TRACE_RECORDING is true"
                )
        return self

    @property
    def normalized_upstream_base_url(self) -> str:
        return str(self.upstream_base_url).rstrip("/")

    @property
    def export_api_keys(self) -> frozenset[str]:
        """Set of API keys allowed to call the export endpoint."""
        return frozenset(
            k.strip()
            for k in self.export_api_key_whitelist.split(",")
            if k.strip()
        )

    @property
    def stripped_header_set(self) -> frozenset[str]:
        """Headers completely removed from raw HTTP recording storage."""
        base = frozenset(
            h.strip().lower()
            for h in self.stripped_header_names.split(",")
            if h.strip()
        )
        # Also strip the discount header if configured (it's a secret).
        if self.upstream_discount_header_name:
            base = base | {self.upstream_discount_header_name.lower()}
        return base


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
