from __future__ import annotations

import pytest
from unittest.mock import patch

from app.config import Settings


@pytest.mark.unit
def test_salt_rejects_default_change_me_when_tracing_enabled():
    with pytest.raises(Exception, match="ANONYMIZATION_HASH_SALT must be set"):
        Settings(
            anonymization_hash_salt="change-me",
            enable_qwen_trace_recording=True,
            database_url="postgresql://x@localhost/db",
        )


@pytest.mark.unit
def test_salt_rejects_empty_when_tracing_enabled():
    with pytest.raises(Exception, match="ANONYMIZATION_HASH_SALT must be set"):
        Settings(
            anonymization_hash_salt="",
            enable_qwen_trace_recording=True,
            database_url="postgresql://x@localhost/db",
        )


@pytest.mark.unit
def test_salt_accepted_empty_when_tracing_disabled():
    s = Settings(
        anonymization_hash_salt="",
        enable_qwen_trace_recording=False,
        database_url="postgresql://x@localhost/db",
    )
    assert s.anonymization_hash_salt == ""


@pytest.mark.unit
def test_salt_accepts_real_value():
    s = Settings(
        anonymization_hash_salt="a-real-secret-value-for-testing",
        database_url="postgresql://x@localhost/db",
    )
    assert s.anonymization_hash_salt == "a-real-secret-value-for-testing"


@pytest.mark.unit
def test_qwen_trace_recording_disabled_by_default():
    s = Settings()
    assert s.enable_qwen_trace_recording is False


@pytest.mark.unit
def test_environment_defaults_from_vercel_target_env():
    with patch.dict("os.environ", {"VERCEL_TARGET_ENV": "production"}, clear=False):
        s = Settings()
    assert s.environment == "production"


@pytest.mark.unit
def test_bool_settings_strip_whitespace():
    s = Settings(
        archive_on_ingest="true\n",
        upstream_http2_enabled="true\n",
    )
    assert s.archive_on_ingest is True
    assert s.upstream_http2_enabled is True


@pytest.mark.unit
def test_numeric_settings_strip_whitespace():
    s = Settings(
        rate_limit_requests="12000\n",
        rate_limit_window_seconds="60\n",
        retention_days="7\n",
        archive_batch_size="500\n",
    )
    assert s.rate_limit_requests == 12000
    assert s.rate_limit_window_seconds == 60
    assert s.retention_days == 7
    assert s.archive_batch_size == 500


@pytest.mark.unit
def test_stripped_header_set_parsed():
    s = Settings(
        anonymization_hash_salt="a-real-secret-value-for-testing",
        stripped_header_names="Authorization, X-Api-Key ,cookie",
    )
    assert s.stripped_header_set >= frozenset({"authorization", "x-api-key", "cookie"})


@pytest.mark.unit
def test_stripped_header_set_includes_discount_header():
    s = Settings(
        anonymization_hash_salt="a-real-secret-value-for-testing",
        upstream_discount_header_name="X-Chutes-Research-OptIn",
    )
    assert "x-chutes-research-optin" in s.stripped_header_set
    assert "x-chutes-trace" in s.stripped_header_set
    assert "x-chutes-correlation-id" in s.stripped_header_set
    assert "x-chutes-realip" in s.stripped_header_set


@pytest.mark.unit
def test_managed_upstream_header_set_contains_proxy_headers():
    s = Settings(
        anonymization_hash_salt="a-real-secret-value-for-testing",
        upstream_discount_header_name="X-Chutes-Research-OptIn",
    )
    assert s.managed_upstream_header_set == frozenset(
        {
            "x-chutes-research-optin",
            "x-chutes-trace",
            "x-chutes-correlation-id",
            "x-chutes-realip",
        }
    )


@pytest.mark.unit
def test_default_max_request_body():
    s = Settings(
        anonymization_hash_salt="a-real-secret-value-for-testing",
    )
    assert s.max_request_body_bytes == 10 * 1024 * 1024


@pytest.mark.unit
def test_default_max_stream_buffer():
    s = Settings(
        anonymization_hash_salt="a-real-secret-value-for-testing",
    )
    assert s.max_stream_buffer_bytes == 50 * 1024 * 1024


@pytest.mark.unit
def test_archive_provider_validation_rejects_unknown():
    with pytest.raises(Exception, match="ARCHIVE_STORAGE_PROVIDER"):
        Settings(
            anonymization_hash_salt="a-real-secret-value-for-testing",
            archive_storage_provider="invalid",
        )


@pytest.mark.unit
def test_archive_batch_size_must_be_positive():
    with pytest.raises(Exception, match="ARCHIVE_BATCH_SIZE"):
        Settings(
            anonymization_hash_salt="a-real-secret-value-for-testing",
            archive_batch_size=0,
        )
