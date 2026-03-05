from __future__ import annotations

import pytest

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
