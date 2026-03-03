# Improvements (v0.2.0)

Summary of all security, performance, and correctness improvements made to the research-data-opt-in-proxy.

## Security Fixes

### 1. Hop-by-hop headers now filtered from client requests
**File:** `app/proxy.py` (`_build_forward_headers`)

Previously, hop-by-hop headers (`connection`, `transfer-encoding`, `upgrade`, `proxy-authorization`, etc.) from incoming client requests were forwarded verbatim to the upstream LLM endpoint. These headers are meant to be consumed by the proxy itself, not forwarded. They are now stripped before forwarding.

### 2. Sensitive headers completely stripped from recorded traces
**File:** `app/proxy.py` (`_headers_to_multimap`), `app/config.py`

Authorization headers, API keys, cookies, and the secret discount header are now **completely removed** from the stored raw HTTP recordings — not just redacted. This means no auth tokens, bearer keys, or session cookies are ever persisted to the database. The discount header (`X-Chutes-Research-OptIn`) is automatically added to the strip list when configured.

**Config:** `STRIPPED_HEADER_NAMES` (default: `authorization,x-api-key,cookie,set-cookie`)

### 3. ANONYMIZATION_HASH_SALT now enforced as required
**File:** `app/config.py`

The salt used for SipHash-2-4 token hashing no longer has a default placeholder value. The app will refuse to start with empty salt or known placeholders like `"change-me"` when Qwen trace recording is enabled. This prevents accidentally reversible anonymization.

### 4. Request body size limit
**File:** `app/proxy.py`, `app/config.py`

A configurable max request body size (default 10 MiB) rejects oversized payloads with HTTP 413 before they're forwarded upstream. This prevents memory exhaustion from malicious or accidental large uploads.

**Config:** `MAX_REQUEST_BODY_BYTES` (default: 10485760, `0` = unlimited)

### 5. Per-IP rate limiting
**File:** `app/rate_limit.py`, `app/main.py`, `app/config.py`

New in-memory sliding-window rate limiter middleware. Configurable per-client-IP request limit with a configurable time window. The `/healthz` endpoint is exempt. Disabled by default (`0` = no limit).

**Config:** `RATE_LIMIT_REQUESTS`, `RATE_LIMIT_WINDOW_SECONDS`

### 6. CORS support for browser-based clients
**File:** `app/main.py`, `app/config.py`

Added FastAPI `CORSMiddleware` so the proxy can be called from browser-based applications. Defaults to `*` (allow all origins). Configurable via `CORS_ALLOW_ORIGINS`.

## Performance Fixes

### 7. Bulk hash mapping upsert (N queries → 1 query)
**File:** `app/recorder.py` (`_bulk_get_or_create_hash_mapped_ids`)

Previously, each token block hash required an individual `INSERT ... ON CONFLICT` query to the `anon_hash_domain_map` table. For a 4K-token prompt (~250 blocks), that was 250 database round-trips per request. Now uses a single `UNNEST`-based bulk upsert that handles all hashes in one query.

### 8. SSE stream buffer size cap
**File:** `app/proxy.py`, `app/config.py`

Streaming responses are now capped at a configurable buffer size for recording purposes (default 50 MiB). The response is still forwarded to the client in full — only the recorded copy is truncated. This prevents memory exhaustion from very long streaming completions.

**Config:** `MAX_STREAM_BUFFER_BYTES` (default: 52428800, `0` = unlimited)

## Correctness Fixes

### 9. tiktoken is now a hard requirement
**File:** `app/anonymizer.py`

Removed the `try/except ImportError` fallback that silently degraded to `ord(char)` character-level "tokenization". Since `tiktoken` is already in `dependencies`, this fallback would only trigger in broken installations and would produce incorrect token counts and wrong hash_ids. Now imports tiktoken directly — a missing dependency will fail loudly at startup.

### 10. Lazy module-level app creation
**File:** `app/main.py`

The module-level `app = create_app()` was replaced with a `__getattr__` lazy pattern. This ensures that importing `app.main` during test collection doesn't require production environment variables, while still working for Vercel's `api/index.py` entrypoint and `uvicorn app.main:app`.

## Data Management

### 11. Data retention cleanup
**File:** `app/db.py` (`cleanup_old_records`), `app/config.py`

New `cleanup_old_records()` function deletes records older than `RETENTION_DAYS` from `raw_http_records` and `anon_usage_traces`. Can be called from a cron job or during application startup. See README for example cron setup.

**Config:** `RETENTION_DAYS` (default: `0` = keep forever)

### 12. Qwen-Bailian JSONL export
**File:** `app/export.py`

New export module that outputs anonymized traces in the exact Qwen-Bailian JSONL format used by https://github.com/alibaba-edu/qwen-bailian-usagetraces-anon. Each line contains: `chat_id`, `parent_chat_id`, `timestamp`, `input_length`, `output_length`, `type`, `turn`, `hash_ids` — matching the reference schema exactly. Supports optional date-range filtering.

## Test Coverage

### 13. Comprehensive new tests (29 total, up from 8)
**New test files:**
- `tests/unit/test_config.py` — salt validation, stripped header parsing, defaults
- `tests/unit/test_proxy_headers.py` — hop-by-hop filtering, header stripping
- `tests/unit/test_rate_limit.py` — rate limit enforcement, healthz exemption, disabled mode
- `tests/integration/test_proxy_extended.py` — upstream error recording, body truncation, noop recorder, body size limit, auth header stripping

**New test scenarios:**
- Upstream connection failure → recorded as 502
- `MAX_RECORDED_BODY_BYTES` truncation verified
- `NoopRecorder` path (both recording modes disabled, no DB needed)
- Request body too large → 413 rejection
- Authorization and discount headers absent from stored recordings
- Malformed payloads return `None` trace (no crash)
- `None` request payload handled gracefully

## Configuration Changes Summary

| Variable | Status | Default |
|----------|--------|---------|
| `STRIPPED_HEADER_NAMES` | New (renamed from `SANITIZED_HEADER_NAMES`) | `authorization,x-api-key,cookie,set-cookie` |
| `CORS_ALLOW_ORIGINS` | New | `*` |
| `MAX_REQUEST_BODY_BYTES` | New | `10485760` (10 MiB) |
| `MAX_STREAM_BUFFER_BYTES` | New | `52428800` (50 MiB) |
| `RATE_LIMIT_REQUESTS` | New | `0` (disabled) |
| `RATE_LIMIT_WINDOW_SECONDS` | New | `60` |
| `RETENTION_DAYS` | New | `0` (keep forever) |
| `ANONYMIZATION_HASH_SALT` | Changed | Now required when tracing enabled |
