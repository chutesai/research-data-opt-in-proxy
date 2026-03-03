from __future__ import annotations

import hashlib
import struct
from datetime import datetime
from typing import Any
from uuid import UUID

import orjson

from app.models import UsageTraceCandidate
from app.siphash import siphash24

import tiktoken


_TOKEN_BLOCK_SIZE = 16


def parse_json_bytes(raw: bytes) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        parsed = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def build_usage_trace_candidate(
    request_id: UUID,
    request_payload: dict[str, Any] | None,
    response_body: bytes,
    response_content_type: str,
    observed_at: datetime,
    anonymization_hash_salt: str,
) -> UsageTraceCandidate | None:
    if not request_payload:
        return None

    messages = request_payload.get("messages")
    if not isinstance(messages, list):
        return None

    prompt_text = _messages_to_text(messages)
    prompt_token_ids = _encode_text(prompt_text)

    output_text, usage_prompt_tokens, usage_completion_tokens, response_model = _extract_response_usage(
        response_body=response_body,
        response_content_type=response_content_type,
    )

    input_length = usage_prompt_tokens if usage_prompt_tokens is not None else len(prompt_token_ids)
    output_length = (
        usage_completion_tokens
        if usage_completion_tokens is not None
        else len(_encode_text(output_text))
    )

    trace_type = _infer_trace_type(messages=messages, request_payload=request_payload)
    turn = len(messages)

    full_context_hash = _context_hash(messages)
    parent_context_hash = _context_hash(messages[:-1]) if len(messages) > 1 else None

    raw_hash_values = _hashed_token_blocks(
        token_ids=prompt_token_ids,
        anonymization_hash_salt=anonymization_hash_salt,
    )

    return UsageTraceCandidate(
        request_id=request_id,
        observed_at=observed_at,
        context_hash=full_context_hash,
        parent_context_hash=parent_context_hash,
        input_length=max(0, int(input_length)),
        output_length=max(0, int(output_length)),
        trace_type=trace_type,
        turn=turn,
        raw_hash_values=raw_hash_values,
        model=(request_payload.get("model") or response_model),
    )


def _messages_to_text(messages: list[Any]) -> str:
    lines: list[str] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "unknown")
        content = message.get("content")
        normalized = _normalize_content(content)
        lines.append(f"{role}: {normalized}")
    return "\n".join(lines)


def _normalize_content(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                continue

            item_type = str(item.get("type") or "")
            if item_type in {"text", "input_text"}:
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
            elif item_type in {"image", "image_url", "input_image"}:
                parts.append("[image]")
            elif item_type in {"file", "input_file"}:
                parts.append("[file]")
            elif isinstance(item.get("text"), str):
                parts.append(item["text"])
        return " ".join(parts)

    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"]
        return orjson.dumps(content).decode("utf-8", errors="ignore")

    return ""


def _infer_trace_type(messages: list[Any], request_payload: dict[str, Any]) -> str:
    saw_image = False
    saw_file = False

    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for item in content:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "")
            if item_type in {"image", "image_url", "input_image"} or "image_url" in item:
                saw_image = True
            if item_type in {"file", "input_file"} or "file" in item or "file_url" in item:
                saw_file = True

    if saw_file:
        return "file"
    if saw_image:
        return "image"

    tools = request_payload.get("tools")
    if isinstance(tools, list):
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            function = tool.get("function")
            if isinstance(function, dict):
                name = str(function.get("name") or "").lower()
                if "search" in name:
                    return "search"

    return "text"


def _extract_response_usage(
    response_body: bytes,
    response_content_type: str,
) -> tuple[str, int | None, int | None, str | None]:
    content_type = response_content_type.lower()
    if "text/event-stream" in content_type:
        return _extract_sse_usage(response_body)
    return _extract_json_usage(response_body)


def _extract_json_usage(response_body: bytes) -> tuple[str, int | None, int | None, str | None]:
    payload = parse_json_bytes(response_body)
    if not payload:
        return "", None, None, None

    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    prompt_tokens = _as_int(usage.get("prompt_tokens"))
    completion_tokens = _as_int(usage.get("completion_tokens"))

    model = payload.get("model") if isinstance(payload.get("model"), str) else None

    output_parts: list[str] = []
    choices = payload.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
            if isinstance(message.get("content"), str):
                output_parts.append(message["content"])
                continue

            content = message.get("content")
            if isinstance(content, list):
                output_parts.append(_normalize_content(content))

            text = choice.get("text")
            if isinstance(text, str):
                output_parts.append(text)

    return "\n".join(output_parts), prompt_tokens, completion_tokens, model


def _extract_sse_usage(response_body: bytes) -> tuple[str, int | None, int | None, str | None]:
    text = response_body.decode("utf-8", errors="ignore")

    output_parts: list[str] = []
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    model: str | None = None

    for line in text.splitlines():
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if not data or data == "[DONE]":
            continue

        try:
            payload = orjson.loads(data)
        except orjson.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        if isinstance(payload.get("model"), str):
            model = payload["model"]

        usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else None
        if usage:
            if _as_int(usage.get("prompt_tokens")) is not None:
                prompt_tokens = _as_int(usage.get("prompt_tokens"))
            if _as_int(usage.get("completion_tokens")) is not None:
                completion_tokens = _as_int(usage.get("completion_tokens"))

        choices = payload.get("choices")
        if not isinstance(choices, list):
            continue

        for choice in choices:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
            if isinstance(delta.get("content"), str):
                output_parts.append(delta["content"])

            # Some providers emit role+content under message even for streaming.
            message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
            if isinstance(message.get("content"), str):
                output_parts.append(message["content"])

            if isinstance(choice.get("text"), str):
                output_parts.append(choice["text"])

    return "".join(output_parts), prompt_tokens, completion_tokens, model


def _context_hash(messages: list[Any]) -> str:
    normalized = orjson.dumps(messages, option=orjson.OPT_SORT_KEYS)
    return hashlib.sha256(normalized).hexdigest()


def _derive_siphash_key(anonymization_hash_salt: str) -> bytes:
    return hashlib.blake2b(
        anonymization_hash_salt.encode("utf-8"),
        digest_size=16,
    ).digest()


def _hashed_token_blocks(token_ids: list[int], anonymization_hash_salt: str) -> list[int]:
    if not token_ids:
        return []

    siphash_key = _derive_siphash_key(anonymization_hash_salt)
    hashed_values: list[int] = []

    for i in range(0, len(token_ids), _TOKEN_BLOCK_SIZE):
        block = token_ids[i : i + _TOKEN_BLOCK_SIZE]
        payload = b"".join(struct.pack("<I", token & 0xFFFFFFFF) for token in block)
        raw_hash = siphash24(siphash_key, payload)
        hashed_values.append(_to_signed_64(raw_hash))

    return hashed_values


def _to_signed_64(value: int) -> int:
    if value >= (1 << 63):
        return value - (1 << 64)
    return value


def _encode_text(text: str) -> list[int]:
    if not text:
        return []

    encoder = tiktoken.get_encoding("cl100k_base")
    return list(encoder.encode(text, disallowed_special=()))


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None
