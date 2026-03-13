from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import orjson

from app.chutes_trace import unwrap_chutes_non_stream_body


@dataclass(slots=True)
class NormalizedStoredResponse:
    json_payload: Any
    content_type: str = "application/json"
    storage_format: str = "json"
    is_complete: bool = True

    @property
    def body_bytes(self) -> bytes:
        return orjson.dumps(self.json_payload)


class StreamingResponseJsonBuilder:
    def __init__(self):
        self._buffer = bytearray()
        self._accumulator = _StreamingResponseAccumulator()
        self._finalized_payload: Any | None = None

    def feed(self, chunk: bytes) -> None:
        self._buffer.extend(chunk)
        while True:
            newline_idx = self._buffer.find(b"\n")
            if newline_idx < 0:
                break
            line = bytes(self._buffer[: newline_idx + 1])
            del self._buffer[: newline_idx + 1]
            self._consume_line(line)

    def finalize(self, *, observed_at: datetime | None = None) -> NormalizedStoredResponse | None:
        if self._buffer:
            trailing = bytes(self._buffer)
            self._buffer.clear()
            if not trailing.endswith(b"\n"):
                trailing += b"\n"
            self._consume_line(trailing)

        if self._finalized_payload is not None:
            return NormalizedStoredResponse(json_payload=self._finalized_payload)

        self._accumulator.observed_at = observed_at
        return self._accumulator.build()

    def _consume_line(self, line: bytes) -> None:
        stripped = line.rstrip(b"\r\n")
        if not stripped.startswith(b"data:"):
            return
        raw = stripped[5:].strip().decode("utf-8", errors="ignore")
        if not raw:
            return
        if raw == "[DONE]":
            self._accumulator.mark_done()
            return

        payload = _try_parse_json(raw.encode("utf-8"))
        if not isinstance(payload, dict):
            return
        if isinstance(payload.get("trace"), dict):
            return

        if "result" in payload:
            result = payload.get("result")
            if isinstance(result, dict):
                finalized = _extract_embedded_json_payload(result)
                if finalized is not None:
                    self._finalized_payload = finalized
                    return
                self._accumulator.consume_event(result)
                return

            if isinstance(result, str):
                for inner_raw in _iter_sse_data_values(result):
                    if inner_raw == "[DONE]":
                        self._accumulator.mark_done()
                        continue
                    inner_payload = _try_parse_json(inner_raw.encode("utf-8"))
                    if not isinstance(inner_payload, dict):
                        continue
                    finalized = _extract_embedded_json_payload(inner_payload)
                    if finalized is not None:
                        self._finalized_payload = finalized
                        return
                    self._accumulator.consume_event(inner_payload)
                return

        finalized = _extract_embedded_json_payload(payload)
        if finalized is not None:
            self._finalized_payload = finalized
            return

        self._accumulator.consume_event(payload)


def normalize_response_for_storage(
    response_body: bytes,
    response_content_type: str,
    *,
    observed_at: datetime | None = None,
) -> NormalizedStoredResponse | None:
    direct = _try_parse_json(response_body)
    if direct is not None:
        return NormalizedStoredResponse(json_payload=direct)

    lowered = response_content_type.lower()
    is_sse = "text/event-stream" in lowered or response_body.lstrip().startswith(b"data:")

    if (not is_sse or _looks_like_wrapped_sse_payload(response_body)) and (
        unwrapped := unwrap_chutes_non_stream_body(response_body)
    ):
        payload_body, content_type = unwrapped
        payload = _try_parse_json(payload_body)
        if payload is not None:
            return NormalizedStoredResponse(
                json_payload=payload,
                content_type=content_type or "application/json",
            )

    if not is_sse:
        return None

    return _normalize_sse_response(response_body, observed_at=observed_at)


def normalize_request_for_storage(request_body: bytes) -> tuple[Any | None, str]:
    payload = _try_parse_json(request_body)
    if payload is None:
        return None, "bytes"
    return payload, "json"


def _normalize_sse_response(
    response_body: bytes,
    *,
    observed_at: datetime | None = None,
) -> NormalizedStoredResponse | None:
    builder = StreamingResponseJsonBuilder()
    builder.feed(response_body)
    return builder.finalize(observed_at=observed_at)


def _iter_sse_data_values(decoded_sse: str):
    for line in decoded_sse.splitlines():
        if not line.startswith("data:"):
            continue
        yield line[5:].strip()


def _extract_embedded_json_payload(payload: dict[str, Any]) -> Any | None:
    if isinstance(payload.get("json"), (dict, list)):
        return payload["json"]
    if isinstance(payload.get("result"), dict):
        return _extract_embedded_json_payload(payload["result"])
    return None


def _looks_like_wrapped_sse_payload(response_body: bytes) -> bool:
    sample = response_body[:4096]
    return (
        b'"trace"' in sample
        or b'"result"' in sample
        or b'"content_type"' in sample
        or b'"json"' in sample
    )


def _try_parse_json(raw: bytes) -> Any | None:
    if not raw:
        return None
    try:
        return orjson.loads(raw)
    except orjson.JSONDecodeError:
        return None


def _normalize_message_content(content: Any) -> Any:
    if isinstance(content, (str, list)):
        return content
    if isinstance(content, dict):
        return content
    return ""


@dataclass(slots=True)
class _ToolCallAccumulator:
    index: int
    tool_id: str | None = None
    tool_type: str | None = None
    function_name_parts: list[str] = field(default_factory=list)
    function_arguments_parts: list[str] = field(default_factory=list)

    def consume(self, payload: dict[str, Any]) -> None:
        if isinstance(payload.get("id"), str):
            self.tool_id = payload["id"]
        if isinstance(payload.get("type"), str):
            self.tool_type = payload["type"]

        function = payload.get("function")
        if isinstance(function, dict):
            if isinstance(function.get("name"), str):
                self.function_name_parts.append(function["name"])
            if isinstance(function.get("arguments"), str):
                self.function_arguments_parts.append(function["arguments"])

    def build(self) -> dict[str, Any]:
        function_payload: dict[str, Any] = {}
        if self.function_name_parts:
            function_payload["name"] = "".join(self.function_name_parts)
        if self.function_arguments_parts:
            function_payload["arguments"] = "".join(self.function_arguments_parts)

        built: dict[str, Any] = {
            "index": self.index,
            "id": self.tool_id,
            "type": self.tool_type or "function",
            "function": function_payload,
        }
        return built


@dataclass(slots=True)
class _ChoiceAccumulator:
    index: int
    role: str | None = None
    content_parts: list[str] = field(default_factory=list)
    refusal_parts: list[str] = field(default_factory=list)
    reasoning_parts: list[str] = field(default_factory=list)
    function_name_parts: list[str] = field(default_factory=list)
    function_argument_parts: list[str] = field(default_factory=list)
    tool_calls: dict[int, _ToolCallAccumulator] = field(default_factory=dict)
    text_parts: list[str] = field(default_factory=list)
    finish_reason: str | None = None

    def consume(self, payload: dict[str, Any]) -> None:
        if isinstance(payload.get("finish_reason"), str):
            self.finish_reason = payload["finish_reason"]

        if isinstance(payload.get("text"), str):
            self.text_parts.append(payload["text"])

        if isinstance(payload.get("message"), dict):
            self._consume_message(payload["message"])

        if isinstance(payload.get("delta"), dict):
            self._consume_delta(payload["delta"])

    def _consume_message(self, payload: dict[str, Any]) -> None:
        if isinstance(payload.get("role"), str):
            self.role = payload["role"]

        content = payload.get("content")
        if isinstance(content, str):
            self.content_parts.append(content)

        if isinstance(payload.get("refusal"), str):
            self.refusal_parts.append(payload["refusal"])
        if isinstance(payload.get("reasoning_content"), str):
            self.reasoning_parts.append(payload["reasoning_content"])

        if isinstance(payload.get("function_call"), dict):
            function_call = payload["function_call"]
            if isinstance(function_call.get("name"), str):
                self.function_name_parts.append(function_call["name"])
            if isinstance(function_call.get("arguments"), str):
                self.function_argument_parts.append(function_call["arguments"])

        if isinstance(payload.get("tool_calls"), list):
            for offset, tool_call in enumerate(payload["tool_calls"]):
                if not isinstance(tool_call, dict):
                    continue
                index = int(tool_call.get("index", offset) or 0)
                accumulator = self.tool_calls.setdefault(index, _ToolCallAccumulator(index=index))
                accumulator.consume(tool_call)

    def _consume_delta(self, payload: dict[str, Any]) -> None:
        if isinstance(payload.get("role"), str):
            self.role = payload["role"]
        if isinstance(payload.get("content"), str):
            self.content_parts.append(payload["content"])
        if isinstance(payload.get("refusal"), str):
            self.refusal_parts.append(payload["refusal"])
        if isinstance(payload.get("reasoning_content"), str):
            self.reasoning_parts.append(payload["reasoning_content"])

        function_call = payload.get("function_call")
        if isinstance(function_call, dict):
            if isinstance(function_call.get("name"), str):
                self.function_name_parts.append(function_call["name"])
            if isinstance(function_call.get("arguments"), str):
                self.function_argument_parts.append(function_call["arguments"])

        tool_calls = payload.get("tool_calls")
        if isinstance(tool_calls, list):
            for offset, tool_call in enumerate(tool_calls):
                if not isinstance(tool_call, dict):
                    continue
                index = int(tool_call.get("index", offset) or 0)
                accumulator = self.tool_calls.setdefault(index, _ToolCallAccumulator(index=index))
                accumulator.consume(tool_call)

    def build(self) -> dict[str, Any]:
        if self.text_parts and not self.content_parts and not self.tool_calls:
            return {
                "index": self.index,
                "text": "".join(self.text_parts),
                "finish_reason": self.finish_reason,
            }

        message: dict[str, Any] = {
            "role": self.role or "assistant",
            "content": "".join(self.content_parts),
        }
        if self.refusal_parts:
            message["refusal"] = "".join(self.refusal_parts)
        if self.reasoning_parts:
            message["reasoning_content"] = "".join(self.reasoning_parts)
        if self.function_name_parts or self.function_argument_parts:
            message["function_call"] = {
                "name": "".join(self.function_name_parts),
                "arguments": "".join(self.function_argument_parts),
            }
        if self.tool_calls:
            message["tool_calls"] = [
                self.tool_calls[index].build()
                for index in sorted(self.tool_calls)
            ]

        return {
            "index": self.index,
            "message": message,
            "finish_reason": self.finish_reason,
        }


@dataclass(slots=True)
class _StreamingResponseAccumulator:
    observed_at: datetime | None = None
    response_id: str | None = None
    response_object: str | None = None
    created: int | None = None
    model: str | None = None
    system_fingerprint: str | None = None
    service_tier: str | None = None
    usage: dict[str, Any] | None = None
    choices: dict[int, _ChoiceAccumulator] = field(default_factory=dict)
    done_seen: bool = False
    saw_event: bool = False

    def consume_event(self, payload: dict[str, Any]) -> None:
        self.saw_event = True
        if isinstance(payload.get("id"), str):
            self.response_id = payload["id"]
        if isinstance(payload.get("object"), str):
            self.response_object = payload["object"]
        if isinstance(payload.get("created"), int):
            self.created = payload["created"]
        if isinstance(payload.get("model"), str):
            self.model = payload["model"]
        if isinstance(payload.get("system_fingerprint"), str):
            self.system_fingerprint = payload["system_fingerprint"]
        if isinstance(payload.get("service_tier"), str):
            self.service_tier = payload["service_tier"]
        if isinstance(payload.get("usage"), dict):
            self.usage = payload["usage"]

        choices = payload.get("choices")
        if not isinstance(choices, list):
            return

        for offset, choice in enumerate(choices):
            if not isinstance(choice, dict):
                continue
            index = int(choice.get("index", offset) or 0)
            accumulator = self.choices.setdefault(index, _ChoiceAccumulator(index=index))
            accumulator.consume(choice)

    def mark_done(self) -> None:
        self.done_seen = True

    def build(self) -> NormalizedStoredResponse | None:
        if not self.done_seen or not self.saw_event:
            return None

        created = self.created
        if created is None:
            when = self.observed_at or datetime.now(timezone.utc)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            created = int(when.timestamp())

        payload: dict[str, Any] = {
            "id": self.response_id or "recorded-stream-completion",
            "object": _normalize_object_name(self.response_object),
            "created": created,
            "choices": [
                self.choices[index].build()
                for index in sorted(self.choices)
            ],
        }
        if self.model:
            payload["model"] = self.model
        if self.system_fingerprint:
            payload["system_fingerprint"] = self.system_fingerprint
        if self.service_tier:
            payload["service_tier"] = self.service_tier
        if self.usage is not None:
            payload["usage"] = self.usage

        return NormalizedStoredResponse(json_payload=payload)


def _normalize_object_name(raw: str | None) -> str:
    if not raw:
        return "chat.completion"
    if raw.endswith(".chunk"):
        return raw[:-6]
    return raw
