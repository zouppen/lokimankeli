from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from .config import PUBLISH_STRICTNESS_VALUES


class FilterError(ValueError):
    pass


def event_id(key: str, cursor: str) -> str:
    digest = hmac.new(key.encode("utf-8"), cursor.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def validate_publish_topic(topic: str, state_topic: str) -> None:
    if not topic:
        raise FilterError("topic filter returned an empty topic")
    if "\x00" in topic:
        raise FilterError("topic contains a null character")
    if "+" in topic or "#" in topic:
        raise FilterError("topic contains an MQTT wildcard")
    try:
        encoded = topic.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise FilterError("topic is not valid UTF-8") from exc
    if len(encoded) > 65535:
        raise FilterError("topic exceeds the MQTT UTF-8 length limit")
    if topic == state_topic:
        raise FilterError("telemetry topic equals the state topic")


@dataclass(frozen=True)
class Publication:
    topic: str
    payload: str
    strictness: str | None = None


class JQPublishFilter:
    def __init__(self, publish_filter: str, state_topic: str):
        try:
            import jq
        except ImportError as exc:  # pragma: no cover - depends on runtime packaging
            raise FilterError("the Python jq binding is not installed") from exc

        self._state_topic = state_topic
        try:
            wrapper = (
                ". as {timestamp_ms: $timestamp_ms, event_id: $event_id} "
                "| .message | ("
                + publish_filter
                + ")"
            )
            self._filter = jq.compile(wrapper)
        except Exception as exc:
            raise FilterError(f"cannot compile jq filter: {exc}") from exc

    def transform(
        self, message: Any, timestamp_ms: int, identifier: str
    ) -> list[Publication]:
        context = {"message": message, "timestamp_ms": timestamp_ms, "event_id": identifier}
        try:
            values = self._filter.input_value(context).all()
        except Exception as exc:
            raise FilterError(f"publish filter failed: {exc}") from exc

        publications: list[Publication] = []
        for index, value in enumerate(values):
            required = {"topic", "payload"}
            allowed = required | {"strictness"}
            if (
                not isinstance(value, dict)
                or not required.issubset(value)
                or not set(value).issubset(allowed)
            ):
                raise FilterError(
                    f"publish filter output {index} must contain topic and payload, "
                    "with optional strictness"
                )
            topic = value["topic"]
            if not isinstance(topic, str):
                raise FilterError(f"publish filter output {index} topic must be a string")
            validate_publish_topic(topic, self._state_topic)
            try:
                payload = json.dumps(
                    value["payload"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                payload.encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise FilterError(f"publish filter output {index} payload is not valid JSON") from exc
            strictness = value.get("strictness")
            if "strictness" in value and (
                not isinstance(strictness, str)
                or strictness not in PUBLISH_STRICTNESS_VALUES
            ):
                raise FilterError(
                    f"publish filter output {index} strictness must be one of "
                    + ", ".join(repr(item) for item in sorted(PUBLISH_STRICTNESS_VALUES))
                )
            publications.append(Publication(topic, payload, strictness))
        return publications
