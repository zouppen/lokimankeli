from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any


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
class TransformedRecord:
    topic: str
    payloads: list[str]


class JQFilters:
    def __init__(self, topic_filter: str, content_filter: str, state_topic: str):
        try:
            import jq
        except ImportError as exc:  # pragma: no cover - depends on runtime packaging
            raise FilterError("the Python jq binding is not installed") from exc

        self._state_topic = state_topic
        try:
            self._topic = jq.compile(topic_filter)
            wrapper = (
                ". as $__journal_mqtt | "
                "$__journal_mqtt.timestamp as $timestamp | "
                "$__journal_mqtt.event_id as $event_id | "
                "$__journal_mqtt.message | ("
                + content_filter
                + ")"
            )
            self._content = jq.compile(wrapper)
        except Exception as exc:
            raise FilterError(f"cannot compile jq filter: {exc}") from exc

    def transform(self, message: dict[str, Any], timestamp: int, identifier: str) -> TransformedRecord:
        try:
            topics = self._topic.input_value(message).all()
        except Exception as exc:
            raise FilterError(f"topic filter failed: {exc}") from exc
        if len(topics) != 1 or not isinstance(topics[0], str):
            raise FilterError("topic filter must produce exactly one string")
        topic = topics[0]
        validate_publish_topic(topic, self._state_topic)

        context = {"message": message, "timestamp": timestamp, "event_id": identifier}
        try:
            values = self._content.input_value(context).all()
            payloads = [
                json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
                for value in values
            ]
        except Exception as exc:
            raise FilterError(f"content filter failed: {exc}") from exc
        return TransformedRecord(topic=topic, payloads=payloads)
