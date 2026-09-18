from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


PUBLISH_STRICTNESS_VALUES = frozenset(
    {"ignore", "warn", "fail", "require-sub"}
)
FILTER_STRICTNESS_VALUES = frozenset({"ignore", "warn", "fail"})


@dataclass(frozen=True)
class TLSConfig:
    enabled: bool = False
    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None


@dataclass(frozen=True)
class MQTTConfig:
    host: str
    client_id: str
    strictness: str
    port: int = 1883
    username: str | None = None
    password: str | None = None
    keepalive: int = 60
    connect_timeout: float = 15.0
    state_timeout: float = 5.0
    publish_timeout: float = 30.0
    tls: TLSConfig = TLSConfig()


@dataclass(frozen=True)
class Config:
    journal_scope: str
    journal_unit: str
    publish_filter: str
    filter_strictness: str
    state_topic: str
    event_id_key: str
    mqtt: MQTTConfig


def _table(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name)
    if not isinstance(value, dict):
        raise ConfigError(f"missing or invalid [{name}] table")
    return value


def _string(table: dict[str, Any], name: str, *, required: bool = True) -> str | None:
    value = table.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{name} must be a non-empty string")
    return value


def _number(
    table: dict[str, Any], name: str, default: float, *, integer: bool = False
) -> int | float:
    value = table.get(name, default)
    expected = int if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, expected) or value <= 0:
        raise ConfigError(f"{name} must be a positive {'integer' if integer else 'number'}")
    return int(value) if integer else float(value)


def _choice(table: dict[str, Any], name: str, choices: frozenset[str]) -> str:
    value = table.get(name)
    if not isinstance(value, str) or value not in choices:
        expected = ", ".join(repr(choice) for choice in sorted(choices))
        raise ConfigError(f"{name} must be one of {expected}")
    return value


def _validate_topic(topic: str, name: str) -> None:
    if "\x00" in topic:
        raise ConfigError(f"{name} must not contain a null character")
    if "+" in topic or "#" in topic:
        raise ConfigError(f"{name} must not contain MQTT wildcards")
    if not topic.encode("utf-8") or len(topic.encode("utf-8")) > 65535:
        raise ConfigError(f"{name} must encode to between 1 and 65535 UTF-8 bytes")


def load_config(path: str | os.PathLike[str]) -> Config:
    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(f"configuration is not a readable regular file: {config_path}")
    try:
        with config_path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read configuration {config_path}: {exc}") from exc

    journal = _table(data, "journal")
    mqtt_data = _table(data, "mqtt")
    routing = _table(data, "routing")
    security = _table(data, "security")

    scope = _string(journal, "scope")
    if scope not in {"system", "user"}:
        raise ConfigError("journal.scope must be 'system' or 'user'")
    unit = _string(journal, "unit")

    legacy_filters = [name for name in ("topic_filter", "content_filter") if name in routing]
    if legacy_filters:
        names = ", ".join(f"routing.{name}" for name in legacy_filters)
        raise ConfigError(f"{names} replaced by routing.publish_filter")
    publish_filter = _string(routing, "publish_filter")
    filter_strictness = _choice(
        routing, "filter_strictness", FILTER_STRICTNESS_VALUES
    )
    state_topic = _string(routing, "state_topic")
    _validate_topic(state_topic, "routing.state_topic")

    event_id_key = _string(security, "event_id_key")
    if len(event_id_key) < 32:
        raise ConfigError("security.event_id_key must contain at least 32 characters")

    tls_data = mqtt_data.get("tls", {})
    if not isinstance(tls_data, dict):
        raise ConfigError("mqtt.tls must be a table")
    tls_enabled = tls_data.get("enabled", False)
    if not isinstance(tls_enabled, bool):
        raise ConfigError("mqtt.tls.enabled must be a boolean")
    cert_file = _string(tls_data, "cert_file", required=False)
    key_file = _string(tls_data, "key_file", required=False)
    if (cert_file is None) != (key_file is None):
        raise ConfigError("mqtt.tls.cert_file and key_file must be configured together")
    tls = TLSConfig(
        enabled=tls_enabled,
        ca_file=_string(tls_data, "ca_file", required=False),
        cert_file=cert_file,
        key_file=key_file,
    )

    username = _string(mqtt_data, "username", required=False)
    password = _string(mqtt_data, "password", required=False)
    if password is not None and username is None:
        raise ConfigError("mqtt.username is required when mqtt.password is configured")

    mqtt = MQTTConfig(
        host=_string(mqtt_data, "host"),
        client_id=_string(mqtt_data, "client_id"),
        strictness=_choice(mqtt_data, "strictness", PUBLISH_STRICTNESS_VALUES),
        port=_number(mqtt_data, "port", 1883, integer=True),
        username=username,
        password=password,
        keepalive=_number(mqtt_data, "keepalive", 60, integer=True),
        connect_timeout=_number(mqtt_data, "connect_timeout", 15.0),
        state_timeout=_number(mqtt_data, "state_timeout", 5.0),
        publish_timeout=_number(mqtt_data, "publish_timeout", 30.0),
        tls=tls,
    )
    if mqtt.port > 65535:
        raise ConfigError("mqtt.port must not exceed 65535")

    return Config(
        journal_scope=scope,
        journal_unit=unit,
        publish_filter=publish_filter,
        filter_strictness=filter_strictness,
        state_topic=state_topic,
        event_id_key=event_id_key,
        mqtt=mqtt,
    )
