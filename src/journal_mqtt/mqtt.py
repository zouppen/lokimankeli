from __future__ import annotations

import json
import logging
import ssl
import threading
import time
from typing import Any

from .config import MQTTConfig

LOG = logging.getLogger(__name__)


class MQTTError(RuntimeError):
    pass


class MQTTBridge:
    def __init__(self, config: MQTTConfig):
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:  # pragma: no cover - depends on runtime packaging
            raise MQTTError("paho-mqtt is not installed") from exc

        self._mqtt = mqtt
        self._config = config
        self._connected = threading.Event()
        self._state_event = threading.Event()
        self._subscribed = threading.Event()
        self._state_payload: bytes | None = None
        self._state_topic: str | None = None
        self._connect_error: str | None = None
        self._subscription_error: str | None = None

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=config.client_id,
            protocol=mqtt.MQTTv311,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        if config.username is not None:
            self._client.username_pw_set(config.username, config.password)
        if config.tls.enabled:
            try:
                context = ssl.create_default_context(cafile=config.tls.ca_file)
                if config.tls.cert_file is not None:
                    context.load_cert_chain(config.tls.cert_file, config.tls.key_file)
                self._client.tls_set_context(context)
            except (OSError, ssl.SSLError) as exc:
                raise MQTTError(f"cannot configure MQTT TLS: {exc}") from exc

    def _on_connect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        if reason_code == 0:
            self._connect_error = None
            self._connected.set()
        else:
            self._connect_error = str(reason_code)
            self._connected.clear()

    def _on_disconnect(
        self, client: Any, userdata: Any, disconnect_flags: Any, reason_code: Any, properties: Any
    ) -> None:
        self._connected.clear()

    def _on_subscribe(
        self,
        client: Any,
        userdata: Any,
        mid: int,
        reason_codes: list[Any],
        properties: Any,
    ) -> None:
        failures = [str(code) for code in reason_codes if getattr(code, "is_failure", False)]
        self._subscription_error = ", ".join(failures) if failures else None
        self._subscribed.set()

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        if message.topic == self._state_topic and message.retain:
            self._state_payload = bytes(message.payload)
            self._state_event.set()

    def connect(self) -> None:
        try:
            self._client.connect(self._config.host, self._config.port, self._config.keepalive)
            self._client.loop_start()
        except Exception as exc:
            raise MQTTError(f"cannot connect to MQTT broker: {exc}") from exc
        if not self._connected.wait(self._config.connect_timeout):
            self.close()
            detail = f": {self._connect_error}" if self._connect_error else ""
            raise MQTTError(f"MQTT connection timed out{detail}")

    def load_cursor(self, topic: str) -> str | None:
        self._state_topic = topic
        self._state_payload = None
        self._state_event.clear()
        self._subscribed.clear()
        self._subscription_error = None
        deadline = time.monotonic() + self._config.state_timeout
        result, _mid = self._client.subscribe(topic, qos=1)
        if result != self._mqtt.MQTT_ERR_SUCCESS:
            raise MQTTError(f"cannot subscribe to state topic: {self._mqtt.error_string(result)}")
        if not self._subscribed.wait(max(0.0, deadline - time.monotonic())):
            raise MQTTError("MQTT state subscription timed out")
        if self._subscription_error:
            raise MQTTError(f"MQTT state subscription was rejected: {self._subscription_error}")
        received = self._state_event.wait(max(0.0, deadline - time.monotonic()))
        self._client.unsubscribe(topic)
        if not received or not self._state_payload:
            return None
        try:
            state = json.loads(self._state_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MQTTError("retained checkpoint is not valid JSON") from exc
        if not isinstance(state, dict) or state.get("version") != 1:
            raise MQTTError("retained checkpoint has an unsupported format")
        cursor = state.get("cursor")
        if not isinstance(cursor, str) or not cursor:
            raise MQTTError("retained checkpoint does not contain a cursor")
        return cursor

    def _wait_connected(self, stop: threading.Event) -> None:
        while not self._connected.wait(1.0):
            if stop.is_set():
                raise MQTTError("stopped while waiting for MQTT reconnection")

    def publish(self, topic: str, payload: str, *, retain: bool, stop: threading.Event) -> None:
        delay = 1.0
        while not stop.is_set():
            self._wait_connected(stop)
            info = self._client.publish(topic, payload, qos=1, retain=retain)
            if info.rc == self._mqtt.MQTT_ERR_SUCCESS:
                try:
                    info.wait_for_publish(timeout=self._config.publish_timeout)
                except (RuntimeError, ValueError) as exc:
                    LOG.warning("MQTT acknowledgement wait failed; retrying: %s", exc)
                else:
                    if info.is_published():
                        return
            else:
                LOG.warning("MQTT publish failed; retrying: %s", self._mqtt.error_string(info.rc))
            stop.wait(delay)
            delay = min(delay * 2, 30.0)
        raise MQTTError("stopped before MQTT publication was acknowledged")

    def save_cursor(self, topic: str, cursor: str, stop: threading.Event) -> None:
        payload = json.dumps({"version": 1, "cursor": cursor}, separators=(",", ":"))
        self.publish(topic, payload, retain=True, stop=stop)

    def close(self) -> None:
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()
