from __future__ import annotations

import threading
import types
import unittest
from unittest import mock

from lokimankeli.config import MQTTConfig
from lokimankeli.mqtt import MQTTBridge, MQTTError


class ReasonCode:
    def __init__(self, value: int, name: str):
        self.value = value
        self.name = name
        self.is_failure = value >= 0x80

    def __eq__(self, other):
        if isinstance(other, int):
            return self.value == other
        return NotImplemented

    def __str__(self):
        return self.name


class MessageInfo:
    def __init__(self, bridge, mid, reason):
        self.bridge = bridge
        self.mid = mid
        self.reason = reason
        self.rc = 0
        self.published = False

    def wait_for_publish(self, timeout):
        self.bridge._on_publish(None, None, self.mid, self.reason, None)
        self.published = True

    def is_published(self):
        return self.published


class Client:
    def __init__(self, bridge, reasons):
        self.bridge = bridge
        self.reasons = iter(reasons)
        self.mid = 0

    def publish(self, topic, payload, qos, retain):
        self.mid += 1
        return MessageInfo(self.bridge, self.mid, next(self.reasons))


def bridge_with_reasons(*reasons):
    bridge = MQTTBridge.__new__(MQTTBridge)
    bridge._mqtt = types.SimpleNamespace(
        MQTT_ERR_SUCCESS=0,
        error_string=lambda value: f"error {value}",
    )
    bridge._config = MQTTConfig(
        host="broker",
        client_id="bridge",
        strictness="warn",
        state_topic="state/bridge",
        publish_timeout=0.01,
    )
    bridge._connected = threading.Event()
    bridge._connected.set()
    bridge._publish_lock = threading.Lock()
    bridge._publish_reasons = {}
    bridge._abandoned_publish_ids = set()
    bridge._client = Client(bridge, reasons)
    return bridge


class MQTTTests(unittest.TestCase):
    def test_client_uses_mqtt_v5(self):
        import paho.mqtt.client as mqtt

        client = mock.Mock()
        with mock.patch.object(mqtt, "Client", return_value=client) as constructor:
            MQTTBridge(
                MQTTConfig(
                    host="broker",
                    client_id="bridge",
                    strictness="warn",
                    state_topic="state/bridge",
                )
            )
        self.assertEqual(constructor.call_args.kwargs["protocol"], mqtt.MQTTv5)

    def test_publish_returns_success_reason(self):
        bridge = bridge_with_reasons(ReasonCode(0, "Success"))
        result = bridge.publish("telemetry/device", "{}", retain=False, stop=threading.Event())
        self.assertFalse(result.rejected)
        self.assertFalse(result.no_matching_subscribers)

    def test_publish_reports_no_matching_subscribers(self):
        bridge = bridge_with_reasons(ReasonCode(0x10, "No matching subscribers"))
        result = bridge.publish("telemetry/device", "{}", retain=False, stop=threading.Event())
        self.assertFalse(result.rejected)
        self.assertTrue(result.no_matching_subscribers)

    def test_publish_returns_broker_rejection(self):
        bridge = bridge_with_reasons(ReasonCode(0x87, "Not authorized"))
        result = bridge.publish("telemetry/device", "{}", retain=False, stop=threading.Event())
        self.assertTrue(result.rejected)
        self.assertEqual(result.reason, "Not authorized")

    def test_checkpoint_rejection_is_fatal(self):
        bridge = bridge_with_reasons(ReasonCode(0x87, "Not authorized"))
        with self.assertRaisesRegex(MQTTError, "checkpoint.*Not authorized"):
            bridge.save_cursor("state/bridge", "cursor", threading.Event())

    def test_late_acknowledgement_is_discarded(self):
        bridge = bridge_with_reasons()
        bridge._abandon_publish(7)
        bridge._on_publish(None, None, 7, ReasonCode(0, "Success"), None)
        self.assertNotIn(7, bridge._publish_reasons)
        self.assertNotIn(7, bridge._abandoned_publish_ids)


if __name__ == "__main__":
    unittest.main()
