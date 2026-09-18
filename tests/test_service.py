from __future__ import annotations

import threading
import unittest
from datetime import UTC, datetime

from lokimankeli.config import Config, MQTTConfig
from lokimankeli.filters import FilterError, Publication
from lokimankeli.mqtt import MQTTError, PublishResult
from lokimankeli.service import BridgeService, ServiceError, timestamp_milliseconds


def config(strictness: str = "warn") -> Config:
    return Config(
        journal_scope="system",
        journal_unit="producer.service",
        publish_filter='{topic: "telemetry/device", payload: .}',
        state_topic="state/producer",
        event_id_key="0123456789abcdef0123456789abcdef",
        mqtt=MQTTConfig(
            host="broker", client_id="bridge", strictness=strictness
        ),
    )


class FakeJournal:
    def __init__(self, entries=()):
        self.entries = list(entries)
        self.seeked = None

    def seek_after(self, cursor):
        self.seeked = cursor

    def follow(self, stop):
        yield from self.entries


class FakeMQTT:
    def __init__(self, stored="stored-cursor", results=None):
        self.stored = stored
        self.connected = False
        self.closed = False
        self.calls = []
        self.results = list(results or [])

    def connect(self):
        self.connected = True

    def load_cursor(self, topic):
        self.calls.append(("load", topic))
        return self.stored

    def publish(self, topic, payload, *, retain, stop):
        self.calls.append(("publish", topic, payload, retain))
        if self.results:
            return self.results.pop(0)
        return PublishResult("Success", False, False)

    def save_cursor(self, topic, cursor, stop):
        self.calls.append(("state", topic, cursor))

    def close(self):
        self.closed = True


class FakeFilters:
    def __init__(self, publications=None, error=None):
        self.publications = (
            publications
            if publications is not None
            else [Publication("telemetry/device", '{"ok":true}')]
        )
        self.error = error
        self.inputs = []

    def transform(self, message, timestamp, identifier):
        self.inputs.append((message, timestamp, identifier))
        if self.error:
            raise self.error
        return self.publications


class ServiceTests(unittest.TestCase):
    def test_datetime_timestamp_is_milliseconds(self):
        value = datetime(2026, 9, 17, 12, 0, 3, 647774, tzinfo=UTC)
        self.assertEqual(timestamp_milliseconds(value), 1789646403647)
        self.assertEqual(timestamp_milliseconds("1789646403647774"), 1789646403647)

    def test_multiple_payloads_are_published_before_checkpoint(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1789646403647774",
            "MESSAGE": '{"device":"a"}',
        }
        mqtt = FakeMQTT()
        filters = FakeFilters(
            [Publication("telemetry/device/rssi", "1"), Publication("telemetry/device/data", "2")]
        )
        service = BridgeService(
            config(), threading.Event(), journal=FakeJournal(), mqtt=mqtt, filters=filters
        )
        service._process(entry)
        self.assertEqual(
            mqtt.calls,
            [
                ("publish", "telemetry/device/rssi", "1", False),
                ("publish", "telemetry/device/data", "2", False),
                ("state", "state/producer", "cursor-2"),
            ],
        )
        self.assertEqual(filters.inputs[0][1], 1789646403647)
        self.assertEqual(len(filters.inputs[0][2]), 43)

    def test_zero_outputs_only_checkpoint(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT()
        service = BridgeService(
            config(), threading.Event(), journal=FakeJournal(), mqtt=mqtt, filters=FakeFilters([])
        )
        service._process(entry)
        self.assertEqual(mqtt.calls, [("state", "state/producer", "cursor-2")])

    def test_rejected_publication_is_warned_and_checkpointed(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT(results=[PublishResult("Not authorized", True, False)])
        service = BridgeService(
            config(),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(
                [
                    Publication("telemetry/denied", "1"),
                    Publication("telemetry/allowed", "2"),
                ]
            ),
        )
        with self.assertLogs("lokimankeli.service", "WARNING") as logs:
            service._process(entry)
        self.assertIn("Not authorized", " ".join(logs.output))
        self.assertEqual(
            [call[1] for call in mqtt.calls if call[0] == "publish"],
            ["telemetry/denied", "telemetry/allowed"],
        )
        self.assertEqual(mqtt.calls[-1], ("state", "state/producer", "cursor-2"))

    def test_rejected_publication_is_ignored_without_logging(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT(results=[PublishResult("Not authorized", True, False)])
        service = BridgeService(
            config("ignore"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(),
        )
        with self.assertNoLogs("lokimankeli.service", "WARNING"):
            service._process(entry)
        self.assertEqual(mqtt.calls[-1], ("state", "state/producer", "cursor-2"))

    def test_rejected_publication_fails_without_checkpoint(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT(results=[PublishResult("Not authorized", True, False)])
        service = BridgeService(
            config("fail"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(),
        )
        with self.assertRaisesRegex(MQTTError, "Not authorized"):
            service._process(entry)
        self.assertFalse(any(call[0] == "state" for call in mqtt.calls))

    def test_require_subscriber_fails_when_broker_reports_none(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT(results=[PublishResult("No matching subscribers", False, True)])
        service = BridgeService(
            config("require-subscriber"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(),
        )
        with self.assertRaisesRegex(MQTTError, "no matching subscribers"):
            service._process(entry)
        self.assertFalse(any(call[0] == "state" for call in mqtt.calls))

    def test_publication_strictness_overrides_configured_policy(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        no_subscribers = PublishResult("No matching subscribers", False, True)
        mqtt = FakeMQTT(results=[no_subscribers, no_subscribers])
        service = BridgeService(
            config("warn"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(
                [
                    Publication("telemetry/rssi", "1"),
                    Publication("telemetry/data", "2", "require-subscriber"),
                ]
            ),
        )
        with self.assertRaisesRegex(MQTTError, "telemetry/data"):
            service._process(entry)
        self.assertEqual(
            [call[1] for call in mqtt.calls if call[0] == "publish"],
            ["telemetry/rssi", "telemetry/data"],
        )
        self.assertFalse(any(call[0] == "state" for call in mqtt.calls))

    def test_filter_error_is_checkpointed(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT()
        service = BridgeService(
            config(),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(error=FilterError("bad")),
        )
        service._process(entry)
        self.assertEqual(mqtt.calls, [("state", "state/producer", "cursor-2")])

    def test_override_is_stored_before_follow(self):
        journal = FakeJournal()
        mqtt = FakeMQTT(stored=None)
        service = BridgeService(
            config(), threading.Event(), journal=journal, mqtt=mqtt, filters=FakeFilters()
        )
        service.run("initial-cursor")
        self.assertEqual(journal.seeked, "initial-cursor")
        self.assertEqual(mqtt.calls, [("state", "state/producer", "initial-cursor")])
        self.assertTrue(mqtt.closed)

    def test_missing_state_refuses_start(self):
        mqtt = FakeMQTT(stored=None)
        service = BridgeService(
            config(), threading.Event(), journal=FakeJournal(), mqtt=mqtt, filters=FakeFilters()
        )
        with self.assertRaises(ServiceError):
            service.run()
        self.assertTrue(mqtt.closed)


if __name__ == "__main__":
    unittest.main()
