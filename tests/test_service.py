from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path

from lokimankeli.config import Config, MQTTConfig, RouteConfig
from lokimankeli.filters import FilterError, Publication
from lokimankeli.journal import JournalError
from lokimankeli.mqtt import MQTTError, PublishResult
from lokimankeli.service import BridgeService, ServiceError, timestamp_milliseconds


def config(strictness: str = "warn", filter_strictness: str = "warn") -> Config:
    return Config(
        log_level="info",
        journal_scope="system",
        routes=(
            RouteConfig(
                unit="producer.service",
                publish_filter='{topic: "telemetry/device", payload: .}',
                filter_strictness=filter_strictness,
            ),
            RouteConfig(
                unit="audit.service",
                publish_filter='{topic: "telemetry/audit", payload: .}',
                filter_strictness="warn",
            ),
        ),
        event_id_key="0123456789abcdef0123456789abcdef",
        mqtt=MQTTConfig(
            host="broker",
            client_id="bridge",
            strictness=strictness,
            state_topic="state/producer",
        ),
    )


class FakeJournal:
    def __init__(self, entries=()):
        self.entries = list(entries)
        self.seeked = None
        self.latest = "latest-cursor"

    def seek_after(self, cursor):
        self.seeked = cursor

    def latest_cursor(self):
        return self.latest

    def entry_unit(self, entry):
        return entry.get("_SYSTEMD_UNIT", "producer.service")

    def follow(self, stop):
        yield from self.entries


class FakeMQTT:
    def __init__(self, stored="stored-cursor", results=None, state_error=None):
        self.stored = stored
        self.connected = False
        self.closed = False
        self.calls = []
        self.results = list(results or [])
        self.state_error = state_error

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
        if self.state_error is not None:
            raise self.state_error
        self.calls.append(("state", topic, cursor))

    def close(self):
        self.closed = True


class FakeFilters(dict):
    def __init__(self, publications=None, error=None):
        super().__init__()
        self["producer.service"] = self
        self["audit.service"] = self
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

    def test_debug_log_reports_route_topics_and_broker_acceptance(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        filters = FakeFilters(
            [
                Publication("victron/mac/rssi", "1"),
                Publication("victron/mac/data", "2"),
            ]
        )
        service = BridgeService(
            config(),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=FakeMQTT(),
            filters=filters,
        )
        with self.assertLogs("lokimankeli.service", "DEBUG") as logs:
            service._process(entry)
        output = " ".join(logs.output)
        self.assertIn("producer.service", output)
        self.assertIn("victron/mac/rssi", output)
        self.assertIn("victron/mac/data", output)
        self.assertEqual(output.count("broker accepted publication"), 2)

    def test_debug_log_reports_empty_filter_output(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        service = BridgeService(
            config(),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=FakeMQTT(),
            filters=FakeFilters([]),
        )
        with self.assertLogs("lokimankeli.service", "DEBUG") as logs:
            service._process(entry)
        self.assertIn("not published anywhere", " ".join(logs.output))

    def test_debug_log_reports_broker_rejection_even_when_ignored(self):
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
        with self.assertLogs("lokimankeli.service", "DEBUG") as logs:
            service._process(entry)
        self.assertIn("broker rejected", " ".join(logs.output))
        self.assertIn("Not authorized", " ".join(logs.output))

    def test_entries_are_dispatched_to_their_unit_filter(self):
        producer_filter = FakeFilters([Publication("producer", "1")])
        audit_filter = FakeFilters([Publication("audit", "2")])
        filters = {
            "producer.service": producer_filter,
            "audit.service": audit_filter,
        }
        mqtt = FakeMQTT()
        service = BridgeService(
            config(), threading.Event(), journal=FakeJournal(), mqtt=mqtt, filters=filters
        )
        service._process(
            {
                "__CURSOR": "cursor-audit",
                "__REALTIME_TIMESTAMP": "1000",
                "_SYSTEMD_UNIT": "audit.service",
                "MESSAGE": '{"kind":"audit"}',
            }
        )
        self.assertEqual(producer_filter.inputs, [])
        self.assertEqual(audit_filter.inputs[0][0], {"kind": "audit"})
        self.assertIn(("publish", "audit", "2", False), mqtt.calls)

    def test_entry_from_unconfigured_unit_is_fatal_without_checkpoint(self):
        mqtt = FakeMQTT()
        service = BridgeService(
            config(),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(),
        )
        with self.assertRaisesRegex(ServiceError, "unconfigured.service"):
            service._process(
                {
                    "__CURSOR": "cursor-other",
                    "_SYSTEMD_UNIT": "unconfigured.service",
                }
            )
        self.assertFalse(any(call[0] == "state" for call in mqtt.calls))

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
            config("require-sub"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(),
        )
        with (
            self.assertLogs("lokimankeli.service", "DEBUG") as logs,
            self.assertRaisesRegex(MQTTError, "no matching subscribers"),
        ):
            service._process(entry)
        self.assertIn("accepted publication", " ".join(logs.output))
        self.assertIn("no matching subscribers", " ".join(logs.output))
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
                    Publication("telemetry/data", "2", "require-sub"),
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

    def test_filter_error_is_warned_and_checkpointed(self):
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
        with self.assertLogs("lokimankeli.service", "WARNING") as logs:
            service._process(entry)
        self.assertIn("bad", " ".join(logs.output))
        self.assertEqual(mqtt.calls, [("state", "state/producer", "cursor-2")])

    def test_filter_strictness_is_selected_by_route(self):
        entry = {
            "__CURSOR": "cursor-audit",
            "__REALTIME_TIMESTAMP": "1000",
            "_SYSTEMD_UNIT": "audit.service",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT()
        failing_filter = FakeFilters(error=FilterError("bad audit record"))
        service = BridgeService(
            config(filter_strictness="fail"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters={
                "producer.service": FakeFilters(),
                "audit.service": failing_filter,
            },
        )
        with self.assertLogs("lokimankeli.service", "WARNING") as logs:
            service._process(entry)
        self.assertIn("audit.service", " ".join(logs.output))
        self.assertEqual(mqtt.calls, [("state", "state/producer", "cursor-audit")])

    def test_filter_error_is_ignored_and_checkpointed(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT()
        service = BridgeService(
            config(filter_strictness="ignore"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(error=FilterError("bad")),
        )
        with self.assertNoLogs("lokimankeli.service", "WARNING"):
            service._process(entry)
        self.assertEqual(mqtt.calls, [("state", "state/producer", "cursor-2")])

    def test_filter_error_fails_without_checkpoint(self):
        entry = {
            "__CURSOR": "cursor-2",
            "__REALTIME_TIMESTAMP": "1000",
            "MESSAGE": "{}",
        }
        mqtt = FakeMQTT()
        service = BridgeService(
            config(filter_strictness="fail"),
            threading.Event(),
            journal=FakeJournal(),
            mqtt=mqtt,
            filters=FakeFilters(error=FilterError("bad")),
        )
        with self.assertRaisesRegex(FilterError, "bad"):
            service._process(entry)
        self.assertFalse(any(call[0] == "state" for call in mqtt.calls))

    def test_missing_state_refuses_start(self):
        mqtt = FakeMQTT(stored=None)
        service = BridgeService(
            config(), threading.Event(), journal=FakeJournal(), mqtt=mqtt, filters=FakeFilters()
        )
        with self.assertRaisesRegex(ServiceError, "start-position"):
            service.run()
        self.assertTrue(mqtt.closed)

    def test_explicit_start_position_overrides_retained_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            path.write_text("chosen-cursor\n")
            journal = FakeJournal()
            mqtt = FakeMQTT(stored="ignored-cursor")
            service = BridgeService(
                config(),
                threading.Event(),
                journal=journal,
                mqtt=mqtt,
                filters=FakeFilters(),
                state_directory=Path(directory),
            )
            service.run()
            self.assertEqual(journal.seeked, "chosen-cursor")
            self.assertNotIn(("load", "state/producer"), mqtt.calls)
            self.assertIn(("state", "state/producer", "chosen-cursor"), mqtt.calls)
            self.assertFalse(path.exists())

    def test_now_is_resolved_before_failed_checkpoint_is_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            path.write_text("now\n")
            journal = FakeJournal()
            mqtt = FakeMQTT(state_error=MQTTError("checkpoint failed"))
            service = BridgeService(
                config(),
                threading.Event(),
                journal=journal,
                mqtt=mqtt,
                filters=FakeFilters(),
                state_directory=Path(directory),
            )
            with self.assertRaisesRegex(MQTTError, "checkpoint failed"):
                service.run()
            self.assertEqual(journal.seeked, "latest-cursor")
            self.assertEqual(path.read_text(), "latest-cursor\n")
            self.assertTrue(mqtt.closed)

    def test_invalid_start_position_is_left_for_correction(self):
        class RejectingJournal(FakeJournal):
            def seek_after(self, cursor):
                raise JournalError("cursor unavailable")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            path.write_text("bad-cursor\n")
            mqtt = FakeMQTT()
            service = BridgeService(
                config(),
                threading.Event(),
                journal=RejectingJournal(),
                mqtt=mqtt,
                filters=FakeFilters(),
                state_directory=Path(directory),
            )
            with self.assertRaisesRegex(JournalError, "unavailable"):
                service.run()
            self.assertEqual(path.read_text(), "bad-cursor\n")
            self.assertFalse(mqtt.connected)


if __name__ == "__main__":
    unittest.main()
