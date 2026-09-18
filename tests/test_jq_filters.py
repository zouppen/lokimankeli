from __future__ import annotations

import importlib.util
import json
import unittest

from journal_mqtt.filters import FilterError, JQPublishFilter


@unittest.skipIf(importlib.util.find_spec("jq") is None, "Python jq binding is not installed")
class JQFilterTests(unittest.TestCase):
    def test_multiple_publications_have_independent_topics(self) -> None:
        publish_filter = JQPublishFilter(
            '''
            {
              topic: ("devices/" + .address + "/details"),
              payload: (del(.secret) | .ts = $timestamp | .id = $event_id)
            },
            {
              topic: ("devices/" + .address + "/summary"),
              payload: {summary: .value, id: $event_id}
            }
            ''',
            "state/bridge",
        )
        result = publish_filter.transform(
            {"address": "a/b", "value": 3, "secret": "remove"}, 1234, "event-id"
        )
        self.assertEqual(result[0].topic, "devices/a/b/details")
        self.assertEqual(
            json.loads(result[0].payload),
            {"address": "a/b", "value": 3, "ts": 1234, "id": "event-id"},
        )
        self.assertEqual(result[1].topic, "devices/a/b/summary")
        self.assertEqual(json.loads(result[1].payload), {"summary": 3, "id": "event-id"})

    def test_zero_outputs_are_allowed(self) -> None:
        publish_filter = JQPublishFilter("empty", "state/bridge")
        self.assertEqual(publish_filter.transform({}, 1234, "event-id"), [])

    def test_duplicate_topics_are_allowed(self) -> None:
        publish_filter = JQPublishFilter(
            '{topic: "events", payload: 1}, {topic: "events", payload: null}',
            "state/bridge",
        )
        result = publish_filter.transform({}, 1234, "event-id")
        self.assertEqual([item.topic for item in result], ["events", "events"])
        self.assertEqual([item.payload for item in result], ["1", "null"])

    def test_invalid_descriptor_rejects_the_complete_result(self) -> None:
        cases = {
            "not an object": '"events"',
            "missing payload": '{topic: "events"}',
            "extra field": '{topic: "events", payload: {}, extra: true}',
            "non-string topic": '{topic: 1, payload: {}}',
            "wildcard topic": '{topic: "events/+", payload: {}}',
            "state topic": '{topic: "state/bridge", payload: {}}',
        }
        for name, expression in cases.items():
            with self.subTest(name=name):
                publish_filter = JQPublishFilter(expression, "state/bridge")
                with self.assertRaises(FilterError):
                    publish_filter.transform({}, 1234, "event-id")

    def test_later_invalid_descriptor_prevents_a_result(self) -> None:
        publish_filter = JQPublishFilter(
            '{topic: "valid", payload: 1}, {topic: "invalid/#", payload: 2}',
            "state/bridge",
        )
        with self.assertRaises(FilterError):
            publish_filter.transform({}, 1234, "event-id")


if __name__ == "__main__":
    unittest.main()
