from __future__ import annotations

import importlib.util
import json
import unittest

from journal_mqtt.filters import JQFilters


@unittest.skipIf(importlib.util.find_spec("jq") is None, "Python jq binding is not installed")
class JQFilterTests(unittest.TestCase):
    def test_topic_then_multiple_transformed_payloads(self) -> None:
        filters = JQFilters(
            '"devices/" + .address',
            'del(.secret) | .ts = $timestamp | .id = $event_id, {summary: .value}',
            "state/bridge",
        )
        result = filters.transform(
            {"address": "a/b", "value": 3, "secret": "remove"}, 1234, "event-id"
        )
        self.assertEqual(result.topic, "devices/a/b")
        self.assertEqual(
            json.loads(result.payloads[0]),
            {"address": "a/b", "value": 3, "ts": 1234, "id": "event-id"},
        )
        self.assertEqual(json.loads(result.payloads[1]), {"summary": 3})

    def test_zero_content_outputs_are_allowed(self) -> None:
        filters = JQFilters('"events/topic"', "empty", "state/bridge")
        result = filters.transform({}, 1234, "event-id")
        self.assertEqual(result.payloads, [])


if __name__ == "__main__":
    unittest.main()
