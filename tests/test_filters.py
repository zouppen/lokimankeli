from __future__ import annotations

import unittest

from lokimankeli.filters import FilterError, event_id, validate_publish_topic


class FilterUtilityTests(unittest.TestCase):
    def test_event_id_is_stable_unpadded_base64url(self) -> None:
        key = "0123456789abcdef0123456789abcdef"
        first = event_id(key, "cursor")
        self.assertEqual(first, event_id(key, "cursor"))
        self.assertEqual(len(first), 43)
        self.assertNotIn("=", first)
        self.assertNotEqual(first, event_id(key, "other"))

    def test_topic_validation(self) -> None:
        validate_publish_topic("devices/a/b", "state/bridge")
        for topic in ("", "bad/+", "bad/#", "bad\x00topic", "state/bridge"):
            with self.subTest(topic=topic), self.assertRaises(FilterError):
                validate_publish_topic(topic, "state/bridge")


if __name__ == "__main__":
    unittest.main()
