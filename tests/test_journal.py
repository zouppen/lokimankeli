from __future__ import annotations

import threading
import unittest

from lokimankeli.journal import JournalError, JournalSource


class FakeReader:
    def __init__(self, entries, flags):
        self.entries = entries
        self.flags = flags
        self.matches = []
        self.position = -1
        self.current = None

    def add_match(self, **match):
        self.matches.append(match)

    def _visible(self):
        alternatives = {}
        for match in self.matches:
            for field, value in match.items():
                alternatives.setdefault(field, set()).add(value)
        return [
            entry
            for entry in self.entries
            if all(entry.get(field) in values for field, values in alternatives.items())
        ]

    def seek_cursor(self, cursor):
        all_index = next(
            (index for index, entry in enumerate(self.entries) if entry["__CURSOR"] == cursor),
            0,
        )
        visible = self._visible()
        self.position = next(
            (
                index - 1
                for index, entry in enumerate(visible)
                if self.entries.index(entry) >= all_index
            ),
            len(visible) - 1,
        )
        self.current = None

    def seek_tail(self):
        self.position = len(self._visible())
        self.current = None

    def get_next(self):
        visible = self._visible()
        self.position += 1
        self.current = visible[self.position] if self.position < len(visible) else None
        return self.current or {}

    def get_previous(self):
        visible = self._visible()
        self.position -= 1
        self.current = visible[self.position] if 0 <= self.position < len(visible) else None
        return self.current or {}

    def test_cursor(self, cursor):
        return self.current is not None and self.current["__CURSOR"] == cursor

    def wait(self, timeout):
        return None


class FakeJournalModule:
    LOCAL_ONLY = 1
    SYSTEM_ONLY = 2
    CURRENT_USER = 4

    def __init__(self, entries):
        self.entries = entries
        self.readers = []

    def Reader(self, flags):
        reader = FakeReader(self.entries, flags)
        self.readers.append(reader)
        return reader


def entry(cursor, unit="other.service", transport="journal"):
    return {
        "__CURSOR": cursor,
        "_SYSTEMD_UNIT": unit,
        "_TRANSPORT": transport,
    }


class JournalTests(unittest.TestCase):
    def source(self, entries):
        module = FakeJournalModule(entries)
        return (
            JournalSource(
                "system",
                ("producer.service", "audit.service"),
                _journal_module=module,
            ),
            module,
        )

    def test_latest_cursor_ignores_delivery_filters(self):
        source, _module = self.source([entry("one"), entry("global-tail")])
        self.assertEqual(source.latest_cursor(), "global-tail")

    def test_reader_matches_all_configured_units_and_stdout_only(self):
        entries = [
            entry("producer", "producer.service", "stdout"),
            entry("audit", "audit.service", "stdout"),
            entry("wrong-transport", "producer.service", "journal"),
            entry("wrong-unit", "other.service", "stdout"),
        ]
        source, _module = self.source(entries)
        source.seek_after("producer")
        stop = threading.Event()
        following = source.follow(stop)
        self.assertEqual(next(following), entries[1])
        stop.set()
        self.assertEqual(list(following), [])

    def test_entry_unit_uses_scope_specific_field(self):
        system_source, _module = self.source([])
        self.assertEqual(
            system_source.entry_unit({"_SYSTEMD_UNIT": "producer.service"}),
            "producer.service",
        )
        user_module = FakeJournalModule([])
        user_source = JournalSource(
            "user", ("producer.service",), _journal_module=user_module
        )
        self.assertEqual(
            user_source.entry_unit({"_SYSTEMD_USER_UNIT": "producer.service"}),
            "producer.service",
        )

    def test_latest_cursor_rejects_empty_scope(self):
        source, _module = self.source([])
        with self.assertRaisesRegex(JournalError, "contains no entries"):
            source.latest_cursor()

    def test_validate_cursor_accepts_other_unit(self):
        source, _module = self.source([entry("global")])
        source.validate_cursor("global")

    def test_validate_cursor_rejects_missing_cursor(self):
        source, _module = self.source([entry("available")])
        with self.assertRaisesRegex(JournalError, "unavailable"):
            source.validate_cursor("purged")

    def test_seek_after_preserves_first_later_matching_entry(self):
        entries = [
            entry("global"),
            entry("matching", "producer.service", "stdout"),
        ]
        source, _module = self.source(entries)
        source.seek_after("global")
        stop = threading.Event()
        following = source.follow(stop)
        self.assertEqual(next(following), entries[1])
        stop.set()
        self.assertEqual(list(following), [])

    def test_seek_after_discards_matching_cursor_itself(self):
        entries = [
            entry("matching", "producer.service", "stdout"),
        ]
        source, _module = self.source(entries)
        source.seek_after("matching")
        stop = threading.Event()
        stop.set()
        self.assertEqual(list(source.follow(stop)), [])


if __name__ == "__main__":
    unittest.main()
