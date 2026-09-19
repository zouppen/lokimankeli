from __future__ import annotations

import threading
import unittest

from lokimankeli.config import RouteConfig
from lokimankeli.journal import JournalError, JournalSource


class FakeReader:
    def __init__(self, entries, flags):
        self.entries = entries
        self.flags = flags
        self.expression = [[{}]]
        self.position = -1
        self.current = None

    def add_match(self, **match):
        term = self.expression[-1][-1]
        for field, value in match.items():
            term.setdefault(field, set()).add(value)

    def add_conjunction(self):
        self.expression[-1].append({})

    def add_disjunction(self):
        self.expression.append([{}])

    def _visible(self):
        return [
            entry
            for entry in self.entries
            if any(
                all(
                    all(entry.get(field) in values for field, values in term.items())
                    for term in clause
                )
                for clause in self.expression
            )
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


def route(unit, journal_match=()):
    return RouteConfig(
        unit=unit,
        publish_filter="empty",
        filter_strictness="warn",
        journal_match=journal_match,
    )


class JournalTests(unittest.TestCase):
    def source(self, entries):
        module = FakeJournalModule(entries)
        return (
            JournalSource(
                "system",
                (route("producer.service"), route("audit.service")),
                _journal_module=module,
            ),
            module,
        )

    def test_latest_cursor_ignores_delivery_filters(self):
        source, _module = self.source([entry("one"), entry("global-tail")])
        self.assertEqual(source.latest_cursor(), "global-tail")

    def test_reader_matches_all_transports_for_configured_units_by_default(self):
        entries = [
            entry("producer", "producer.service", "stdout"),
            entry("audit", "audit.service", "journal"),
            entry("wrong-unit", "other.service", "stdout"),
        ]
        source, _module = self.source(entries)
        source.seek_after("producer")
        stop = threading.Event()
        following = source.follow(stop)
        self.assertEqual(next(following), entries[1])
        stop.set()
        self.assertEqual(list(following), [])

    def test_route_fields_are_anded_and_values_are_ored(self):
        entries = [
            {**entry("stdout", "producer.service", "stdout"), "SYSLOG_IDENTIFIER": "app"},
            {**entry("journal", "producer.service", "journal"), "SYSLOG_IDENTIFIER": "app"},
            {**entry("wrong-id", "producer.service", "journal"), "SYSLOG_IDENTIFIER": "other"},
            {**entry("audit", "audit.service", "syslog"), "SYSLOG_IDENTIFIER": "audit"},
        ]
        module = FakeJournalModule(entries)
        source = JournalSource(
            "system",
            (
                route(
                    "producer.service",
                    (
                        ("_TRANSPORT", ("stdout", "journal")),
                        ("SYSLOG_IDENTIFIER", ("app",)),
                    ),
                ),
                route("audit.service"),
            ),
            _journal_module=module,
        )
        source.seek_after("stdout")
        stop = threading.Event()
        following = source.follow(stop)
        self.assertEqual(next(following), entries[1])
        self.assertEqual(next(following), entries[3])
        stop.set()

    def test_repeated_unit_field_cannot_broaden_another_route(self):
        entries = [
            entry("producer", "producer.service"),
            entry("other", "other.service"),
            entry("audit", "audit.service"),
        ]
        module = FakeJournalModule(entries)
        source = JournalSource(
            "system",
            (
                route(
                    "producer.service",
                    (("_SYSTEMD_UNIT", ("other.service",)),),
                ),
                route("audit.service"),
            ),
            _journal_module=module,
        )
        source.seek_after("producer")
        stop = threading.Event()
        following = source.follow(stop)
        self.assertEqual(next(following), entries[2])
        stop.set()

    def test_entry_unit_uses_scope_specific_field(self):
        system_source, _module = self.source([])
        self.assertEqual(
            system_source.entry_unit({"_SYSTEMD_UNIT": "producer.service"}),
            "producer.service",
        )
        user_module = FakeJournalModule([])
        user_source = JournalSource(
            "user", (route("producer.service"),), _journal_module=user_module
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
            entry("matching", "producer.service", "journal"),
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
