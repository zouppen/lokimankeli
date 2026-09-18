from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any


class JournalError(RuntimeError):
    pass


class JournalSource:
    def __init__(self, scope: str, unit: str, *, _journal_module: Any | None = None):
        if _journal_module is None:
            try:
                from systemd import journal
            except ImportError as exc:  # pragma: no cover - depends on runtime packaging
                raise JournalError("the systemd Python bindings are not installed") from exc
        else:
            journal = _journal_module

        flags = journal.LOCAL_ONLY
        if scope == "system":
            flags |= journal.SYSTEM_ONLY
            unit_field = "_SYSTEMD_UNIT"
        elif scope == "user":
            flags |= journal.CURRENT_USER
            unit_field = "_SYSTEMD_USER_UNIT"
        else:  # Configuration validation should make this unreachable.
            raise JournalError(f"unsupported journal scope: {scope}")

        self._journal = journal
        self._flags = flags
        self._pending: dict[str, Any] | None = None
        try:
            self._reader = self._new_reader()
            self._reader.add_match(**{unit_field: unit})
            self._reader.add_match(_TRANSPORT="stdout")
        except Exception as exc:
            raise JournalError("cannot open or filter the selected journal") from exc

    def _new_reader(self) -> Any:
        return self._journal.Reader(flags=self._flags)

    def validate_cursor(self, cursor: str) -> None:
        try:
            reader = self._new_reader()
            reader.seek_cursor(cursor)
            entry = reader.get_next()
        except Exception as exc:
            raise JournalError("cannot seek to the configured journal cursor") from exc
        if not entry or not reader.test_cursor(cursor):
            raise JournalError("journal cursor is unavailable in the configured journal scope")

    def latest_cursor(self) -> str:
        try:
            reader = self._new_reader()
            reader.seek_tail()
            entry = reader.get_previous()
        except Exception as exc:
            raise JournalError("cannot find the end of the configured journal scope") from exc
        cursor = entry.get("__CURSOR") if entry else None
        if not isinstance(cursor, str) or not cursor:
            raise JournalError("the configured journal scope contains no entries")
        return cursor

    def seek_after(self, cursor: str) -> None:
        self.validate_cursor(cursor)
        try:
            self._reader.seek_cursor(cursor)
            entry = self._reader.get_next()
            self._pending = entry if entry and not self._reader.test_cursor(cursor) else None
        except Exception as exc:
            raise JournalError("cannot seek after the configured journal cursor") from exc

    def follow(self, stop: threading.Event) -> Iterator[dict[str, Any]]:
        if self._pending is not None and not stop.is_set():
            yield self._pending
            self._pending = None
        while not stop.is_set():
            entry = self._reader.get_next()
            if entry:
                yield entry
                continue
            try:
                self._reader.wait(1_000_000)
            except Exception as exc:
                raise JournalError("journal follow failed") from exc
