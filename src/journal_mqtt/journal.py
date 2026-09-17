from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any


class JournalError(RuntimeError):
    pass


class JournalSource:
    def __init__(self, scope: str, unit: str):
        try:
            from systemd import journal
        except ImportError as exc:  # pragma: no cover - depends on runtime packaging
            raise JournalError("the systemd Python bindings are not installed") from exc

        flags = journal.LOCAL_ONLY
        if scope == "system":
            flags |= journal.SYSTEM_ONLY
            unit_field = "_SYSTEMD_UNIT"
        elif scope == "user":
            flags |= journal.CURRENT_USER
            unit_field = "_SYSTEMD_USER_UNIT"
        else:  # Configuration validation should make this unreachable.
            raise JournalError(f"unsupported journal scope: {scope}")

        try:
            self._reader = journal.Reader(flags=flags)
            self._reader.add_match(**{unit_field: unit})
            self._reader.add_match(_TRANSPORT="stdout")
        except Exception as exc:
            raise JournalError("cannot open or filter the selected journal") from exc

    def seek_after(self, cursor: str) -> None:
        try:
            self._reader.seek_cursor(cursor)
            entry = self._reader.get_next()
        except Exception as exc:
            raise JournalError("cannot seek to the configured journal cursor") from exc
        if not entry or entry.get("__CURSOR") != cursor:
            raise JournalError("journal cursor is unavailable or does not match the configured source")

    def follow(self, stop: threading.Event) -> Iterator[dict[str, Any]]:
        while not stop.is_set():
            entry = self._reader.get_next()
            if entry:
                yield entry
                continue
            try:
                self._reader.wait(1_000_000)
            except Exception as exc:
                raise JournalError("journal follow failed") from exc
