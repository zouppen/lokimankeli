from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lokimankeli.start_position import (
    MAX_START_POSITION_BYTES,
    StartPosition,
    StartPositionError,
)


class StartPositionTests(unittest.TestCase):
    def test_absent_file_returns_none(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(StartPosition.load(Path(directory)))

    def test_loads_single_line_with_optional_trailing_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            for contents in ("cursor", "cursor\n"):
                with self.subTest(contents=contents):
                    path.write_text(contents)
                    self.assertEqual(StartPosition.load(Path(directory)).value, "cursor")

    def test_rejects_malformed_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            for contents in (b"", b"\n", b"one\ntwo\n", b"one\r\n", b"\xff"):
                with self.subTest(contents=contents):
                    path.write_bytes(contents)
                    with self.assertRaises(StartPositionError):
                        StartPosition.load(Path(directory))

    def test_rejects_oversized_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            path.write_bytes(b"x" * (MAX_START_POSITION_BYTES + 1))
            with self.assertRaisesRegex(StartPositionError, "exceeds"):
                StartPosition.load(Path(directory))

    def test_rejects_non_regular_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            path.symlink_to("target")
            with self.assertRaises(StartPositionError):
                StartPosition.load(Path(directory))

    def test_replace_and_consume_are_durable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "start-position")
            path.write_text("now\n")
            request = StartPosition.load(Path(directory))
            with patch("lokimankeli.start_position.os.fsync", wraps=os.fsync) as sync:
                resolved = request.replace("resolved-cursor")
                self.assertEqual(path.read_text(), "resolved-cursor\n")
                resolved.consume()
            self.assertFalse(path.exists())
            self.assertGreaterEqual(sync.call_count, 3)


if __name__ == "__main__":
    unittest.main()
