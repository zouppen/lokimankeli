from __future__ import annotations

import unittest

from lokimankeli.cli import build_parser


class CLITests(unittest.TestCase):
    def test_daemon_no_longer_accepts_cursor_override(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["--config", "config.toml", "--cursor", "old-cursor"]
            )


if __name__ == "__main__":
    unittest.main()
