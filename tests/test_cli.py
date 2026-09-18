from __future__ import annotations

import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from lokimankeli.cli import build_parser, main


class CLITests(unittest.TestCase):
    def test_daemon_no_longer_accepts_cursor_override(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["--config", "config.toml", "--cursor", "old-cursor"]
            )

    def test_log_level_is_applied_before_semantic_config_validation(self):
        with TemporaryDirectory() as directory:
            path = Path(directory, "config.toml")
            path.write_text('[general]\nlog_level = "debug"\n')
            with patch.object(logging.getLogger(), "setLevel") as set_level:
                self.assertEqual(main(["--config", str(path)]), 1)
        self.assertEqual(set_level.call_args_list[-1].args, ("DEBUG",))


if __name__ == "__main__":
    unittest.main()
