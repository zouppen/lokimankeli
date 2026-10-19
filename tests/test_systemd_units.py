from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SystemdUnitTests(unittest.TestCase):
    def test_system_service_is_ordered_against_persistent_journal_flush(self):
        contents = (ROOT / "systemd" / "lokimankeli.service").read_text()

        for directive in ("After", "Wants"):
            match = re.search(rf"^{directive}=(.*)$", contents, re.MULTILINE)
            self.assertIsNotNone(match, f"missing {directive}= directive")
            self.assertIn("systemd-journal-flush.service", match.group(1).split())


if __name__ == "__main__":
    unittest.main()
