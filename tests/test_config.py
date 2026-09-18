from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from lokimankeli.config import ConfigError, load_config

VALID = '''
[journal]
scope = "system"
unit = "producer.service"
[mqtt]
host = "broker"
client_id = "bridge"
[routing]
publish_filter = '{topic: ("events/" + .id), payload: .}'
state_topic = "state/producer"
[security]
event_id_key = "0123456789abcdef0123456789abcdef"
'''


class ConfigTests(unittest.TestCase):
    def write(self, text: str) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name, "config.toml")
        path.write_text(text)
        return path

    def test_loads_required_configuration(self) -> None:
        config = load_config(self.write(VALID))
        self.assertEqual(config.journal_scope, "system")
        self.assertEqual(config.mqtt.port, 1883)
        self.assertEqual(config.publish_filter, '{topic: ("events/" + .id), payload: .}')

    def test_rejects_unknown_scope(self) -> None:
        with self.assertRaisesRegex(ConfigError, "scope"):
            load_config(self.write(VALID.replace('scope = "system"', 'scope = "both"')))

    def test_rejects_short_hmac_key(self) -> None:
        with self.assertRaisesRegex(ConfigError, "32"):
            load_config(
                self.write(VALID.replace("0123456789abcdef0123456789abcdef", "short"))
            )

    def test_rejects_state_wildcard(self) -> None:
        with self.assertRaisesRegex(ConfigError, "wildcard"):
            load_config(self.write(VALID.replace("state/producer", "state/+")))

    def test_rejects_legacy_filter_configuration_with_migration_hint(self) -> None:
        legacy = VALID.replace(
            'publish_filter = \'{topic: ("events/" + .id), payload: .}\'',
            'topic_filter = \'"events/" + .id\'\ncontent_filter = "."',
        )
        with self.assertRaisesRegex(ConfigError, "replaced by routing.publish_filter"):
            load_config(self.write(legacy))

    def test_requires_a_regular_file(self) -> None:
        with self.assertRaises(ConfigError):
            load_config("/definitely/not/a/config.toml")


if __name__ == "__main__":
    unittest.main()
