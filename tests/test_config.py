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
strictness = "warn"
[routing]
filter_strictness = "warn"
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
        self.assertEqual(config.filter_strictness, "warn")
        self.assertEqual(config.mqtt.strictness, "warn")

    def test_requires_strictness(self) -> None:
        with self.assertRaisesRegex(ConfigError, "strictness"):
            load_config(
                self.write(
                    VALID.replace(
                        'client_id = "bridge"\nstrictness = "warn"\n',
                        'client_id = "bridge"\n',
                    )
                )
            )

    def test_requires_filter_strictness(self) -> None:
        with self.assertRaisesRegex(ConfigError, "filter_strictness"):
            load_config(self.write(VALID.replace('filter_strictness = "warn"\n', "")))

    def test_accepts_filter_strictness_values(self) -> None:
        for value in ("ignore", "warn", "fail"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'filter_strictness = "warn"', f'filter_strictness = "{value}"'
                )
                self.assertEqual(
                    load_config(self.write(configured)).filter_strictness, value
                )

    def test_rejects_invalid_filter_strictness(self) -> None:
        for value in ('"loud"', '"require-sub"', "true"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'filter_strictness = "warn"', f"filter_strictness = {value}"
                )
                with self.assertRaisesRegex(ConfigError, "filter_strictness"):
                    load_config(self.write(configured))

    def test_accepts_strictness_values(self) -> None:
        for value in ("ignore", "warn", "fail", "require-sub"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'client_id = "bridge"\nstrictness = "warn"',
                    f'client_id = "bridge"\nstrictness = "{value}"',
                )
                self.assertEqual(
                    load_config(self.write(configured)).mqtt.strictness,
                    value,
                )

    def test_rejects_invalid_strictness(self) -> None:
        for value in ('"loud"', '"require-subscriber"', "true"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'client_id = "bridge"\nstrictness = "warn"',
                    f'client_id = "bridge"\nstrictness = {value}',
                )
                with self.assertRaisesRegex(ConfigError, "strictness"):
                    load_config(self.write(configured))

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
