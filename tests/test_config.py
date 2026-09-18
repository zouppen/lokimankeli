from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from lokimankeli.config import ConfigError, config_log_level, load_config, read_config

VALID = '''
[general]
event_id_key = "0123456789abcdef0123456789abcdef"
[journal]
scope = "system"
[mqtt]
host = "broker"
client_id = "bridge"
strictness = "warn"
state_topic = "state/producer"
[[route]]
unit = "producer.service"
filter_strictness = "warn"
publish_filter = '{topic: ("events/" + .id), payload: .}'
[[route]]
unit = "audit.service"
filter_strictness = "fail"
publish_filter = '{topic: "audit", payload: .}'
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
        self.assertEqual(config.log_level, "info")
        self.assertEqual(config.journal_scope, "system")
        self.assertEqual(config.mqtt.port, 1883)
        self.assertEqual([route.unit for route in config.routes], ["producer.service", "audit.service"])
        self.assertEqual(
            config.routes[0].publish_filter,
            '{topic: ("events/" + .id), payload: .}',
        )
        self.assertEqual(config.routes[0].filter_strictness, "warn")
        self.assertEqual(config.routes[0].message_format, "json")
        self.assertEqual(config.routes[1].filter_strictness, "fail")
        self.assertEqual(config.mqtt.strictness, "warn")
        self.assertEqual(config.mqtt.state_topic, "state/producer")

    def test_accepts_message_formats(self) -> None:
        for value in ("json", "string"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'unit = "producer.service"',
                    f'unit = "producer.service"\nmessage_format = "{value}"',
                )
                self.assertEqual(
                    load_config(self.write(configured)).routes[0].message_format,
                    value,
                )

    def test_rejects_invalid_message_format(self) -> None:
        for value in ('"yaml"', "true"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'unit = "producer.service"',
                    f'unit = "producer.service"\nmessage_format = {value}',
                )
                with self.assertRaisesRegex(ConfigError, "message_format"):
                    load_config(self.write(configured))

    def test_accepts_log_levels(self) -> None:
        for value in ("debug", "info", "warning", "error", "critical"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    "[general]", f'[general]\nlog_level = "{value}"'
                )
                self.assertEqual(load_config(self.write(configured)).log_level, value)

    def test_rejects_invalid_log_level(self) -> None:
        with self.assertRaisesRegex(ConfigError, "general.log_level"):
            load_config(
                self.write(
                    VALID.replace(
                        "[general]", '[general]\nlog_level = "verbose"'
                    )
                )
            )

    def test_log_level_can_be_processed_before_remaining_config(self) -> None:
        data = read_config(self.write('[general]\nlog_level = "debug"\n'))
        self.assertEqual(config_log_level(data), "debug")
        with self.assertRaisesRegex(ConfigError, "journal"):
            load_config(self.write('[general]\nlog_level = "debug"\n'))

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
                    load_config(self.write(configured)).routes[0].filter_strictness,
                    value,
                )

    def test_rejects_invalid_filter_strictness(self) -> None:
        for value in ('"loud"', '"require-sub"', "true"):
            with self.subTest(value=value):
                configured = VALID.replace(
                    'filter_strictness = "warn"', f"filter_strictness = {value}"
                )
                with self.assertRaisesRegex(ConfigError, "filter_strictness"):
                    load_config(self.write(configured))

    def test_requires_at_least_one_route(self) -> None:
        without_routes = VALID.split("[[route]]", 1)[0]
        with self.assertRaisesRegex(ConfigError, "route"):
            load_config(self.write(without_routes))

    def test_rejects_duplicate_route_units(self) -> None:
        with self.assertRaisesRegex(ConfigError, "duplicate"):
            load_config(self.write(VALID.replace('unit = "audit.service"', 'unit = "producer.service"')))

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
        with self.assertRaisesRegex(ConfigError, "general.event_id_key.*32"):
            load_config(
                self.write(VALID.replace("0123456789abcdef0123456789abcdef", "short"))
            )

    def test_requires_event_id_key_in_general(self) -> None:
        old_location = VALID.replace(
            'event_id_key = "0123456789abcdef0123456789abcdef"\n',
            '',
        ) + '''
[security]
event_id_key = "0123456789abcdef0123456789abcdef"
'''
        with self.assertRaisesRegex(ConfigError, "event_id_key"):
            load_config(self.write(old_location))

    def test_rejects_non_string_event_id_key(self) -> None:
        configured = VALID.replace(
            'event_id_key = "0123456789abcdef0123456789abcdef"',
            "event_id_key = true",
        )
        with self.assertRaisesRegex(ConfigError, "general.event_id_key"):
            load_config(self.write(configured))

    def test_rejects_state_wildcard(self) -> None:
        with self.assertRaisesRegex(ConfigError, "wildcard"):
            load_config(self.write(VALID.replace("state/producer", "state/+")))

    def test_requires_a_regular_file(self) -> None:
        with self.assertRaises(ConfigError):
            load_config("/definitely/not/a/config.toml")


if __name__ == "__main__":
    unittest.main()
