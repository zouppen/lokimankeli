from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any

from .config import Config
from .filters import FilterError, JQPublishFilter, event_id
from .journal import JournalSource
from .mqtt import MQTTBridge, MQTTError

LOG = logging.getLogger(__name__)


class ServiceError(RuntimeError):
    pass


def timestamp_milliseconds(value: object) -> int:
    """Normalize python-systemd's datetime or a journal JSON microsecond value."""
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, bool):
        raise TypeError("boolean is not a journal timestamp")
    return int(value) // 1000


class BridgeService:
    def __init__(
        self,
        config: Config,
        stop: threading.Event,
        *,
        journal: JournalSource | None = None,
        mqtt: MQTTBridge | None = None,
        filters: JQPublishFilter | None = None,
    ):
        self.config = config
        self.stop = stop
        self.journal = journal or JournalSource(config.journal_scope, config.journal_unit)
        self.mqtt = mqtt or MQTTBridge(config.mqtt)
        self.filters = filters or JQPublishFilter(config.publish_filter, config.state_topic)

    def _checkpoint(self, cursor: str) -> None:
        self.mqtt.save_cursor(self.config.state_topic, cursor, self.stop)

    def _process(self, entry: dict[str, Any]) -> None:
        cursor = entry.get("__CURSOR")
        if not isinstance(cursor, str) or not cursor:
            LOG.warning("skipping journal entry without a cursor")
            return

        raw = entry.get("MESSAGE")
        try:
            message = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            LOG.warning("skipping journal entry whose MESSAGE is not JSON: %s", exc)
            self._checkpoint(cursor)
            return
        if not isinstance(message, dict):
            LOG.warning("skipping journal entry whose MESSAGE is not a JSON object")
            self._checkpoint(cursor)
            return

        realtime = entry.get("__REALTIME_TIMESTAMP")
        try:
            timestamp_ms = timestamp_milliseconds(realtime)
        except (TypeError, ValueError):
            LOG.warning("skipping journal entry without a valid realtime timestamp")
            self._checkpoint(cursor)
            return

        identifier = event_id(self.config.event_id_key, cursor)
        try:
            publications = self.filters.transform(message, timestamp_ms, identifier)
        except FilterError as exc:
            LOG.warning("skipping journal entry rejected by jq: %s", exc)
            self._checkpoint(cursor)
            return

        for publication in publications:
            strictness = publication.strictness or self.config.mqtt.strictness
            result = self.mqtt.publish(
                publication.topic, publication.payload, retain=False, stop=self.stop
            )
            if result.rejected:
                detail = (
                    f"MQTT broker rejected publication to {publication.topic!r}: {result.reason}"
                )
                if strictness in {"fail", "require-subscriber"}:
                    raise MQTTError(detail)
                if strictness == "warn":
                    LOG.warning("%s; skipping", detail)
            elif (
                result.no_matching_subscribers
                and strictness == "require-subscriber"
            ):
                raise MQTTError(
                    f"MQTT broker reported no matching subscribers for {publication.topic!r}"
                )
        self._checkpoint(cursor)

    def run(self, cursor_override: str | None = None) -> None:
        self.mqtt.connect()
        try:
            if cursor_override is not None:
                cursor = cursor_override
                self.journal.seek_after(cursor)
                self._checkpoint(cursor)
                LOG.info("validated and stored command-line journal cursor")
            else:
                cursor = self.mqtt.load_cursor(self.config.state_topic)
                if cursor is None:
                    raise ServiceError(
                        "no retained checkpoint exists; provide --cursor for the first start"
                    )
                self.journal.seek_after(cursor)
                LOG.info("resuming after retained journal cursor")

            for entry in self.journal.follow(self.stop):
                self._process(entry)
        finally:
            self.mqtt.close()
