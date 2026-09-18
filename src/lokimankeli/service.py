from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import Config, RouteConfig
from .filters import FilterError, JQPublishFilter, event_id
from .journal import JournalSource
from .mqtt import MQTTBridge, MQTTError
from .start_position import START_POSITION_FILENAME, StartPosition

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
        filters: dict[str, JQPublishFilter] | None = None,
        state_directory: Path | None = None,
    ):
        self.config = config
        self.stop = stop
        self.routes = {route.unit: route for route in config.routes}
        self.journal = journal or JournalSource(
            config.journal_scope, tuple(self.routes)
        )
        self.mqtt = mqtt or MQTTBridge(config.mqtt)
        self.filters = filters or {
            route.unit: JQPublishFilter(route.publish_filter, config.mqtt.state_topic)
            for route in config.routes
        }
        if state_directory is None:
            configured_state_directory = os.environ.get("STATE_DIRECTORY")
            if configured_state_directory and ":" in configured_state_directory:
                raise ServiceError("STATE_DIRECTORY must contain exactly one directory")
            state_directory = (
                Path(configured_state_directory) if configured_state_directory else None
            )
        self.state_directory = state_directory

    def _checkpoint(self, cursor: str) -> None:
        self.mqtt.save_cursor(self.config.mqtt.state_topic, cursor, self.stop)

    def _process(self, entry: dict[str, Any]) -> None:
        cursor = entry.get("__CURSOR")
        if not isinstance(cursor, str) or not cursor:
            LOG.warning("skipping journal entry without a cursor")
            return

        unit = self.journal.entry_unit(entry)
        route: RouteConfig | None = self.routes.get(unit) if unit is not None else None
        if route is None:
            raise ServiceError(f"journal entry has no configured route for unit {unit!r}")

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
            publications = self.filters[route.unit].transform(
                message, timestamp_ms, identifier
            )
        except FilterError as exc:
            if route.filter_strictness == "fail":
                raise
            if route.filter_strictness == "warn":
                LOG.warning(
                    "skipping journal entry from %s rejected by jq: %s", unit, exc
                )
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
                if strictness in {"fail", "require-sub"}:
                    raise MQTTError(detail)
                if strictness == "warn":
                    LOG.warning("%s; skipping", detail)
            elif (
                result.no_matching_subscribers
                and strictness == "require-sub"
            ):
                raise MQTTError(
                    f"MQTT broker reported no matching subscribers for {publication.topic!r}"
                )
        self._checkpoint(cursor)

    def run(self) -> None:
        request = StartPosition.load(self.state_directory)
        if request is not None:
            cursor = request.value
            if cursor == "now":
                cursor = self.journal.latest_cursor()
                request = request.replace(cursor)
            self.journal.seek_after(cursor)

        self.mqtt.connect()
        try:
            if request is not None:
                self._checkpoint(cursor)
                request.consume()
                LOG.info("stored and consumed one-shot journal start position")
            else:
                cursor = self.mqtt.load_cursor(self.config.mqtt.state_topic)
                if cursor is None:
                    location = (
                        self.state_directory / START_POSITION_FILENAME
                        if self.state_directory is not None
                        else "a StateDirectory start-position file"
                    )
                    raise ServiceError(
                        f"no retained checkpoint exists; create {location} and restart"
                    )
                self.journal.seek_after(cursor)
                LOG.info("resuming after retained journal cursor")

            for entry in self.journal.follow(self.stop):
                self._process(entry)
        finally:
            self.mqtt.close()
