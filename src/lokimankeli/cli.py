from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

from .config import ConfigError, load_config
from .filters import FilterError
from .journal import JournalError
from .mqtt import MQTTError
from .service import BridgeService, ServiceError
from .start_position import StartPositionError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lokimankeli", description="Stream JSON journal messages to MQTT"
    )
    parser.add_argument("--config", required=True, help="path to the TOML configuration file")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    stop = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        logging.getLogger(__name__).info("received signal %s; stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    try:
        config = load_config(args.config)
        BridgeService(config, stop).run()
    except (
        ConfigError,
        FilterError,
        JournalError,
        MQTTError,
        ServiceError,
        StartPositionError,
    ) as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1
    except KeyboardInterrupt:
        stop.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())
