from __future__ import annotations

import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path


class StartPositionError(ValueError):
    pass


MAX_START_POSITION_BYTES = 65536
START_POSITION_FILENAME = "start-position"


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class StartPosition:
    path: Path
    value: str

    @classmethod
    def load(cls, state_directory: Path | None) -> StartPosition | None:
        if state_directory is None:
            return None
        path = state_directory / START_POSITION_FILENAME
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise StartPositionError(f"cannot open start position {path}: {exc}") from exc

        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise StartPositionError(f"start position is not a regular file: {path}")
            if metadata.st_size > MAX_START_POSITION_BYTES:
                raise StartPositionError(f"start position exceeds {MAX_START_POSITION_BYTES} bytes")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                contents = handle.read(MAX_START_POSITION_BYTES + 1)
        except OSError as exc:
            raise StartPositionError(f"cannot read start position {path}: {exc}") from exc
        finally:
            os.close(descriptor)

        if len(contents) > MAX_START_POSITION_BYTES:
            raise StartPositionError(f"start position exceeds {MAX_START_POSITION_BYTES} bytes")
        try:
            value = contents.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise StartPositionError("start position is not valid UTF-8") from exc
        value = value.removesuffix("\n")
        if not value or "\n" in value or "\r" in value or "\x00" in value:
            raise StartPositionError(
                "start position must contain exactly one non-empty line"
            )
        return cls(path, value)

    def replace(self, value: str) -> StartPosition:
        temporary_path: str | None = None
        try:
            descriptor, temporary_path = tempfile.mkstemp(
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
            )
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(value.encode("utf-8") + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, self.path)
            temporary_path = None
            _sync_directory(self.path.parent)
        except OSError as exc:
            raise StartPositionError(
                f"cannot persist resolved start position {self.path}: {exc}"
            ) from exc
        finally:
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass
        return StartPosition(self.path, value)

    def consume(self) -> None:
        try:
            self.path.unlink()
            _sync_directory(self.path.parent)
        except OSError as exc:
            raise StartPositionError(
                f"cannot consume start position {self.path}: {exc}"
            ) from exc
