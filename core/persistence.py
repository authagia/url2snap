from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


class PersistenceError(RuntimeError):
    """Raised when a persisted JSON document cannot be loaded safely."""


async def atomic_write_json(path: str | Path, payload: Any) -> None:
    """Atomically replace a JSON file.

    The temporary file is created in the destination directory so os.replace()
    remains an atomic same-filesystem operation. Data is fsynced before the
    replacement to reduce the chance of a successful return with a truncated
    file after a power/process failure.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    def _write() -> None:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            temporary = Path(tmp.name)
            try:
                tmp.write(text)
                tmp.flush()
                os.fsync(tmp.fileno())
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
        try:
            os.replace(temporary, destination)
            # Persist the directory entry when the platform supports it.
            try:
                dir_fd = os.open(destination.parent, os.O_RDONLY)
            except OSError:
                dir_fd = None
            if dir_fd is not None:
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise

    await asyncio.to_thread(_write)


async def read_json(path: str | Path) -> Any | None:
    document = Path(path)
    if not document.exists():
        return None
    try:
        text = await asyncio.to_thread(document.read_text, encoding="utf-8")
        if not text.strip():
            return None
        return json.loads(text)
    except (OSError, json.JSONDecodeError) as exc:
        raise PersistenceError(f"failed to read JSON persistence file: {document}") from exc


def unwrap_versioned(raw: Any, *, key: str, path: str | Path) -> list[dict]:
    """Read the current versioned envelope, while accepting legacy list files."""
    if raw is None:
        return []
    if isinstance(raw, list):
        # Backward compatibility with the pre-Phase-15 array format.
        return raw
    if not isinstance(raw, dict):
        raise PersistenceError(f"invalid JSON persistence shape: {path}")
    version = raw.get("version")
    if version != 1:
        raise PersistenceError(f"unsupported persistence version {version!r}: {path}")
    value = raw.get(key)
    if not isinstance(value, list):
        raise PersistenceError(f"invalid persistence field {key!r}: {path}")
    return value
