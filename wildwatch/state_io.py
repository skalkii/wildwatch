"""Durable atomic JSON write helper.

``.tmp + rename`` alone is not crash-safe: ``tmp.write_text`` returns once
the Python buffer is flushed to the OS, but the OS may not have actually
written the bytes to the disk platter yet. A power loss after ``replace``
but before the dirty pages flush leaves the new file present but empty (or
truncated). The atomic rename guarantee assumes both files are durable.

This helper:
  1. Writes data to ``path.tmp``
  2. fsync()s the file handle so the bytes hit the disk
  3. Replaces ``path`` with ``path.tmp`` (atomic on POSIX)
  4. fsync()s the parent directory so the new dirent is durable

Used by every hot-path state writer (sources, sandbox, bootstrap,
start_live_test). Smoke scripts left on the simpler pattern.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def atomic_write_json(path: Path, data: Any, *, indent: int | None = 2) -> None:
    """Durably write ``data`` as JSON to ``path``.

    On any failure the temp file is cleaned up and the original
    exception is re-raised (with the cleanup failure suppressed so it
    can't mask the root cause).
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        payload = json.dumps(data, indent=indent)
        # Open + write + fsync the file handle. Using a raw open so we
        # can call fsync; write_text doesn't expose the descriptor.
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError as e:
                # fsync can fail on some filesystems (e.g. some FUSE mounts
                # in CI). Log but proceed — the rename below is still
                # crash-atomic at the kernel level.
                logger.warning("atomic_write_json: fsync failed on %s: %r", tmp, e)
        tmp.replace(path)
        # fsync the parent dir so the new dirent survives a crash.
        try:
            dir_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as e:
            logger.warning(
                "atomic_write_json: parent-dir fsync failed on %s: %r",
                path.parent,
                e,
            )
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass  # cleanup failure must not mask original error
        raise
