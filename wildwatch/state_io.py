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
        # Open with `os.open(..., O_CREAT | O_WRONLY | O_TRUNC, 0o600)`
        # so the file is created with 0o600 permissions ATOMICALLY at
        # creation. The previous `open(...)` + `chmod` sequence had a
        # window (between open and chmod) where the file existed with
        # the process umask permissions (usually 0o644 — world-readable).
        # On containers with umask=0o000 this window let any local user
        # read .state.json before the chmod corrected it.
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        fd = os.open(str(tmp), flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError as e:
                    logger.warning("atomic_write_json: fsync failed on %s: %r", tmp, e)
        except BaseException:
            # If fdopen / write raised after the fd was opened, the fd
            # was given to fdopen which closes it. If os.open succeeded
            # but fdopen never ran, close fd here.
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        # Belt-and-braces: re-chmod in case the underlying filesystem
        # ignored the mode argument (some network filesystems do).
        try:
            os.chmod(tmp, 0o600)
        except OSError as e:
            logger.warning("atomic_write_json: chmod 0600 failed on %s: %r", tmp, e)
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
        except Exception as cleanup_exc:
            # Cleanup failure must not mask the original error, but log
            # at DEBUG so a stale .state.json.tmp left on disk has SOME
            # trace in the log rather than being totally invisible.
            logger.debug(
                "atomic_write_json: tmp cleanup failed (orig error still raised): %r",
                cleanup_exc,
            )
        raise
