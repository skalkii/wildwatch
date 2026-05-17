"""Append-only JSONL log of received webhook alerts.

Every alert delivered to the webhook receiver is recorded as one line. The
digest builder (``wildwatch.digest``) reads this file to pick highlights for
the daily reel.

Append is atomic at the OS line level (single write() under the GIL), which
is enough for our single-process hackathon scope. We don't claim multi-
process concurrency.

Corrupt lines (e.g. partial write on crash) are silently skipped on read so
the digest job can still proceed.

Reads stream the file line-by-line — a 24/7 deployment produces an
unbounded log file, and the previous ``read_text().splitlines()``
pattern would materialise the entire file into memory. The streaming
iterator below caps memory usage at one parsed record.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Allow tests + Docker volume mounts to override the log path without
# monkeypatching the module attribute. Falls back to the in-repo default.
LOG_FILE = Path(
    os.getenv(
        "WILDWATCH_LOG_FILE",
        str(Path(__file__).resolve().parent.parent / "data" / "live_event_log.jsonl"),
    )
)


def append(record: dict[str, Any]) -> None:
    """Append one record as a single JSON line. Creates parent dir on first call."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, separators=(",", ":")) + "\n"
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def _iter_records() -> Iterator[dict[str, Any]]:
    """Yield one parsed JSON record per line.

    Streams the log file line-by-line so multi-MB logs don't load into
    memory all at once. Corrupt lines are skipped + counted; the
    aggregate is logged once when the iterator drains.
    """
    if not LOG_FILE.exists():
        return
    n_skipped = 0
    with LOG_FILE.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                n_skipped += 1
                continue
    if n_skipped:
        # Visible in production logs so a partial-write crash doesn't
        # silently shrink the digest pool.
        logger.warning(
            "event_log: skipped %s corrupt line(s) in %s; digest pool may be smaller than expected",
            n_skipped,
            LOG_FILE,
        )


def read_all() -> list[dict[str, Any]]:
    """Return every record. Corrupt lines skipped + counted in a WARNING.

    Backed by the streaming iterator so memory peaks at one parsed
    record plus the result list rather than the entire file.
    """
    return list(_iter_records())


def read_since(min_ts: float) -> list[dict[str, Any]]:
    """Return records whose ``received_at`` is >= ``min_ts`` (Unix seconds).

    Records with a missing, non-numeric, or wrong-type ``received_at``
    are skipped (with a single aggregated WARNING) so one corrupt entry
    can't take down the digest pipeline. Streams via ``_iter_records``
    so the file doesn't materialise twice in memory.
    """
    out: list[dict[str, Any]] = []
    n_skipped = 0
    for r in _iter_records():
        raw = r.get("received_at")
        if raw is None:
            n_skipped += 1
            continue
        try:
            ts = float(raw)
        except (TypeError, ValueError):
            n_skipped += 1
            continue
        if ts >= min_ts:
            out.append(r)
    if n_skipped:
        logger.warning(
            "event_log.read_since: skipped %s record(s) with bad received_at",
            n_skipped,
        )
    return out
