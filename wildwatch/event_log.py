"""Append-only JSONL log of received webhook alerts.

Every alert delivered to the webhook receiver is recorded as one line. The
digest builder (``wildwatch.digest``) reads this file to pick highlights for
the daily reel.

Append is atomic at the OS line level (single write() under the GIL), which
is enough for our single-process hackathon scope. We don't claim multi-
process concurrency.

Corrupt lines (e.g. partial write on crash) are silently skipped on read so
the digest job can still proceed.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LOG_FILE = Path(__file__).resolve().parent.parent / "data" / "live_event_log.jsonl"


def append(record: dict[str, Any]) -> None:
    """Append one record as a single JSON line. Creates parent dir on first call."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, separators=(",", ":")) + "\n"
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def read_all() -> list[dict[str, Any]]:
    """Return every record. Corrupt lines are skipped + counted in a WARNING."""
    if not LOG_FILE.exists():
        return []
    out: list[dict[str, Any]] = []
    n_skipped = 0
    for line in LOG_FILE.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            n_skipped += 1
            continue
    if n_skipped:
        # Visible in production logs so a partial-write crash doesn't silently
        # shrink the digest pool.
        logger.warning(
            "event_log: skipped %s corrupt line(s) in %s; digest pool may be smaller than expected",
            n_skipped,
            LOG_FILE,
        )
    return out


def read_since(min_ts: float) -> list[dict[str, Any]]:
    """Return records whose ``received_at`` is >= ``min_ts`` (Unix seconds)."""
    return [r for r in read_all() if float(r.get("received_at", 0)) >= min_ts]
