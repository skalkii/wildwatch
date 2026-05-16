"""Tests for the append-only event log (JSONL)."""

from __future__ import annotations

from pathlib import Path

import pytest

from wildwatch import event_log as el_mod
from wildwatch.event_log import append, read_all, read_since


@pytest.fixture
def log_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    p = tmp_path / "live_event_log.jsonl"
    monkeypatch.setattr(el_mod, "LOG_FILE", p)
    return p


def test_append_creates_file_and_one_line_per_call(log_path: Path) -> None:
    append({"event_id": "a", "label": "x", "tier": 1, "received_at": 1000.0})
    append({"event_id": "b", "label": "y", "tier": 3, "received_at": 1010.0})
    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 2


def test_read_all_returns_records_in_order(log_path: Path) -> None:
    append({"event_id": "a", "label": "x", "received_at": 1.0})
    append({"event_id": "b", "label": "y", "received_at": 2.0})
    records = read_all()
    assert [r["event_id"] for r in records] == ["a", "b"]


def test_read_since_filters_by_timestamp(log_path: Path) -> None:
    append({"event_id": "old", "received_at": 100.0})
    append({"event_id": "recent", "received_at": 1000.0})
    # since=500 -> only the recent one
    recent = read_since(min_ts=500.0)
    assert [r["event_id"] for r in recent] == ["recent"]


def test_read_all_empty_when_no_file(log_path: Path) -> None:
    assert read_all() == []


def test_append_atomic_on_concurrent_lines(log_path: Path) -> None:
    # We don't claim full concurrency safety, but each append should
    # write a complete line — partial-line writes would break read_all.
    for i in range(50):
        append({"event_id": str(i), "received_at": float(i)})
    records = read_all()
    assert len(records) == 50
    assert all("event_id" in r for r in records)


def test_corrupt_line_skipped_in_read(log_path: Path, tmp_path: Path) -> None:
    log_path.write_text(
        '{"event_id":"good","received_at":1.0}\n'
        "this is not json at all\n"
        '{"event_id":"alsogood","received_at":2.0}\n'
    )
    records = read_all()
    assert [r["event_id"] for r in records] == ["good", "alsogood"]
