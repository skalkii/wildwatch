"""Tests for the live dashboard broadcaster + stats."""

from __future__ import annotations

import asyncio
import json

import pytest

from wildwatch import dashboard as db


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    db.reset_state()
    yield
    db.reset_state()


def test_broadcast_increments_tier_counter() -> None:
    db.broadcast({"tier": 1, "label": "a"})
    db.broadcast({"tier": 3, "label": "b"})
    db.broadcast({"tier": 3, "label": "c"})
    stats = db.get_stats()
    assert stats["tier_counts"] == {1: 1, 3: 2}
    assert stats["total"] == 3


def test_broadcast_keeps_recent_events_bounded() -> None:
    for i in range(150):
        db.broadcast({"tier": 1, "label": f"e{i}"})
    stats = db.get_stats()
    # Bounded at MAX_RECENT_EVENTS
    assert len(stats["recent_events"]) <= db.MAX_RECENT_EVENTS
    assert stats["total"] == 150


def test_recent_events_newest_first() -> None:
    db.broadcast({"tier": 1, "label": "first"})
    db.broadcast({"tier": 1, "label": "second"})
    db.broadcast({"tier": 1, "label": "third"})
    stats = db.get_stats()
    labels = [e["label"] for e in stats["recent_events"]]
    assert labels[0] == "third"
    assert labels[-1] == "first"


@pytest.mark.asyncio
async def test_subscribe_receives_broadcasts() -> None:
    received = []

    async def reader() -> None:
        async for ev in db.subscribe():
            received.append(ev)
            if len(received) >= 2:
                return

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.05)  # let subscriber register
    db.broadcast({"tier": 1, "label": "a"})
    db.broadcast({"tier": 2, "label": "b"})
    await asyncio.wait_for(task, timeout=1.0)
    assert [e["label"] for e in received] == ["a", "b"]


@pytest.mark.asyncio
async def test_multiple_subscribers_each_receive_event() -> None:
    received_1 = []
    received_2 = []

    async def reader(buf: list) -> None:
        async for ev in db.subscribe():
            buf.append(ev)
            return  # take 1 and exit

    t1 = asyncio.create_task(reader(received_1))
    t2 = asyncio.create_task(reader(received_2))
    await asyncio.sleep(0.05)
    db.broadcast({"tier": 3, "label": "fanout"})
    await asyncio.gather(t1, t2)
    assert received_1 == received_2 == [{"tier": 3, "label": "fanout"}]


def test_get_dashboard_html_contains_expected_anchors() -> None:
    html = db.get_dashboard_html()
    assert "<title>" in html
    assert "WildWatch" in html
    assert "tier-counts" in html or "tier_counts" in html
    assert "/events/stream" in html
    assert "/api/stats" in html


def test_stats_serializable_to_json() -> None:
    db.broadcast({"tier": 1, "label": "json_test", "stream_url": "https://x"})
    stats = db.get_stats()
    # Roundtrip through JSON to confirm no non-serializable types
    json.dumps(stats)
