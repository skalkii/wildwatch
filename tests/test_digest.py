"""Tests for the digest builder pure logic (Timeline is mocked)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from wildwatch.digest import (
    DEFAULT_CLIP_SECONDS,
    TIER_SLUG_PREFERENCE,
    pick_corpus_video_id,
    pick_top_events,
)


def _ev(tier: int, ts: float, event_id: str = "x") -> dict:
    return {"tier": tier, "received_at": ts, "event_id": event_id}


def test_pick_top_events_sorts_by_tier_desc_then_recency() -> None:
    events = [
        _ev(1, 100.0, "a"),
        _ev(3, 50.0, "b"),
        _ev(2, 200.0, "c"),
        _ev(3, 150.0, "d"),
        _ev(1, 300.0, "e"),
    ]
    picked = pick_top_events(events, top_n=3)
    # tier 3 newest first, then tier 2, then tier 1 newest
    assert [e["event_id"] for e in picked] == ["d", "b", "c"]


def test_pick_top_events_respects_top_n() -> None:
    events = [_ev(2, float(i), str(i)) for i in range(20)]
    assert len(pick_top_events(events, top_n=5)) == 5


def test_pick_top_events_empty() -> None:
    assert pick_top_events([], top_n=5) == []


def test_pick_corpus_video_id_uses_tier_preference_order() -> None:
    corpus = {
        "namibia_live_segment": {"video_id": "v1"},
        "hwange_live_segment": {"video_id": "v2"},
        "poaching_synth": {"video_id": "v3"},
    }
    # tier 1 prefers namibia first
    assert pick_corpus_video_id(1, corpus) == "v1"
    # tier 2 prefers hwange first
    assert pick_corpus_video_id(2, corpus) == "v2"
    # tier 3 prefers poaching first
    assert pick_corpus_video_id(3, corpus) == "v3"


def test_pick_corpus_video_id_falls_back_to_any_clip() -> None:
    corpus = {"only_synth": {"video_id": "vX"}}
    # tier 1's preferences don't match, but fallback should grab the only clip
    assert pick_corpus_video_id(1, corpus) == "vX"


def test_pick_corpus_video_id_returns_none_when_empty() -> None:
    assert pick_corpus_video_id(1, {}) is None


def test_tier_preference_table_covers_1_2_3() -> None:
    assert set(TIER_SLUG_PREFERENCE.keys()) >= {1, 2, 3}
    for slugs in TIER_SLUG_PREFERENCE.values():
        assert isinstance(slugs, list)
        assert len(slugs) >= 1


def test_default_clip_seconds_reasonable() -> None:
    assert 1 <= DEFAULT_CLIP_SECONDS <= 30


def test_build_digest_returns_none_when_no_corpus(monkeypatch: pytest.MonkeyPatch) -> None:
    from wildwatch import digest

    # No corpus -> n_clips=0, no stream
    fake_conn = MagicMock()
    monkeypatch.setattr(
        digest.event_log,
        "read_since",
        lambda min_ts: [],
    )
    result = digest.build_digest(fake_conn, state={"corpus": {}}, since_hours=24)
    assert result["n_clips"] == 0
    assert result["stream_url"] is None
