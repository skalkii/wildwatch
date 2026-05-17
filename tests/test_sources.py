"""Tests for wildwatch.sources — Source dataclass + state CRUD."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from wildwatch import sources as src_mod
from wildwatch.sources import (
    KIND_VALUES,
    STATUS_VALUES,
    Source,
    add_source,
    delete_source,
    get_source,
    list_sources,
    update_source,
)


@pytest.fixture
def state_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    p = tmp_path / ".state.json"
    monkeypatch.setattr(src_mod, "STATE_FILE", p)
    return p


def test_add_source_returns_source_with_uuid(state_file: Path) -> None:
    s = add_source(kind="upload", input="/tmp/foo.mp4", name="foo")
    assert isinstance(s, Source)
    assert s.id
    assert s.kind == "upload"
    assert s.status == "queued"
    assert s.created_at <= s.updated_at


def test_add_source_persists_to_state_json(state_file: Path) -> None:
    s = add_source(kind="rtsp", input="rtsp://x/y", name="r")
    state = json.loads(state_file.read_text())
    assert "sources" in state
    assert s.id in state["sources"]
    assert state["sources"][s.id]["kind"] == "rtsp"


def test_add_source_rejects_invalid_kind(state_file: Path) -> None:
    with pytest.raises(ValueError, match="kind"):
        add_source(kind="bogus", input="x", name="n")


def test_get_source_returns_existing(state_file: Path) -> None:
    s = add_source(kind="hls", input="https://x/y.m3u8", name="h")
    fetched = get_source(s.id)
    assert fetched is not None
    assert fetched.id == s.id


def test_get_source_returns_none_for_missing(state_file: Path) -> None:
    assert get_source("doesnotexist") is None


def test_update_source_changes_fields_and_updated_at(state_file: Path) -> None:
    s = add_source(kind="upload", input="x", name="n")
    orig_updated = s.updated_at
    out = update_source(s.id, status="ingesting", progress_pct=42, stage_msg="reading file")
    assert out.status == "ingesting"
    assert out.progress_pct == 42
    assert out.stage_msg == "reading file"
    assert out.updated_at >= orig_updated


def test_update_source_rejects_invalid_status(state_file: Path) -> None:
    s = add_source(kind="upload", input="x", name="n")
    with pytest.raises(ValueError, match="status"):
        update_source(s.id, status="not_a_status")


def test_update_source_missing_id_raises(state_file: Path) -> None:
    with pytest.raises(KeyError):
        update_source("missing", status="ready")


def test_delete_source_removes_entry(state_file: Path) -> None:
    s = add_source(kind="rtmp", input="rtmp://x/y", name="rm")
    delete_source(s.id)
    assert get_source(s.id) is None
    state = json.loads(state_file.read_text())
    assert s.id not in state.get("sources", {})


def test_list_sources_returns_all(state_file: Path) -> None:
    a = add_source(kind="upload", input="a", name="A")
    b = add_source(kind="rtsp", input="b", name="B")
    c = add_source(kind="hls", input="c", name="C")
    out = list_sources()
    ids = {s.id for s in out}
    assert ids == {a.id, b.id, c.id}


def test_list_sources_empty(state_file: Path) -> None:
    assert list_sources() == []


def test_list_sources_with_no_sources_key_works(state_file: Path) -> None:
    """Migration safety: existing .state.json without 'sources' key must load cleanly."""
    state_file.write_text(json.dumps({"webhook_base_url": "https://x"}))
    assert list_sources() == []
    # And subsequent add still works
    s = add_source(kind="upload", input="x", name="n")
    assert get_source(s.id) is not None


def test_kind_values_complete() -> None:
    assert KIND_VALUES == ("upload", "youtube", "hls", "rtsp", "rtmp")


def test_status_values_complete() -> None:
    assert STATUS_VALUES == (
        "queued",
        "connecting",
        "ingesting",
        "indexing",
        "ready",
        "error",
        "disconnected",
        "needs_bridge",
    )


def test_source_to_dict_round_trip(state_file: Path) -> None:
    s = add_source(kind="upload", input="x", name="n")
    s = update_source(s.id, video_id="m-z-abc", indexes={"species": "idx1"})
    fetched = get_source(s.id)
    assert fetched is not None
    assert fetched.video_id == "m-z-abc"
    assert fetched.indexes == {"species": "idx1"}
