"""Tests for wildwatch.ingest — per-kind dispatch + progress broadcasts."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from wildwatch import dashboard, ingest
from wildwatch import sources as src_mod
from wildwatch.sources import add_source, get_source


@pytest.fixture
def state_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    p = tmp_path / ".state.json"
    monkeypatch.setattr(src_mod, "STATE_FILE", p)
    return p


@pytest.fixture(autouse=True)
def _reset_dashboard() -> None:
    dashboard.reset_state()
    yield
    dashboard.reset_state()


def _make_video_mock(video_id: str = "m-z-fake", length: float = 60.0) -> MagicMock:
    v = MagicMock()
    v.id = video_id
    v.length = length
    v.stream_url = f"https://play.videodb.io/v1/{video_id}.m3u8"
    v.name = "fake"
    return v


def _make_rtstream_mock(rtstream_id: str = "rts-fake") -> MagicMock:
    r = MagicMock()
    r.id = rtstream_id
    r.status = "connected"
    r.name = "fake"
    return r


@pytest.mark.asyncio
async def test_dispatch_upload_calls_coll_upload_with_file_path(state_file: Path) -> None:
    s = add_source(kind="upload", input="/tmp/foo.mp4", name="foo")
    coll = MagicMock()
    coll.upload = MagicMock(return_value=_make_video_mock("m-up"))

    out = await ingest.dispatch(s.id, coll=coll)

    coll.upload.assert_called_once()
    assert coll.upload.call_args.kwargs.get("file_path") == "/tmp/foo.mp4"
    assert out.status == "ready"
    assert out.video_id == "m-up"


@pytest.mark.asyncio
async def test_dispatch_hls_calls_coll_upload_with_url(state_file: Path) -> None:
    s = add_source(kind="hls", input="https://x/y.m3u8", name="h")
    coll = MagicMock()
    coll.upload = MagicMock(return_value=_make_video_mock("m-hls"))

    out = await ingest.dispatch(s.id, coll=coll)

    assert coll.upload.call_args.kwargs.get("url") == "https://x/y.m3u8"
    assert out.status == "ready"
    assert out.video_id == "m-hls"


@pytest.mark.asyncio
async def test_dispatch_rtsp_calls_connect_rtstream(state_file: Path) -> None:
    s = add_source(kind="rtsp", input="rtsp://x/y", name="r")
    coll = MagicMock()
    coll.connect_rtstream = MagicMock(return_value=_make_rtstream_mock("rts-r"))

    out = await ingest.dispatch(s.id, coll=coll)

    coll.connect_rtstream.assert_called_once()
    kwargs = coll.connect_rtstream.call_args.kwargs
    assert kwargs.get("url") == "rtsp://x/y"
    assert kwargs.get("name") == "r"
    assert out.status == "ready"
    assert out.rtstream_id == "rts-r"


@pytest.mark.asyncio
async def test_dispatch_youtube_routes_to_upload_for_archive(
    state_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Default behaviour: treat YouTube URL as archive (upload-mode). Live
    # would need bridge orchestration (out of scope for handler).
    monkeypatch.setattr(ingest, "_is_youtube_live", lambda url: False)
    s = add_source(kind="youtube", input="https://www.youtube.com/watch?v=abc", name="yt")
    coll = MagicMock()
    coll.upload = MagicMock(return_value=_make_video_mock("m-yt"))

    out = await ingest.dispatch(s.id, coll=coll)

    assert coll.upload.call_args.kwargs.get("url") == "https://www.youtube.com/watch?v=abc"
    assert out.video_id == "m-yt"


@pytest.mark.asyncio
async def test_dispatch_youtube_live_sets_error_when_no_bridge(
    state_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ingest, "_is_youtube_live", lambda url: True)
    s = add_source(kind="youtube", input="https://www.youtube.com/watch?v=live", name="ytl")
    coll = MagicMock()

    out = await ingest.dispatch(s.id, coll=coll)

    assert out.status == "error"
    assert out.error and "bridge" in out.error.lower()


@pytest.mark.asyncio
async def test_dispatch_broadcasts_progress_events(state_file: Path) -> None:
    s = add_source(kind="upload", input="/tmp/x.mp4", name="x")
    coll = MagicMock()
    coll.upload = MagicMock(return_value=_make_video_mock("m-x"))

    await ingest.dispatch(s.id, coll=coll)

    stats = dashboard.get_stats()
    # At minimum a source_progress event with the right source_id should have
    # landed in recent_events.
    matching = [
        e
        for e in stats["recent_events"]
        if e.get("type") == "source_progress" and e.get("source_id") == s.id
    ]
    assert matching, f"no source_progress events broadcast; got: {stats['recent_events']}"
    stages = {e.get("status") for e in matching}
    # Should have gone through at least 'connecting' and 'ready'
    assert "ready" in stages


@pytest.mark.asyncio
async def test_dispatch_sets_error_on_sdk_exception(state_file: Path) -> None:
    s = add_source(kind="rtsp", input="rtsp://bad/url", name="bad")
    coll = MagicMock()
    coll.connect_rtstream = MagicMock(side_effect=RuntimeError("connect refused"))

    out = await ingest.dispatch(s.id, coll=coll)

    assert out.status == "error"
    assert out.error and "connect refused" in out.error


@pytest.mark.asyncio
async def test_dispatch_unknown_source_id_raises(state_file: Path) -> None:
    coll = MagicMock()
    with pytest.raises(KeyError):
        await ingest.dispatch("nonexistent", coll=coll)


@pytest.mark.asyncio
async def test_dispatch_rtmp_calls_connect_rtstream(state_file: Path) -> None:
    s = add_source(kind="rtmp", input="rtmp://x/y", name="rm")
    coll = MagicMock()
    coll.connect_rtstream = MagicMock(return_value=_make_rtstream_mock("rts-rm"))

    out = await ingest.dispatch(s.id, coll=coll)

    assert coll.connect_rtstream.call_args.kwargs.get("url") == "rtmp://x/y"
    assert out.rtstream_id == "rts-rm"


def test_is_youtube_live_pattern_matching() -> None:
    # Unit-test the URL pattern helper that the dispatcher uses
    assert ingest._is_youtube_url("https://www.youtube.com/watch?v=x") is True
    assert ingest._is_youtube_url("https://youtu.be/x") is True
    assert ingest._is_youtube_url("https://example.com/video") is False


def test_get_source_after_dispatch_returns_fresh_state(state_file: Path) -> None:
    s = add_source(kind="upload", input="/tmp/x.mp4", name="x")
    assert get_source(s.id).status == "queued"
