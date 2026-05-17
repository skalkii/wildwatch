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
async def test_dispatch_youtube_live_parks_in_needs_bridge(
    state_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live YouTube URLs no longer error — they park in `needs_bridge`
    status with a copy-paste command for the dashboard helper card.
    `coll.upload` MUST NOT be called for a live URL — uploading a YouTube
    live URL into VideoDB's archive endpoint silently bills credits for
    a stream we can't read."""
    monkeypatch.setattr(ingest, "_is_youtube_live", lambda url: True)
    s = add_source(kind="youtube", input="https://www.youtube.com/watch?v=live", name="ytl")
    coll = MagicMock()

    out = await ingest.dispatch(s.id, coll=coll)

    assert out.status == "needs_bridge"
    assert out.bridge_command and "start_bridge.sh" in out.bridge_command
    # bridge_rtsp is intentionally empty — VideoDB rejects rtsp://localhost
    # so we don't pre-fill the input. The bridge script prints the real
    # rtsp://bore.pub:<port>/<slug> public URL which the operator pastes.
    assert out.bridge_rtsp == ""
    coll.upload.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_broadcasts_progress_events(
    state_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    s = add_source(kind="upload", input="/tmp/x.mp4", name="x")
    coll = MagicMock()
    coll.upload = MagicMock(return_value=_make_video_mock("m-x"))

    # source_progress dicts are UI signals (they carry a `type` key) and
    # dashboard.broadcast now correctly filters them out of the alert log
    # and tier counters. To verify ingest STILL emits them for the SSE
    # channel, spy on broadcast directly rather than reading recent_events.
    broadcast_calls: list[dict] = []

    def _spy(event):
        broadcast_calls.append(event)

    monkeypatch.setattr(dashboard, "broadcast", _spy)

    await ingest.dispatch(s.id, coll=coll)

    matching = [
        e
        for e in broadcast_calls
        if e.get("type") == "source_progress" and e.get("source_id") == s.id
    ]
    assert matching, f"no source_progress events broadcast; got: {broadcast_calls}"
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


# ──── rt.status verification (reg-tests for silent-failure-hunter findings) ────


@pytest.mark.asyncio
async def test_dispatch_rtsp_marks_error_when_rtstream_status_is_error(
    state_file: Path,
) -> None:
    """rt.status='error' must surface as source.status='error', not 'ready'."""
    s = add_source(kind="rtsp", input="rtsp://x/y", name="bad-rt")
    bad_rt = _make_rtstream_mock("rts-bad")
    bad_rt.status = "error"
    coll = MagicMock()
    coll.connect_rtstream = MagicMock(return_value=bad_rt)

    out = await ingest.dispatch(s.id, coll=coll)
    assert out.status == "error", f"got status={out.status!r}; rt.status='error' must propagate"
    assert out.error and "error" in out.error.lower()


@pytest.mark.asyncio
async def test_dispatch_rtsp_pending_status_marks_ingesting_not_ready(state_file: Path) -> None:
    """rt.status='pending' (or any non-terminal) must NOT claim 'ready'."""
    s = add_source(kind="rtsp", input="rtsp://x/y", name="p")
    pending = _make_rtstream_mock("rts-pending")
    pending.status = "pending"
    coll = MagicMock()
    coll.connect_rtstream = MagicMock(return_value=pending)

    out = await ingest.dispatch(s.id, coll=coll)
    assert out.status == "ingesting", (
        f"got status={out.status!r}; pending rt.status must be 'ingesting' not 'ready'"
    )
    assert out.rtstream_id == "rts-pending"


@pytest.mark.asyncio
async def test_dispatch_rtsp_connected_status_marks_ready(state_file: Path) -> None:
    s = add_source(kind="rtsp", input="rtsp://x/y", name="ok")
    rt = _make_rtstream_mock("rts-ok")
    rt.status = "connected"
    coll = MagicMock()
    coll.connect_rtstream = MagicMock(return_value=rt)

    out = await ingest.dispatch(s.id, coll=coll)
    assert out.status == "ready"


# ──── yt-dlp probe error surfacing ─────────────────────────────────────────


def test_is_youtube_live_returns_none_on_probe_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probe failure must NOT be silently classified as 'not live'.

    Before fix: subprocess timeout / non-zero exit returned False, so a live
    URL routed to _ingest_url and failed deep inside VideoDB upload with no
    indication the probe was the root cause.
    """
    import shutil
    import subprocess

    monkeypatch.setattr(shutil, "which", lambda _: "/usr/bin/yt-dlp")

    def _bad_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="yt-dlp", timeout=20)

    monkeypatch.setattr(subprocess, "run", _bad_run)
    assert ingest._is_youtube_live("https://www.youtube.com/watch?v=x") is None


def test_is_youtube_live_returns_none_on_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    import shutil
    import subprocess

    monkeypatch.setattr(shutil, "which", lambda _: "/usr/bin/yt-dlp")

    fake_result = MagicMock()
    fake_result.returncode = 1
    fake_result.stdout = ""
    fake_result.stderr = "bad URL"
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: fake_result)
    assert ingest._is_youtube_live("https://www.youtube.com/watch?v=x") is None


def test_is_youtube_live_returns_false_when_yt_dlp_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """yt-dlp missing is the ONE case where False is reasonable — operator
    explicitly chose not to install probe; archive-mode is the safe default."""
    import shutil

    monkeypatch.setattr(shutil, "which", lambda _: None)
    assert ingest._is_youtube_live("https://www.youtube.com/watch?v=x") is False


@pytest.mark.asyncio
async def test_dispatch_youtube_routes_to_upload_when_probe_unknown_with_warning(
    state_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Probe-unknown (None) routes to upload BUT records warning in stage_msg
    so operators can see the source booted on incomplete info."""
    monkeypatch.setattr(ingest, "_is_youtube_live", lambda url: None)
    s = add_source(kind="youtube", input="https://www.youtube.com/watch?v=unk", name="unk")
    coll = MagicMock()
    coll.upload = MagicMock(return_value=_make_video_mock("m-unk"))

    # Spy on broadcast since source_progress is UI-signal, no longer
    # mirrored into recent_events.
    broadcast_calls: list[dict] = []
    monkeypatch.setattr(dashboard, "broadcast", broadcast_calls.append)

    out = await ingest.dispatch(s.id, coll=coll)

    # Routed to upload like archive mode
    coll.upload.assert_called_once()
    assert out.video_id == "m-unk"
    msgs = [
        e.get("stage_msg", "")
        for e in broadcast_calls
        if e.get("type") == "source_progress" and e.get("source_id") == s.id
    ]
    assert any("probe" in m.lower() for m in msgs), (
        f"expected a probe-warning stage_msg; got: {msgs}"
    )
