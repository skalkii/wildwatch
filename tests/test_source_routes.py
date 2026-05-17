"""Route-level tests for /api/sources (FastAPI TestClient)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from wildwatch import sources as src_mod
from wildwatch import webhooks as wh_mod
from wildwatch.webhooks import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def state_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    p = tmp_path / ".state.json"
    monkeypatch.setattr(src_mod, "STATE_FILE", p)
    return p


@pytest.fixture
def mock_coll(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace _get_coll with a MagicMock collection. Skip async dispatch."""
    coll = MagicMock()
    monkeypatch.setattr(wh_mod, "_get_coll", lambda: coll)
    # Stub dispatch so background task no-ops (we test it elsewhere)
    monkeypatch.setattr(wh_mod.ingest, "dispatch", AsyncMock(return_value=None))
    return coll


def test_list_sources_empty(client: TestClient, state_file: Path) -> None:
    r = client.get("/api/sources")
    assert r.status_code == 200
    assert r.json() == {"sources": []}


def test_create_rtsp_source_returns_201_shape(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    r = client.post(
        "/api/sources",
        json={"kind": "rtsp", "input": "rtsp://x/y", "name": "test-cam"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["kind"] == "rtsp"
    assert body["name"] == "test-cam"
    assert body["input"] == "rtsp://x/y"
    assert body["status"] == "queued"
    assert "id" in body


def test_create_upload_via_json_rejected_with_helpful_message(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    # SourceCreate.kind is now Literal["youtube","hls","rtsp","rtmp"] —
    # "upload" is rejected at the Pydantic layer with 422 (not the old 400
    # handler-side reject). Cleaner rejection, just earlier.
    r = client.post(
        "/api/sources",
        json={"kind": "upload", "input": "/tmp/x.mp4", "name": "x"},
    )
    assert r.status_code == 422


def test_create_invalid_kind_returns_400(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    # Same Pydantic-layer rejection for arbitrary unknown kinds.
    r = client.post(
        "/api/sources",
        json={"kind": "bogus", "input": "x", "name": "x"},
    )
    assert r.status_code == 422


def test_get_source_404_when_missing(client: TestClient, state_file: Path) -> None:
    r = client.get("/api/sources/nonexistent")
    assert r.status_code == 404


def test_get_source_returns_full_record(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    create_r = client.post(
        "/api/sources",
        json={"kind": "hls", "input": "https://x/y.m3u8", "name": "h"},
    )
    sid = create_r.json()["id"]
    r = client.get(f"/api/sources/{sid}")
    assert r.status_code == 200
    assert r.json()["id"] == sid
    assert r.json()["kind"] == "hls"


def test_delete_source_local_only_when_no_remote_ids(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    create_r = client.post(
        "/api/sources",
        json={"kind": "rtsp", "input": "rtsp://x", "name": "rm"},
    )
    sid = create_r.json()["id"]
    r = client.delete(f"/api/sources/{sid}")
    assert r.status_code == 200
    assert r.json()["status"] == "deleted"
    assert client.get(f"/api/sources/{sid}").status_code == 404


def test_delete_source_calls_remote_cleanup_when_video_id_set(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    create_r = client.post(
        "/api/sources",
        json={"kind": "rtsp", "input": "rtsp://x", "name": "rm"},
    )
    sid = create_r.json()["id"]
    # Simulate ingest having attached a video_id + rtstream_id
    src_mod.update_source(sid, video_id="m-z-fake", rtstream_id="rts-fake")
    mock_coll.get_rtstream = MagicMock(return_value=MagicMock(stop=MagicMock()))
    mock_coll.delete_video = MagicMock()

    r = client.delete(f"/api/sources/{sid}")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "deleted"
    assert body.get("warnings", []) == []
    mock_coll.get_rtstream.assert_called_once_with("rts-fake")
    mock_coll.delete_video.assert_called_once_with("m-z-fake")


def test_delete_source_surfaces_warning_when_rt_stop_fails(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    """Regression: api_delete_source used to return {'status':'deleted'} even
    when rt.stop() raised, so operators had no signal that the remote
    rtstream may still be running and burning credits."""
    create_r = client.post(
        "/api/sources",
        json={"kind": "rtsp", "input": "rtsp://x", "name": "leaky"},
    )
    sid = create_r.json()["id"]
    src_mod.update_source(sid, rtstream_id="rts-leaky")

    bad_rt = MagicMock()
    bad_rt.stop = MagicMock(side_effect=RuntimeError("network unreachable"))
    mock_coll.get_rtstream = MagicMock(return_value=bad_rt)

    r = client.delete(f"/api/sources/{sid}")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "deleted_with_warnings"
    warnings = body["warnings"]
    assert any("rt.stop" in w.lower() and "network unreachable" in w for w in warnings)


def test_delete_source_surfaces_warning_when_delete_video_fails(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    create_r = client.post(
        "/api/sources",
        json={"kind": "hls", "input": "https://x/y.m3u8", "name": "v"},
    )
    sid = create_r.json()["id"]
    src_mod.update_source(sid, video_id="m-leaky")

    mock_coll.delete_video = MagicMock(side_effect=RuntimeError("permission denied"))

    r = client.delete(f"/api/sources/{sid}")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "deleted_with_warnings"
    assert any("delete_video" in w.lower() and "permission denied" in w for w in body["warnings"])


def test_disconnect_source_noop_when_no_rtstream(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    create_r = client.post(
        "/api/sources",
        json={"kind": "hls", "input": "https://x/y.m3u8", "name": "h"},
    )
    sid = create_r.json()["id"]
    r = client.post(f"/api/sources/{sid}/disconnect")
    assert r.status_code == 200
    assert r.json()["status"] == "noop"


def test_reconnect_source_resets_status_to_queued(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    create_r = client.post(
        "/api/sources",
        json={"kind": "rtsp", "input": "rtsp://x", "name": "r"},
    )
    sid = create_r.json()["id"]
    src_mod.update_source(sid, status="error", error="prev failure")
    r = client.post(f"/api/sources/{sid}/reconnect")
    assert r.status_code == 200
    assert r.json()["status"] == "queued"


def test_upload_multipart_creates_source_and_dispatches(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    # Upload endpoint now magic-byte sniffs the first 32 bytes and rejects
    # non-video uploads as a security measure. Use a minimal real MP4
    # `ftyp` header at offset 4 so the sniff accepts.
    mp4_header = b"\x00\x00\x00\x20ftypmp42\x00\x00\x00\x00mp42isom"
    fake_bytes = mp4_header + b"\x00" * (100 * 1024 - len(mp4_header))
    r = client.post(
        "/api/sources/upload",
        files={"file": ("test.mp4", fake_bytes, "video/mp4")},
        data={"name": "smoke-upload"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["kind"] == "upload"
    assert body["name"] == "smoke-upload"
    # input is the tempfile path after streaming
    assert body["input"].startswith("/")
    assert "wildwatch-upload-" in body["input"]
