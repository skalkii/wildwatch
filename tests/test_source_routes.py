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
    r = client.post(
        "/api/sources",
        json={"kind": "upload", "input": "/tmp/x.mp4", "name": "x"},
    )
    assert r.status_code == 400
    assert "multipart" in r.json()["detail"].lower()


def test_create_invalid_kind_returns_400(
    client: TestClient, state_file: Path, mock_coll: MagicMock
) -> None:
    r = client.post(
        "/api/sources",
        json={"kind": "bogus", "input": "x", "name": "x"},
    )
    assert r.status_code == 400


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
    mock_coll.get_rtstream.assert_called_once_with("rts-fake")
    mock_coll.delete_video.assert_called_once_with("m-z-fake")


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
    fake_bytes = b"\x00" * (100 * 1024)  # 100 KB
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
