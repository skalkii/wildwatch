"""Tests for /api/videos /api/rtstreams /api/search routes."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from wildwatch import webhooks as wh_mod
from wildwatch.webhooks import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    # Bust the videos cache between tests
    wh_mod._videos_cache["at"] = 0.0
    wh_mod._videos_cache["data"] = {"videos": []}


@pytest.fixture
def mock_coll(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    coll = MagicMock()
    monkeypatch.setattr(wh_mod, "_get_coll", lambda: coll)
    return coll


def _video_mock(vid_id: str, name: str = "v") -> MagicMock:
    v = MagicMock()
    v.id = vid_id
    v.name = name
    v.length = 60.0
    v.stream_url = f"https://play.videodb.io/v1/{vid_id}.m3u8"
    v.thumbnail_url = None
    return v


def _shot_mock(text: str, start: float = 0, end: float = 5) -> MagicMock:
    s = MagicMock()
    s.start = start
    s.end = end
    s.text = text
    s.search_score = 0.9
    s.scene_index_id = "idx"
    s.scene_index_name = "smoke"
    return s


def test_list_videos_returns_summary(client: TestClient, mock_coll: MagicMock) -> None:
    mock_coll.get_videos = MagicMock(return_value=[_video_mock("a"), _video_mock("b")])
    r = client.get("/api/videos")
    assert r.status_code == 200
    body = r.json()
    assert len(body["videos"]) == 2
    assert body["videos"][0]["id"] == "a"


def test_list_videos_handles_sdk_error_gracefully(client: TestClient, mock_coll: MagicMock) -> None:
    mock_coll.get_videos = MagicMock(side_effect=RuntimeError("api down"))
    r = client.get("/api/videos")
    # 502 (not 200): the dashboard now surfaces a degraded state as a
    # visible error rather than an empty list that looks identical to
    # "no videos uploaded yet." Body still carries the empty list + error
    # for diagnostic display.
    assert r.status_code == 502
    body = r.json()
    assert body["videos"] == []
    assert "error" in body


def test_video_indexes_returns_list(client: TestClient, mock_coll: MagicMock) -> None:
    v = _video_mock("vid1")
    v.list_scene_index = MagicMock(return_value=[{"scene_index_id": "i1", "name": "species"}])
    mock_coll.get_video = MagicMock(return_value=v)
    r = client.get("/api/videos/vid1/indexes")
    assert r.status_code == 200
    assert r.json()["indexes"][0]["scene_index_id"] == "i1"


def test_video_scenes_returns_capped_records(client: TestClient, mock_coll: MagicMock) -> None:
    v = _video_mock("vid1")
    # api_video_scenes lists indexes FIRST and only fetches scenes when
    # the requested index has status=ready — otherwise it returns the
    # status so the dashboard can render "still processing" rather than
    # blocking the SDK on get_scene_index.
    v.list_scene_index = MagicMock(
        return_value=[{"scene_index_id": "i1", "name": "test", "status": "ready"}]
    )
    v.get_scene_index = MagicMock(return_value=[{"description": f"s{i}"} for i in range(50)])
    mock_coll.get_video = MagicMock(return_value=v)
    r = client.get("/api/videos/vid1/scenes/i1?limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ready"
    assert len(body["scenes"]) == 5


def test_video_scenes_skips_get_when_processing(client: TestClient, mock_coll: MagicMock) -> None:
    v = _video_mock("vid1")
    v.list_scene_index = MagicMock(
        return_value=[{"scene_index_id": "i1", "name": "test", "status": "processing"}]
    )
    v.get_scene_index = MagicMock(side_effect=AssertionError("should not be called"))
    mock_coll.get_video = MagicMock(return_value=v)
    r = client.get("/api/videos/vid1/scenes/i1?limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "processing"
    assert body["scenes"] == []


def test_rtstream_indexes_normalises_objects(client: TestClient, mock_coll: MagicMock) -> None:
    idx = MagicMock()
    idx.rtstream_index_id = "ix"
    idx.name = "audio"
    idx.status = "connected"
    idx.prompt = "describe"
    rt = MagicMock()
    rt.list_scene_indexes = MagicMock(return_value=[idx])
    mock_coll.get_rtstream = MagicMock(return_value=rt)
    r = client.get("/api/rtstreams/rt1/indexes")
    assert r.status_code == 200
    assert r.json()["indexes"][0]["name"] == "audio"


def test_search_collection_fans_out_per_video_scene_index(
    client: TestClient, mock_coll: MagicMock
) -> None:
    # api_search collection scope now fans out to v.search per video
    # because coll.search only hits the spoken-word index and wildlife
    # clips have no transcripts. Each video with a `ready` scene index
    # gets queried and results are merged + sorted by score.
    v1 = _video_mock("a")
    v1.list_scene_index = MagicMock(return_value=[{"status": "ready"}])
    r1 = MagicMock()
    r1.shots = [_shot_mock("oryx at water")]
    v1.search = MagicMock(return_value=r1)

    v2 = _video_mock("b")
    v2.list_scene_index = MagicMock(return_value=[])  # no index → skipped
    v2.search = MagicMock(side_effect=AssertionError("should not search"))

    mock_coll.get_videos = MagicMock(return_value=[v1, v2])

    r = client.post("/api/search", json={"query": "oryx", "scope": "collection"})
    assert r.status_code == 200
    body = r.json()
    assert body["scope"] == "collection"
    assert body["videos_searched"] == 2
    assert body["shots"][0]["text"] == "oryx at water"
    assert body["shots"][0]["video_id"] == "a"


def test_search_video_requires_target_id(client: TestClient, mock_coll: MagicMock) -> None:
    r = client.post("/api/search", json={"query": "x", "scope": "video"})
    assert r.status_code == 400


def test_search_rtstream_passes_index_id_when_given(
    client: TestClient, mock_coll: MagicMock
) -> None:
    rt = MagicMock()
    result = MagicMock()
    result.shots = [_shot_mock("person")]
    rt.search = MagicMock(return_value=result)
    mock_coll.get_rtstream = MagicMock(return_value=rt)

    r = client.post(
        "/api/search",
        json={
            "query": "person",
            "scope": "rtstream",
            "target_id": "rt1",
            "index_id": "idx-x",
        },
    )
    assert r.status_code == 200
    rt.search.assert_called_once()
    kwargs = rt.search.call_args.kwargs
    assert kwargs.get("query") == "person"
    assert kwargs.get("index_id") == "idx-x"


def test_search_unknown_scope_returns_400(client: TestClient, mock_coll: MagicMock) -> None:
    r = client.post("/api/search", json={"query": "x", "scope": "bogus"})
    assert r.status_code == 400


def test_video_indexes_500_on_sdk_error(client: TestClient, mock_coll: MagicMock) -> None:
    mock_coll.get_video = MagicMock(side_effect=RuntimeError("nope"))
    r = client.get("/api/videos/x/indexes")
    assert r.status_code == 500
