"""Tests for FastAPI webhook receiver (TestClient + send_alert mocked)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from wildwatch import webhooks as wh_mod
from wildwatch.webhooks import app

VALID_PAYLOAD = {
    "event_id": "ev-abc",
    "label": "POACHING_ALERT_GUNSHOT",
    "confidence": 0.92,
    "explanation": "Gunshot detected in audio index window 14:02-14:03",
    "timestamp": "2026-05-16T08:00:00Z",
    "start_time": "2026-05-16T08:00:00Z",
    "end_time": "2026-05-16T08:00:05Z",
    "stream_url": "https://rt.stream.videodb.io/manifests/x.m3u8",
}


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def mock_send(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    m = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr(wh_mod, "send_alert", m)
    # Bypass the GenAI rewrite so tests assert against the raw
    # explanation text. Real path runs `coll.generate_text` which
    # would make tests flaky + slow + need a live VideoDB connection.
    monkeypatch.setattr(wh_mod, "genai_friendly_explanation", lambda *a, **k: None)
    return m


def test_post_tier1_returns_200_and_calls_send_alert(
    client: TestClient, mock_send: AsyncMock
) -> None:
    res = client.post("/webhook/1", json=VALID_PAYLOAD)
    assert res.status_code == 200
    assert res.json() == {"status": "received"}
    mock_send.assert_awaited_once()
    kwargs = mock_send.await_args.kwargs
    assert kwargs["tier"] == 1
    assert kwargs["label"] == "POACHING_ALERT_GUNSHOT"
    assert "Gunshot detected" in kwargs["explanation"]
    assert kwargs["stream_url"] == VALID_PAYLOAD["stream_url"]


def test_post_tier3_routes_to_send_alert_with_tier_3(
    client: TestClient, mock_send: AsyncMock
) -> None:
    res = client.post("/webhook/3", json=VALID_PAYLOAD)
    assert res.status_code == 200
    assert mock_send.await_args.kwargs["tier"] == 3


def test_post_tier4_rejected(client: TestClient, mock_send: AsyncMock) -> None:
    res = client.post("/webhook/4", json=VALID_PAYLOAD)
    assert res.status_code == 422
    mock_send.assert_not_called()


def test_post_tier0_rejected(client: TestClient, mock_send: AsyncMock) -> None:
    res = client.post("/webhook/0", json=VALID_PAYLOAD)
    assert res.status_code == 422


def test_missing_label_rejected(client: TestClient, mock_send: AsyncMock) -> None:
    bad = {k: v for k, v in VALID_PAYLOAD.items() if k != "label"}
    res = client.post("/webhook/2", json=bad)
    assert res.status_code == 422
    mock_send.assert_not_called()


def test_optional_fields_default_cleanly(client: TestClient, mock_send: AsyncMock) -> None:
    minimal = {"label": "alarm_call_detected", "event_id": "ev-x"}
    res = client.post("/webhook/2", json=minimal)
    assert res.status_code == 200
    kwargs = mock_send.await_args.kwargs
    assert kwargs["explanation"] is None
    assert kwargs["stream_url"] is None


def test_health_endpoint(client: TestClient) -> None:
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "ok"}
