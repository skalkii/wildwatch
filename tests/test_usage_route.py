"""Tests for /api/usage."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from wildwatch import webhooks as wh_mod
from wildwatch.webhooks import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    wh_mod._usage_cache["at"] = 0.0
    wh_mod._usage_cache["data"] = {}


def test_usage_returns_estimate_plus_usage_and_invoices() -> None:
    fake_conn = MagicMock()
    fake_conn.check_usage = MagicMock(return_value={"plan": "hackathon", "credits_remaining": 999})
    fake_conn.get_invoices = MagicMock(return_value=[{"id": "inv1"}])

    with patch("videodb.connect", return_value=fake_conn):
        client = TestClient(app)
        r = client.get("/api/usage")
    assert r.status_code == 200
    body = r.json()
    assert "estimate" in body
    assert "total_usd" in body["estimate"]
    assert body["usage"]["credits_remaining"] == 999
    assert body["invoices"][0]["id"] == "inv1"


def test_usage_handles_check_usage_failure() -> None:
    fake_conn = MagicMock()
    fake_conn.check_usage = MagicMock(side_effect=RuntimeError("billing down"))
    fake_conn.get_invoices = MagicMock(return_value=[])

    with patch("videodb.connect", return_value=fake_conn):
        client = TestClient(app)
        r = client.get("/api/usage")
    assert r.status_code == 200
    body = r.json()
    assert "usage_error" in body
    assert "billing down" in body["usage_error"]


def test_usage_cache_hits_dont_re_call_sdk() -> None:
    call_counter = {"n": 0}

    def _fake_check_usage():
        call_counter["n"] += 1
        return {"plan": "hackathon"}

    fake_conn = MagicMock()
    fake_conn.check_usage = _fake_check_usage
    fake_conn.get_invoices = MagicMock(return_value=[])

    with patch("videodb.connect", return_value=fake_conn):
        client = TestClient(app)
        client.get("/api/usage")
        client.get("/api/usage")
    assert call_counter["n"] == 1  # second call served from cache


def test_estimate_handles_missing_state(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    # Point estimator at a nonexistent state file
    fake_state = tmp_path / "nope.json"
    monkeypatch.setattr(wh_mod, "__file__", str(fake_state.parent / "fakewebhooks.py"))
    # Just call the estimator directly
    out = wh_mod._estimate_credit_burn_usd()
    assert out["total_usd"] == 0.0
