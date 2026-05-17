"""Tests that API caches (remote/videos/usage) absorb failures.

Regression for review finding: on SDK failure, /api/remote and
/api/list_videos returned ``{"...": [], "error": str(e)}`` BUT never
updated the cache timestamp. Every subsequent request during the outage
hammered the SDK again with no backoff.

Fix: failures populate the cache and bump the timestamp so TTL gates
retries the same as a successful response.
"""

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
def _reset_all_caches() -> None:
    wh_mod._remote_cache["at"] = 0.0
    wh_mod._remote_cache["data"] = {"rtstreams": [], "sandboxes": []}
    wh_mod._videos_cache["at"] = 0.0
    wh_mod._videos_cache["data"] = {"videos": []}
    wh_mod._usage_cache["at"] = 0.0
    wh_mod._usage_cache["data"] = {}


def test_api_remote_caches_error_response_to_throttle_retries(client: TestClient) -> None:
    call_counter = {"n": 0}

    def _flaky_connect():
        call_counter["n"] += 1
        raise RuntimeError("videodb down")

    with patch("videodb.connect", side_effect=_flaky_connect):
        r1 = client.get("/api/remote")
        r2 = client.get("/api/remote")
    # 502 (not 200) — endpoint now surfaces SDK failures as a visible
    # degraded state instead of pretending everything is fine. Error body
    # still carries the diagnostic info + still cached to throttle retries.
    assert r1.status_code == 502
    assert "error" in r1.json()
    assert "videodb down" in r1.json()["error"]
    # Second call within TTL must serve cached error, NOT re-hammer SDK
    assert call_counter["n"] == 1, (
        f"SDK called {call_counter['n']} times; failure must update cache timestamp"
    )
    assert r2.json() == r1.json()


def test_api_list_videos_caches_error_response_to_throttle_retries(client: TestClient) -> None:
    call_counter = {"n": 0}

    def _flaky_connect():
        call_counter["n"] += 1
        raise RuntimeError("collection unavailable")

    with patch("videodb.connect", side_effect=_flaky_connect):
        r1 = client.get("/api/videos")
        r2 = client.get("/api/videos")
    # 502 — same rationale as above.
    assert r1.status_code == 502
    assert "error" in r1.json()
    assert call_counter["n"] == 1
    assert r2.json() == r1.json()


def test_api_usage_caches_top_level_connect_failure(client: TestClient) -> None:
    """If videodb.connect itself raises, api_usage must still cache so
    repeated dashboard polls during outage don't all crash through."""
    call_counter = {"n": 0}

    def _flaky_connect():
        call_counter["n"] += 1
        raise RuntimeError("auth expired")

    with patch("videodb.connect", side_effect=_flaky_connect):
        r1 = client.get("/api/usage")
        # api_usage now calls _get_conn from THREE places (the local-burn
        # estimator's list_rtstreams + sandbox probe + the outer
        # check_usage path). Each will retry videodb.connect on first
        # miss. We don't measure r1's count — we measure that r2 adds
        # ZERO new calls because the /api/usage 60s cache served it.
        calls_after_r1 = call_counter["n"]
        r2 = client.get("/api/usage")
    assert r1.status_code == 200
    body = r1.json()
    assert "usage_error" in body
    assert "auth expired" in body["usage_error"]
    # The contract is: SDK call count does not grow on subsequent polls
    # while the failure is still cached. That's what throttles the SDK
    # during an outage.
    assert call_counter["n"] == calls_after_r1, (
        f"cache didn't throttle: r2 added {call_counter['n'] - calls_after_r1} more SDK calls"
    )
    assert r2.json() == r1.json()


def test_api_remote_cache_recovers_after_ttl_expires(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure cache is temporary; after TTL, SDK is retried."""
    # Tighten TTL so the test runs fast
    monkeypatch.setattr(wh_mod, "_REMOTE_TTL_S", 0.05)

    call_counter = {"n": 0}

    def _flaky_connect():
        call_counter["n"] += 1
        raise RuntimeError("transient")

    import time as _time

    with patch("videodb.connect", side_effect=_flaky_connect):
        client.get("/api/remote")
        _time.sleep(0.1)  # exceed tightened TTL
        client.get("/api/remote")
    assert call_counter["n"] == 2, "after TTL, SDK must be retried"


def test_api_remote_success_overwrites_cached_error(client: TestClient) -> None:
    """After SDK recovers, the next call must replace the error in cache
    with the real data — no permanent failure latch."""
    # First call: failure caches "videodb down" error
    with patch("videodb.connect", side_effect=RuntimeError("videodb down")):
        client.get("/api/remote")

    # Tighten TTL so the next request fetches fresh
    wh_mod._remote_cache["at"] = 0.0

    fake_conn = MagicMock()
    fake_conn.list_rtstreams = MagicMock(return_value=[])
    fake_conn.list_sandboxes = MagicMock(return_value=[])
    fake_coll = MagicMock()
    fake_conn.get_collection = MagicMock(return_value=fake_coll)
    fake_coll.list_rtstreams = MagicMock(return_value=[])

    with patch("videodb.connect", return_value=fake_conn):
        r = client.get("/api/remote")
    body = r.json()
    assert "error" not in body
    assert body["rtstreams"] == []
