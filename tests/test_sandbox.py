"""Tests for wildwatch.sandbox lifecycle helpers (mocked SDK)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from wildwatch import sandbox as sb_mod
from wildwatch.sandbox import ensure_sandbox, managed_sandbox, stop_sandbox


@pytest.fixture
def state_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect STATE_FILE to a temp path so tests don't touch real state."""
    p = tmp_path / ".state.json"
    monkeypatch.setattr(sb_mod, "STATE_FILE", p)
    return p


def _fake_sandbox(sandbox_id: str = "sb-abc", is_active: bool = True) -> MagicMock:
    sb = MagicMock()
    sb.id = sandbox_id
    sb.is_active = is_active
    sb.status = "active" if is_active else "stopped"
    sb.tier = MagicMock(value="medium")
    sb.wait_for_ready = MagicMock()
    sb.wait_for_stop = MagicMock()
    sb.refresh = MagicMock()
    sb.stop = MagicMock()
    return sb


def _fake_conn(create_returns: MagicMock, get_returns: MagicMock | None = None) -> MagicMock:
    conn = MagicMock()
    conn.create_sandbox = MagicMock(return_value=create_returns)
    conn.get_sandbox = MagicMock(return_value=get_returns)
    return conn


def test_ensure_sandbox_creates_new_and_waits(state_file: Path) -> None:
    fake = _fake_sandbox("sb-new", is_active=True)
    conn = _fake_conn(create_returns=fake)

    result = ensure_sandbox(conn, tier="medium")

    assert result is fake
    conn.create_sandbox.assert_called_once()
    call_kwargs = conn.create_sandbox.call_args.kwargs
    assert call_kwargs.get("tier") == "medium"
    assert call_kwargs.get("idle_timeout") == 600
    fake.wait_for_ready.assert_called_once_with(timeout=300, interval=5)


def test_ensure_sandbox_asserts_is_active_after_wait(state_file: Path) -> None:
    # Critical guide rule: only run jobs after status == 'active'
    fake = _fake_sandbox("sb-stuck", is_active=False)
    conn = _fake_conn(create_returns=fake)

    with pytest.raises(RuntimeError, match="not active"):
        ensure_sandbox(conn, tier="small")


def test_ensure_sandbox_persists_state(state_file: Path) -> None:
    fake = _fake_sandbox("sb-persist", is_active=True)
    conn = _fake_conn(create_returns=fake)

    ensure_sandbox(conn, tier="medium")

    state = json.loads(state_file.read_text())
    assert state["sandbox"]["id"] == "sb-persist"
    assert state["sandbox"]["tier"] == "medium"
    assert "created_at" in state["sandbox"]


def test_ensure_sandbox_reuses_active_from_state(state_file: Path) -> None:
    state_file.write_text(
        json.dumps({"sandbox": {"id": "sb-old", "tier": "medium", "created_at": "x"}})
    )
    reused = _fake_sandbox("sb-old", is_active=True)
    conn = _fake_conn(create_returns=_fake_sandbox("sb-NEW"), get_returns=reused)

    result = ensure_sandbox(conn, tier="medium")

    assert result is reused
    conn.get_sandbox.assert_called_once_with("sb-old")
    conn.create_sandbox.assert_not_called()
    reused.refresh.assert_called_once()


def test_ensure_sandbox_falls_through_when_reused_is_stale(state_file: Path) -> None:
    state_file.write_text(
        json.dumps({"sandbox": {"id": "sb-stale", "tier": "medium", "created_at": "x"}})
    )
    stale = _fake_sandbox("sb-stale", is_active=False)
    fresh = _fake_sandbox("sb-FRESH", is_active=True)
    conn = _fake_conn(create_returns=fresh, get_returns=stale)

    result = ensure_sandbox(conn, tier="medium")

    assert result is fresh
    conn.create_sandbox.assert_called_once()


def test_stop_sandbox_calls_stop_and_waits(state_file: Path) -> None:
    fake = _fake_sandbox("sb-going", is_active=True)
    conn = _fake_conn(create_returns=fake, get_returns=fake)

    stop_sandbox(conn, "sb-going")

    fake.stop.assert_called_once()
    fake.wait_for_stop.assert_called_once_with(timeout=120)


def test_managed_sandbox_auto_stops_on_normal_exit(state_file: Path) -> None:
    fake = _fake_sandbox("sb-ctx", is_active=True)
    conn = _fake_conn(create_returns=fake)

    with managed_sandbox(conn, tier="small") as sb:
        assert sb is fake

    fake.stop.assert_called_once()


def test_managed_sandbox_auto_stops_on_exception(state_file: Path) -> None:
    fake = _fake_sandbox("sb-boom", is_active=True)
    conn = _fake_conn(create_returns=fake)

    with pytest.raises(ValueError, match="boom"):
        with managed_sandbox(conn, tier="small") as sb:
            assert sb is fake
            raise ValueError("boom")

    fake.stop.assert_called_once()
