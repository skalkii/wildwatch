"""VideoDB sandbox lifecycle helper.

Single chokepoint for sandbox creation/teardown. Enforces the sandbox
guide's critical rules:

- ``idle_timeout=600`` always passed to ``create_sandbox`` so a forgotten
  sandbox auto-stops in 10 minutes.
- ``wait_for_ready`` always called before returning the sandbox to callers.
- ``sandbox.is_active`` is asserted after wait — guide rule: "Only run jobs
  after ``sandbox.status == 'active'``".
- State is persisted to ``.state.json`` so re-running bootstrap reuses an
  active sandbox instead of leaking compute.
- ``managed_sandbox`` context manager guarantees ``stop()`` runs even on
  exception, preventing overnight credit burn.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATE_FILE = Path(__file__).resolve().parent.parent / ".state.json"


def _load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _record_sandbox(sb: Any, tier: Any) -> None:
    state = _load_state()
    state["sandbox"] = {
        "id": sb.id,
        "tier": str(tier),
        "created_at": datetime.now(UTC).isoformat(),
    }
    _save_state(state)


def _try_reuse(conn: Any, sandbox_id: str) -> Any | None:
    """Return an active sandbox by id, or None if it's gone/stopped."""
    try:
        sb = conn.get_sandbox(sandbox_id)
    except Exception:
        return None
    if sb is None:
        return None
    try:
        sb.refresh()
    except Exception:
        pass
    return sb if getattr(sb, "is_active", False) else None


def ensure_sandbox(
    conn: Any,
    tier: Any,
    idle_timeout: int = 600,
) -> Any:
    """Return a ready, active sandbox. Reuses ``.state.json`` if possible."""
    state = _load_state()
    cached_id = state.get("sandbox", {}).get("id")
    if cached_id:
        reused = _try_reuse(conn, cached_id)
        if reused is not None:
            return reused

    sb = conn.create_sandbox(tier=tier, idle_timeout=idle_timeout)
    sb.wait_for_ready(timeout=300, interval=5)
    if not getattr(sb, "is_active", False):
        raise RuntimeError(
            f"sandbox {sb.id} not active after wait_for_ready (status={getattr(sb, 'status', '?')})"
        )
    _record_sandbox(sb, tier)
    return sb


def stop_sandbox(conn: Any, sandbox_id: str) -> None:
    """Stop a sandbox by id and wait for confirmation."""
    sb = conn.get_sandbox(sandbox_id)
    if sb is None:
        return
    sb.stop()
    sb.wait_for_stop(timeout=120)


@contextmanager
def managed_sandbox(
    conn: Any,
    tier: Any,
    idle_timeout: int = 600,
) -> Iterator[Any]:
    """Context manager that auto-stops the sandbox on exit (including exceptions).

    Reuses ``ensure_sandbox`` for creation, then ``sb.stop()`` + wait on exit.
    The state file is intentionally left in place so callers see the last-used
    sandbox id even after teardown.
    """
    sb = ensure_sandbox(conn, tier=tier, idle_timeout=idle_timeout)
    try:
        yield sb
    finally:
        try:
            sb.stop()
            sb.wait_for_stop(timeout=120)
        except Exception:
            # Best-effort teardown; surface via state file inspection instead
            # of swallowing the original exception (if any).
            pass
