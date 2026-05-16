"""VideoDB sandbox lifecycle helper.

Single chokepoint for sandbox creation/teardown. Enforces the sandbox
guide's critical rules:

- ``wait_for_ready`` always called before returning the sandbox to callers.
- ``sandbox.is_active`` is asserted after wait — guide rule: "Only run jobs
  after ``sandbox.status == 'active'``".
- State is persisted to ``.state.json`` so re-running bootstrap reuses an
  active sandbox instead of leaking compute.
- ``managed_sandbox`` context manager guarantees ``stop()`` runs even on
  exception, preventing overnight credit burn.

KNOWN GAP: the hackathon-branch SDK's ``Connection.create_sandbox`` does
NOT accept the ``idle_timeout`` kwarg shown in the sandbox guide. Until
the SDK catches up, every caller MUST explicitly ``stop_sandbox`` (or use
``managed_sandbox``). No safety net.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parent.parent / ".state.json"


def _load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict[str, Any]) -> None:
    """Atomic write: stage to .tmp, then rename. POSIX rename is atomic so
    a crash mid-write cannot leave a partial JSON file that breaks resume."""
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


def _record_sandbox(sb: Any, tier: Any) -> None:
    state = _load_state()
    state["sandbox"] = {
        "id": sb.id,
        "tier": str(tier),
        "created_at": datetime.now(UTC).isoformat(),
    }
    _save_state(state)


def _try_reuse(conn: Any, sandbox_id: str) -> Any | None:
    """Return an active sandbox by id, or None if it's gone/stopped/refresh-failed."""
    try:
        sb = conn.get_sandbox(sandbox_id)
    except Exception as e:
        logger.warning("get_sandbox(%s) failed: %s", sandbox_id, e)
        return None
    if sb is None:
        return None
    try:
        sb.refresh()
    except Exception as e:
        # Don't return a stale-state sandbox — would let downstream jobs
        # hit a sandbox the server thinks is dead.
        logger.warning("sb.refresh() failed for %s: %s — discarding cached id", sandbox_id, e)
        return None
    return sb if getattr(sb, "is_active", False) else None


def ensure_sandbox(
    conn: Any,
    tier: Any,
    name: str | None = "wildwatch",
) -> Any:
    """Return a ready, active sandbox. Reuses ``.state.json`` if possible.

    Note: ``create_sandbox`` does not accept ``idle_timeout`` on this SDK
    branch; explicit ``stop_sandbox`` / ``managed_sandbox`` is required.
    """
    state = _load_state()
    cached_id = state.get("sandbox", {}).get("id")
    if cached_id:
        reused = _try_reuse(conn, cached_id)
        if reused is not None:
            return reused

    sb = conn.create_sandbox(tier=tier, name=name)
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
    name: str | None = "wildwatch",
) -> Iterator[Any]:
    """Context manager that auto-stops the sandbox on exit (including exceptions).

    Reuses ``ensure_sandbox`` for creation, then ``sb.stop()`` + wait on exit.
    The state file is intentionally left in place so callers see the last-used
    sandbox id even after teardown.
    """
    sb = ensure_sandbox(conn, tier=tier, name=name)
    try:
        yield sb
    finally:
        try:
            sb.stop()
            sb.wait_for_stop(timeout=120)
        except Exception:
            # Best-effort teardown; log loudly so a 3am credit-leak gets
            # surfaced via the log instead of silent .state.json inspection.
            logger.warning(
                "managed_sandbox: stop failed for %s — sandbox may still be billing",
                getattr(sb, "id", "?"),
                exc_info=True,
            )
