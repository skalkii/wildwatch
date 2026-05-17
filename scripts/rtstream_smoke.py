"""T-19 RTSP connect smoke — try sandbox-guide signature first.

Sandbox guide says:
    coll.connect_rtstream(url, name, media_types=["video"], store=True)
    rtstream.start()  # mandatory

Intrusion-detection notebook (canonical) does:
    coll.connect_rtstream(name=..., url=...)
    # no start() — auto-runs

Run guide path first. Fall back to notebook path on TypeError.
Uses FALLBACK_RTSP (VideoDB-hosted sample, the same URL the canonical
notebook uses, guaranteed reachable).

Cost: $0. No sandbox, no indexing. Just stream lifecycle smoke.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from config import FALLBACK_RTSP  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict) -> None:
    # Use the shared atomic helper so the file is fsynced AND chmod-600
    # before publication. The previous inline write left .state.json
    # world-readable (umask 0644), undoing the perm tightening that
    # production writers apply.
    from wildwatch.state_io import atomic_write_json

    atomic_write_json(STATE_FILE, state)


def _try_connect_with_guide_signature(coll) -> tuple[object | None, str]:
    """Sandbox guide signature with media_types + store.

    Live test (2026-05-16) confirmed:
    - media_types=['video'] and store=True ARE valid kwargs (not rejected).
    - rt.start() is NOT mandatory — stream is already 'connected' after
      connect_rtstream returns. Calling .start() on an active stream raises
      InvalidRequestError. So we DON'T call start() here, contrary to what
      the sandbox guide example shows.
    """
    print("[guide-path] coll.connect_rtstream(media_types=['video'], store=True) ...")
    try:
        rt = coll.connect_rtstream(
            url=FALLBACK_RTSP,
            name="smoke_guide",
            media_types=["video"],
            store=True,
        )
        print(f"  connected   id={rt.id}  status={rt.status}")
    except TypeError as e:
        return None, f"TypeError on connect_rtstream: {e}"
    except Exception as e:
        return None, f"{type(e).__name__} on connect_rtstream: {e}"

    return rt, "guide-path-ok"


def _try_connect_with_notebook_signature(coll) -> tuple[object | None, str]:
    """Canonical notebook signature: minimal kwargs, no start()."""
    print("[notebook-path] coll.connect_rtstream(name, url) only ...")
    try:
        rt = coll.connect_rtstream(url=FALLBACK_RTSP, name="smoke_notebook")
        print(f"  connected   id={rt.id}  status={rt.status}")
        return rt, "notebook-path-ok"
    except Exception as e:
        return None, f"{type(e).__name__} on connect_rtstream: {e}"


def main() -> int:
    load_dotenv()
    conn = videodb.connect()
    coll = conn.get_collection()
    print(f"collection.id = {coll.id}\n")

    rt, status = _try_connect_with_guide_signature(coll)
    if rt is None:
        print(f"  guide path failed: {status}")
        print("  falling back to notebook signature ...\n")
        rt, status = _try_connect_with_notebook_signature(coll)
        if rt is None:
            print(f"  notebook path also failed: {status}")
            return 1

    print(f"\nresult: {status}")
    print(f"rtstream.id            = {rt.id}")
    print(f"rtstream.name          = {getattr(rt, 'name', '?')}")
    print(f"rtstream.status        = {rt.status}")
    print(f"rtstream.created_at    = {getattr(rt, 'created_at', '?')}")
    print(f"rtstream.sample_rate   = {getattr(rt, 'sample_rate', '?')}")

    state = _load_state()
    state.setdefault("rtstreams", {})["smoke"] = {
        "id": rt.id,
        "status": rt.status,
        "url": FALLBACK_RTSP,
        "path_used": status,
        "created_at": datetime.now(UTC).isoformat(),
    }
    _save_state(state)
    print(f"\nstate -> {STATE_FILE}")

    print("\nstopping (we don't want to keep ingesting) ...")
    try:
        rt.stop()
        try:
            rt.refresh()
        except Exception:
            pass
        print(f"  stopped  status={rt.status}")
    except Exception as e:
        print(f"  WARN stop failed: {type(e).__name__}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
