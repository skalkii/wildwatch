"""T-20 RTStream visual-index smoke.

Try guide-path first (gemma-4-31B-it on Medium sandbox + sandbox_id),
fall back to notebook-path (no model_name, no sandbox_id) per the
"guidelines first" rule.

Pipeline:
  1. ensure_sandbox(Medium)   — gemma-4-31B-it requires Medium
  2. connect_rtstream(FALLBACK_RTSP)
  3. index_visuals(prompt, batch_config, model_name, sandbox_id)
  4. wait ~90s for scenes to populate
  5. get_scenes(page_size=5) and print
  6. rt.stop()
  7. managed_sandbox exits -> sandbox.stop()

Cost: ~$0.20 (Medium $3.50/h x ~3-5 min wall).

Uses a SIMPLE generic prompt for smoke (not the full bioacoustic /
species prompt) to isolate "does index_visuals on RTStream work" from
"does our prompt produce parseable output". Prompt iteration comes in
T-17-style sweeps, not here.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from videodb import SandboxTier  # noqa: E402

from config import FALLBACK_RTSP  # noqa: E402
from wildwatch.sandbox import managed_sandbox  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"
SIMPLE_PROMPT = "Describe what is visible in this video chunk in 1-2 sentences."
BATCH_CONFIG = {"type": "time", "value": 5, "frame_count": 2}
MODEL = "google/gemma-4-31B-it"
WAIT_SECONDS = 90


def _save_state(state: dict) -> None:
    # Use the shared atomic helper so the file is fsynced AND chmod-600
    # before publication. The previous inline write left .state.json
    # world-readable (umask 0644), undoing the perm tightening that
    # production writers apply.
    from wildwatch.state_io import atomic_write_json

    atomic_write_json(STATE_FILE, state)


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _try_guide_index(rt, sb_id: str):
    """Guide-path: pass model_name + sandbox_id."""
    print(f"[guide-path] rt.index_visuals(model_name={MODEL!r}, sandbox_id=...) ...")
    return rt.index_visuals(
        prompt=SIMPLE_PROMPT,
        batch_config=BATCH_CONFIG,
        model_name=MODEL,
        sandbox_id=sb_id,
        name="smoke_visual_guide",
    )


def _try_notebook_index(rt):
    """Notebook-path: minimal kwargs, no model_name, no sandbox_id."""
    print("[notebook-path] rt.index_visuals(prompt, batch_config, name) only ...")
    return rt.index_visuals(
        prompt=SIMPLE_PROMPT,
        batch_config=BATCH_CONFIG,
        name="smoke_visual_notebook",
    )


def main() -> int:
    load_dotenv()
    conn = videodb.connect()
    coll = conn.get_collection()
    print(f"collection.id = {coll.id}\n")

    rt = None
    idx = None
    path_used = None
    try:
        with managed_sandbox(conn, tier=SandboxTier.medium) as sb:
            print(f"sandbox: {sb.id}  status={sb.status}\n")

            print("connecting RTSP (guide signature) ...")
            rt = coll.connect_rtstream(
                url=FALLBACK_RTSP,
                name="smoke_index",
                media_types=["video"],
                store=True,
            )
            print(f"  rtstream.id = {rt.id}  status={rt.status}\n")

            try:
                idx = _try_guide_index(rt, sb.id)
                path_used = "guide-path-ok"
            except TypeError as e:
                print(f"  guide path TypeError on index_visuals: {e}")
                print("  falling back to notebook signature ...\n")
                try:
                    idx = _try_notebook_index(rt)
                    path_used = "notebook-path-ok"
                except Exception as e2:
                    print(f"  notebook fallback ALSO failed: {type(e2).__name__}: {e2}")
                    raise

            idx_id = getattr(idx, "rtstream_index_id", None) or getattr(idx, "id", None)
            print(f"  index_id = {idx_id}\n")

            print(f"waiting {WAIT_SECONDS}s for scenes to populate ...")
            time.sleep(WAIT_SECONDS)

            print("\nfetching scenes (page_size=5) ...")
            scenes_data = idx.get_scenes(page_size=5) if hasattr(idx, "get_scenes") else None
            if scenes_data is None:
                # Fallback path — try rt.get_scenes directly
                scenes_data = rt.get_scenes(start=None, end=None, page=1, page_size=5)
            scenes = (
                scenes_data.get("scenes", [])
                if isinstance(scenes_data, dict)
                else (scenes_data or [])
            )
            print(f"  {len(scenes)} scenes returned\n")
            for i, sc in enumerate(scenes[:5]):
                start = sc.get("start") if isinstance(sc, dict) else getattr(sc, "start", "?")
                end = sc.get("end") if isinstance(sc, dict) else getattr(sc, "end", "?")
                desc = (
                    sc.get("description")
                    if isinstance(sc, dict)
                    else getattr(sc, "description", "")
                )
                print(f"  [{i}] {start}-{end}: {str(desc)[:200]}")

            state = _load_state()
            state.setdefault("rtstreams", {})["smoke_index"] = {
                "id": rt.id,
                "status": rt.status,
                "index_id": idx_id,
                "path_used": path_used,
                "scenes_seen": len(scenes),
                "created_at": datetime.now(UTC).isoformat(),
            }
            _save_state(state)
    finally:
        if rt is not None:
            try:
                rt.stop()
                try:
                    rt.refresh()
                except Exception:
                    pass
                print(f"\nrtstream stopped  status={rt.status}")
            except Exception as e:
                print(f"\nWARN rtstream stop failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
