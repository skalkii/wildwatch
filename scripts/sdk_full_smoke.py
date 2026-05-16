"""Round 1 — cheap SDK paths not yet exercised in production.

A. rtstream.generate_stream(start, end) -> playable HLS URL (Telegram links)
B. Timeline.generate_stream() actual render to playable URL (digest reel)
C. rtstream.search(query) over existing index (correlation engine)
D. idx.enable_alert / disable_alert (dynamic toggle)
E. coll.generate_text(prompt, model_name='pro', response_type='json')

Reuses the T-21 rtstream + index + alert (already in .state.json).
Cost: ~$0 (no new ingest, no new sandbox, no new indexes).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from videodb.editor import Clip, Timeline, VideoAsset  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"


def main() -> int:
    load_dotenv()
    state = json.loads(STATE_FILE.read_text())
    conn = videodb.connect()
    coll = conn.get_collection()

    smoke = state["rtstreams"]["smoke_event"]
    rt_id = smoke["id"]
    idx_id = smoke["index_id"]
    alert_id = smoke["alert_id"]

    # Find the rtstream object
    rt = None
    for s in coll.list_rtstreams():
        if s.id == rt_id:
            rt = s
            break
    print(f"rtstream {rt_id}  status={rt.status}\n")

    idx = rt.get_scene_index(idx_id)

    # ──── A. rtstream.generate_stream(start, end) ─────────────────────────
    print("[A] rtstream.generate_stream(start, end) ...")
    scenes_data = idx.get_scenes(page_size=1)
    scenes = scenes_data.get("scenes", []) if isinstance(scenes_data, dict) else []
    if scenes:
        sc = scenes[0]
        s_start = int(sc["start"])
        s_end = int(sc["end"])
        print(f"      using scene window {s_start}-{s_end} (Unix seconds)")
        try:
            # Try seconds first (per SDK docstring)
            url = rt.generate_stream(s_start, s_end)
            print(f"      ok (sec) -> {url[:100]}")
        except Exception as e:
            print(f"      sec form failed: {e}")
            try:
                # Try microseconds (per the URL pattern in payload)
                url = rt.generate_stream(s_start * 1_000_000, s_end * 1_000_000)
                print(f"      ok (us)  -> {url[:100]}")
            except Exception as e2:
                print(f"      us form also failed: {e2}")
    else:
        print("      SKIP: no scenes available")

    # ──── B. Timeline.generate_stream() with a corpus VideoAsset ──────────
    print("\n[B] Timeline.generate_stream() with corpus VideoAsset ...")
    corpus = state.get("corpus", {})
    # Pick a short clip — camera_failure_synth is 10s, lowest storage
    pick = corpus.get("camera_failure_synth") or next(iter(corpus.values()), None)
    if pick:
        vid_id = pick["video_id"]
        print(f"      using corpus video {vid_id}")
        timeline = Timeline(conn)
        timeline.resolution = "1280x720"
        track = videodb.editor.Track()
        # Trim 0-5s from the source clip
        # videodb.editor.VideoAsset signature: (id, start, volume, crop). No 'end'.
        # Trim handled via Clip.duration.
        track.add_clip(0, Clip(asset=VideoAsset(id=vid_id, start=0), duration=5))
        timeline.add_track(track)
        try:
            url = timeline.generate_stream()
            print(f"      ok -> {url[:120]}")
            print(f"      player: https://console.videodb.io/player?url={url}")
        except Exception as e:
            print(f"      FAILED: {type(e).__name__}: {e}")
    else:
        print("      SKIP: no corpus videos in state")

    # ──── C. rtstream.search ───────────────────────────────────────────────
    print("\n[C] rtstream.search('person OR motion') ...")
    try:
        result = rt.search(query="person OR motion")
        shots = getattr(result, "shots", None) or []
        print(f"      ok   {len(shots)} shots")
        for sh in shots[:3]:
            print(
                f"      [{getattr(sh, 'start', '?')}-{getattr(sh, 'end', '?')}] "
                f"{str(getattr(sh, 'text', ''))[:120]}"
            )
    except Exception as e:
        print(f"      FAILED: {type(e).__name__}: {e}")

    # ──── D. idx.disable_alert + enable_alert ─────────────────────────────
    print("\n[D] idx.disable_alert / enable_alert toggle ...")
    try:
        idx.disable_alert(alert_id)
        # Read back to confirm
        alerts = idx.list_alerts()
        status_after_disable = next((a["status"] for a in alerts if a["alert_id"] == alert_id), "?")
        print(f"      after disable: status={status_after_disable}")
        idx.enable_alert(alert_id)
        alerts = idx.list_alerts()
        status_after_enable = next((a["status"] for a in alerts if a["alert_id"] == alert_id), "?")
        print(f"      after enable:  status={status_after_enable}")
    except Exception as e:
        print(f"      FAILED: {type(e).__name__}: {e}")

    # ──── E. coll.generate_text ────────────────────────────────────────────
    print("\n[E] coll.generate_text(model_name='pro', response_type='json') ...")
    try:
        result = coll.generate_text(
            prompt="Return a JSON object with a single key 'hello' set to 'world'.",
            model_name="pro",
            response_type="json",
        )
        print(f"      ok   {result}")
    except Exception as e:
        print(f"      FAILED: {type(e).__name__}: {e}")

    print("\nround 1 complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
