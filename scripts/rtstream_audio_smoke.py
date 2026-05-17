"""Round 2 — rtstream.index_audio on a live RTSP stream.

Different from video.index_audio (transcript-segmented, speech-only).
rtstream.index_audio segments raw audio by time and runs LLM per chunk
— this is the bioacoustic-friendly path that our audio prompt actually
fits.

Test: connect FALLBACK_RTSP -> index_audio with a SIMPLE prompt (not the
full 1500-char bioacoustic prompt; isolate API correctness from prompt
behaviour) -> observe scenes for 120s -> stop.

Cost: ~$0.20 ingest + audio LLM batches over 2 min.
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

from config import FALLBACK_RTSP  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"

SIMPLE_AUDIO_PROMPT = (
    "Describe in 1-2 sentences any distinct sounds heard in this audio chunk "
    "(speech, footsteps, mechanical noise, ambient noise, silence, etc)."
)
BATCH_CONFIG = {"type": "time", "value": 30}
OBSERVE_SECONDS = 120


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


def main() -> int:
    load_dotenv()
    conn = videodb.connect()
    coll = conn.get_collection()
    print(f"collection.id = {coll.id}\n")

    print("connecting rtstream (with audio media_type) ...")
    rt = coll.connect_rtstream(
        url=FALLBACK_RTSP,
        name="smoke_audio",
        media_types=["video", "audio"],
        store=True,
    )
    print(f"  rtstream.id = {rt.id}  status={rt.status}\n")

    try:
        print("creating audio index ...")
        idx = rt.index_audio(
            prompt=SIMPLE_AUDIO_PROMPT,
            batch_config=BATCH_CONFIG,
            name="smoke_audio_index",
        )
        idx_id = getattr(idx, "rtstream_index_id", None) or getattr(idx, "id", None)
        print(f"  audio_index_id = {idx_id}\n")

        print(f"observing {OBSERVE_SECONDS}s for audio scenes ...")
        time.sleep(OBSERVE_SECONDS)

        print("\nfetching audio scenes ...")
        try:
            data = idx.get_scenes(page_size=10) if hasattr(idx, "get_scenes") else None
        except Exception as e:
            print(f"  get_scenes via idx failed: {e}")
            data = None
        if data is None:
            try:
                data = rt.get_scenes(page=1, page_size=10)
            except Exception as e:
                print(f"  fallback rt.get_scenes failed: {e}")
                data = {}
        scenes = data.get("scenes", []) if isinstance(data, dict) else (data or [])
        print(f"  {len(scenes)} audio scenes returned\n")
        for i, sc in enumerate(scenes[:10]):
            start = sc.get("start") if isinstance(sc, dict) else getattr(sc, "start", "?")
            end = sc.get("end") if isinstance(sc, dict) else getattr(sc, "end", "?")
            desc = sc.get("description") if isinstance(sc, dict) else getattr(sc, "description", "")
            print(f"  [{i}] {start}-{end}: {str(desc)[:200]}")

        state = _load_state()
        state.setdefault("rtstreams", {})["smoke_audio"] = {
            "id": rt.id,
            "audio_index_id": idx_id,
            "scenes_seen": len(scenes),
            "created_at": datetime.now(UTC).isoformat(),
        }
        _save_state(state)
    finally:
        print("\nstopping rtstream ...")
        try:
            rt.stop()
            print(f"  stopped  status={rt.status}")
        except Exception as e:
            print(f"  WARN stop failed: {type(e).__name__}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
