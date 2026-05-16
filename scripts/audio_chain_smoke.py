"""Test whether the audio path works after explicit transcription.

Hypothesis: video.index_audio uses transcript-segmented LLM; without
prior transcript, polling hangs. Chain: index_spoken_words (Whisper),
wait, then index_audio.

Cost: small. Runs on camera_failure_synth (10s of pink noise — Whisper
should return empty transcript quickly).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from wildwatch.prompts import format_prompt  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"
SLUG = "waterhole_rich_scene"  # Africam Show has commentary VO -> Whisper should bite


def main() -> int:
    load_dotenv()
    state = json.loads(STATE_FILE.read_text())
    video_id = state["corpus"][SLUG]["video_id"]
    print(f"slug={SLUG}  video_id={video_id}")

    conn = videodb.connect()
    coll = conn.get_collection()
    video = coll.get_video(video_id)

    print("\n[1] Submitting index_spoken_words ...")
    t0 = time.time()
    try:
        video.index_spoken_words()
        print(f"   ok  ({time.time() - t0:.1f}s)")
    except Exception as e:
        print(f"   FAIL  {type(e).__name__}: {e}")
        return 1

    print("\n[2] Submitting index_audio (model_name='pro') ...")
    audio_prompt = format_prompt(
        "audio",
        location_context="synthesized test clip",
        expected_sounds="ambient pink noise only",
    )
    t1 = time.time()
    try:
        index_id = video.index_audio(
            prompt=audio_prompt,
            model_name="pro",
            batch_config={"type": "time", "value": 30},
        )
        print(f"   index_id={index_id}  ({time.time() - t1:.1f}s)")
    except Exception as e:
        print(f"   FAIL  {type(e).__name__}: {e}")
        return 2

    print("\n[3] Fetching scenes via get_scene_index ...")
    t2 = time.time()
    try:
        scenes = video.get_scene_index(index_id)
        print(f"   ok  {len(scenes) if scenes else 0} scenes  ({time.time() - t2:.1f}s)")
        if scenes:
            for sc in scenes[:3]:
                print(f"   {sc}")
    except Exception as e:
        print(f"   FAIL  {type(e).__name__}: {e}")
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
