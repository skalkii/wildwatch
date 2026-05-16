"""Run one visual prompt against one uploaded corpus clip; save raw output.

Usage:
    python scripts/iterate_prompt.py --slug waterhole_rich_scene --kind species

Visual kinds (species, behavior, environment) use video.index_scenes with
gemma-4-31B-it on a Medium sandbox. Audio kind handled separately in T-17b
sweep (different SDK call).

Output: samples/triggers/{slug}.{kind}.txt with one scene per block:
    [t=12.3-22.4]
    <raw VLM description>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Repo root on path so `from config import STREAMS` works when run as script.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from videodb import SandboxTier  # noqa: E402
from videodb._constants import SceneExtractionType  # noqa: E402

from config import STREAMS  # noqa: E402
from wildwatch.prompts import format_prompt  # noqa: E402
from wildwatch.sandbox import managed_sandbox  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"
SAMPLES_DIR = REPO_ROOT / "samples" / "triggers"
MANIFEST = SAMPLES_DIR / "manifest.json"

VISUAL_KINDS = {"species", "behavior", "environment"}
AUDIO_KINDS = {"audio"}
ALL_KINDS = VISUAL_KINDS | AUDIO_KINDS

# Visual: gemma-4-31B-it on Medium sandbox.
# Audio: model_name uses VideoDB's tier strings ('basic'/'pro'/'ultra'), NOT
# raw model identifiers — server picks the underlying LLM. No sandbox needed
# (server-side managed compute).
MODEL_BY_KIND = {
    "species": "google/gemma-4-31B-it",
    "behavior": "google/gemma-4-31B-it",
    "environment": "google/gemma-4-31B-it",
    "audio": "pro",
}
# Per CLAUDE.md sec 13: env is sampled slower than species/behavior.
# Audio batch_config uses transcript-segmenter shape (time/word/sentence).
EXTRACTION_BY_KIND: dict[str, dict] = {
    "species": {"time": 10, "select_frames": ["first"], "frame_count": 1},
    "behavior": {"time": 10, "select_frames": ["first"], "frame_count": 1},
    "environment": {"time": 60, "select_frames": ["first"], "frame_count": 1},
    "audio": {"type": "time", "value": 30},
}


def _load_state() -> dict:
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}


def _find_clip(manifest: dict, slug: str) -> dict:
    for clip in manifest["clips"]:
        if clip["slug"] == slug:
            return clip
    sys.exit(f"unknown slug: {slug}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", required=True, help="manifest clip slug")
    ap.add_argument("--kind", required=True, choices=sorted(ALL_KINDS))
    ap.add_argument("--tier", default="medium", choices=["small", "medium"], help="sandbox tier")
    args = ap.parse_args()

    load_dotenv()
    manifest = json.loads(MANIFEST.read_text())
    state = _load_state()
    clip = _find_clip(manifest, args.slug)
    corpus_entry = state.get("corpus", {}).get(args.slug)
    if not corpus_entry:
        sys.exit(f"slug {args.slug} not in .state.json corpus — run upload_corpus.py first")

    stream_ctx_key = clip["stream_context"]
    stream = STREAMS[stream_ctx_key]
    # Only the placeholder-relevant keys (avoid 'name' clashing with format_prompt's positional)
    stream_ctx = {
        k: stream[k] for k in ("location_context", "species_list", "expected_sounds") if k in stream
    }
    prompt = format_prompt(args.kind, **stream_ctx)

    print(f"slug:     {args.slug}")
    print(f"kind:     {args.kind}")
    print(f"video_id: {corpus_entry['video_id']}")
    print(f"model:    {MODEL_BY_KIND[args.kind]}")
    print(f"tier:     {args.tier}")
    print(f"prompt:   {len(prompt)} chars")

    conn = videodb.connect()
    coll = conn.get_collection()
    video = coll.get_video(corpus_entry["video_id"])

    if args.kind in AUDIO_KINDS:
        # Audio path: server-side managed compute, no sandbox.
        print("audio:    server-managed (no sandbox)")
        print("indexing ...")
        index_id = video.index_audio(
            prompt=prompt,
            model_name=MODEL_BY_KIND[args.kind],
            batch_config=EXTRACTION_BY_KIND[args.kind],
        )
        scenes = video.get_scene_index(index_id)
    else:
        tier = SandboxTier.medium if args.tier == "medium" else SandboxTier.small
        with managed_sandbox(conn, tier=tier) as sb:
            print(f"sandbox:  {sb.id}  status={sb.status}")
            print("indexing ...")
            index_id = video.index_scenes(
                extraction_type=SceneExtractionType.time_based,
                extraction_config=EXTRACTION_BY_KIND[args.kind],
                model_name=MODEL_BY_KIND[args.kind],
                prompt=prompt,
                sandbox_id=sb.id,
            )
            scenes = video.get_scene_index(index_id)
    print(f"index_id: {index_id}")
    print(f"scenes:   {len(scenes) if scenes else 0}")

    out_path = SAMPLES_DIR / f"{args.slug}.{args.kind}.txt"
    with out_path.open("w") as f:
        f.write(f"# slug={args.slug} kind={args.kind} model={MODEL_BY_KIND[args.kind]}\n")
        f.write(f"# index_id={index_id}\n")
        f.write(f"# scenes={len(scenes) if scenes else 0}\n\n")
        if scenes:
            for sc in scenes:
                start = sc.get("start") if isinstance(sc, dict) else getattr(sc, "start", "?")
                end = sc.get("end") if isinstance(sc, dict) else getattr(sc, "end", "?")
                desc = (
                    sc.get("description")
                    if isinstance(sc, dict)
                    else getattr(sc, "description", "")
                )
                f.write(f"[t={start}-{end}]\n{desc}\n\n")
    print(f"wrote -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
