"""Bulk-upload every locally-built corpus clip to VideoDB.

Walks samples/triggers/manifest.json:
- For each clip whose mp4 exists at samples/triggers/{slug}.mp4 and
  whose video_id is not already in .state.json["corpus"][slug],
  call coll.upload(file_path=...) and record the returned id.

Idempotent: existing entries in state are skipped. Re-run safely.

Usage:
    python scripts/upload_corpus.py [--only SLUG ...]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import videodb
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = REPO_ROOT / "samples" / "triggers" / "manifest.json"
SAMPLES_DIR = REPO_ROOT / "samples" / "triggers"
STATE_FILE = REPO_ROOT / ".state.json"


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", help="only upload these slugs")
    args = ap.parse_args()

    load_dotenv()
    manifest = json.loads(MANIFEST.read_text())
    state = _load_state()
    corpus = state.setdefault("corpus", {})

    conn = videodb.connect()
    coll = conn.get_collection()
    print(f"collection.id = {coll.id}")

    n_uploaded = 0
    n_skipped = 0
    n_missing = 0
    for clip in manifest["clips"]:
        slug = clip["slug"]
        if args.only and slug not in args.only:
            continue
        mp4 = SAMPLES_DIR / f"{slug}.mp4"
        if not mp4.exists():
            print(f"  miss   {slug}  (no mp4; run scripts/build_corpus.py first)")
            n_missing += 1
            continue
        if slug in corpus and corpus[slug].get("video_id"):
            print(f"  skip   {slug}  (already uploaded: {corpus[slug]['video_id']})")
            n_skipped += 1
            continue
        print(f"  upload {slug}  ({mp4.stat().st_size / 1024 / 1024:.1f} MB) ...")
        video = coll.upload(file_path=str(mp4))
        corpus[slug] = {
            "video_id": video.id,
            "length": getattr(video, "length", None),
            "stream_url": getattr(video, "stream_url", None),
            "mp4_path": str(mp4),
        }
        _save_state(state)
        print(f"         video_id={video.id}  length={corpus[slug]['length']}")
        n_uploaded += 1

    print(f"\nsummary: uploaded={n_uploaded} skipped={n_skipped} missing={n_missing}")
    return 0 if n_missing == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
