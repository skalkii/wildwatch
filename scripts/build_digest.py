"""T-35 daily digest reel — CLI orchestrator.

Reads webhook event log + corpus state, stitches a Timeline from the top-N
recent events, prints the playable URL.

Usage:
    python scripts/build_digest.py --since-hours 24 --top 10
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from wildwatch.digest import build_digest  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=int, default=24)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--clip-seconds", type=int, default=4)
    args = ap.parse_args()

    load_dotenv()
    if not STATE_FILE.exists():
        sys.exit("ERROR: .state.json missing — run bootstrap.py first")
    try:
        state = json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        sys.exit("ERROR: .state.json corrupt")

    conn = videodb.connect()

    print(f"building digest: since={args.since_hours}h top={args.top} clip={args.clip_seconds}s")
    result = build_digest(
        conn,
        state,
        since_hours=args.since_hours,
        top_n=args.top,
        clip_seconds=args.clip_seconds,
    )

    print(f"\nn_events seen: {result['n_events']}")
    print(f"n_clips used:  {result['n_clips']}")
    if result["stream_url"]:
        print(f"\n  stream:  {result['stream_url']}")
        print(f"  player:  {result['player_url']}")
        return 0
    print("\nno digest produced (no clips).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
