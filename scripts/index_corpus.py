"""Run scene indexing on every corpus video, then search-smoke-test.

For each entry in `.state.json["corpus"]`:
  1. List existing scene indexes on the VideoDB video.
  2. If none yet, create a fresh scene index with the species prompt.
  3. Wait (or skip-and-poll) until the index reaches `ready`.
  4. Once ready, run a quick `video.search(query="any animal")` to prove
     the index is queryable; print result count + first shot.

Idempotent — re-running skips videos that already have a ready index.

Usage:
    python scripts/index_corpus.py                  # all corpus entries
    python scripts/index_corpus.py --slug poaching_synth  # one
    python scripts/index_corpus.py --wait 600       # wait 10min per index
    python scripts/index_corpus.py --no-search      # skip search test
"""

from __future__ import annotations

import argparse
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

# Default search query used to smoke-test the index post-build.
SMOKE_QUERY = "any animal"

# Per-slug context for prompt formatting. Most corpus clips are wildlife;
# a few are synth/threat clips where the species list doesn't apply.
DEFAULT_CONTEXT = {
    "location_context": "Africam wildlife site (uploaded corpus clip)",
    "species_list": "common african fauna — oryx, springbok, elephant, lion, giraffe, zebra, leopard, hyena, jackal",
    "expected_sounds": "wind, drinking, hooves, occasional bird/mammal vocalisations",
}


def _load_state() -> dict:
    if not STATE_FILE.exists():
        sys.exit(f"ERROR: {STATE_FILE} missing — run scripts/build_corpus.py first")
    return json.loads(STATE_FILE.read_text())


def _existing_index(video) -> dict | None:
    """Return the first scene index dict on the video, or None."""
    try:
        idxs = video.list_scene_index() or []
    except Exception as e:
        print(f"    list_scene_index failed: {e!r}")
        return None
    return idxs[0] if idxs else None


def _wait_for_ready(video, index_id: str, max_wait_s: int) -> str:
    """Poll the index status until ready/failed or timeout.

    Returns the final status string (lowercased).
    """
    deadline = time.time() + max_wait_s
    last_status = "unknown"
    while time.time() < deadline:
        try:
            idxs = video.list_scene_index() or []
        except Exception as e:
            print(f"      poll: list_scene_index failed: {e!r}")
            time.sleep(5)
            continue
        target = next(
            (i for i in idxs if (i.get("scene_index_id") or i.get("id")) == index_id),
            None,
        )
        if target is None:
            return "missing"
        status = str(target.get("status", "unknown")).lower()
        if status != last_status:
            print(f"      poll: status={status}")
            last_status = status
        if status in ("ready", "indexed", "complete", "completed", "done"):
            return status
        if status in ("failed", "error"):
            return status
        time.sleep(8)
    return f"timeout (last={last_status})"


def _index_one(coll, slug: str, video_id: str, *, wait_s: int, do_search: bool) -> dict:
    print(f"\n[{slug}] video={video_id}")
    try:
        v = coll.get_video(video_id)
    except Exception as e:
        return {"slug": slug, "video_id": video_id, "status": "get_video_failed", "error": repr(e)}

    existing = _existing_index(v)
    if existing:
        idx_id = existing.get("scene_index_id") or existing.get("id")
        status = str(existing.get("status", "unknown")).lower()
        print(f"  existing index: id={idx_id} status={status}")
        if status not in ("ready", "indexed", "complete", "completed", "done"):
            status = _wait_for_ready(v, idx_id, wait_s)
            print(f"  waited → {status}")
    else:
        prompt = format_prompt("species", **DEFAULT_CONTEXT)
        print(f"  no existing index — creating ({len(prompt)} chars of prompt)")
        try:
            idx_id = v.index_scenes(prompt=prompt, name=f"wildwatch:{slug}")
        except Exception as e:
            return {
                "slug": slug,
                "video_id": video_id,
                "status": "index_scenes_failed",
                "error": repr(e),
            }
        print(f"  created index_id={idx_id}")
        status = _wait_for_ready(v, idx_id, wait_s)
        print(f"  waited → {status}")

    out: dict = {"slug": slug, "video_id": video_id, "index_id": idx_id, "status": status}
    if status not in ("ready", "indexed", "complete", "completed", "done"):
        return out

    if do_search:
        try:
            from videodb import IndexType

            kwargs: dict = {"query": SMOKE_QUERY, "score_threshold": 0.3}
            try:
                kwargs["index_type"] = IndexType.scene
            except Exception:
                pass
            try:
                result = v.search(**kwargs)
                shots = getattr(result, "shots", None) or []
                out["search_hits"] = len(shots)
                if shots:
                    first = shots[0]
                    out["first_hit"] = {
                        "start": getattr(first, "start", None),
                        "end": getattr(first, "end", None),
                        "text": (getattr(first, "text", "") or "")[:120],
                    }
                print(f"  search '{SMOKE_QUERY}' → {len(shots)} hits")
            except Exception as e:
                if "No results found" in str(e):
                    out["search_hits"] = 0
                    print(f"  search '{SMOKE_QUERY}' → no results")
                else:
                    out["search_error"] = repr(e)
                    print(f"  search failed: {e!r}")
        except Exception as e:
            out["search_error"] = repr(e)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", help="Only run for this single corpus slug")
    ap.add_argument("--wait", type=int, default=300, help="Max wait per index (seconds)")
    ap.add_argument("--no-search", action="store_true", help="Skip the search smoke-test")
    args = ap.parse_args()

    load_dotenv()
    state = _load_state()
    corpus = state.get("corpus", {})
    if not corpus:
        sys.exit("ERROR: state.corpus is empty — run scripts/build_corpus.py first")

    if args.slug:
        if args.slug not in corpus:
            sys.exit(f"ERROR: slug {args.slug!r} not in state.corpus")
        targets = {args.slug: corpus[args.slug]}
    else:
        targets = corpus

    conn = videodb.connect()
    coll = conn.get_collection()

    results: list[dict] = []
    for slug, entry in targets.items():
        video_id = entry.get("video_id")
        if not video_id:
            print(f"[{slug}] skip — no video_id in state")
            continue
        r = _index_one(coll, slug, video_id, wait_s=args.wait, do_search=not args.no_search)
        results.append(r)

    print("\n=== SUMMARY ===")
    ok = sum(
        1
        for r in results
        if r.get("status") in ("ready", "indexed", "complete", "completed", "done")
    )
    print(f"{ok}/{len(results)} indexes ready")
    for r in results:
        slug = r.get("slug")
        status = r.get("status")
        hits = r.get("search_hits", "—")
        print(f"  {slug:30s} → {status:30s} search_hits={hits}")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
