"""Daily digest reel from the event log.

Pulls the top-N events by (tier desc, recency desc) from
``wildwatch.event_log``, picks a corpus clip per event tier, and stitches
them into a Timeline whose ``generate_stream()`` URL is the digest reel.

Tier -> corpus clip slug mapping:
  tier 1 (info)     -> waterhole-style scene
  tier 2 (notable)  -> behavior-style scene
  tier 3 (urgent)   -> threat-style scene (synth)

When the event log is empty we still produce a montage of the most-recent
corpus clips so the demo always has a reel to play.
"""

from __future__ import annotations

import logging
from typing import Any

from wildwatch import event_log

logger = logging.getLogger(__name__)


# Map tier -> ordered list of preferred corpus slugs to represent that tier.
# First match wins, so populate with the strongest fit per tier.
TIER_SLUG_PREFERENCE: dict[int, list[str]] = {
    1: ["namibia_live_segment", "hwange_live_segment", "dry_waterhole"],
    2: ["hwange_live_segment", "namibia_live_segment", "pre_storm_silence"],
    3: ["poaching_synth", "logging_synth", "camera_failure_synth"],
}
# Per-event clip duration in seconds.
DEFAULT_CLIP_SECONDS = 4


def pick_top_events(
    events: list[dict[str, Any]],
    top_n: int = 10,
) -> list[dict[str, Any]]:
    """Sort events by (tier desc, received_at desc) and take top N."""

    def _key(e: dict) -> tuple:
        tier = int(e.get("tier", 0))
        ts = float(e.get("received_at", 0.0))
        return (-tier, -ts)

    return sorted(events, key=_key)[:top_n]


def pick_corpus_video_id(tier: int, corpus_state: dict[str, dict]) -> str | None:
    """Return the video_id of the best corpus clip for ``tier``, or None."""
    for slug in TIER_SLUG_PREFERENCE.get(tier, []):
        entry = corpus_state.get(slug)
        if entry and entry.get("video_id"):
            return entry["video_id"]
    # Fallback: any corpus video
    for entry in corpus_state.values():
        if entry.get("video_id"):
            return entry["video_id"]
    return None


def build_timeline(
    events: list[dict[str, Any]],
    corpus_state: dict[str, dict],
    conn: Any,
    clip_seconds: int = DEFAULT_CLIP_SECONDS,
) -> Any:
    """Compose a Timeline from the picked events + corpus mapping.

    Returns a videodb.editor.Timeline object ready for generate_stream().
    Imports are local so this module is importable in test envs that don't
    have full videodb installed.
    """
    from videodb.editor import Clip, Timeline, Track, VideoAsset

    timeline = Timeline(conn)
    timeline.resolution = "1280x720"
    track = Track()
    cursor = 0
    n_clips = 0
    for ev in events:
        tier = int(ev.get("tier", 1))
        vid_id = pick_corpus_video_id(tier, corpus_state)
        if not vid_id:
            logger.warning("digest: no corpus clip for tier=%s; skipping event", tier)
            continue
        track.add_clip(
            cursor,
            Clip(asset=VideoAsset(id=vid_id, start=0), duration=clip_seconds),
        )
        cursor += clip_seconds
        n_clips += 1
    timeline.add_track(track)
    return timeline, n_clips


def build_digest(
    conn: Any,
    state: dict[str, Any],
    since_hours: int = 24,
    top_n: int = 10,
    clip_seconds: int = DEFAULT_CLIP_SECONDS,
) -> dict[str, Any]:
    """End-to-end: read log -> pick top N -> Timeline -> playable URL.

    Returns dict { "n_events": int, "n_clips": int, "stream_url": str | None,
    "player_url": str | None }.
    """
    import time

    min_ts = time.time() - (since_hours * 3600)
    events = event_log.read_since(min_ts)
    picked = pick_top_events(events, top_n=top_n)
    corpus = state.get("corpus", {})

    if not picked:
        # Empty log: synthesise a demo montage from any corpus clips we have.
        logger.info("digest: event log empty; synthesising default montage")
        picked = [{"tier": 3}, {"tier": 2}, {"tier": 1}]

    timeline, n_clips = build_timeline(picked, corpus, conn, clip_seconds=clip_seconds)
    if n_clips == 0:
        return {
            "n_events": len(events),
            "n_clips": 0,
            "stream_url": None,
            "player_url": None,
        }

    stream_url = timeline.generate_stream()
    from urllib.parse import quote

    player_url = f"https://console.videodb.io/player?url={quote(stream_url, safe='')}"
    return {
        "n_events": len(events),
        "n_clips": n_clips,
        "stream_url": stream_url,
        "player_url": player_url,
    }
