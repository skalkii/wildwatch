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

# Eager import of the editor surface so an import error surfaces at module
# load (e.g. application startup) rather than when an operator triggers a
# digest build mid-demo. Wrapped in try/except so test environments without
# the full SDK can still import this module.
try:
    from videodb.editor import (  # type: ignore[import-not-found]
        AudioAsset,
        Background,
        Clip,
        Font,
        TextAsset,
        Timeline,
        Track,
        Transition,
        VideoAsset,
    )

    _EDITOR_AVAILABLE = True
except Exception as _editor_err:  # pragma: no cover — env-dependent
    logger.warning(
        "digest: videodb.editor import failed (%s); build_timeline will fail loudly", _editor_err
    )
    AudioAsset = Background = Clip = Font = TextAsset = None  # type: ignore[assignment]
    Timeline = Track = Transition = VideoAsset = None  # type: ignore[assignment]
    _EDITOR_AVAILABLE = False


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


_TIER_OVERLAY = {
    1: ("🟦 INFO", "#38bdf8"),
    2: ("🟡 NOTABLE", "#f59e0b"),
    3: ("🔴 URGENT", "#ef4444"),
}


def build_timeline(
    events: list[dict[str, Any]],
    corpus_state: dict[str, dict],
    conn: Any,
    clip_seconds: int = DEFAULT_CLIP_SECONDS,
    *,
    add_text_overlays: bool = True,
    add_music: bool = False,
    music_prompt: str = (
        "documentary ambient, soft african savanna at dawn, "
        "low strings, sparse percussion, calm but tense"
    ),
) -> tuple[Any, int]:
    """Compose a Timeline from the picked events + corpus mapping.

    Skill conformance (video-db/skills editor model):
      - Multi-track Timeline (video + text-overlay + optional music).
      - Each Clip gets a ``Transition(in_="fade", out="fade")`` so the
        reel doesn't jump-cut every 4 seconds.
      - TextAsset overlays render the tier label per clip so a non-tech
        viewer can tell what the AI flagged at a glance.
      - Optional ``coll.generate_music(prompt=...)`` background track —
        single SDK call, zero extra wiring.

    Returns (Timeline, n_clips).
    """
    if clip_seconds <= 0:
        raise ValueError(f"clip_seconds must be > 0, got {clip_seconds}")
    if not _EDITOR_AVAILABLE:
        raise RuntimeError(
            "videodb.editor unavailable — install videodb-python with editor extras "
            "(this should have been caught at import time; see startup logs)."
        )

    timeline = Timeline(conn)
    timeline.resolution = "1280x720"

    video_track = Track()
    overlay_track = Track() if add_text_overlays else None

    cursor = 0
    n_clips = 0
    for ev in events:
        tier = int(ev.get("tier", 1))
        vid_id = pick_corpus_video_id(tier, corpus_state)
        if not vid_id:
            logger.warning("digest: no corpus clip for tier=%s; skipping event", tier)
            continue

        # Video clip with fade in/out — skill prescribes Transition on every clip
        # to avoid jarring hard cuts. Duration 0.4s is short enough to fit a
        # 4s clip without eating perceived runtime.
        transition = Transition(in_="fade", out="fade", duration=0.4)
        video_track.add_clip(
            cursor,
            Clip(
                asset=VideoAsset(id=vid_id, start=0),
                duration=clip_seconds,
                transition=transition,
            ),
        )

        # Text overlay: tier label + event label (truncated). Skill flags
        # TextAsset Background as the readable way to render burn-in text.
        if overlay_track is not None:
            label_text, accent = _TIER_OVERLAY.get(tier, ("EVENT", "#94a3b8"))
            ev_label = (ev.get("label") or "").replace("_", " ").upper()[:38]
            overlay = TextAsset(
                text=f"{label_text}\n{ev_label}" if ev_label else label_text,
                font=Font(family="Clear Sans", size=42, color="#ffffff"),
                background=Background(color=accent, opacity=0.85),
            )
            overlay_track.add_clip(
                cursor,
                Clip(asset=overlay, duration=clip_seconds, transition=transition),
            )

        cursor += clip_seconds
        n_clips += 1

    timeline.add_track(video_track)
    if overlay_track is not None:
        timeline.add_track(overlay_track)

    # Background music — best-effort, never fatal.
    if add_music and n_clips > 0:
        try:
            total = cursor  # seconds
            music = conn.get_collection().generate_music(prompt=music_prompt, duration=total)
            music_id = getattr(music, "id", None)
            if music_id:
                music_track = Track()
                music_track.add_clip(
                    0,
                    Clip(asset=AudioAsset(id=music_id, start=0, volume=0.25), duration=total),
                )
                timeline.add_track(music_track)
                logger.info("digest: added generated music track id=%s (%ss)", music_id, total)
        except Exception as e:
            logger.warning("digest: generate_music failed (%s); reel will be silent", e)

    return timeline, n_clips


def build_digest(
    conn: Any,
    state: dict[str, Any],
    since_hours: int = 24,
    top_n: int = 10,
    clip_seconds: int = DEFAULT_CLIP_SECONDS,
    *,
    add_text_overlays: bool = True,
    add_music: bool = False,
) -> dict[str, Any]:
    """End-to-end: read log -> pick top N -> Timeline -> playable URL.

    Returns dict { "n_events", "n_clips", "stream_url", "player_url",
    "summary" }.
    """
    import time

    min_ts = time.time() - (since_hours * 3600)
    events = event_log.read_since(min_ts)
    picked = pick_top_events(events, top_n=top_n)
    corpus = state.get("corpus", {})

    if not picked:
        # Empty log: synthesise a demo montage from any corpus clips we have.
        # Include explicit empty "label" so downstream consumers that read
        # ev.get("label") see the expected key (rather than relying on
        # `or ""` everywhere).
        logger.info("digest: event log empty; synthesising default montage")
        picked = [{"tier": 3, "label": ""}, {"tier": 2, "label": ""}, {"tier": 1, "label": ""}]

    timeline, n_clips = build_timeline(
        picked,
        corpus,
        conn,
        clip_seconds=clip_seconds,
        add_text_overlays=add_text_overlays,
        add_music=add_music,
    )
    if n_clips == 0:
        return {
            "n_events": len(events),
            "n_clips": 0,
            "stream_url": None,
            "player_url": None,
            "summary": None,
        }

    stream_url = timeline.generate_stream()

    # Prefer the SDK's own player_url helper over hand-built console URLs —
    # skill explicitly flags this as the canonical playback link.
    player_url: str | None = None
    try:
        pu = timeline.player_url
        player_url = pu() if callable(pu) else pu
    except Exception as e:
        logger.warning(
            "digest: timeline.player_url failed (%s: %s); falling back to console URL",
            type(e).__name__,
            e,
        )
        player_url = None
    if not player_url:
        from urllib.parse import quote

        player_url = f"https://console.videodb.io/player?url={quote(stream_url, safe='')}"

    # Best-effort natural-language summary card for the reel intro. Uses
    # `coll.generate_text` (skill's go-to for LLM-as-tool); failure is silent.
    summary: str | None = None
    try:
        n_t1 = sum(1 for e in events if int(e.get("tier", 0)) == 1)
        n_t2 = sum(1 for e in events if int(e.get("tier", 0)) == 2)
        n_t3 = sum(1 for e in events if int(e.get("tier", 0)) == 3)
        prompt = (
            "Summarise this wildlife monitoring digest in one short paragraph "
            "(<=40 words) for a non-technical conservation audience. "
            f"Last {since_hours}h: {n_t1} routine sightings, {n_t2} notable "
            f"events, {n_t3} urgent events. Highlight labels: "
            + ", ".join((ev.get("label") or "") for ev in picked[:5])
            + "."
        )
        coll = conn.get_collection()
        if hasattr(coll, "generate_text"):
            summary = coll.generate_text(prompt=prompt, model_name="basic")
    except Exception as e:
        # WARNING not INFO — a quota/auth failure on the LLM call should
        # be visible at the same level as other SDK errors so it isn't
        # confused with the legitimate "module not installed" skip.
        logger.warning("digest: generate_text failed (%s: %s)", type(e).__name__, e)

    return {
        "n_events": len(events),
        "n_clips": n_clips,
        "stream_url": stream_url,
        "player_url": player_url,
        "summary": summary,
    }
