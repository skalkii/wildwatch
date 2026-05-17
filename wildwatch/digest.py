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
#
# Pre-bind every editor name to a guard sentinel BEFORE attempting the
# import. The `from ... import (...)` statement is not atomic — if it
# fails midway, Python leaves earlier names bound. Pre-binding to a
# callable sentinel ensures `from wildwatch.digest import Clip; Clip(...)`
# fails LOUDLY with a clear ImportError message rather than the silent
# `TypeError: 'NoneType' object is not callable` that bare None produced.


class _EditorUnavailable:
    """Callable sentinel placeholder for editor names when SDK import fails.

    Any attempt to call or instantiate raises ImportError with a clear
    message pointing at the missing dependency. Attribute access also
    surfaces the same error so `Clip.add_clip(...)` doesn't silently
    return a Mock.
    """

    def __init__(self, name: str) -> None:
        self._name = name

    def __call__(self, *_a: Any, **_kw: Any) -> Any:
        raise ImportError(
            f"videodb.editor.{self._name} is unavailable. Install videodb-python "
            "with editor extras (`pip install -e .[dev]` should pull it). "
            "build_timeline will fail; check startup logs for the underlying "
            "import error."
        )

    def __getattr__(self, attr: str) -> Any:  # pragma: no cover — diagnostic
        # Dunder / private names → AttributeError, not ImportError.
        # pytest's parametrize machinery, mypy's runtime inspection,
        # copy.copy, pickle, repr() etc. all introspect dunders. If we
        # raise ImportError on `__class__`, `__reduce__`, `__module__`,
        # collection silently dies with a confusing ImportError. Returning
        # AttributeError lets the normal protocol skip the attribute.
        # Underscored attrs (`_name` etc.) also use AttributeError so a
        # mid-construction recursion (if __init__ raised before _name was
        # bound) doesn't infinitely loop through this dispatch.
        if attr.startswith("_"):
            raise AttributeError(attr)
        raise ImportError(
            f"videodb.editor.{self._name}.{attr} access requested but the editor "
            "import failed at startup. See logger.warning above."
        )


AudioAsset = _EditorUnavailable("AudioAsset")  # type: ignore[assignment]
Background = _EditorUnavailable("Background")  # type: ignore[assignment]
Clip = _EditorUnavailable("Clip")  # type: ignore[assignment]
Font = _EditorUnavailable("Font")  # type: ignore[assignment]
TextAsset = _EditorUnavailable("TextAsset")  # type: ignore[assignment]
Timeline = _EditorUnavailable("Timeline")  # type: ignore[assignment]
Track = _EditorUnavailable("Track")  # type: ignore[assignment]
Transition = _EditorUnavailable("Transition")  # type: ignore[assignment]
VideoAsset = _EditorUnavailable("VideoAsset")  # type: ignore[assignment]
_EDITOR_AVAILABLE = False

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
        "digest: videodb.editor import failed (%s); build_timeline will fail loudly",
        _editor_err,
    )


# Map tier -> ordered list of preferred corpus slugs to represent that tier.
# First match wins, so populate with the strongest fit per tier.
TIER_SLUG_PREFERENCE: dict[int, list[str]] = {
    1: ["namibia_live_segment", "hwange_live_segment", "dry_waterhole"],
    2: ["hwange_live_segment", "namibia_live_segment", "pre_storm_silence"],
    3: ["poaching_synth", "logging_synth", "camera_failure_synth"],
}
# Per-event clip duration in seconds.
DEFAULT_CLIP_SECONDS = 4


def dedupe_events(events: list[dict]) -> list[dict]:
    """Collapse near-duplicate alert fires before timeline construction.

    Same event-engine event prompt can match the same scene multiple times
    in a short window (especially for `potential_human_intrusion_visual`
    on the intruder cam). One reel shouldn't include the same shot ten
    times. Dedupe key is `(label, video_id_or_stream_label, 60s bucket of
    start_time)` — same label, same source, same minute = same scene.
    """
    seen: set[tuple[str, str, int]] = set()
    out: list[dict] = []
    for ev in events:
        label = str(ev.get("label") or "")
        source = str(ev.get("video_id") or ev.get("stream_url") or "")[:48]
        # Coerce start_time to a 60-second bucket. Accepts float / int /
        # iso string. Falls back to received_at when start_time is missing.
        st = ev.get("start_time") or ev.get("received_at") or 0
        try:
            bucket = int(float(st)) // 60
        except (TypeError, ValueError):
            bucket = 0
        key = (label, source, bucket)
        if key in seen:
            continue
        seen.add(key)
        out.append(ev)
    return out


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


def pick_corpus_video_id(
    tier: int,
    corpus_state: dict[str, dict],
    skip: set[str] | None = None,
) -> str | None:
    """Return the video_id of the best corpus clip for ``tier``, or None.

    ``skip`` excludes video_ids that have already been determined to be
    unusable (e.g. an upload with no video stream — VideoDB raises
    ``Invalid request: Video info not available for video_id`` when the
    Timeline tries to render it). The fallback loop walks every other
    slug + every other corpus entry before giving up.
    """
    skip = skip or set()
    for slug in TIER_SLUG_PREFERENCE.get(tier, []):
        entry = corpus_state.get(slug)
        if entry and entry.get("video_id") and entry["video_id"] not in skip:
            return entry["video_id"]
    # Fallback: any corpus video not on the skip list.
    for entry in corpus_state.values():
        vid = entry.get("video_id")
        if vid and vid not in skip:
            return vid
    return None


def compute_analytics(events: list[dict]) -> dict:
    """Aggregate event-log records into chart-ready analytics.

    Returns one dict the dashboard renders as a panel of KPIs + charts.
    All shapes are list-of-pairs (label, count) so the JS can feed
    them straight into Chart.js without re-shaping. Pure function —
    no SDK / no IO.

    Surfaces (per UX brainstorm):
      - tier_counts: 3-up KPI row (info/notable/urgent).
      - total: hero number.
      - top_labels: ranked horizontal bar — what fired most.
      - hourly: 24-slot bar — daily rhythm of activity.
      - species: top 8 species donut (parsed from labels + explanations).
      - light_modes: day vs night vs IR pie (parsed from explanations).
      - categories: visual / audio / threat counts.
    """
    import re
    from collections import Counter

    tier_counts = {1: 0, 2: 0, 3: 0}
    label_counter: Counter[str] = Counter()
    species_counter: Counter[str] = Counter()
    hourly = [0] * 24
    light_modes: Counter[str] = Counter()
    categories = {"visual": 0, "audio": 0, "threat": 0, "behaviour": 0, "environment": 0}

    # Vocab keys for category bucketing — keep aligned with the event
    # labels defined in wildwatch/events.py.
    _AUDIO_LABELS = {
        "POACHING_ALERT_GUNSHOT",
        "ILLEGAL_LOGGING_ALERT",
        "human_intrusion_audio",
        "alarm_call_detected",
        "predator_vocalization",
        "acoustic_anomaly_silence",
    }
    _THREAT_LABELS = {
        "POACHING_ALERT_GUNSHOT",
        "ILLEGAL_LOGGING_ALERT",
        "potential_human_intrusion_visual",
        "human_intrusion_audio",
        "mortality_event",
    }
    _BEHAV_LABELS = {
        "predator_activity",
        "parental_care",
        "welfare_concern",
        "notable_social_behavior",
    }
    _ENV_LABELS = {
        "mortality_event",
        "potential_human_intrusion_visual",
        "camera_health_issue",
        "water_critical",
    }
    # Species token harvest: scan free-text fields for ``species=X``
    # markers (the species index's bracket-tag output format) AND a
    # small whitelist of common-name mentions in rewritten prose.
    _COMMON_SPECIES = {
        "lion",
        "leopard",
        "elephant",
        "rhino",
        "buffalo",
        "zebra",
        "giraffe",
        "wildebeest",
        "impala",
        "kudu",
        "oryx",
        "springbok",
        "gemsbok",
        "warthog",
        "hippo",
        "crocodile",
        "hyena",
        "jackal",
        "baboon",
        "vulture",
        "eagle",
        "hornbill",
        "antelope",
        "wild dog",
        "cheetah",
        "pangolin",
    }
    _LIGHT_RX = re.compile(
        r"light_mode\s*=\s*(daylight|ir_night|low_light|dusk|dawn|low_light_color|golden_hour)",
        re.I,
    )
    _SPECIES_TAG_RX = re.compile(r"species\s*=\s*([a-zA-Z_][\w \-]+)", re.I)

    for ev in events:
        tier = int(ev.get("tier") or 0)
        if tier in tier_counts:
            tier_counts[tier] += 1
        label = ev.get("label") or ""
        if label:
            label_counter[label] += 1
        if label in _AUDIO_LABELS:
            categories["audio"] += 1
        if label in _THREAT_LABELS:
            categories["threat"] += 1
        if label in _BEHAV_LABELS:
            categories["behaviour"] += 1
        if label in _ENV_LABELS:
            categories["environment"] += 1
        if label and label not in _AUDIO_LABELS and label not in _ENV_LABELS:
            categories["visual"] += 1
        # Hour bucket from received_at (local time good enough for demo).
        try:
            ts = float(ev.get("received_at") or 0)
            if ts > 0:
                import datetime as _dt

                hourly[_dt.datetime.fromtimestamp(ts).hour] += 1
        except (TypeError, ValueError):
            pass
        # Species + light_mode harvest from raw + rewritten explanation.
        haystack = " ".join(
            str(ev.get(k) or "") for k in ("explanation", "raw_explanation", "label")
        ).lower()
        for m in _SPECIES_TAG_RX.findall(haystack):
            sp = m.strip().lower()
            if sp and sp != "unknown" and not sp.startswith("unidentified"):
                species_counter[sp] += 1
        for sp in _COMMON_SPECIES:
            if sp in haystack:
                species_counter[sp] += 1
        m = _LIGHT_RX.search(haystack)
        if m:
            light_modes[m.group(1).lower()] += 1
    return {
        "total": sum(tier_counts.values()),
        "tier_counts": tier_counts,
        "top_labels": label_counter.most_common(8),
        "species": species_counter.most_common(8),
        "hourly": hourly,
        "light_modes": list(light_modes.most_common()),
        "categories": categories,
    }


def _maybe_seconds(v: Any) -> float | None:
    """Coerce an event payload's start/end_time to seconds-since-video-start.

    Path-B webhooks send floats (seconds offset from upload start).
    rtstream callbacks send wall-clock ISO strings or unix epochs, which
    aren't meaningful as a Timeline offset into a corpus video — those
    return None so the caller falls back to the corpus/collection path.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        # Reject epoch-like values that obviously aren't intra-video
        # offsets (>= 24h would clip past most uploaded videos).
        return f if 0 <= f < 86400 else None
    if isinstance(v, str):
        # Pure-number string like "12.5" or "12.500s".
        s = v.strip().rstrip("s")
        try:
            f = float(s)
            return f if 0 <= f < 86400 else None
        except ValueError:
            return None
    return None


def _discover_collection_fallback(
    conn: Any,
    cache: dict[str, bool],
    skip: set[str],
) -> list[str]:
    """List collection videos that pass the video_info probe.

    Called only when every corpus_state entry has been rejected, so the
    cost (one ``coll.get_videos`` + N probes) is bounded and worth it —
    the alternative is an empty reel.
    """
    try:
        vids = conn.get_collection().get_videos() or []
    except Exception as e:
        logger.warning("digest: collection-scan failed (%s); no fallback", e)
        return []
    out: list[str] = []
    for v in vids:
        vid = getattr(v, "id", None)
        if not vid or vid in skip:
            continue
        if _video_has_info(conn, vid, cache):
            out.append(vid)
    logger.info("digest: collection fallback discovered %d usable videos", len(out))
    return out


def _video_has_info(conn: Any, vid_id: str, cache: dict[str, bool]) -> bool:
    """True if VideoDB can render this video into a Timeline.

    Touches ``video.length`` — the attribute that triggers the
    server-side video_info lookup. Audio-only uploads, corrupt files,
    and deleted videos all surface here as the same
    ``Invalid request: Video info not available`` error.
    """
    if vid_id in cache:
        return cache[vid_id]
    try:
        v = conn.get_collection().get_video(vid_id)
        _ = v.length
        cache[vid_id] = True
        return True
    except Exception as e:
        msg = str(e)
        if "Video info not available" in msg or "video_info" in msg.lower():
            logger.warning(
                "digest: skipping corpus video %s — no video_info on VideoDB",
                vid_id,
            )
        else:
            logger.warning(
                "digest: probe failed for %s (%s); skipping",
                vid_id,
                type(e).__name__,
            )
        cache[vid_id] = False
        return False


_TIER_OVERLAY = {
    1: ("🟦 INFO", "#38bdf8"),
    2: ("🟡 NOTABLE", "#f59e0b"),
    3: ("🔴 URGENT", "#ef4444"),
}


def build_timeline(
    events: list[dict],
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
    video_info_cache: dict[str, bool] = {}
    skip_ids: set[str] = set()
    # Live-collection fallback list. When every entry in corpus_state is
    # stale (videos deleted off VideoDB), scan the collection ONCE for
    # any usable upload and use those instead. Lazy-built so we don't
    # pay the SDK call when state's corpus works.
    collection_fallback: list[str] | None = None
    for ev in events:
        tier = int(ev.get("tier", 1))

        # 1st choice: the actual triggering scene. Path-B post-upload
        # webhooks carry ``video_id`` + numeric ``start_time`` /
        # ``end_time`` so the reel shows the real moment that fired
        # the alert (a gunshot frame, the rare-species sighting),
        # not a stand-in from the corpus library. rtstream callbacks
        # omit video_id — those fall through to the corpus path.
        vid_id: str | None = None
        clip_start: float = 0.0
        clip_dur: float = float(clip_seconds)
        ev_vid = ev.get("video_id")
        ev_start = _maybe_seconds(ev.get("start_time"))
        ev_end = _maybe_seconds(ev.get("end_time"))
        if ev_vid and ev_start is not None and _video_has_info(conn, ev_vid, video_info_cache):
            vid_id = ev_vid
            clip_start = max(0.0, float(ev_start))
            if ev_end is not None and ev_end > ev_start:
                clip_dur = min(float(clip_seconds), float(ev_end) - clip_start)
                if clip_dur < 1.0:  # don't include sub-second slivers
                    clip_dur = float(clip_seconds)
        else:
            # 2nd choice: tier-preferred corpus slug.
            while True:
                cand = pick_corpus_video_id(tier, corpus_state, skip=skip_ids)
                if cand is None:
                    break
                if _video_has_info(conn, cand, video_info_cache):
                    vid_id = cand
                    break
                skip_ids.add(cand)
            # 3rd choice: any usable upload on the live collection.
            if not vid_id:
                if collection_fallback is None:
                    collection_fallback = _discover_collection_fallback(
                        conn, video_info_cache, skip_ids
                    )
                if collection_fallback:
                    vid_id = collection_fallback[n_clips % len(collection_fallback)]
        if not vid_id:
            logger.warning(
                "digest: no usable clip for event=%s tier=%s; skipping",
                (ev.get("label") or "?")[:30],
                tier,
            )
            continue

        # Video clip with fade in/out — skill prescribes Transition on every clip
        # to avoid jarring hard cuts. Duration 0.4s is short enough to fit a
        # 4s clip without eating perceived runtime.
        transition = Transition(in_="fade", out="fade", duration=0.4)
        video_track.add_clip(
            cursor,
            Clip(
                # volume=0 mutes the underlying clip audio so the
                # voiceover track is the only thing the viewer hears.
                # videodb.editor.VideoAsset (not the deprecated
                # videodb.asset.VideoAsset) accepts ``volume`` directly.
                asset=VideoAsset(id=vid_id, start=clip_start, volume=0),
                duration=clip_dur,
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
                Clip(asset=overlay, duration=clip_dur, transition=transition),
            )

        cursor += clip_dur
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
                    Clip(
                        # Background music at low volume so the
                        # voiceover sits clearly on top.
                        asset=AudioAsset(id=music_id, start=0, volume=0.25),
                        duration=total,
                    ),
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
    add_voiceover: bool = False,
) -> dict:
    """End-to-end: read log -> pick top N -> Timeline -> playable URL.

    Returns ``{n_events, n_clips, stream_url, player_url, summary}``.
    """
    import time

    min_ts = time.time() - (since_hours * 3600)
    events = event_log.read_since(min_ts)
    # Dedupe BEFORE pick_top — same label/source/minute is one scene.
    events = dedupe_events(events)
    picked = pick_top_events(events, top_n=top_n)
    corpus = state.get("corpus", {})

    if not picked:
        # Empty log: synthesise a demo montage from any corpus clips we have.
        # Include explicit empty "label" so downstream consumers that read
        # ev.get("label") see the expected key.
        logger.info("digest: event log empty; synthesising default montage")
        picked = [
            {"tier": 3, "label": ""},
            {"tier": 2, "label": ""},
            {"tier": 1, "label": ""},
        ]

    timeline, n_clips = build_timeline(
        picked,
        corpus,
        conn,
        clip_seconds=clip_seconds,
        add_text_overlays=add_text_overlays,
        add_music=add_music,
    )
    analytics = compute_analytics(events)
    if n_clips == 0:
        return {
            "n_events": len(events),
            "n_clips": 0,
            "stream_url": None,
            "player_url": None,
            "summary": None,
            "analytics": analytics,
        }

    # ──── 1. Natural-language summary FIRST (drives voiceover script too).
    summary: str | None = None
    coll = conn.get_collection()
    try:
        n_t1 = sum(1 for e in events if int(e.get("tier", 0)) == 1)
        n_t2 = sum(1 for e in events if int(e.get("tier", 0)) == 2)
        n_t3 = sum(1 for e in events if int(e.get("tier", 0)) == 3)
        prompt = (
            "You are a wildlife documentary narrator. Summarise this 24h "
            "monitoring digest as ONE flowing paragraph of 130-170 words, "
            "to be read aloud over a highlight reel by a deep, slow voice. "
            "Open with the day's most urgent finding, then weave in the "
            "notable behaviours and routine sightings, and close with one "
            "line on what rangers should watch for tonight. Use plain "
            "English suitable for non-technical donors and field staff. "
            "No bracket tags, no event-engine terminology, no bullet "
            "points, no headings — just continuous prose. "
            f"Window: last {since_hours}h. Counts: {n_t1} routine sightings, "
            f"{n_t2} notable events, {n_t3} urgent events. Top events: "
            + "; ".join((ev.get("label") or "").replace("_", " ") for ev in picked[:8])
            + "."
        )
        if hasattr(coll, "generate_text"):
            out = coll.generate_text(prompt=prompt, model_name="basic")
            if isinstance(out, dict):
                out = out.get("output") or out.get("text") or out.get("response") or ""
            summary = (out or "").strip() or None
    except Exception as e:
        logger.warning("digest: generate_text failed (%s: %s)", type(e).__name__, e)

    # ──── 2. Optional voiceover — turn the summary into a narration track
    # that plays over the visual reel. AudioAsset volume=0.9 because the
    # corpus clips themselves are usually silent or near-silent.
    if add_voiceover and summary and hasattr(coll, "generate_voice"):
        try:
            # Deep, slow narration. ``George`` is a low/resonant
            # ElevenLabs voice that suits documentary pacing. ``speed``
            # < 1 slows delivery; ``stability`` high keeps the tone
            # steady rather than dramatic. Wrapped in try/except in
            # case the model rejects unknown config keys — fall back
            # to defaults on TypeError.
            voice_config = {"speed": 0.85, "stability": 0.75}
            try:
                audio = coll.generate_voice(
                    text=summary,
                    voice_name="George",
                    config=voice_config,
                    wait=True,
                )
            except Exception as cfg_err:
                logger.warning(
                    "digest: generate_voice rejected slow-deep config (%s); retrying with defaults",
                    cfg_err,
                )
                audio = coll.generate_voice(text=summary, voice_name="George", wait=True)
            audio_id = getattr(audio, "id", None)
            if audio_id:
                # The narration is a fixed length per the text we
                # passed; VideoDB rejects ``Clip duration > audio
                # length``. Cap clip duration at min(audio_length,
                # reel_length). ``disable_other_tracks=True`` is the
                # default on AudioAsset and mutes the underlying clip
                # audio for the span the narration plays — combined
                # with the 130-170 word target that means the entire
                # reel is muted under the voiceover.
                reel_seconds = n_clips * clip_seconds
                audio_len = getattr(audio, "length", None) or clip_seconds
                vo_duration = min(float(audio_len), float(reel_seconds))
                vo_track = Track()
                vo_track.add_clip(
                    0,
                    Clip(
                        # volume=1.5 boosts the narration above the
                        # mixer baseline. Range per SDK is 0..5.
                        asset=AudioAsset(id=audio_id, start=0, volume=1.5),
                        duration=vo_duration,
                    ),
                )
                timeline.add_track(vo_track)
                logger.info(
                    "digest: added voiceover track id=%s duration=%ss (audio_len=%s reel=%s)",
                    audio_id,
                    vo_duration,
                    audio_len,
                    reel_seconds,
                )
        except Exception as e:
            logger.warning(
                "digest: generate_voice failed (%s: %s); reel will have no narration",
                type(e).__name__,
                e,
            )

    # ──── 3. Commit timeline → playable stream + player URLs.
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

    return {
        "n_events": len(events),
        "n_clips": n_clips,
        "stream_url": stream_url,
        "player_url": player_url,
        "summary": summary,
        "analytics": analytics,
    }
