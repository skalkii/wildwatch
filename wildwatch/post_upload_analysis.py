"""Post-upload event analysis for archive videos (Path B).

VideoDB's `create_alert` only attaches to **rtstream** scene indexes — the
SDK does not expose alerts on an uploaded video's scene index. So for a
URL/file upload we can't get a server-side push when the AI matches an
event prompt.

Workaround: AFTER both the visual species index and the audio index reach
``done``, run a search per event-of-interest against the matching index
and synthesise a webhook for every hit. The synthesised webhook flows
through the SAME ``/webhook/{tier}`` pipeline as VideoDB-fired ones, so
the dashboard feed, event log, and Telegram bot all fire as if the alert
had been pushed by VideoDB itself.

This is NOT real-time on the upload itself (we wait for the indexes to
finish), but the user uploaded an *archive* — there is no "real-time" to
miss. The whole clip is analysed once at upload time, and any matches
land in Telegram a minute or two after the upload completes.

Concurrency:
- Runs as a fire-and-forget asyncio task spawned from ``ingest.dispatch``.
- Polls VideoDB at most every 8 s for index readiness.
- Total wall-time deadline 20 min (long enough for 90 min clips at typical
  VideoDB indexing speed) — beyond that the task gives up and logs.

Failure handling:
- Every SDK error is logged but never re-raised. The upload is still
  usable (operator can search manually) even when this post-pass fails.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import httpx

from wildwatch.events import EVENT_DEFINITIONS

logger = logging.getLogger(__name__)


# Per-event search query for the post-upload analysis pass. The event
# *prompts* in ``events.py`` are written for VideoDB's event engine
# ("Detect when sound type is gunshot, confidence medium or high"); those
# don't translate directly to natural-language scene search. The queries
# below are tuned for the species/audio indexes' bracket-tagged output.
#
# The mapping is keyed on ``id_var`` (the stable Python identifier from
# EVENT_DEFINITIONS) so events.py reorgs don't break this mapping silently.
_EVENT_QUERY = {
    # ──── audio events (run against the audio index when available, else
    #      against the visual index as a fallback). The queries below
    #      include both audio-flavoured terms (for hits in the audio
    #      index's transcript-classified output) AND visual-flavoured
    #      terms (so the same query produces useful hits when the
    #      sweep falls back to the visual scene index on a silent clip). ──
    "gunshot": (
        "gunshot OR firearm discharge OR weapon OR rifle OR muzzle flash OR "
        "person aiming firearm OR shooting"
    ),
    "chainsaw": "chainsaw OR power saw OR logging equipment OR person cutting tree",
    "human_intrusion_audio": (
        "vehicle engine OR motorcycle OR human voices OR machinery OR "
        "vehicle visible OR person in scene OR human-made object"
    ),
    "alarm_call": "alarm call OR alarm vocalization OR animal alarm posture OR fleeing",
    "predator_vocal": (
        "lion roar OR leopard sawing OR hyena whoop OR predator vocalization OR "
        "predator visible OR large carnivore"
    ),
    "acoustic_silence": "abnormal silence OR frozen animals OR vigilant scanning",
    # ──── species/visual events (run against the species scene index) ────
    "rare_species": "leopard OR rhino OR wild dog OR cheetah OR pangolin",
    "mixed_aggregation": "mixed species aggregation",
    "juvenile_present": "juvenile OR calf OR cub OR chick",
    "large_aggregation": "large aggregation OR many animals",
    "mortality_event": "carcass OR remains OR kill",
    "human_intrusion_visual": "vehicle OR structure OR fence OR human-made object OR person",
}

# Which index kind each event id_var lives on (mirror INDEX_EVENT_MAP).
_EVENT_INDEX_KIND = {
    "gunshot": "audio",
    "chainsaw": "audio",
    "human_intrusion_audio": "audio",
    "alarm_call": "audio",
    "predator_vocal": "audio",
    "acoustic_silence": "audio",
    "rare_species": "species",
    "mixed_aggregation": "species",
    "juvenile_present": "species",
    "large_aggregation": "species",
    "mortality_event": "environment",
    "human_intrusion_visual": "environment",
}

# Ready-status set — matches `webhooks.py` / `ingest.py`.
_READY = {"ready", "indexed", "complete", "completed", "done"}

# How long to wait for an index to finish, total (seconds).
_INDEX_WAIT_S = 20 * 60
_POLL_INTERVAL_S = 6
# How long to wait for the audio index specifically. VideoDB's
# `video.index_audio` is transcript-based (extraction_type=transcript),
# so for a silent clip with no speech it never produces output. We
# gate audio kickoff on transcript existence (see `kick_off_audio_index`),
# but as a belt-and-braces cap, give up after 3 min if it's still stuck.
_AUDIO_WAIT_S = 3 * 60

# Cap of webhook posts per upload — prevents an overly-permissive prompt
# from spamming Telegram with 200 "gunshot" alerts on a single clip.
_MAX_FIRES_PER_UPLOAD = 12

_DEFAULT_PROMPT_CONTEXT = {
    "location_context": "uploaded clip (any environment)",
    "species_list": (
        "common wildlife — oryx, springbok, elephant, lion, giraffe, zebra, "
        "leopard, hyena, jackal, kudu, buffalo, hippo, crocodile, baboon"
    ),
    "expected_sounds": (
        "wind, drinking, hooves, occasional bird and mammal vocalisations, "
        "possible anthropogenic sounds (gunshot, chainsaw, vehicle)"
    ),
}


def _local_base_url() -> str:
    """Resolve the URL the analyser POSTs synthesised webhooks to.

    Default is `http://localhost:8000` — same process as the analyser is
    running in. The user can override with `LOCAL_WEBHOOK_URL` for
    deployments where uvicorn binds a different port or host.
    """
    return os.getenv("LOCAL_WEBHOOK_URL", "http://localhost:8000").rstrip("/")


def _has_transcript(video: Any, source_id: str) -> bool:
    """Return True iff the video has a non-empty transcript.

    VideoDB's `video.index_audio` is **transcript-based**
    (`extraction_type=SceneExtractionType.transcript`) — it processes
    transcript segments through an LLM with the audio prompt. A clip
    with no spoken words has no transcript → `index_audio` sits in
    `processing` forever waiting for segments that never come.

    This helper triggers transcript generation if missing (idempotent
    via `force=False`), then checks for non-empty content. If the
    transcript is empty (silent / SFX-only clip), the caller should
    skip the audio index entirely — VideoDB's audio path can't help.
    """
    try:
        # Try to fetch existing transcript first — cheap if already generated.
        existing = video.get_transcript()
        if existing:
            # Different shapes across SDK versions: list of {start,end,text}
            # OR plain string. Treat any text presence as a transcript.
            if isinstance(existing, list):
                if any(str(seg.get("text", "")).strip() for seg in existing):
                    return True
            elif isinstance(existing, str) and existing.strip():
                return True
    except Exception as e:
        logger.debug("post-analysis: get_transcript probe failed for %s: %r", source_id, e)

    # No cached transcript — try to generate one.
    try:
        result = video.generate_transcript(force=False)
    except Exception as e:
        logger.info(
            "post-analysis: generate_transcript failed for source=%s: %r — treating as silent clip",
            source_id,
            e,
        )
        return False

    # The SDK returns either {"success": True, "text": "...", ...} OR a
    # string OR a dict with word_timestamps. Tolerate all shapes.
    if isinstance(result, dict):
        text = (result.get("text") or "").strip()
        if text:
            return True
        wts = result.get("word_timestamps") or []
        return any(str(w.get("word", "")).strip() for w in wts)
    if isinstance(result, str):
        return bool(result.strip())
    return False


def _audio_indexes(video: Any) -> list[dict]:
    """Return all audio-named scene indexes on the video (best-effort)."""
    try:
        idxs = video.list_scene_index() or []
    except Exception as e:
        logger.warning("post-analysis: list_scene_index failed: %r", e)
        return []
    return [i for i in idxs if "audio" in str(i.get("name", "")).lower()]


def purge_stuck_audio_indexes(video: Any, source_id: str) -> int:
    """Delete every audio-named index in `processing` state on the video.

    VideoDB's `video.index_audio` is transcript-based — for a clip with
    no spoken words it never finishes, leaving the index stuck in
    `processing` indefinitely. There is no native API to "cancel" the
    job, but `video.delete_scene_index(idx_id)` does remove it. Without
    this purge, the dashboard's index list grows a new stuck audio
    index every time the operator clicks Re-index.

    Returns the count of indexes deleted. Logs each removal so the
    operator can see what happened in the uvicorn output.
    """
    removed = 0
    for idx in _audio_indexes(video):
        status = str(idx.get("status", "")).lower()
        if status not in ("processing", "queued", "pending", "initiated"):
            continue
        idx_id = idx.get("scene_index_id") or idx.get("id")
        if not idx_id:
            continue
        try:
            video.delete_scene_index(idx_id)
            removed += 1
            logger.info(
                "post-analysis: deleted stuck audio index source=%s id=%s status=%s",
                source_id,
                idx_id,
                status,
            )
        except Exception as e:
            logger.warning("post-analysis: delete_scene_index failed for id=%s: %r", idx_id, e)
    return removed


def kick_off_audio_index(video: Any, source_id: str, *, force: bool = False) -> None:
    """Fire-and-forget audio index on a freshly-uploaded video.

    Five things happen, in order:
      1. List existing audio-named indexes on the video.
      2. If `force=True`, purge any audio indexes regardless of status
         (used by the explicit "Re-index Audio" CTA).
      3. If a ready audio index already exists AND `force` is False,
         no-op (idempotent).
      4. Purge stuck `processing` audio indexes — VideoDB will never
         finish them if there's no transcript, and they clutter the UI.
      5. Verify the clip has a transcript. VideoDB's `index_audio` is
         transcript-based (`extraction_type=SceneExtractionType.transcript`,
         confirmed in the SDK source) — for a silent clip it would hang
         in `processing` forever. Skip audio kickoff on silent clips
         and let the post-analysis sweep fall back to the visual index.
      6. Otherwise, call `video.index_audio` with the audio prompt.

    Synchronous worker — call from a thread via `asyncio.to_thread`.
    """
    from wildwatch.prompts import format_prompt

    try:
        prompt = format_prompt("audio", **_DEFAULT_PROMPT_CONTEXT)
    except Exception as e:
        logger.warning("post-analysis: audio prompt format failed for %s: %r", source_id, e)
        return

    audio_idxs = _audio_indexes(video)

    if force:
        # Purge ALL audio indexes (ready + processing) so the rebuild starts clean.
        for idx in audio_idxs:
            idx_id = idx.get("scene_index_id") or idx.get("id")
            if not idx_id:
                continue
            try:
                video.delete_scene_index(idx_id)
                logger.info(
                    "post-analysis: deleted audio index source=%s id=%s (force=True)",
                    source_id,
                    idx_id,
                )
            except Exception as e:
                logger.warning(
                    "post-analysis: force delete_scene_index failed id=%s: %r", idx_id, e
                )
        audio_idxs = []
    else:
        # Already have a ready audio index? Idempotent skip.
        for idx in audio_idxs:
            status = str(idx.get("status", "")).lower()
            if status in _READY:
                logger.info(
                    "post-analysis: source=%s already has a ready audio index; skipping",
                    source_id,
                )
                return
        # Otherwise, purge stuck ones before deciding.
        purge_stuck_audio_indexes(video, source_id)

    # Gate on transcript — VideoDB's index_audio is transcript-based, so
    # a silent / SFX-only clip will leave the index stuck in `processing`
    # forever waiting for transcript segments that never come.
    if not _has_transcript(video, source_id):
        logger.info(
            "post-analysis: source=%s has no transcript (silent / SFX-only clip); "
            "skipping audio index — VideoDB's index_audio is transcript-based and "
            "cannot classify wordless sound events. The visual index will still "
            "be searched for events.",
            source_id,
        )
        return

    try:
        idx_id = video.index_audio(prompt=prompt, name=f"wildwatch-audio-{source_id[:8]}")
    except Exception as e:
        logger.warning("post-analysis: index_audio kickoff failed for %s: %r", source_id, e)
        return
    logger.info("post-analysis: kicked off audio index for source=%s idx=%s", source_id, idx_id)


async def _wait_for_index(video: Any, index_name_substr: str, max_wait_s: int) -> dict | None:
    """Poll ``video.list_scene_index()`` until an index whose name contains
    ``index_name_substr`` is in a ready state.

    Returns the index metadata dict, or ``None`` if the deadline is hit or
    the index never appears.
    """
    deadline = time.time() + max_wait_s
    last_status: str | None = None
    while time.time() < deadline:
        try:
            idxs = await asyncio.to_thread(video.list_scene_index) or []
        except Exception as e:
            logger.debug("post-analysis: list_scene_index poll failed: %r", e)
            await asyncio.sleep(_POLL_INTERVAL_S)
            continue
        target = next(
            (i for i in idxs if index_name_substr.lower() in str(i.get("name", "")).lower()),
            None,
        )
        if target is None:
            await asyncio.sleep(_POLL_INTERVAL_S)
            continue
        status = str(target.get("status", "unknown")).lower()
        if status != last_status:
            logger.info("post-analysis: %s index status=%s", index_name_substr, status)
            last_status = status
        if status in _READY:
            return target
        if status in ("failed", "error"):
            logger.warning(
                "post-analysis: %s index failed (status=%s); giving up",
                index_name_substr,
                status,
            )
            return None
        await asyncio.sleep(_POLL_INTERVAL_S)
    logger.info(
        "post-analysis: %s index did not finish within %ss; giving up",
        index_name_substr,
        max_wait_s,
    )
    return None


async def _search_index(
    video: Any, scene_index_id: str, query: str, score_threshold: float = 0.35
) -> list[Any]:
    """Run a per-index scene-search and return its shots (or `[]`)."""
    try:
        from videodb import IndexType, SearchType  # late import
    except Exception:
        IndexType = SearchType = None  # type: ignore[assignment]
    kwargs: dict[str, Any] = {
        "query": query,
        "score_threshold": score_threshold,
        "scene_index_id": scene_index_id,
    }
    if IndexType is not None:
        kwargs["index_type"] = IndexType.scene
    if SearchType is not None:
        kwargs["search_type"] = SearchType.semantic
    try:
        result = await asyncio.to_thread(lambda: video.search(**kwargs))
    except Exception as e:
        if "No results found" in str(e):
            return []
        logger.debug("post-analysis: search %r failed: %r", query, e)
        return []
    return list(getattr(result, "shots", None) or [])


async def _fire_synthesised(
    client: httpx.AsyncClient,
    base_url: str,
    *,
    tier: int,
    label: str,
    explanation: str,
    score: float,
    video_id: str,
    start: float,
    end: float,
    stream_url: str | None,
    source_id: str,
) -> None:
    payload = {
        "event_id": f"upload-{source_id[:8]}-{label.lower()}-{int(start)}-{int(end)}",
        "label": label,
        "tier": tier,
        "confidence": round(min(max(float(score), 0.0), 1.0), 3),
        "explanation": explanation,
        "video_id": video_id,
        "start_time": start,
        "end_time": end,
        "stream_url": stream_url or "",
    }
    try:
        r = await client.post(f"{base_url}/webhook/{tier}", json=payload, timeout=10.0)
        if r.status_code >= 400:
            logger.warning(
                "post-analysis: webhook %s/{tier=%s} returned %s: %s",
                base_url,
                tier,
                r.status_code,
                r.text[:200],
            )
        else:
            logger.info(
                "post-analysis: fired synthesised %s (tier %s) for source=%s",
                label,
                tier,
                source_id,
            )
    except Exception as e:
        logger.warning("post-analysis: webhook POST failed: %r", e)


async def _generate_clip_url(video: Any, start: float, end: float) -> str | None:
    """Get a playable HLS URL for the matched segment (best-effort)."""
    try:
        return await asyncio.to_thread(
            lambda: video.generate_stream(timeline=[(float(start), float(end))])
        )
    except Exception as e:
        logger.debug("post-analysis: generate_stream failed: %r", e)
        return None


async def run_post_upload_analysis(video: Any, source_id: str) -> None:
    """Wait for indexes, search each event, synthesise webhooks on hits.

    Spawned as a fire-and-forget task from ``ingest.dispatch``. Total
    wall-time is bounded by the index-wait deadlines + a small search
    budget; will exit cleanly on cancellation.
    """
    logger.info("post-analysis: starting for source=%s video=%s", source_id, video.id)

    # Wait for the species index (visuals — already kicked off by ingest).
    species_idx = await _wait_for_index(video, "wildwatch-auto", _INDEX_WAIT_S)
    # Wait for the audio index in parallel; gracefully tolerate it never
    # appearing (e.g. silent clip — VideoDB sometimes errors out).
    audio_idx = await _wait_for_index(video, "wildwatch-audio", _AUDIO_WAIT_S)

    if species_idx is None and audio_idx is None:
        logger.info(
            "post-analysis: neither index ready for source=%s; nothing to analyse",
            source_id,
        )
        return

    # Build event id_var → definition map for tier/label lookup.
    by_id_var = {ev["id_var"]: ev for ev in EVENT_DEFINITIONS}

    base_url = _local_base_url()
    fired = 0
    async with httpx.AsyncClient() as client:
        for id_var, query in _EVENT_QUERY.items():
            if fired >= _MAX_FIRES_PER_UPLOAD:
                logger.info(
                    "post-analysis: hit fire cap (%s) for source=%s; stopping",
                    _MAX_FIRES_PER_UPLOAD,
                    source_id,
                )
                break
            kind = _EVENT_INDEX_KIND.get(id_var)
            ev = by_id_var.get(id_var)
            if ev is None or kind is None:
                continue
            # Pick the right index per event kind, with a fallback: audio
            # events run against the audio index when available, else the
            # visual index (where scene descriptions may mention weapons,
            # vehicles, etc. for clips with no transcript). Visual events
            # always run against the visual scene index.
            if kind == "audio":
                idx_meta = audio_idx or species_idx
                if audio_idx is None and species_idx is not None:
                    logger.debug(
                        "post-analysis: audio index unavailable for %s; falling "
                        "back to visual index for event=%s",
                        source_id,
                        id_var,
                    )
            else:  # species/environment both live on the species visual index
                idx_meta = species_idx
            if idx_meta is None:
                continue
            scene_index_id = idx_meta.get("scene_index_id") or idx_meta.get("id") or ""
            if not scene_index_id:
                continue
            shots = await _search_index(video, scene_index_id, query)
            if not shots:
                continue
            # Take the top-scoring shot only (cheap and avoids duplicate-y noise).
            top = max(shots, key=lambda s: getattr(s, "search_score", 0.0) or 0.0)
            start = float(getattr(top, "start", 0.0) or 0.0)
            end = float(getattr(top, "end", start + 5.0) or (start + 5.0))
            score = float(getattr(top, "search_score", 0.0) or 0.0)
            text = (getattr(top, "text", "") or "")[:240]
            stream_url = await _generate_clip_url(video, start, end)
            await _fire_synthesised(
                client,
                base_url,
                tier=int(ev["tier"]),
                label=str(ev["label"]),
                explanation=(
                    f"Auto-detected during post-upload analysis. "
                    f"Query: {query!r}. Best match: {text}"
                ),
                score=score,
                video_id=video.id,
                start=start,
                end=end,
                stream_url=stream_url,
                source_id=source_id,
            )
            fired += 1

    logger.info(
        "post-analysis: complete for source=%s — fired %d synthesised alert(s)",
        source_id,
        fired,
    )
