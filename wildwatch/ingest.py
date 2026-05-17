"""Source ingestion router.

``dispatch(source_id, coll)`` looks up a source from state, picks the
right handler by ``kind``, runs it, and updates the source's status as
it progresses. Each transition is broadcast through the dashboard so
the SPA can show per-source live progress.

For file / URL uploads (`_ingest_upload`, `_ingest_url`) the handler
ALSO kicks off a scene index automatically — the video is unsearchable
until at least one scene index exists, and asking users to run a CLI
script after every upload was the wrong UX. The index call is fire-
and-forget; the dashboard's Indexed Content tab shows the index in
``processing`` state until VideoDB finishes.

For live rtstreams (`_ingest_rtstream`) scene indexing is handled
separately by ``scripts/bootstrap.py`` which wires four prompt-based
indexes (species / behavior / environment / audio) plus alerts.
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import subprocess
from typing import Any

from wildwatch import dashboard, sources

logger = logging.getLogger(__name__)


# ──── progress helper ─────────────────────────────────────────────────────


def _emit(source_id: str, status: str, stage_msg: str | None = None, **extra: Any) -> None:
    """Persist + broadcast a status transition."""
    fields: dict[str, Any] = {"status": status}
    if stage_msg is not None:
        fields["stage_msg"] = stage_msg
    fields.update(extra)
    try:
        src = sources.update_source(source_id, **fields)
    except KeyError:
        logger.warning("ingest: source %s vanished during dispatch", source_id)
        return
    try:
        dashboard.broadcast(
            {
                "type": "source_progress",
                "source_id": source_id,
                "status": src.status,
                "stage_msg": src.stage_msg,
                "progress_pct": src.progress_pct,
                "kind": src.kind,
                "name": src.name,
                "video_id": src.video_id,
                "rtstream_id": src.rtstream_id,
                "error": src.error,
                "bridge_command": src.bridge_command,
                "bridge_rtsp": src.bridge_rtsp,
            }
        )
    except Exception:
        logger.exception("ingest: dashboard.broadcast failed (non-fatal)")


# ──── URL classification helpers ──────────────────────────────────────────


_YT_HOST_RE = re.compile(r"(?:^|//)(?:www\.)?(?:youtube\.com|youtu\.be)/")


def _is_youtube_url(url: str) -> bool:
    return bool(_YT_HOST_RE.search(url))


def _is_youtube_live(url: str) -> bool | None:
    """Probe YouTube URL for live status via yt-dlp.

    Returns:
        True  — confirmed live.
        False — confirmed VOD (or yt-dlp not installed; operator opted out).
        None  — probe failed (timeout, non-zero exit, missing output).
                Caller must surface this to the operator and pick a default
                rather than silently treating it as ``False``.

    Pre-fix this function swallowed every error as ``False``, so a live URL
    that failed the probe routed to archive-mode upload and failed deep
    inside VideoDB with no indication the probe was the root cause.
    """
    if not shutil.which("yt-dlp"):
        # yt-dlp absent is an operator choice, not a transient error.
        return False
    try:
        out = subprocess.run(
            ["yt-dlp", "--simulate", "--no-warnings", "--print", "%(is_live)s", url],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except Exception as e:
        logger.warning("yt-dlp probe failed for %s: %r", url, e)
        return None
    if out.returncode != 0:
        logger.warning(
            "yt-dlp probe non-zero exit (%d) for %s: stderr=%r",
            out.returncode,
            url,
            (out.stderr or "")[:200],
        )
        return None
    val = out.stdout.strip().lower()
    if val == "true":
        return True
    if val == "false":
        return False
    # Unrecognised output (e.g. "NA" for non-YouTube URL or empty for error)
    logger.warning("yt-dlp probe gave unexpected output for %s: %r", url, val)
    return None


# ──── handlers ────────────────────────────────────────────────────────────


_DEFAULT_SCENE_PROMPT_CONTEXT = {
    "location_context": "uploaded clip (any environment)",
    "species_list": (
        "common wildlife — oryx, springbok, elephant, lion, giraffe, zebra, "
        "leopard, hyena, jackal, kudu, buffalo, hippo, crocodile, baboon, "
        "warthog, wild dog, various birds. If no wildlife, describe what is "
        "in the scene."
    ),
    "expected_sounds": "any ambient sound",
}


async def _kick_off_scene_index(video, source_id: str) -> None:
    """Fire-and-forget scene index on a freshly-uploaded video.

    The SDK call returns immediately with an index_id; the actual
    indexing runs on VideoDB's side and shows up as `processing` then
    `done` when polled via `video.list_scene_index()`. The dashboard's
    Indexed Content tab reads that status and renders the right state.

    Failures are logged but NEVER propagated — a working upload that
    couldn't be indexed is still useful (operator can retry via
    `scripts/index_corpus.py`). The source row stays `ready`.
    """
    from wildwatch.prompts import format_prompt

    try:
        prompt = format_prompt("species", **_DEFAULT_SCENE_PROMPT_CONTEXT)
    except Exception as e:
        logger.warning("ingest: prompt format failed for %s: %r", source_id, e)
        return

    def _call() -> Any:
        # Skip if the video already has any scene index (idempotent).
        try:
            existing = video.list_scene_index() or []
        except Exception as e:
            logger.warning("ingest: list_scene_index probe failed for source=%s: %r", source_id, e)
            existing = []
        if existing:
            logger.info(
                "ingest: source=%s already has %d scene index(es); skipping auto-index",
                source_id,
                len(existing),
            )
            return None
        return video.index_scenes(prompt=prompt, name=f"wildwatch-auto-{source_id[:8]}")

    try:
        idx_id = await asyncio.to_thread(_call)
    except Exception as e:
        logger.warning(
            "ingest: index_scenes kickoff failed for source=%s: %r — video usable but unsearchable until re-indexed",
            source_id,
            e,
        )
        return
    if idx_id:
        logger.info("ingest: kicked off scene index for source=%s idx=%s", source_id, idx_id)


async def _kick_off_audio_index_async(video, source_id: str) -> None:
    """Async wrapper around the post_upload_analysis audio-index kickoff.

    Same fire-and-forget contract as ``_kick_off_scene_index`` — failures
    are logged but never propagate.
    """
    from wildwatch.post_upload_analysis import kick_off_audio_index

    try:
        await asyncio.to_thread(kick_off_audio_index, video, source_id)
    except Exception as e:
        logger.warning(
            "ingest: audio index kickoff failed for source=%s: %r — video usable but no audio alerts",
            source_id,
            e,
        )


# Strong refs to background analysis tasks so the GC can't drop them
# mid-execution. Tasks are popped from the set when they finish.
_post_analysis_tasks: set[asyncio.Task[Any]] = set()


def _spawn_post_upload_analysis(video, source_id: str) -> None:
    """Spawn the Path-B post-upload event sweep as a tracked bg task.

    The function returns immediately. The task lives for as long as the
    audio + species indexes need to reach `done` (capped at 20 min) plus
    a short search budget. Errors surface through ``done_callback`` log
    instead of an unawaited-coroutine warning.
    """
    from wildwatch.post_upload_analysis import run_post_upload_analysis

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning(
            "ingest: cannot spawn post-upload analysis (no running loop) for %s",
            source_id,
        )
        return

    task = loop.create_task(run_post_upload_analysis(video, source_id))
    _post_analysis_tasks.add(task)

    def _on_done(t: asyncio.Task[Any]) -> None:
        _post_analysis_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            logger.warning(
                "ingest: post-upload analysis task for %s raised: %r",
                source_id,
                exc,
            )

    task.add_done_callback(_on_done)


async def _ingest_upload(source, coll: Any) -> None:
    _emit(source.id, "ingesting", stage_msg="uploading file to VideoDB")
    video = await asyncio.to_thread(coll.upload, file_path=source.input)
    if video is None:
        raise RuntimeError("coll.upload returned None")
    # Auto-kick scene indexing so the video becomes searchable without
    # the operator running a CLI script. Status pulse so the dashboard
    # shows "indexing" briefly before flipping to "ready".
    _emit(
        source.id,
        "indexing",
        stage_msg="kicking off scene + audio indexes on VideoDB",
        video_id=video.id,
    )
    await _kick_off_scene_index(video, source.id)
    await _kick_off_audio_index_async(video, source.id)
    _emit(
        source.id,
        "ready",
        stage_msg=(
            f"uploaded {getattr(video, 'length', '?')}s — "
            "scene + audio indexes processing; auto-analysis running in background"
        ),
        video_id=video.id,
    )
    _spawn_post_upload_analysis(video, source.id)


async def _ingest_url(source, coll: Any) -> None:
    _emit(source.id, "ingesting", stage_msg=f"uploading {source.kind} URL to VideoDB")
    video = await asyncio.to_thread(coll.upload, url=source.input)
    if video is None:
        raise RuntimeError("coll.upload returned None")
    _emit(
        source.id,
        "indexing",
        stage_msg="kicking off scene + audio indexes on VideoDB",
        video_id=video.id,
    )
    await _kick_off_scene_index(video, source.id)
    await _kick_off_audio_index_async(video, source.id)
    _emit(
        source.id,
        "ready",
        stage_msg=(
            f"uploaded {getattr(video, 'length', '?')}s — "
            "scene + audio indexes processing; auto-analysis running in background"
        ),
        video_id=video.id,
    )
    _spawn_post_upload_analysis(video, source.id)


_RT_READY_STATUSES = {"connected", "streaming", "running", "active"}
_RT_ERROR_STATUSES = {"error", "failed", "disconnected"}


async def _ingest_rtstream(source, coll: Any) -> None:
    _emit(source.id, "connecting", stage_msg=f"connect_rtstream({source.input})")
    rt = await asyncio.to_thread(
        coll.connect_rtstream,
        url=source.input,
        name=source.name,
        media_types=["video", "audio"],
        store=True,
    )
    rt_status = str(getattr(rt, "status", "") or "").lower()
    # Map rt.status to source status explicitly — don't blindly claim 'ready'
    # when the SDK still has the rtstream in 'pending'/'error'/etc.
    if rt_status in _RT_ERROR_STATUSES:
        _emit(
            source.id,
            "error",
            stage_msg=f"rtstream status={rt_status}",
            rtstream_id=rt.id,
            error=f"rtstream reported status={rt_status} after connect",
        )
    elif rt_status in _RT_READY_STATUSES:
        _emit(
            source.id,
            "ready",
            stage_msg=f"rtstream status={rt_status}",
            rtstream_id=rt.id,
        )
    else:
        # 'pending', '', unknown — surface but don't lie about readiness.
        _emit(
            source.id,
            "ingesting",
            stage_msg=f"rtstream status={rt_status or 'unknown'} (waiting)",
            rtstream_id=rt.id,
        )


async def _ingest_youtube(source, coll: Any) -> None:
    _emit(source.id, "connecting", stage_msg="probing youtube live status")
    is_live = await asyncio.to_thread(_is_youtube_live, source.input)
    if is_live is True:
        # Live YouTube can't be handed to VideoDB directly — it only accepts
        # rtsp:// / rtmp:// for live streams. Don't error out; park the
        # source in `needs_bridge` status. The dashboard's renderSource
        # picks this up and shows a copy-paste helper card with the exact
        # command + an input to paste the bridge RTSP URL back.
        #
        # Use a short slug derived from the source id so concurrent bridges
        # on the same host don't collide on /stream-name.
        slug = source.id[:8]
        bridge_cmd = f'./bridge/start_bridge.sh "{source.input}" {slug}'
        bridge_rtsp = f"rtsp://localhost:8554/{slug}"
        _emit(
            source.id,
            "needs_bridge",
            stage_msg=(
                "Live YouTube needs an RTSP bridge — VideoDB only accepts "
                "rtsp:// for live streams. Run the bridge command in a new "
                "terminal, then paste the resulting RTSP URL back."
            ),
            bridge_command=bridge_cmd,
            bridge_rtsp=bridge_rtsp,
        )
        return
    if is_live is None:
        # Probe failed — fall through to archive-mode upload but surface a
        # warning so the operator can see the source booted on incomplete
        # info. If the URL was actually live, _ingest_url will fail loud.
        _emit(
            source.id,
            "connecting",
            stage_msg="yt-dlp probe failed; treating as archive (may fail if live)",
        )
    await _ingest_url(source, coll)


# ──── dispatch ────────────────────────────────────────────────────────────


async def dispatch(source_id: str, coll: Any) -> Any:
    src = sources.get_source(source_id)
    if src is None:
        raise KeyError(source_id)

    _emit(source_id, "connecting", stage_msg=f"starting {src.kind} handler")
    try:
        if src.kind == "upload":
            await _ingest_upload(src, coll)
        elif src.kind == "hls":
            await _ingest_url(src, coll)
        elif src.kind in ("rtsp", "rtmp"):
            await _ingest_rtstream(src, coll)
        elif src.kind == "youtube":
            await _ingest_youtube(src, coll)
        else:
            raise ValueError(f"unsupported kind: {src.kind}")
    except Exception as e:
        logger.exception("ingest: dispatch failed for source %s", source_id)
        _emit(source_id, "error", stage_msg="dispatch failed", error=str(e))
    return sources.get_source(source_id)
