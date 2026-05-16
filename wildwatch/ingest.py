"""Source ingestion router.

``dispatch(source_id, coll)`` looks up a source from state, picks the
right handler by ``kind``, runs it, and updates the source's status as
it progresses. Each transition is broadcast through the dashboard so
the SPA can show per-source live progress.

Handlers are intentionally thin: they get the source into VideoDB
(uploaded video OR connected rtstream). Indexing + alert wiring are
separate operations a user triggers explicitly via the UI on a ready
source — that keeps the failure surface here small.
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
            }
        )
    except Exception:
        logger.exception("ingest: dashboard.broadcast failed (non-fatal)")


# ──── URL classification helpers ──────────────────────────────────────────


_YT_HOST_RE = re.compile(r"(?:^|//)(?:www\.)?(?:youtube\.com|youtu\.be)/")


def _is_youtube_url(url: str) -> bool:
    return bool(_YT_HOST_RE.search(url))


def _is_youtube_live(url: str) -> bool:
    """Use yt-dlp to detect live status. False if yt-dlp unavailable."""
    if not shutil.which("yt-dlp"):
        return False
    try:
        out = subprocess.run(
            ["yt-dlp", "--simulate", "--no-warnings", "--print", "%(is_live)s", url],
            capture_output=True,
            text=True,
            timeout=20,
        )
        return out.stdout.strip().lower() == "true"
    except Exception:
        return False


# ──── handlers ────────────────────────────────────────────────────────────


async def _ingest_upload(source, coll: Any) -> None:
    _emit(source.id, "ingesting", stage_msg="uploading file to VideoDB")
    video = await asyncio.to_thread(coll.upload, file_path=source.input)
    if video is None:
        raise RuntimeError("coll.upload returned None")
    _emit(
        source.id,
        "ready",
        stage_msg=f"uploaded {getattr(video, 'length', '?')}s",
        video_id=video.id,
    )


async def _ingest_url(source, coll: Any) -> None:
    _emit(source.id, "ingesting", stage_msg=f"uploading {source.kind} URL to VideoDB")
    video = await asyncio.to_thread(coll.upload, url=source.input)
    if video is None:
        raise RuntimeError("coll.upload returned None")
    _emit(
        source.id,
        "ready",
        stage_msg=f"uploaded {getattr(video, 'length', '?')}s",
        video_id=video.id,
    )


async def _ingest_rtstream(source, coll: Any) -> None:
    _emit(source.id, "connecting", stage_msg=f"connect_rtstream({source.input})")
    rt = await asyncio.to_thread(
        coll.connect_rtstream,
        url=source.input,
        name=source.name,
        media_types=["video", "audio"],
        store=True,
    )
    _emit(
        source.id,
        "ready",
        stage_msg=f"rtstream status={getattr(rt, 'status', '?')}",
        rtstream_id=rt.id,
    )


async def _ingest_youtube(source, coll: Any) -> None:
    _emit(source.id, "connecting", stage_msg="probing youtube live status")
    is_live = await asyncio.to_thread(_is_youtube_live, source.input)
    if is_live:
        raise RuntimeError(
            "YouTube live URL detected — needs streamlink+ffmpeg+mediamtx bridge "
            "(not handled by dispatch). Use scripts/start_live_test.py or paste the "
            "bridge-public RTSP URL instead."
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
