"""FastAPI webhook receiver: accept VideoDB alert callbacks, fan to Telegram.

VideoDB POSTs to a public URL we register via ``index.create_alert(
callback_url=...)``. We mount one path per tier so the same handler can
route by URL segment (cheaper than parsing payload + looking up tier).

Run locally:
    uvicorn wildwatch.webhooks:app --reload --port 8000

Expose to VideoDB:
    cloudflared tunnel --url http://localhost:8000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import asynccontextmanager
from pathlib import Path as PPath
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

import aiofiles
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Path, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

# Load .env at import time so uvicorn-launched processes see TELEGRAM_*
# and other vars without requiring the operator to pre-export them.
load_dotenv()

from wildwatch import dashboard, event_log, ingest, sources  # noqa: E402
from wildwatch.telegram import send_alert  # noqa: E402

logger = logging.getLogger(__name__)

UPLOAD_MAX_BYTES = 500 * 1024 * 1024  # 500 MB

# Strong refs to in-flight background dispatch tasks so the GC doesn't drop
# them mid-flight (RUF006). Each task removes itself via a done_callback that
# ALSO logs exceptions — otherwise a failed task only surfaces as
# "Task exception was never retrieved" at GC time.
_BG_TASKS: set[asyncio.Task] = set()


def _spawn_bg(coro, *, label: str) -> asyncio.Task:
    """Create + track a background task with auto-cleanup and error logging.

    Without the done_callback, ``_BG_TASKS`` grows unbounded and any
    exception raised inside the task is swallowed until the garbage
    collector eventually emits a warning.
    """
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)

    def _on_done(t: asyncio.Task) -> None:
        _BG_TASKS.discard(t)
        if t.cancelled():
            return  # operator-initiated, not an error
        exc = t.exception()
        if exc is not None:
            logger.error("background task %s failed: %r", label, exc, exc_info=exc)

    task.add_done_callback(_on_done)
    return task


# Cache for /api/remote: list_rtstreams + list_sandboxes (10s TTL to avoid
# hammering the SDK from dashboard polls). Locks guard the read-TTL-then-
# write pattern from concurrent route handlers — without them the same
# stale entry can be re-fetched in parallel by two requests racing past
# the TTL check.
_remote_cache: dict = {"at": 0.0, "data": {"rtstreams": [], "sandboxes": []}}
_REMOTE_TTL_S = 10.0
_remote_lock = threading.Lock()

# Cache for /api/videos (30s TTL).
_videos_cache: dict = {"at": 0.0, "data": {"videos": []}}
_VIDEOS_TTL_S = 30.0
_videos_lock = threading.Lock()

# Cache for /api/usage (60s TTL — billing endpoints are slow).
# `data` defaults to None (not {}) so the `data is not None` check below is
# unambiguous — an empty-dict response would otherwise miss the cache and
# hammer the SDK on every poll.
_usage_cache: dict = {"at": 0.0, "data": None}
_USAGE_TTL_S = 60.0
_usage_lock = threading.Lock()

# Bounded executor for SDK calls that lack native timeouts (VideoDB SDK is
# blocking and offers no per-call deadline). Created LAZILY on first use so
# importing this module (e.g. from pytest) doesn't spawn non-daemon worker
# threads that hold up interpreter exit. atexit + lifespan shut it down
# cleanly when it has been created.
_SDK_EXECUTOR: ThreadPoolExecutor | None = None
_executor_lock = threading.Lock()


def _get_executor() -> ThreadPoolExecutor:
    global _SDK_EXECUTOR
    if _SDK_EXECUTOR is None:
        with _executor_lock:
            if _SDK_EXECUTOR is None:
                _SDK_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sdk")
                import atexit

                atexit.register(
                    lambda: (
                        _SDK_EXECUTOR and _SDK_EXECUTOR.shutdown(wait=False, cancel_futures=True)
                    )
                )
    return _SDK_EXECUTOR


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """FastAPI lifespan: clean up the SDK executor at shutdown so uvicorn
    reload / SIGINT doesn't leak worker threads."""
    try:
        yield
    finally:
        if _SDK_EXECUTOR is not None:
            _SDK_EXECUTOR.shutdown(wait=False, cancel_futures=True)


app = FastAPI(title="WildWatch webhook receiver", lifespan=_lifespan)


def _with_timeout(fn, *args, timeout_s: float = 5.0, **kwargs):
    """Run a blocking SDK call with a hard deadline; raises TimeoutError.

    Includes the callable name in the timeout message so debugging a hung
    /api/usage doesn't require grepping for which of five SDK calls
    actually timed out.
    """
    fut = _get_executor().submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=timeout_s)
    except FutureTimeoutError as e:
        fut.cancel()
        fn_name = getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
        raise TimeoutError(f"SDK call {fn_name!r} timed out after {timeout_s}s") from e


async def _async_sdk(fn, *args, timeout_s: float = 5.0, **kwargs):
    """Async wrapper: dispatch a blocking SDK call to the thread pool
    without blocking the asyncio event loop. Use this from `async def`
    route handlers. `_with_timeout` is fine from sync handlers."""
    return await asyncio.to_thread(_with_timeout, fn, *args, timeout_s=timeout_s, **kwargs)


# ── CSRF / cross-origin guard for mutating endpoints ────────────────────
#
# FastAPI is bound to 0.0.0.0 in the docker compose / quickstart path so any
# browser on the LAN can reach it. Without an Origin check, a malicious page
# anywhere on that LAN can fire POST /api/sources or DELETE /api/sources/{id}
# against http://<host-lan-ip>:8000 by the user's browser. Mitigate by
# rejecting mutating requests whose Origin/Referer doesn't match a known-good
# host. GET/HEAD/OPTIONS are skipped — they don't change state.

_ALLOWED_ORIGIN_HOSTS = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
}
# Operators can extend with WILDWATCH_ALLOWED_ORIGINS=hostA,hostB
for _extra in (os.getenv("WILDWATCH_ALLOWED_ORIGINS") or "").split(","):
    _extra = _extra.strip()
    if _extra:
        _ALLOWED_ORIGIN_HOSTS.add(_extra)

# Paths that legitimately receive cross-origin POSTs and must NOT be guarded:
# the VideoDB webhook callback comes from VideoDB's own infra.
_CSRF_EXEMPT_PATH_PREFIXES = ("/webhook/",)


@app.middleware("http")
async def _csrf_origin_guard(request: Request, call_next):
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return await call_next(request)
    path = request.url.path
    if any(path.startswith(p) for p in _CSRF_EXEMPT_PATH_PREFIXES):
        return await call_next(request)

    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin:
        # DEFAULT-DENY on missing Origin for mutating requests. The previous
        # UA-sniff bypass let any client without "mozilla/chrome/..." in its
        # UA through (curl, browser extensions, service workers, attackers
        # spoofing UA), defeating the purpose. CLI users can add their host
        # via WILDWATCH_ALLOW_NO_ORIGIN=1 if they really need it.
        if os.environ.get("WILDWATCH_ALLOW_NO_ORIGIN") == "1":
            return await call_next(request)
        return JSONResponse(
            status_code=403,
            content={
                "detail": (
                    "missing Origin/Referer on mutating request. "
                    "Set WILDWATCH_ALLOW_NO_ORIGIN=1 for trusted CLI use."
                )
            },
        )
    try:
        host = (urlparse(origin).hostname or "").lower()
    except Exception:
        return JSONResponse(status_code=403, content={"detail": "bad Origin"})
    if host not in _ALLOWED_ORIGIN_HOSTS:
        return JSONResponse(
            status_code=403,
            content={"detail": f"Origin {host!r} not allowed"},
        )
    return await call_next(request)


class AlertPayload(BaseModel):
    """Subset of VideoDB callback payload we actually consume."""

    label: str = Field(..., description="Event label (e.g. POACHING_ALERT_GUNSHOT)")
    event_id: str | None = None
    confidence: float | None = None
    explanation: str | None = None
    timestamp: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    stream_url: str | None = None

    # `extra="ignore"` rather than "allow": we build the persisted record
    # explicitly from named fields below, so extras would be silently
    # dropped anyway — `ignore` makes that contract clear and prevents a
    # future `payload.model_dump()` call from accidentally splashing
    # unknown attacker-controlled fields into the SSE feed.
    model_config = {"extra": "ignore"}


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/webhook/{tier}")
async def receive_alert(
    tier: Annotated[int, Path(ge=1, le=3, description="Alert tier 1=info, 2=notable, 3=urgent")],
    payload: AlertPayload,
) -> dict:
    # Log the alert BEFORE attempting Telegram delivery so the digest
    # builder still sees the event even if Telegram is down.
    record = {
        "received_at": time.time(),
        "tier": tier,
        "label": payload.label,
        "event_id": payload.event_id,
        "confidence": payload.confidence,
        "explanation": payload.explanation,
        "timestamp": payload.timestamp,
        "start_time": payload.start_time,
        "end_time": payload.end_time,
        "stream_url": payload.stream_url,
    }
    try:
        event_log.append(record)
    except Exception:
        logger.exception("event_log.append failed; alert will still attempt delivery")

    # Push to the in-memory dashboard broadcaster (SSE subscribers + stats).
    try:
        dashboard.broadcast(record)
    except Exception:
        logger.exception("dashboard.broadcast failed (non-fatal)")

    try:
        await send_alert(
            tier=tier,
            label=payload.label,
            explanation=payload.explanation,
            stream_url=payload.stream_url,
        )
    except Exception as e:
        # Log with full traceback so the operator can see WHY a VideoDB
        # callback didn't reach the phone. Re-raise as 500 so VideoDB's
        # retry logic engages (it backs off + retries on 5xx).
        logger.exception(
            "send_alert failed for tier=%s label=%s event_id=%s",
            tier,
            payload.label,
            payload.event_id,
        )
        raise HTTPException(status_code=500, detail="send_alert failed; see server logs") from e
    return {"status": "received"}


# ──── Dashboard routes ────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
def dashboard_index() -> str:
    return dashboard.get_dashboard_html()


@app.get("/api/stats")
def api_stats() -> dict:
    return dashboard.get_stats()


@app.get("/api/remote")
async def api_remote() -> JSONResponse:
    """List active VideoDB rtstreams + sandboxes (cached 10s).

    Async so the blocking SDK call doesn't park the event loop.
    """
    now = time.time()
    # Short-lock just for the TTL check; the SDK call itself happens
    # outside the lock so concurrent cache misses can both wait on the
    # SDK rather than serialise on the lock.
    with _remote_lock:
        if (now - _remote_cache["at"]) < _REMOTE_TTL_S:
            return JSONResponse(_remote_cache["data"])
    try:
        coll = _get_coll()
        rts = await _async_sdk(coll.list_rtstreams, timeout_s=5.0) or []
        sbs = await _async_sdk(_get_conn().list_sandboxes, timeout_s=5.0) or []
        rtstreams = [
            {"id": rt.id, "name": getattr(rt, "name", "?"), "status": getattr(rt, "status", "?")}
            for rt in rts
        ]
        sandboxes = [
            {
                "id": sb.id,
                "tier": str(getattr(sb, "tier", "?")),
                "status": getattr(sb, "status", "?"),
                "is_active": bool(getattr(sb, "is_active", False)),
            }
            for sb in sbs
        ]
        data = {"rtstreams": rtstreams, "sandboxes": sandboxes}
        with _remote_lock:
            _remote_cache.update({"at": now, "data": data})
        return JSONResponse(data)
    except Exception as e:
        logger.warning("api_remote SDK call failed: %s: %s", type(e).__name__, e)
        # Cache the error short-window so concurrent polls don't hammer
        # during an outage, but RETURN 502 so the dashboard surfaces a
        # visible degraded state rather than rendering an empty list as
        # if "no streams" were the truth.
        error_payload = {"rtstreams": [], "sandboxes": [], "error": str(e)}
        with _remote_lock:
            _remote_cache.update({"at": now, "data": error_payload})
        return JSONResponse(error_payload, status_code=502)


@app.get("/events/stream")
async def events_stream() -> StreamingResponse:
    """SSE feed — pushes each new webhook event as it arrives."""

    async def gen():
        # Initial keepalive so the browser knows the connection is open
        yield b": connected\n\n"
        try:
            async for ev in dashboard.subscribe():
                # Per-event try/except so one non-JSON-serializable payload
                # can't kill the whole SSE stream (forcing every dashboard
                # client to reconnect + losing future events in the window).
                try:
                    encoded = f"data: {json.dumps(ev)}\n\n".encode()
                except (TypeError, ValueError) as e:
                    logger.warning(
                        "SSE event not JSON-serializable; skipped: %r (event keys=%s)",
                        e,
                        list(ev.keys()) if isinstance(ev, dict) else type(ev).__name__,
                    )
                    continue
                yield encoded
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ──── Source CRUD routes ──────────────────────────────────────────────────


class SourceCreate(BaseModel):
    # `upload` excluded — uploads use the dedicated multipart endpoint
    # /api/sources/upload. Narrowing here means Pydantic rejects "upload"
    # at the parse layer with a clear 422 instead of allowing it through
    # to be rejected by handler-side checks.
    kind: Literal["youtube", "hls", "rtsp", "rtmp"] = Field(
        ..., description="youtube|hls|rtsp|rtmp"
    )
    input: str
    name: str


# Process-cached VideoDB connection. `videodb.connect()` does an auth round
# trip on every call — recreating it per request blocks the thread on auth
# latency and turns the dashboard into a slow disaster. Lock prevents two
# cold-cache callers from each running `videodb.connect()` and the second
# overwriting the first.
_conn_cache: dict[str, Any] = {"conn": None, "coll": None}
# RLock not Lock — _get_coll acquires the lock and then calls _get_conn
# which acquires it again. A plain threading.Lock would deadlock on the
# same thread. RLock allows recursive acquisition by the holder.
_conn_lock = threading.RLock()


def _get_conn() -> Any:
    if _conn_cache["conn"] is None:
        with _conn_lock:
            # Double-checked locking: the first caller paid the auth cost,
            # subsequent waiters get the cached handle without re-running.
            if _conn_cache["conn"] is None:
                import videodb

                _conn_cache["conn"] = videodb.connect()
    return _conn_cache["conn"]


def _get_coll() -> Any:
    """Lazy-load VideoDB collection, cached for the life of the process."""
    if _conn_cache["coll"] is None:
        with _conn_lock:
            if _conn_cache["coll"] is None:
                _conn_cache["coll"] = _get_conn().get_collection()
    return _conn_cache["coll"]


@app.get("/api/sources")
async def api_list_sources() -> dict:
    # sources.list_sources reads .state.json from disk; off the event loop.
    return await asyncio.to_thread(
        lambda: {"sources": [s.__dict__ for s in sources.list_sources()]}
    )


@app.get("/api/sources/{source_id}")
async def api_get_source(source_id: str) -> dict:
    s = await asyncio.to_thread(sources.get_source, source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    return s.__dict__


@app.post("/api/sources")
async def api_create_source(payload: SourceCreate) -> dict:
    """JSON path: create a URL/RTSP/RTMP/YouTube source, kick off ingest.

    `upload` is rejected at the Pydantic Literal layer — use the dedicated
    multipart endpoint /api/sources/upload.
    """
    try:
        s = sources.add_source(kind=payload.kind, input=payload.input, name=payload.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    coll = _get_coll()
    _spawn_bg(ingest.dispatch(s.id, coll=coll), label=f"ingest.dispatch({s.id})")
    return s.__dict__


def _looks_like_video(head: bytes) -> bool:
    """Magic-byte sniff for the common video container formats.

    Without this the upload endpoint accepts any bytes, renames them to
    `.mp4`, and hands them to VideoDB. An attacker can upload HTML/script
    that any downstream player or VideoDB previewer might interpret.
    Cheap defence: require the first 32 bytes to match a known video
    container signature.
    """
    if len(head) < 12:
        return False
    # MP4 / MOV / M4V / 3GP — 'ftyp' box at offset 4
    if head[4:8] == b"ftyp":
        return True
    # WebM / MKV — EBML signature
    if head[:4] == b"\x1a\x45\xdf\xa3":
        return True
    # AVI — RIFF....AVI
    if head[:4] == b"RIFF" and head[8:12] == b"AVI ":
        return True
    # MPEG-TS — sync byte every 188 bytes; first byte 0x47
    if head[:1] == b"\x47":
        return True
    # MPEG-PS / MPEG-1 / MPEG-2 video
    if head[:4] == b"\x00\x00\x01\xba" or head[:4] == b"\x00\x00\x01\xb3":
        return True
    # FLV
    if head[:3] == b"FLV":
        return True
    return False


@app.post("/api/sources/upload")
async def api_upload_source(
    file: Annotated[UploadFile, File(...)],
    name: Annotated[str, Form(...)],
) -> dict:
    """Multipart upload path. Streams to a tempfile, then dispatches."""
    s = sources.add_source(kind="upload", input="", name=name)

    tmp = tempfile.NamedTemporaryFile(
        delete=False, prefix=f"wildwatch-upload-{s.id[:8]}-", suffix=".mp4"
    )
    tmp_path = PPath(tmp.name)
    tmp.close()
    written = 0
    sniff_done = False
    try:
        async with aiofiles.open(tmp_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):
                # Reject obviously non-video uploads on the first chunk.
                # Renaming attacker bytes to `.mp4` and feeding them to
                # VideoDB is a real risk if any downstream service ever
                # treats the file as HTML/JS.
                if not sniff_done:
                    if not _looks_like_video(chunk[:32]):
                        raise HTTPException(
                            status_code=415,
                            detail=(
                                "uploaded file does not look like a supported video "
                                "container (mp4/mov/webm/mkv/avi/ts/mpeg/flv). "
                                "Refusing to forward to VideoDB."
                            ),
                        )
                    sniff_done = True
                written += len(chunk)
                if written > UPLOAD_MAX_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"upload exceeds {UPLOAD_MAX_BYTES // (1024 * 1024)} MB cap",
                    )
                await out.write(chunk)
        sources.update_source(
            s.id,
            input=str(tmp_path),
            stage_msg=f"received {written} bytes; queued for upload",
        )
    except HTTPException as e:
        tmp_path.unlink(missing_ok=True)
        sources.update_source(s.id, status="error", error=str(e.detail))
        raise
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        sources.update_source(s.id, status="error", error=str(e))
        raise

    coll = _get_coll()
    _spawn_bg(ingest.dispatch(s.id, coll=coll), label=f"ingest.dispatch.upload({s.id})")
    return sources.get_source(s.id).__dict__


@app.delete("/api/sources/{source_id}")
async def api_delete_source(source_id: str) -> dict:
    s = await asyncio.to_thread(sources.get_source, source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    # Best-effort remote cleanup (don't fail the local delete if remote fails)
    # but collect every failure so the caller can see remote resources that
    # may still be running (and burning credits). All blocking SDK calls go
    # through asyncio.to_thread so the event loop stays responsive.
    coll = _get_coll()
    warnings: list[str] = []
    if s.rtstream_id:
        try:
            rt = await asyncio.to_thread(coll.get_rtstream, s.rtstream_id)
            await asyncio.to_thread(rt.stop)
        except Exception as e:
            msg = f"rt.stop failed for {s.rtstream_id}: {e}"
            logger.warning("delete: %s", msg)
            warnings.append(msg)
    if s.video_id:
        try:
            await asyncio.to_thread(coll.delete_video, s.video_id)
        except Exception as e:
            msg = f"coll.delete_video failed for {s.video_id}: {e}"
            logger.warning("delete: %s", msg)
            warnings.append(msg)
    await asyncio.to_thread(sources.delete_source, source_id)
    status = "deleted_with_warnings" if warnings else "deleted"
    return {"status": status, "id": source_id, "warnings": warnings}


@app.post("/api/sources/{source_id}/disconnect")
async def api_disconnect_source(source_id: str) -> dict:
    s = await asyncio.to_thread(sources.get_source, source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    if not s.rtstream_id:
        return {"status": "noop", "reason": "no rtstream attached"}
    coll = _get_coll()
    try:
        rt = await asyncio.to_thread(coll.get_rtstream, s.rtstream_id)
        await asyncio.to_thread(rt.stop)
        await asyncio.to_thread(
            sources.update_source, source_id, status="disconnected", stage_msg="rtstream stopped"
        )
    except Exception as e:
        await asyncio.to_thread(sources.update_source, source_id, status="error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
    return (await asyncio.to_thread(sources.get_source, source_id)).__dict__


@app.post("/api/sources/{source_id}/reconnect")
async def api_reconnect_source(source_id: str) -> dict:
    s = sources.get_source(source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    sources.update_source(source_id, status="queued", error=None, stage_msg="reconnect requested")
    coll = _get_coll()
    _spawn_bg(
        ingest.dispatch(source_id, coll=coll), label=f"ingest.dispatch.reconnect({source_id})"
    )
    return sources.get_source(source_id).__dict__


# ──── Indexed Content explorer routes ────────────────────────────────────


@app.get("/api/videos")
async def api_list_videos() -> JSONResponse:
    """List videos in the collection (30s server cache).

    Async + asyncio.to_thread for the blocking SDK call. Returns 502
    on SDK failure so the dashboard surfaces a visible degraded state
    rather than rendering an empty list as if "no videos" were true.
    """
    now = time.time()
    with _videos_lock:
        if (now - _videos_cache["at"]) < _VIDEOS_TTL_S:
            return JSONResponse(_videos_cache["data"])
    try:
        coll = _get_coll()
        videos = await _async_sdk(coll.get_videos, timeout_s=8.0)
        items = []
        for v in videos:
            items.append(
                {
                    "id": v.id,
                    "name": getattr(v, "name", None),
                    "length": getattr(v, "length", None),
                    "stream_url": getattr(v, "stream_url", None),
                    "thumbnail_url": getattr(v, "thumbnail_url", None),
                }
            )
        data = {"videos": items}
        with _videos_lock:
            _videos_cache.update({"at": now, "data": data})
        return JSONResponse(data)
    except Exception as e:
        logger.warning("api_list_videos failed: %s: %s", type(e).__name__, e)
        # Cache the error short-window to throttle retries during the outage.
        error_payload = {"videos": [], "error": str(e)}
        with _videos_lock:
            _videos_cache.update({"at": now, "data": error_payload})
        return JSONResponse(error_payload, status_code=502)


def _coerce_to_list(value: Any, *, source: str) -> list:
    """Normalise SDK return values to a list.

    Pre-fix sites used ``sdk_call() or []`` which silently coerced None,
    empty dict, or any other falsy non-list into an empty list. That
    masked SDK contract violations (None when a list was promised) as
    "no results", so operators had no way to tell apart a healthy
    no-data response from a broken SDK call.
    """
    if value is None:
        logger.warning("%s returned None; treating as empty list", source)
        return []
    if isinstance(value, list):
        return value
    logger.warning(
        "%s returned %s (expected list); attempting coerce",
        source,
        type(value).__name__,
    )
    try:
        return list(value)
    except Exception:
        return []


@app.get("/api/videos/{video_id}/indexes")
def api_video_indexes(video_id: str) -> dict:
    try:
        coll = _get_coll()
        video = coll.get_video(video_id)
        indexes = _coerce_to_list(
            video.list_scene_index(),
            source=f"video({video_id}).list_scene_index",
        )
        return {"video_id": video_id, "indexes": indexes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/videos/{video_id}/scenes/{index_id}")
def api_video_scenes(video_id: str, index_id: str, limit: int = 20) -> dict:
    try:
        coll = _get_coll()
        video = coll.get_video(video_id)
        scenes = _coerce_to_list(
            video.get_scene_index(index_id),
            source=f"video({video_id}).get_scene_index({index_id})",
        )
        # Server-side slice -- callers can paginate later
        return {"video_id": video_id, "index_id": index_id, "scenes": scenes[:limit]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/rtstreams/{rt_id}/indexes")
def api_rtstream_indexes(rt_id: str) -> dict:
    try:
        coll = _get_coll()
        rt = coll.get_rtstream(rt_id)
        raw = _coerce_to_list(
            rt.list_scene_indexes(),
            source=f"rtstream({rt_id}).list_scene_indexes",
        )
        indexes = [
            {
                "rtstream_index_id": getattr(idx, "rtstream_index_id", None)
                or getattr(idx, "id", None),
                "name": getattr(idx, "name", None),
                "status": getattr(idx, "status", None),
                "prompt": getattr(idx, "prompt", None),
            }
            for idx in raw
        ]
        return {"rtstream_id": rt_id, "indexes": indexes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/rtstreams/{rt_id}/scenes/{index_id}")
def api_rtstream_scenes(rt_id: str, index_id: str, page_size: int = 20) -> dict:
    try:
        coll = _get_coll()
        rt = coll.get_rtstream(rt_id)
        idx = rt.get_scene_index(index_id)
        data = idx.get_scenes(page=1, page_size=page_size)
        return {"rtstream_id": rt_id, "index_id": index_id, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


class SearchRequest(BaseModel):
    query: str
    scope: str = Field(..., description="collection|video|rtstream")
    target_id: str | None = None
    index_id: str | None = None
    result_threshold: int | None = None


def _shot_to_dict(sh: Any) -> dict:
    """Normalise a shot to dict, accommodating dict OR object shapes.

    The SDK has historically returned both — `getattr(sh, field, None)`
    silently produces a six-None dict when handed a dict, masking SDK
    contract violations as empty data. Explicitly handle both shapes
    and log a warning when neither matches.
    """
    if isinstance(sh, dict):
        return {
            "start": sh.get("start"),
            "end": sh.get("end"),
            "text": sh.get("text"),
            "score": sh.get("search_score", sh.get("score")),
            "scene_index_id": sh.get("scene_index_id"),
            "scene_index_name": sh.get("scene_index_name"),
        }
    if hasattr(sh, "start") or hasattr(sh, "text"):
        return {
            "start": getattr(sh, "start", None),
            "end": getattr(sh, "end", None),
            "text": getattr(sh, "text", None),
            "score": getattr(sh, "search_score", None),
            "scene_index_id": getattr(sh, "scene_index_id", None),
            "scene_index_name": getattr(sh, "scene_index_name", None),
        }
    logger.warning(
        "_shot_to_dict: unexpected shape %s; returning empty dict",
        type(sh).__name__,
    )
    return {
        "start": None,
        "end": None,
        "text": None,
        "score": None,
        "scene_index_id": None,
        "scene_index_name": None,
    }


# ──── Usage / billing route ───────────────────────────────────────────────


def _estimate_credit_burn_usd() -> dict:
    """Back-of-napkin estimate from .state.json + best-guess rates.

    Rates (from PARALLEL_STREAM_HANDOVER analysis):
      - Live RTStream visual+audio indexing at RELAXED batch_config: ~$5/h
      - Sandbox Medium tier: $3.50/h flat
      - Sandbox Small tier: $1/h flat
    """
    import json as _json
    from pathlib import Path as _PPath

    state_file = _PPath(__file__).resolve().parent.parent / ".state.json"
    if not state_file.exists():
        return {"total_usd": 0.0, "rtstreams_usd": 0.0, "sandboxes_usd": 0.0, "details": []}
    try:
        state = _json.loads(state_file.read_text())
    except Exception as e:
        logger.warning("_estimate_credit_burn_usd: %s unreadable: %r", state_file, e)
        return {"total_usd": 0.0, "error": f"state unreadable: {e}"}

    now = time.time()
    rt_total = 0.0
    sb_total = 0.0
    details: list[dict] = []

    # Live VideoDB-side status — only meter rtstreams actually running.
    # IMPORTANT: if the SDK call fails (network blip, auth glitch), we DO NOT
    # silently zero the estimate — that masks real burn. We flag the result
    # as `live_status_unknown` and fall back to the legacy upper-bound for
    # every entry that has a started_at, so operators still see a number.
    live_status_by_id: dict[str, str] = {}
    live_status_available = False
    sdk_warning: str | None = None
    try:
        coll = _get_coll()
        # _coerce_to_list rather than `or []` — the latter masks SDK
        # contract violations (None when a list was promised) as "no
        # rtstreams", which silently zeros the entire estimate.
        rts = _coerce_to_list(
            _with_timeout(coll.list_rtstreams, timeout_s=5.0),
            source="list_rtstreams",
        )
        for rt in rts:
            rt_id = getattr(rt, "id", None) or (rt.get("id") if isinstance(rt, dict) else None)
            status = getattr(rt, "status", None) or (
                rt.get("status") if isinstance(rt, dict) else None
            )
            if rt_id:
                live_status_by_id[str(rt_id)] = str(status or "").lower()
        live_status_available = True
    except Exception as e:
        logger.warning("_estimate_credit_burn_usd: list_rtstreams failed: %r", e)
        sdk_warning = (
            f"Could not verify live status against VideoDB ({type(e).__name__}). "
            "Showing upper-bound estimate from local state — value may include stopped streams."
        )

    # Statuses VideoDB exposes that we treat as "billing is happening right now".
    _RT_LIVE_STATUSES = {"connected", "running", "ingesting", "indexing", "ready"}

    for key, rt_state in state.get("rtstreams", {}).items():
        started_iso = rt_state.get("started_at") or rt_state.get("created_at")
        if not started_iso:
            continue
        rt_id = rt_state.get("rtstream_id") or rt_state.get("id")
        live_status = live_status_by_id.get(str(rt_id), "")

        if live_status_available:
            # SDK responded: trust it. Skip non-running or unknown-id entries.
            if not rt_id or live_status not in _RT_LIVE_STATUSES:
                continue
            displayed_status = live_status
        else:
            # SDK unreachable: fall back to legacy upper-bound — better to
            # show maybe-too-high than maybe-zero.
            displayed_status = "unknown_sdk_unreachable"
        try:
            from datetime import datetime as _dt

            if isinstance(started_iso, str):
                started_ts = _dt.fromisoformat(started_iso.replace("Z", "+00:00")).timestamp()
            else:
                started_ts = float(started_iso)
        except Exception as e:
            logger.warning(
                "_estimate_credit_burn_usd: rtstream %s has unparseable started_at=%r: %r",
                key,
                started_iso,
                e,
            )
            continue
        hours = max(0.0, (now - started_ts) / 3600.0)
        rate = 5.0  # relaxed cost rate
        burn = hours * rate
        rt_total += burn
        details.append(
            {
                "kind": "rtstream",
                "key": key,
                "rtstream_id": rt_id,
                "status": displayed_status,
                "hours": round(hours, 2),
                "rate_usd_per_h": rate,
                "burn_usd": round(burn, 2),
            }
        )

    sb = state.get("sandbox")
    if sb and sb.get("created_at") and sb.get("id"):
        # Verify sandbox is actually live on VideoDB. If the probe FAILS,
        # default to "assume active" — masking real burn is worse than
        # showing an unverified row, and the dashboard surfaces the
        # `status` so operators see it.
        sb_active = True
        sb_status = "unverified"
        try:
            conn = _get_conn()
            if hasattr(conn, "get_sandbox"):
                sb_obj = _with_timeout(conn.get_sandbox, sb["id"], timeout_s=5.0)
                if sb_obj is not None:
                    status = str(
                        getattr(sb_obj, "status", None)
                        or (sb_obj.get("status") if isinstance(sb_obj, dict) else None)
                        or ""
                    ).lower()
                    sb_status = status or "unknown"
                    sb_active = status in ("active", "running", "ready")
                else:
                    # SDK returned None — sandbox is gone (idle-timeout)
                    sb_active = False
                    sb_status = "expired"
        except Exception as e:
            logger.warning("_estimate_credit_burn_usd: sandbox status probe failed: %r", e)
            if sdk_warning is None:
                sdk_warning = (
                    f"Could not verify sandbox status ({type(e).__name__}). "
                    "Showing upper-bound estimate — sandbox may have already shut down."
                )

        if sb_active:
            try:
                from datetime import datetime as _dt

                started_ts = _dt.fromisoformat(sb["created_at"].replace("Z", "+00:00")).timestamp()
                hours = max(0.0, (now - started_ts) / 3600.0)
                rate = 3.5 if str(sb.get("tier", "")).endswith("medium") else 1.0
                burn = hours * rate
                sb_total += burn
                details.append(
                    {
                        "kind": "sandbox",
                        "id": sb.get("id"),
                        "tier": sb.get("tier"),
                        "status": sb_status,
                        "hours": round(hours, 2),
                        "rate_usd_per_h": rate,
                        "burn_usd": round(burn, 2),
                    }
                )
            except Exception as e:
                logger.warning(
                    "_estimate_credit_burn_usd: sandbox burn calc failed: %r (sb=%r)",
                    e,
                    sb,
                )

    return {
        "total_usd": round(rt_total + sb_total, 2),
        "rtstreams_usd": round(rt_total, 2),
        "sandboxes_usd": round(sb_total, 2),
        "details": details,
        "live_status_available": live_status_available,
        "warning": sdk_warning,
        "note": (
            "Upper-bound estimate from .state.json start timestamps to now. "
            "Real usage shown by conn.check_usage() above. "
            "Sandbox cost only counts the most-recent slot in state."
        ),
    }


@app.get("/api/usage")
async def api_usage() -> dict:
    """VideoDB check_usage + invoices + local back-of-napkin estimate (60s cache).

    Async + locked TTL check + `data is not None` guard so an empty-dict
    response from the SDK doesn't keep missing the cache and hammering
    the slow billing endpoint on every poll.
    """
    now = time.time()
    with _usage_lock:
        if (now - _usage_cache["at"]) < _USAGE_TTL_S and _usage_cache["data"] is not None:
            return _usage_cache["data"]

    # _estimate_credit_burn_usd itself does blocking SDK calls (list_rtstreams,
    # get_sandbox). Run it on the thread pool so we don't park the loop.
    out: dict[str, Any] = {"estimate": await asyncio.to_thread(_estimate_credit_burn_usd)}
    try:
        conn = await asyncio.to_thread(_get_conn)
    except Exception as e:
        out["usage_error"] = str(e)
        out["invoices_error"] = "no connection"
        with _usage_lock:
            _usage_cache.update({"at": now, "data": out})
        return out
    try:
        out["usage"] = await _async_sdk(conn.check_usage, timeout_s=5.0)
    except Exception as e:
        out["usage_error"] = str(e)
    try:
        invoices_raw = await _async_sdk(conn.get_invoices, timeout_s=5.0)
        out["invoices"] = _coerce_to_list(invoices_raw, source="get_invoices")[:10]
    except Exception as e:
        out["invoices_error"] = str(e)
    with _usage_lock:
        _usage_cache.update({"at": now, "data": out})
    return out


@app.post("/api/search")
def api_search(req: SearchRequest) -> dict:
    """Search across collection/video/rtstream scopes.

    Skill conformance (video-db/skills · search-reference.md):
      - Collection search only supports ``SearchType.semantic`` — pass it
        explicitly; otherwise the SDK raises ``NotImplementedError``.
      - Video/rtstream scene search needs ``index_type=IndexType.scene`` +
        a ``score_threshold`` so noise gets filtered.
      - The SDK raises a custom error on empty results (string contains
        "No results found"); catch it and return ``shots: []`` instead of
        a 500. That's what every skill example does.
    """
    coll = _get_coll()
    # Late import — videodb is heavy + we want the SDK constants if available.
    try:
        from videodb import IndexType, SearchType
    except Exception:  # pragma: no cover — SDK shape may move
        IndexType = SearchType = None  # type: ignore[assignment]

    def _empty(scope: str, extra: dict | None = None) -> dict:
        out = {"scope": scope, "shots": []}
        if extra:
            out.update(extra)
        return out

    try:
        if req.scope == "collection":
            kwargs: dict[str, Any] = {"query": req.query}
            if SearchType is not None:
                kwargs["search_type"] = SearchType.semantic
            try:
                result = coll.search(**kwargs)
            except Exception as e:
                if "No results found" in str(e):
                    return _empty("collection")
                raise
            shots = getattr(result, "shots", None) or []
            return {"scope": "collection", "shots": [_shot_to_dict(s) for s in shots]}

        if req.scope == "video":
            if not req.target_id:
                raise HTTPException(status_code=400, detail="target_id required for video scope")
            v = coll.get_video(req.target_id)
            kwargs = {"query": req.query, "score_threshold": 0.3}
            if IndexType is not None:
                kwargs["index_type"] = IndexType.scene
            try:
                result = v.search(**kwargs)
            except Exception as e:
                if "No results found" in str(e):
                    return _empty("video", {"video_id": req.target_id})
                raise
            shots = getattr(result, "shots", None) or []
            return {
                "scope": "video",
                "video_id": req.target_id,
                "shots": [_shot_to_dict(s) for s in shots],
            }

        if req.scope == "rtstream":
            if not req.target_id:
                raise HTTPException(status_code=400, detail="target_id required for rtstream scope")
            rt = coll.get_rtstream(req.target_id)
            kwargs = {"query": req.query, "score_threshold": 0.3}
            if IndexType is not None:
                kwargs["index_type"] = IndexType.scene
            if req.index_id:
                kwargs["index_id"] = req.index_id
            try:
                result = rt.search(**kwargs)
            except Exception as e:
                if "No results found" in str(e):
                    return _empty("rtstream", {"rtstream_id": req.target_id})
                raise
            shots = getattr(result, "shots", None) or []
            return {
                "scope": "rtstream",
                "rtstream_id": req.target_id,
                "shots": [_shot_to_dict(s) for s in shots],
            }
        raise HTTPException(status_code=400, detail=f"unknown scope: {req.scope}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
