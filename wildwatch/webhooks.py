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
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path as PPath
from typing import Annotated, Any
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
# hammering the SDK from dashboard polls).
_remote_cache: dict = {"at": 0.0, "data": {"rtstreams": [], "sandboxes": []}}
_REMOTE_TTL_S = 10.0

# Cache for /api/videos (30s TTL).
_videos_cache: dict = {"at": 0.0, "data": {"videos": []}}
_VIDEOS_TTL_S = 30.0

# Cache for /api/usage (60s TTL — billing endpoints are slow).
_usage_cache: dict = {"at": 0.0, "data": {}}
_USAGE_TTL_S = 60.0

app = FastAPI(title="WildWatch webhook receiver")

# Bounded executor for SDK calls that lack native timeouts (VideoDB SDK is
# blocking and offers no per-call deadline). Wrapping in a thread pool with
# `future.result(timeout=N)` lets us reject hung calls so the Usage tab
# can't freeze the dashboard mid-demo.
_SDK_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sdk")


def _with_timeout(fn, *args, timeout_s: float = 5.0, **kwargs):
    """Run a blocking SDK call with a hard deadline; raises TimeoutError."""
    fut = _SDK_EXECUTOR.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=timeout_s)
    except FutureTimeoutError as e:
        # Best-effort cancel; if the underlying call is in C-land it may still run.
        fut.cancel()
        raise TimeoutError(f"SDK call timed out after {timeout_s}s") from e


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
        # Same-origin XHR/fetch will send an Origin header in modern browsers.
        # CLI tools (curl) won't — allow when there's no browser context.
        ua = (request.headers.get("user-agent") or "").lower()
        if any(t in ua for t in ("mozilla", "chrome", "safari", "firefox", "edg/")):
            return JSONResponse(status_code=403, content={"detail": "missing Origin"})
        return await call_next(request)
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

    model_config = {"extra": "allow"}  # don't reject unknown fields VideoDB may add


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
def api_remote() -> dict:
    """List active VideoDB rtstreams + sandboxes (cached 10s)."""
    now = time.time()
    if (now - _remote_cache["at"]) < _REMOTE_TTL_S:
        return _remote_cache["data"]
    try:
        coll = _get_coll()
        rtstreams = [
            {"id": rt.id, "name": getattr(rt, "name", "?"), "status": getattr(rt, "status", "?")}
            for rt in (_with_timeout(coll.list_rtstreams, timeout_s=5.0) or [])
        ]
        sandboxes = [
            {
                "id": sb.id,
                "tier": str(getattr(sb, "tier", "?")),
                "status": getattr(sb, "status", "?"),
                "is_active": bool(getattr(sb, "is_active", False)),
            }
            for sb in (_with_timeout(_get_conn().list_sandboxes, timeout_s=5.0) or [])
        ]
        data = {"rtstreams": rtstreams, "sandboxes": sandboxes}
        _remote_cache.update({"at": now, "data": data})
        return data
    except Exception as e:
        logger.warning("api_remote SDK call failed: %s", e)
        # Cache the error too — otherwise every poll during an outage
        # hammers the SDK with no backoff (one cache miss per dashboard tick).
        data = {"rtstreams": [], "sandboxes": [], "error": str(e)}
        _remote_cache.update({"at": now, "data": data})
        return data


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
    kind: str = Field(..., description="upload|youtube|hls|rtsp|rtmp")
    input: str
    name: str


# Process-cached VideoDB connection. `videodb.connect()` does an auth round
# trip on every call — recreating it per request blocks the thread on auth
# latency and turns the dashboard into a slow disaster.
_conn_cache: dict[str, Any] = {"conn": None, "coll": None}


def _get_conn() -> Any:
    if _conn_cache["conn"] is None:
        import videodb

        _conn_cache["conn"] = videodb.connect()
    return _conn_cache["conn"]


def _get_coll() -> Any:
    """Lazy-load VideoDB collection, cached for the life of the process."""
    if _conn_cache["coll"] is None:
        _conn_cache["coll"] = _get_conn().get_collection()
    return _conn_cache["coll"]


@app.get("/api/sources")
def api_list_sources() -> dict:
    return {"sources": [s.__dict__ for s in sources.list_sources()]}


@app.get("/api/sources/{source_id}")
def api_get_source(source_id: str) -> dict:
    s = sources.get_source(source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    return s.__dict__


@app.post("/api/sources")
async def api_create_source(payload: SourceCreate) -> dict:
    """JSON path: create a URL/RTSP/RTMP/YouTube source, kick off ingest."""
    if payload.kind == "upload":
        raise HTTPException(
            status_code=400,
            detail="use POST /api/sources/upload (multipart) for file uploads",
        )
    try:
        s = sources.add_source(kind=payload.kind, input=payload.input, name=payload.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    coll = _get_coll()
    _spawn_bg(ingest.dispatch(s.id, coll=coll), label=f"ingest.dispatch({s.id})")
    return s.__dict__


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
    try:
        async with aiofiles.open(tmp_path, "wb") as out:
            while chunk := await file.read(1024 * 1024):
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
    except HTTPException:
        tmp_path.unlink(missing_ok=True)
        sources.update_source(s.id, status="error", error="upload too large")
        raise
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        sources.update_source(s.id, status="error", error=str(e))
        raise

    coll = _get_coll()
    _spawn_bg(ingest.dispatch(s.id, coll=coll), label=f"ingest.dispatch.upload({s.id})")
    return sources.get_source(s.id).__dict__


@app.delete("/api/sources/{source_id}")
def api_delete_source(source_id: str) -> dict:
    s = sources.get_source(source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    # Best-effort remote cleanup (don't fail the local delete if remote fails)
    # but collect every failure so the caller can see remote resources that
    # may still be running (and burning credits).
    coll = _get_coll()
    warnings: list[str] = []
    if s.rtstream_id:
        try:
            rt = coll.get_rtstream(s.rtstream_id)
            rt.stop()
        except Exception as e:
            msg = f"rt.stop failed for {s.rtstream_id}: {e}"
            logger.warning("delete: %s", msg)
            warnings.append(msg)
    if s.video_id:
        try:
            coll.delete_video(s.video_id)
        except Exception as e:
            msg = f"coll.delete_video failed for {s.video_id}: {e}"
            logger.warning("delete: %s", msg)
            warnings.append(msg)
    sources.delete_source(source_id)
    status = "deleted_with_warnings" if warnings else "deleted"
    return {"status": status, "id": source_id, "warnings": warnings}


@app.post("/api/sources/{source_id}/disconnect")
def api_disconnect_source(source_id: str) -> dict:
    s = sources.get_source(source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    if not s.rtstream_id:
        return {"status": "noop", "reason": "no rtstream attached"}
    coll = _get_coll()
    try:
        rt = coll.get_rtstream(s.rtstream_id)
        rt.stop()
        sources.update_source(source_id, status="disconnected", stage_msg="rtstream stopped")
    except Exception as e:
        sources.update_source(source_id, status="error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
    return sources.get_source(source_id).__dict__


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
def api_list_videos() -> dict:
    """List videos in the collection (30s server cache)."""
    now = time.time()
    if (now - _videos_cache["at"]) < _VIDEOS_TTL_S:
        return _videos_cache["data"]
    try:
        coll = _get_coll()
        items = []
        for v in coll.get_videos():
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
        _videos_cache.update({"at": now, "data": data})
        return data
    except Exception as e:
        logger.warning("api_list_videos failed: %s", e)
        # Cache the error to throttle retries during the outage.
        data = {"videos": [], "error": str(e)}
        _videos_cache.update({"at": now, "data": data})
        return data


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
    return {
        "start": getattr(sh, "start", None),
        "end": getattr(sh, "end", None),
        "text": getattr(sh, "text", None),
        "score": getattr(sh, "search_score", None),
        "scene_index_id": getattr(sh, "scene_index_id", None),
        "scene_index_name": getattr(sh, "scene_index_name", None),
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
        rts = _with_timeout(coll.list_rtstreams, timeout_s=5.0) or []
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
def api_usage() -> dict:
    """VideoDB check_usage + invoices + local back-of-napkin estimate (60s cache)."""
    now = time.time()
    if (now - _usage_cache["at"]) < _USAGE_TTL_S and _usage_cache["data"]:
        return _usage_cache["data"]

    out: dict[str, Any] = {"estimate": _estimate_credit_burn_usd()}
    try:
        conn = _get_conn()
    except Exception as e:
        # Connect itself failed — cache the failure so repeated polls don't
        # all re-attempt the broken connect.
        out["usage_error"] = str(e)
        out["invoices_error"] = "no connection"
        _usage_cache.update({"at": now, "data": out})
        return out
    try:
        out["usage"] = _with_timeout(conn.check_usage, timeout_s=5.0)
    except Exception as e:
        out["usage_error"] = str(e)
    try:
        out["invoices"] = (_with_timeout(conn.get_invoices, timeout_s=5.0) or [])[:10]
    except Exception as e:
        out["invoices_error"] = str(e)
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
