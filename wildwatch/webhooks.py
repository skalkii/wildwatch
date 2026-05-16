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
import time
from typing import Annotated

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

# Load .env at import time so uvicorn-launched processes see TELEGRAM_*
# and other vars without requiring the operator to pre-export them.
load_dotenv()

from wildwatch import dashboard, event_log  # noqa: E402
from wildwatch.telegram import send_alert  # noqa: E402

logger = logging.getLogger(__name__)

# Cache for /api/remote: list_rtstreams + list_sandboxes (10s TTL to avoid
# hammering the SDK from dashboard polls).
_remote_cache: dict = {"at": 0.0, "data": {"rtstreams": [], "sandboxes": []}}
_REMOTE_TTL_S = 10.0

app = FastAPI(title="WildWatch webhook receiver")


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
        import videodb

        conn = videodb.connect()
        coll = conn.get_collection()
        rtstreams = [
            {"id": rt.id, "name": getattr(rt, "name", "?"), "status": getattr(rt, "status", "?")}
            for rt in coll.list_rtstreams()
        ]
        sandboxes = [
            {
                "id": sb.id,
                "tier": str(getattr(sb, "tier", "?")),
                "status": getattr(sb, "status", "?"),
                "is_active": bool(getattr(sb, "is_active", False)),
            }
            for sb in conn.list_sandboxes()
        ]
        data = {"rtstreams": rtstreams, "sandboxes": sandboxes}
        _remote_cache.update({"at": now, "data": data})
        return data
    except Exception as e:
        logger.warning("api_remote SDK call failed: %s", e)
        return {"rtstreams": [], "sandboxes": [], "error": str(e)}


@app.get("/events/stream")
async def events_stream() -> StreamingResponse:
    """SSE feed — pushes each new webhook event as it arrives."""

    async def gen():
        # Initial keepalive so the browser knows the connection is open
        yield b": connected\n\n"
        try:
            async for ev in dashboard.subscribe():
                yield f"data: {json.dumps(ev)}\n\n".encode()
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
