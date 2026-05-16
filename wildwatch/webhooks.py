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

from typing import Annotated

from fastapi import FastAPI, Path
from pydantic import BaseModel, Field

from wildwatch.telegram import send_alert

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
    await send_alert(
        tier=tier,
        label=payload.label,
        explanation=payload.explanation,
        stream_url=payload.stream_url,
    )
    return {"status": "received"}
