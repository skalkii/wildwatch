"""Real-time dashboard broadcaster + HTML.

Webhook receiver calls ``broadcast(payload)`` after logging each alert.
SSE subscribers ``async for ev in subscribe()`` receive every broadcast.
``get_stats()`` returns a JSON-serialisable snapshot for the polling
endpoints.

Single-process in-memory state — fine for hackathon scope, lost on
uvicorn restart.
"""

from __future__ import annotations

import asyncio
import importlib.resources as _resources
import logging
import time
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from functools import lru_cache as _lru_cache
from typing import Any, Literal, TypedDict

logger = logging.getLogger(__name__)

MAX_RECENT_EVENTS = 50


# ── Broadcast event types ───────────────────────────────────────────────
#
# Two distinct payload shapes flow through the same SSE channel:
#   * AlertEvent — produced by webhook handler. Counted into tier stats
#     and stored in _recent_events. No `type` key.
#   * UISignalEvent — produced by ingest / delete paths to push reactive
#     UI updates. Tagged with `type: source_progress | source_deleted`.
#     Never counted as an alert.
#
# Runtime discriminator: presence/absence of the `type` key.


class AlertEvent(TypedDict, total=False):
    """Alert payload — counted into tier stats. No `type` key."""

    tier: int
    label: str
    event_id: str | None
    confidence: float | None
    explanation: str | None
    timestamp: str | None
    start_time: str | float | None
    end_time: str | float | None
    stream_url: str | None
    video_id: str | None
    received_at: float


class UISignalEvent(TypedDict, total=False):
    """UI signal payload — routed to SSE only, not counted as alerts."""

    type: Literal["source_progress", "source_deleted"]
    source_id: str
    status: str
    stage_msg: str | None
    progress_pct: int | None
    kind: str
    name: str
    video_id: str | None
    rtstream_id: str | None
    error: str | None
    reason: str
    received_at: float


# Module-level state ------------------------------------------------------

_subscribers: list[asyncio.Queue] = []
_tier_counts: dict[int, int] = defaultdict(int)
# deque(maxlen=N) does O(1) pop-on-overflow, vs list.pop(0) which shifts
# the whole list. At MAX_RECENT_EVENTS=50 the perf difference is moot,
# but the maxlen invariant also removes the manual `if len > MAX: pop(0)`
# branch that future maintainers could miss.
_recent_events: deque[dict] = deque(maxlen=MAX_RECENT_EVENTS)
_total: int = 0
_dropped_total: int = 0  # SSE events lost to QueueFull (slow subscriber)
_started_at: float = time.time()


def reset_state() -> None:
    """Test helper — clears all counters + subscribers."""
    global _total, _started_at, _dropped_total
    _subscribers.clear()
    _tier_counts.clear()
    _recent_events.clear()
    _total = 0
    _dropped_total = 0
    _started_at = time.time()


def broadcast(event: AlertEvent | UISignalEvent) -> None:
    """Record + fanout to every subscriber.

    Two classes of broadcasts share this channel:

    1. **Alerts** — payloads coming through ``/webhook/{tier}``. These have
       a numeric ``tier`` and live in the alert feed + KPI counters.

    2. **UI signals** — non-alert pushes the server emits to make the
       dashboard reactive (currently only ``type="source_progress"`` from
       ``wildwatch.ingest``). These MUST flow through SSE so cards animate,
       but they must NOT pollute the alert feed or the tier counters.

    We discriminate on ``event.get("type")``: alerts never set ``type``;
    UI signals always do. Older callers that don't set ``type`` are
    therefore treated as alerts — backwards-compatible.
    """
    global _total, _dropped_total
    is_alert = "type" not in event
    if is_alert:
        _total += 1
        tier = int(event.get("tier", 0))
        _tier_counts[tier] += 1
        # deque(maxlen=...) auto-trims oldest on overflow — no manual pop needed.
        _recent_events.append({**event, "received_at": event.get("received_at", time.time())})
    else:
        # Surface UI-signal pass-throughs at DEBUG so a future regression in
        # this discriminator (e.g. a real alert that mistakenly carries a
        # `type` field) is greppable in the log rather than silently lost.
        logger.debug(
            "dashboard.broadcast: ui-signal type=%s source_id=%s",
            event.get("type"),
            event.get("source_id"),
        )
    # Fanout to subscribers (sync put_nowait so the webhook response path
    # never blocks on a slow SSE client). A full queue means the client
    # is too slow to drain — we drop the event but COUNT and LOG it so
    # operators can see drops in /api/stats and the log stream.
    for q in list(_subscribers):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            _dropped_total += 1
            logger.warning(
                "SSE event dropped: subscriber queue full (qsize=%d maxsize=%d total_dropped=%d)",
                q.qsize(),
                q.maxsize,
                _dropped_total,
            )


async def subscribe() -> AsyncIterator[dict]:
    """Yield each broadcast until subscriber drops.

    The ``finally`` block runs on normal completion, cancellation, AND any
    exception raised at the yield point (Python generator semantics), so
    the queue is always removed from ``_subscribers``. The defensive
    ``logger.warning`` here exists in case some future refactor breaks
    that invariant — better to notice the leak than to leak silently.
    """
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subscribers.append(q)
    try:
        while True:
            ev = await q.get()
            yield ev
    finally:
        try:
            _subscribers.remove(q)
        except ValueError:
            logger.warning("subscribe(): queue already removed from _subscribers on cleanup")


def get_stats() -> dict[str, Any]:
    """Snapshot for polling endpoints (JSON-serialisable)."""
    return {
        "total": _total,
        "tier_counts": dict(_tier_counts),
        "recent_events": list(reversed(_recent_events)),
        "subscribers": len(_subscribers),
        "dropped": _dropped_total,
        "uptime_s": int(time.time() - _started_at),
    }


# ──── HTML template ────────────────────────────────────────────────────────
# The dashboard is one large HTML+CSS+JS string. Previously inlined as a
# 2700-line Python literal — too big to maintain and too noisy for diffs
# unrelated to UI changes. Now loaded from ``static/dashboard.html`` on
# first call and cached for the process lifetime. The file is shipped
# inside the wildwatch package so it's discoverable via
# ``importlib.resources`` even when the package is installed as a wheel.


_FALLBACK_HTML = (
    "<!doctype html><meta charset='utf-8'>"
    "<title>WildWatch — dashboard unavailable</title>"
    "<body style='font-family:system-ui;padding:2rem;background:#0e1715;color:#e6edeb;'>"
    "<h1>Dashboard static asset missing</h1>"
    "<p><code>wildwatch/static/dashboard.html</code> could not be located. "
    "If you installed via <code>pip install</code> (not editable), the wheel "
    "may have been built without <code>[tool.setuptools.package-data]</code> "
    "for the static dir. Reinstall with <code>pip install -e .</code> or "
    "rebuild the wheel.</p>"
    "<p>The API and webhook receiver still work — only the UI shell is missing.</p>"
    "</body>"
)


@_lru_cache(maxsize=1)
def get_dashboard_html() -> str:
    """Return the dashboard HTML, read once and cached.

    The HTML lives in ``wildwatch/static/dashboard.html`` and ships
    inside the wheel via ``[tool.setuptools.package-data]``. A missing
    file means a broken install (or someone deleted the static dir
    in development) — return a small ranger-readable fallback page
    instead of a 500 stack trace, and ERROR-log the cause so the
    operator can fix it.

    ``@lru_cache`` does not cache exceptions, so a transient
    ``FileNotFoundError`` is retried on every request. Catching here
    means the cache stores the fallback string and the operator
    sees a consistent state.
    """
    try:
        return (
            _resources.files("wildwatch")
            .joinpath("static/dashboard.html")
            .read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError) as e:
        logger.error(
            "dashboard.get_dashboard_html: static/dashboard.html unavailable "
            "(%s: %s) — serving fallback page",
            type(e).__name__,
            e,
        )
        return _FALLBACK_HTML
