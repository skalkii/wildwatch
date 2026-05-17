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
import logging
import time
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from typing import Any, Literal, Never, TypedDict

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
# Runtime discriminator: presence/absence of the `type` key. The
# TypedDicts below document the invariant; static checkers can flag a
# caller that accidentally puts a `type` key in an alert dict, which
# would silently route it to the UI-signal branch and drop it from
# tier counts. The runtime check stays as `"type" not in event` to
# avoid touching every existing emit site.


# Split into required-base + optional-extras using TypedDict inheritance.
# Static checkers reject `{}` as an AlertEvent or `{}` as a UISignalEvent
# now — tier/label and type are mandatory, respectively. Without this an
# empty dict matched either TypedDict structurally and the runtime
# discriminator (`"type" not in event`) would treat it as an alert and
# increment _total / _tier_counts.


class _AlertEventRequired(TypedDict):
    tier: int
    label: str
    # `type: Never` makes the absence of this key structurally
    # load-bearing — a caller that constructs `{"tier": 1, "label": "x",
    # "type": "source_progress"}` is rejected at the type-check layer.
    # The runtime discriminator (`"type" not in event`) aligns with the
    # static contract so the two can't drift.
    type: Never


class AlertEvent(_AlertEventRequired, total=False):
    """Alert payload — NO `type` key, tier+label REQUIRED."""

    event_id: str | None
    confidence: float | None
    explanation: str | None
    timestamp: str | None
    start_time: str | None
    end_time: str | None
    stream_url: str | None
    received_at: float


class _UISignalRequired(TypedDict):
    type: Literal["source_progress", "source_deleted"]
    source_id: str


class UISignalEvent(_UISignalRequired, total=False):
    """UI signal payload — `type` + `source_id` REQUIRED. Routed to SSE only."""

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


BroadcastEvent = AlertEvent | UISignalEvent

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

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <title>WildWatch — Live Dashboard</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="description" content="WildWatch — real-time perception agent for protected-area wildlife monitoring.">
  <meta name="theme-color" content="#0b0f0e">
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'><stop offset='0' stop-color='%2334d399'/><stop offset='1' stop-color='%230ea5e9'/></linearGradient></defs><rect width='64' height='64' rx='14' fill='%23051210'/><path d='M8 32c8-12 18-18 24-18s16 6 24 18c-8 12-18 18-24 18S16 44 8 32Z' fill='none' stroke='url(%23g)' stroke-width='3'/><circle cx='32' cy='32' r='8' fill='url(%23g)'/><circle cx='34' cy='30' r='2.4' fill='%23051210'/></svg>">
  <script>
    // Init theme BEFORE Tailwind loads (avoids flash)
    (function () {
      try {
        var saved = localStorage.getItem('ww-theme');
        var sysDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        var dark = saved ? saved === 'dark' : sysDark;
        document.documentElement.classList.toggle('dark', dark);
      } catch (e) {}
    })();
  </script>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.13/dist/hls.min.js"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          fontFamily: { sans: ['Inter', 'ui-sans-serif', 'system-ui', '-apple-system', 'sans-serif'] },
          colors: {
            brand: { 50:'#ecfdf5', 100:'#d1fae5', 200:'#a7f3d0', 300:'#6ee7b7', 400:'#34d399', 500:'#10b981', 600:'#059669', 700:'#047857', 800:'#065f46', 900:'#064e3b' },
          },
          boxShadow: { soft: '0 1px 2px rgba(0,0,0,.04), 0 6px 24px -8px rgba(0,0,0,.12)' },
        },
      },
    };
  </script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #f8fafc;
      --bg-elev: #ffffff;
      --bg-soft: #f1f5f9;
      --border: #e2e8f0;
      --border-strong: #cbd5e1;
      --text: #0f172a;
      --text-muted: #475569;
      --text-faint: #64748b;
      --accent: #0ea5e9;
      --grid: rgba(15,23,42,.04);
    }
    html.dark {
      --bg: #07100e;
      --bg-elev: #0e1715;
      --bg-soft: #131e1c;
      --border: #1f2a27;
      --border-strong: #2c3a36;
      --text: #e6edeb;
      --text-muted: #9aa9a4;
      --text-faint: #6b7a76;
      --accent: #34d399;
      --grid: rgba(255,255,255,.03);
    }
    html, body { background: var(--bg); color: var(--text); }
    body {
      font-family: 'Inter', ui-sans-serif, system-ui, -apple-system, sans-serif;
      font-feature-settings: 'cv11','ss01','ss02';
      background-image:
        radial-gradient(1200px 600px at 100% -10%, rgba(16,185,129,.07), transparent 60%),
        radial-gradient(900px 500px at -10% 110%, rgba(14,165,233,.06), transparent 60%);
      background-attachment: fixed;
    }
    /* Light theme: dim the dark-tuned ambient gradient + give cards depth */
    html:not(.dark) body {
      background-image:
        radial-gradient(1200px 600px at 100% -10%, rgba(16,185,129,.04), transparent 60%),
        radial-gradient(900px 500px at -10% 110%, rgba(14,165,233,.03), transparent 60%);
    }
    html:not(.dark) .card,
    html:not(.dark) .modal-card { box-shadow: 0 1px 2px rgba(15,23,42,.04), 0 8px 24px -10px rgba(15,23,42,.08); }
    html:not(.dark) .card-soft { box-shadow: 0 1px 1px rgba(15,23,42,.03); }
    code, pre, .mono { font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace; }
    pre { font-size: 11.5px; line-height: 1.55; }

    .card { background: var(--bg-elev); border: 1px solid var(--border); border-radius: 14px; }
    .card-soft { background: var(--bg-soft); border: 1px solid var(--border); border-radius: 12px; }
    .divider { border-color: var(--border) !important; }
    .muted { color: var(--text-muted); }
    .faint { color: var(--text-faint); }
    .input { background: var(--bg-soft); border: 1px solid var(--border); border-radius: 10px; color: var(--text); }
    .input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px color-mix(in oklab, var(--accent) 25%, transparent); }
    .btn { border-radius: 10px; padding: 0.5rem 0.9rem; font-size: 13px; font-weight: 500; transition: background-color .15s, transform .05s, border-color .15s; }
    .btn-primary { background: var(--accent); color: #052e23; }
    .btn-primary:hover { filter: brightness(1.05); }
    .btn-ghost { background: var(--bg-soft); color: var(--text); border: 1px solid var(--border); }
    .btn-ghost:hover { background: var(--bg-elev); border-color: var(--border-strong); }
    .btn:active { transform: translateY(1px); }

    .pulse { animation: pulse 1.6s ease-in-out infinite; }
    @keyframes pulse { 0%,100% { opacity: 1 } 50% { opacity: 0.35 } }

    .ev { border-left-width: 4px; background: var(--bg-elev); border: 1px solid var(--border); border-left-width: 4px; border-radius: 10px; padding: .6rem .8rem; transition: transform .15s, border-color .15s; }
    .ev:hover { border-color: var(--border-strong); }
    .ev-1 { border-left-color: #38bdf8; }
    .ev-2 { border-left-color: #f59e0b; }
    .ev-3 { border-left-color: #ef4444; }

    /* Sticky nav tabs */
    .tab-btn {
      position: relative; padding: 0.7rem 1rem; font-size: 13.5px; font-weight: 500;
      color: var(--text-muted); border-radius: 8px 8px 0 0; transition: color .15s, background-color .15s;
    }
    .tab-btn:hover { color: var(--text); background: var(--bg-soft); }
    .tab-btn.tab-active { color: var(--text); background: transparent; }
    .tab-btn.tab-active::after {
      content: ''; position: absolute; left: 12px; right: 12px; bottom: -1px; height: 2px;
      background: var(--accent); border-radius: 2px;
    }
    .modal-tab-btn { padding: .4rem .7rem; font-size: 12px; border-radius: 8px; color: var(--text-muted); border: 1px solid transparent; }
    .modal-tab-btn:hover { color: var(--text); background: var(--bg-soft); }
    .modal-tab-btn.tab-active { color: var(--text); background: var(--bg-soft); border-color: var(--border); }

    /* Status pills */
    .pill { display: inline-flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 500; padding: 3px 9px; border-radius: 999px; letter-spacing: .02em; border: 1px solid transparent; }
    .id-pill { display: inline-flex; align-items: center; gap: 4px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 10.5px; padding: 2px 7px; border-radius: 999px; background: var(--bg-soft); color: var(--text); border: 1px solid var(--border); cursor: pointer; vertical-align: middle; transition: background 120ms ease, border-color 120ms ease, color 120ms ease; max-width: 100%; }
    .id-pill:hover { background: color-mix(in oklab, var(--accent) 12%, var(--bg-soft)); border-color: color-mix(in oklab, var(--accent) 40%, var(--border)); color: var(--text); }
    .id-pill:active { transform: translateY(1px); }
    .id-pill .id-pill-text { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .id-pill .id-pill-icon { flex-shrink: 0; opacity: 0.55; }
    .id-pill:hover .id-pill-icon { opacity: 1; }
    .id-pill.copied { background: color-mix(in oklab, #10b981 18%, var(--bg-soft)); border-color: color-mix(in oklab, #10b981 50%, var(--border)); color: #10b981; }
    .pill::before { content:''; width: 6px; height: 6px; border-radius: 999px; background: currentColor; }
    .status-queued       { color: #94a3b8; background: color-mix(in oklab, #94a3b8 15%, transparent); }
    .status-connecting   { color: #38bdf8; background: color-mix(in oklab, #38bdf8 15%, transparent); }
    .status-ingesting    { color: #a78bfa; background: color-mix(in oklab, #a78bfa 15%, transparent); }
    .status-indexing     { color: #f472b6; background: color-mix(in oklab, #f472b6 15%, transparent); }
    .status-ready        { color: #10b981; background: color-mix(in oklab, #10b981 18%, transparent); }
    .status-error        { color: #ef4444; background: color-mix(in oklab, #ef4444 18%, transparent); }
    .status-disconnected { color: #94a3b8; background: color-mix(in oklab, #94a3b8 15%, transparent); }
    /* Light-mode pills lose contrast on white-ish bg — bump mix percent */
    html:not(.dark) .status-queued       { background: color-mix(in oklab, #94a3b8 32%, transparent); color: #475569; }
    html:not(.dark) .status-connecting   { background: color-mix(in oklab, #38bdf8 28%, transparent); color: #0369a1; }
    html:not(.dark) .status-ingesting    { background: color-mix(in oklab, #a78bfa 28%, transparent); color: #5b21b6; }
    html:not(.dark) .status-indexing     { background: color-mix(in oklab, #f472b6 28%, transparent); color: #9d174d; }
    html:not(.dark) .status-ready        { background: color-mix(in oklab, #10b981 30%, transparent); color: #065f46; }
    html:not(.dark) .status-error        { background: color-mix(in oklab, #ef4444 28%, transparent); color: #991b1b; }
    html:not(.dark) .status-disconnected { background: color-mix(in oklab, #94a3b8 32%, transparent); color: #475569; }

    /* KPI cards */
    .kpi { background: var(--bg-elev); border: 1px solid var(--border); border-radius: 14px; padding: 1rem 1.1rem; position: relative; overflow: hidden; }
    .kpi::after { content:''; position:absolute; inset:0; background: linear-gradient(180deg, transparent 50%, var(--grid)); pointer-events:none; }
    .kpi-label { font-size: 11px; font-weight: 600; letter-spacing: .08em; color: var(--text-faint); text-transform: uppercase; }
    .kpi-value { font-size: 30px; font-weight: 700; line-height: 1.1; margin-top: .35rem; letter-spacing: -.01em; }
    .kpi-accent-1 { box-shadow: inset 0 0 0 1px color-mix(in oklab, #38bdf8 30%, transparent); }
    .kpi-accent-2 { box-shadow: inset 0 0 0 1px color-mix(in oklab, #f59e0b 30%, transparent); }
    .kpi-accent-3 { box-shadow: inset 0 0 0 1px color-mix(in oklab, #ef4444 30%, transparent); }
    /* Dark-mode-only KPI bottom-fade — light mode renders it as a dirty smudge */
    html.dark .kpi::after { display: block; }
    html:not(.dark) .kpi::after { display: none; }

    /* Header */
    .site-header {
      position: sticky; top: 0; z-index: 40;
      backdrop-filter: saturate(140%) blur(10px);
      -webkit-backdrop-filter: saturate(140%) blur(10px);
      background: color-mix(in oklab, var(--bg) 78%, transparent);
      border-bottom: 1px solid var(--border);
    }
    .site-nav {
      background: color-mix(in oklab, var(--bg) 92%, transparent);
      border-bottom: 1px solid var(--border);
    }
    .brand-mark {
      display:inline-flex; align-items:center; justify-content:center;
      width: 30px; height: 30px; border-radius: 9px;
      background: linear-gradient(135deg, #10b981 0%, #0ea5e9 100%);
      box-shadow: 0 4px 14px -6px rgba(16,185,129,.6);
    }
    .live-dot { width:8px; height:8px; border-radius:999px; display:inline-block; }
    .live-on  { background:#10b981; box-shadow: 0 0 0 3px color-mix(in oklab, #10b981 30%, transparent); }
    .live-off { background:#64748b; }
    .live-err { background:#ef4444; box-shadow: 0 0 0 3px color-mix(in oklab, #ef4444 30%, transparent); }

    /* Footer */
    .site-footer {
      margin-top: 3rem; padding: 1.25rem 1.5rem; border-top: 1px solid var(--border);
      color: var(--text-faint); font-size: 12px;
      display: flex; flex-wrap: wrap; gap: .75rem; justify-content: space-between; align-items: center;
      background: color-mix(in oklab, var(--bg) 70%, transparent);
    }
    .site-footer a { color: var(--text-muted); }
    .site-footer a:hover { color: var(--text); text-decoration: underline; }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 10px; height: 10px; }
    ::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 999px; border: 2px solid var(--bg); }
    ::-webkit-scrollbar-thumb:hover { background: var(--text-faint); }

    /* Modal */
    .modal-backdrop { background: rgba(2,6,12,.55); }
    html.dark .modal-backdrop { background: rgba(0,0,0,.65); }
    .modal-card { background: var(--bg-elev); border: 1px solid var(--border); border-radius: 16px; box-shadow: 0 30px 80px -20px rgba(0,0,0,.5); }

    a.link { color: var(--accent); }
    a.link:hover { text-decoration: underline; }

    /* Reveal animation for newly streamed feed items */
    @keyframes fadeUp { from { opacity:0; transform: translateY(4px); } to { opacity:1; transform:none; } }
    .feed-enter { animation: fadeUp .25s ease-out; }
  </style>
</head>
<body class="min-h-screen flex flex-col">
  <header class="site-header">
    <div class="px-6 py-2.5 flex items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <span class="brand-mark" aria-hidden="true">
          <svg width="18" height="18" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M8 32c8-12 18-18 24-18s16 6 24 18c-8 12-18 18-24 18S16 44 8 32Z" stroke="white" stroke-width="3"/>
            <circle cx="32" cy="32" r="8" fill="white"/>
            <circle cx="34" cy="30" r="2.4" fill="#0b1f1a"/>
          </svg>
        </span>
        <div class="leading-tight">
          <h1 class="text-[15px] font-bold tracking-tight">WildWatch <span class="faint font-medium">/ Live</span></h1>
          <p class="text-[11px] faint">Real-time perception agent for protected-area monitoring</p>
        </div>
      </div>
      <div class="flex items-center gap-3 text-xs">
        <div class="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full card-soft" title="Dashboard health: push-channel status, server uptime, and number of open browser tabs.">
          <span id="conn-dot" class="live-dot live-off" title="Live push channel between this page and the server."></span>
          <span id="conn-text" class="muted" title="green 'live' = real-time updates flowing. 'reconnecting' = stream dropped, retrying.">connecting…</span>
          <span class="faint">·</span>
          <span class="faint" title="Time since the WildWatch server process started.">up</span> <span id="uptime" class="mono" title="Server uptime">0s</span>
          <span class="faint">·</span>
          <span class="faint" title="Open browser tabs subscribed to the live feed.">tabs</span> <span id="subs" class="mono" title="SSE subscriber count">0</span>
        </div>
        <button id="theme-toggle" class="btn btn-ghost flex items-center gap-2" type="button" aria-label="Toggle theme">
          <svg id="theme-icon-sun" class="hidden" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
          <svg id="theme-icon-moon" class="hidden" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79Z"/></svg>
          <span id="theme-label" class="hidden md:inline">Theme</span>
        </button>
        <a class="btn btn-ghost hidden md:inline-flex items-center gap-2" href="https://github.com/skalkii/wildwatch" target="_blank" rel="noopener" aria-label="GitHub repository">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 .5C5.73.5.86 5.37.86 11.64c0 4.93 3.2 9.11 7.64 10.59.56.1.76-.24.76-.54v-2.03c-3.11.68-3.77-1.32-3.77-1.32-.51-1.3-1.25-1.65-1.25-1.65-1.02-.7.08-.68.08-.68 1.13.08 1.72 1.16 1.72 1.16 1.01 1.72 2.64 1.22 3.28.93.1-.73.4-1.22.72-1.5-2.48-.28-5.09-1.24-5.09-5.52 0-1.22.44-2.21 1.16-2.99-.12-.28-.5-1.42.1-2.96 0 0 .94-.3 3.09 1.14a10.7 10.7 0 0 1 5.62 0c2.15-1.44 3.09-1.14 3.09-1.14.61 1.54.23 2.68.11 2.96.72.78 1.16 1.77 1.16 2.99 0 4.29-2.62 5.24-5.11 5.51.41.36.77 1.06.77 2.13v3.15c0 .31.21.65.77.54a11.14 11.14 0 0 0 7.64-10.59C23.14 5.37 18.27.5 12 .5Z"/></svg>
        </a>
      </div>
    </div>
  </header>

  <nav class="site-nav">
    <div class="px-6 flex items-end gap-1 overflow-x-auto">
      <button data-tab="alerts" class="tab-btn tab-active">Alerts</button>
      <button data-tab="sources" class="tab-btn">Sources</button>
      <button data-tab="content" class="tab-btn">Indexed Content</button>
      <button data-tab="usage" class="tab-btn">Usage</button>
    </div>
  </nav>

  <!-- ALERTS TAB -->
  <main id="tab-alerts" class="tab-pane p-6 grid grid-cols-1 lg:grid-cols-3 gap-6 flex-1">
    <div class="lg:col-span-3">
      <h2 class="text-2xl font-bold tracking-tight">What's happening in the wild</h2>
      <p class="text-xs faint mt-0.5">Every notable thing WildWatch sees or hears in the live stream lands here, ranked by how urgent it is.</p>
    </div>

    <section class="lg:col-span-3 grid grid-cols-2 md:grid-cols-4 gap-3">
      <div class="kpi">
        <div class="kpi-label">Total events</div>
        <div class="kpi-value" id="stat-total">0</div>
        <div class="text-[11px] faint mt-1">All alerts since the dashboard started.</div>
      </div>
      <div class="kpi kpi-accent-1">
        <div class="kpi-label" style="color:#38bdf8">🟦 Info · tier 1</div>
        <div class="kpi-value" id="stat-t1" style="color:#38bdf8">0</div>
        <div class="text-[11px] faint mt-1">Routine sightings — animals at the waterhole, normal behavior.</div>
      </div>
      <div class="kpi kpi-accent-2">
        <div class="kpi-label" style="color:#f59e0b">🟡 Notable · tier 2</div>
        <div class="kpi-value" id="stat-t2" style="color:#f59e0b">0</div>
        <div class="text-[11px] faint mt-1">Worth a look — alarm calls, predator activity, big herds.</div>
      </div>
      <div class="kpi kpi-accent-3">
        <div class="kpi-label" style="color:#ef4444">🔴 Urgent · tier 3</div>
        <div class="kpi-value" id="stat-t3" style="color:#ef4444">0</div>
        <div class="text-[11px] faint mt-1">Act now — gunshots, vehicles, possible poaching.</div>
      </div>
    </section>

    <section class="lg:col-span-2 card p-4">
      <div class="flex items-start justify-between mb-3 gap-3">
        <div>
          <h2 class="flex items-center text-sm font-semibold tracking-tight">
            Live event feed
            <span id="feed-status" class="ml-2 text-[10px] pulse" style="color:#10b981">●</span>
          </h2>
          <p class="text-[11.5px] faint mt-0.5">Newest first. Each card is one thing the AI noticed — click <span class="muted">▶ play clip</span> to see the moment.</p>
        </div>
      </div>
      <div id="feed" class="space-y-2 max-h-[620px] overflow-y-auto pr-1"></div>
    </section>

    <aside class="space-y-4">
      <div class="card p-4">
        <h2 class="text-sm font-semibold tracking-tight">Live cameras</h2>
        <p class="text-[11px] faint mb-2.5 mt-0.5">Streams currently being watched by VideoDB.</p>
        <div id="rtstreams" class="text-xs space-y-1.5">loading…</div>
      </div>
      <div class="card p-4">
        <h2 class="text-sm font-semibold tracking-tight">Indexes running</h2>
        <p class="text-[11px] faint mb-2.5 mt-0.5">VideoDB sandbox running the vision + audio models.</p>
        <div id="sandboxes" class="text-xs space-y-1.5">loading…</div>
      </div>
      <details class="card p-4">
        <summary class="text-sm font-semibold tracking-tight cursor-pointer select-none flex items-center justify-between">
          <span>Test the alert system</span>
          <span class="faint text-[10px] uppercase tracking-[0.1em]">debug</span>
        </summary>
        <p class="text-[11px] faint mb-2.5 mt-2">Fire a fake event at any tier to verify the pipeline + Telegram are working.</p>
        <div class="flex gap-2">
          <button onclick="fireTest(1)" class="btn btn-ghost flex-1 text-[11.5px]">Info (test)</button>
          <button onclick="fireTest(2)" class="btn btn-ghost flex-1 text-[11.5px]">Notable (test)</button>
          <button onclick="fireTest(3)" class="btn btn-ghost flex-1 text-[11.5px]">Urgent (test)</button>
        </div>
      </details>
    </aside>
  </main>

  <!-- SOURCES TAB -->
  <main id="tab-sources" class="tab-pane p-6 hidden flex-1">
    <div class="flex justify-between items-start mb-4 gap-3 flex-wrap">
      <div>
        <h2 class="text-2xl font-bold tracking-tight">Video sources</h2>
        <p class="text-xs faint mt-0.5 max-w-xl">Anything WildWatch is watching: an uploaded clip, a YouTube link, or a live camera. Each one gets fed through the AI brain for species, behavior, environment and audio analysis.</p>
      </div>
      <button id="add-source-btn" class="btn btn-primary flex items-center gap-1.5">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
        Add source
      </button>
    </div>

    <section id="sources-summary" class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-5">
      <div class="kpi"><div class="kpi-label">Total</div><div class="kpi-value" id="src-stat-total">0</div><div class="text-[11px] faint mt-1">Sources added.</div></div>
      <div class="kpi kpi-accent-1"><div class="kpi-label" style="color:#10b981">Ready</div><div class="kpi-value" id="src-stat-ready" style="color:#10b981">0</div><div class="text-[11px] faint mt-1">Indexed and searchable.</div></div>
      <div class="kpi"><div class="kpi-label">In progress</div><div class="kpi-value" id="src-stat-progress">0</div><div class="text-[11px] faint mt-1">Connecting / ingesting / indexing.</div></div>
      <div class="kpi kpi-accent-3"><div class="kpi-label" style="color:#ef4444">Errors</div><div class="kpi-value" id="src-stat-error" style="color:#ef4444">0</div><div class="text-[11px] faint mt-1">Need attention.</div></div>
    </section>

    <div class="card p-3 mb-3 flex items-center gap-2 text-[12px] muted flex-wrap">
      <span class="font-medium">Statuses:</span>
      <span class="pill status-queued">queued</span><span class="faint">waiting to start</span>
      <span class="faint">·</span>
      <span class="pill status-connecting">connecting</span><span class="faint">opening the stream</span>
      <span class="faint">·</span>
      <span class="pill status-ingesting">ingesting</span><span class="faint">pulling video into VideoDB</span>
      <span class="faint">·</span>
      <span class="pill status-indexing">indexing</span><span class="faint">AI reading frames</span>
      <span class="faint">·</span>
      <span class="pill status-ready">ready</span><span class="faint">live + searchable</span>
    </div>

    <div id="sources-grid" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
      <div class="faint text-sm">loading…</div>
    </div>
  </main>

  <!-- INDEXED CONTENT TAB -->
  <main id="tab-content" class="tab-pane p-6 hidden flex-1">
    <div class="mb-5">
      <h2 class="text-2xl font-bold tracking-tight">Search what WildWatch has seen</h2>
      <p class="text-xs faint mt-0.5 max-w-2xl">Every frame and sound the AI brain has analysed is searchable in plain English. Ask for <span class="muted">"elephant drinking"</span>, <span class="muted">"gunshot"</span>, or <span class="muted">"juvenile near water"</span> — the index will return the matching moments.</p>
    </div>

    <section class="card p-4 mb-5">
      <div class="grid grid-cols-1 lg:grid-cols-12 gap-3 items-stretch">
        <div class="lg:col-span-7 relative">
          <svg class="absolute left-3 top-1/2 -translate-y-1/2 faint" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input id="search-q" placeholder='Try: "lion roar", "vehicle at night", "juvenile elephant"…' class="input w-full pl-9 pr-3 py-2.5 text-sm">
        </div>
        <div class="lg:col-span-3">
          <select id="search-scope" class="input w-full px-3 py-2.5 text-sm">
            <option value="collection">Search everywhere</option>
            <option value="video">A specific uploaded video</option>
            <option value="rtstream">A specific live stream</option>
          </select>
        </div>
        <button id="search-go" class="btn btn-primary lg:col-span-2">Search</button>
      </div>
      <input id="search-target-id" placeholder="Paste the video or rtstream ID…" class="input w-full px-3 py-2 text-sm mt-3 hidden">
      <p class="text-[11px] faint mt-2">Results are ranked by how well each moment matches the query.</p>
    </section>

    <div id="search-results" class="space-y-2 mb-6"></div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <section class="card p-0 overflow-hidden flex flex-col" style="max-height:640px;">
        <header class="p-4 pb-2 border-b divider" style="background:var(--bg); position:sticky; top:0; z-index:2;">
          <div class="flex items-center justify-between gap-2">
            <div>
              <h3 class="text-sm font-semibold tracking-tight">Library</h3>
              <p class="text-[11.5px] faint mt-0.5">Every video uploaded into VideoDB. Click one to see what the AI extracted.</p>
            </div>
            <div class="text-[11px] faint mono" id="library-count">&mdash;</div>
          </div>
          <div class="grid grid-cols-1 sm:grid-cols-12 gap-2 mt-2.5">
            <div class="sm:col-span-7 relative">
              <svg class="absolute left-2.5 top-1/2 -translate-y-1/2 faint" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
              <input id="library-filter" placeholder="Filter by name or ID&hellip;" class="input w-full pl-8 pr-2 py-1.5 text-[12px]">
            </div>
            <select id="library-sort" class="sm:col-span-3 input w-full px-2 py-1.5 text-[12px]">
              <option value="name-asc">Name (A&ndash;Z)</option>
              <option value="name-desc">Name (Z&ndash;A)</option>
              <option value="length-desc" selected>Longest first</option>
              <option value="length-asc">Shortest first</option>
              <option value="id-desc">Newest (by id)</option>
              <option value="id-asc">Oldest (by id)</option>
            </select>
            <select id="library-kind" class="sm:col-span-2 input w-full px-2 py-1.5 text-[12px]">
              <option value="all">All kinds</option>
              <option value="clip">Clips</option>
              <option value="uploaded">Uploaded</option>
              <option value="stream">Stream snippets</option>
              <option value="reel">Reels</option>
            </select>
          </div>
        </header>
        <div id="videos-list" class="text-xs space-y-1 overflow-y-auto p-4 pt-3" style="flex:1; min-height:0;">loading&hellip;</div>
      </section>
      <section class="card p-4">
        <h3 class="text-sm font-semibold tracking-tight">Inside this video</h3>
        <p class="text-[11.5px] faint mt-0.5 mb-2.5">The AI indexes that ran on it and the scene-by-scene descriptions they produced.</p>
        <div id="content-detail" class="text-xs space-y-2 faint">
          Pick a video on the left to see its indexes and recent scenes.
        </div>
      </section>
    </div>
  </main>

  <!-- USAGE TAB -->
  <main id="tab-usage" class="tab-pane p-6 hidden flex-1">
    <div class="mb-5">
      <h2 class="text-2xl font-bold tracking-tight">Cost &amp; usage</h2>
      <p class="text-xs faint mt-0.5">How much WildWatch has spent running on VideoDB so far today.</p>
    </div>

    <!-- HERO: total spend -->
    <section class="card p-6 mb-5 relative overflow-hidden">
      <div class="absolute inset-0 pointer-events-none" style="background: radial-gradient(600px 200px at 100% 0%, color-mix(in oklab,#f59e0b 14%,transparent), transparent 60%);"></div>
      <div class="relative grid grid-cols-1 md:grid-cols-3 gap-4 items-center">
        <div class="md:col-span-2">
          <div class="text-[11px] uppercase tracking-[0.12em] faint font-semibold">Estimated spend so far</div>
          <div class="flex items-baseline gap-2 mt-1">
            <span id="usage-total" class="text-5xl font-bold tracking-tight" style="color:#f59e0b">$0.00</span>
            <span id="usage-since" class="faint text-xs">since start</span>
          </div>
          <p class="text-[12.5px] muted mt-2 max-w-xl leading-relaxed">
            VideoDB charges for two things: <strong class="muted">live streams</strong> we keep watching, and the <strong class="muted">AI brain</strong> that reads them. This is an upper-bound — actual VideoDB billing is shown below.
          </p>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <div class="card-soft p-3">
            <div class="flex items-center gap-1.5 text-[10.5px] uppercase tracking-[0.1em] faint font-semibold">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg>
              Live streams
            </div>
            <div id="usage-rt" class="text-xl font-bold mt-1 mono">$0.00</div>
            <div id="usage-rt-count" class="text-[11px] faint mt-0.5">0 active</div>
          </div>
          <div class="card-soft p-3">
            <div class="flex items-center gap-1.5 text-[10.5px] uppercase tracking-[0.1em] faint font-semibold">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33h0a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82v0a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
              AI brain (sandbox)
            </div>
            <div id="usage-sb" class="text-xl font-bold mt-1 mono">$0.00</div>
            <div id="usage-sb-count" class="text-[11px] faint mt-0.5">idle</div>
          </div>
        </div>
      </div>
    </section>

    <!-- REAL VIDEODB BILLING (from check_usage) -->
    <section id="usage-real" class="card p-5 mb-5 hidden">
      <div class="flex items-center justify-between mb-1 flex-wrap gap-2">
        <h3 class="text-sm font-semibold tracking-tight">Real VideoDB billing this period</h3>
        <span id="usage-plan" class="text-[11px] faint mono"></span>
      </div>
      <p class="text-[12px] faint mb-3">Live numbers from <code class="mono">conn.check_usage()</code> — what VideoDB will actually charge.</p>
      <div class="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
        <div class="card-soft p-3">
          <div class="text-[10.5px] uppercase tracking-[0.1em] faint font-semibold">Credit used this period</div>
          <div id="usage-credit-used" class="text-xl font-bold mt-1 mono">$0.00</div>
        </div>
        <div class="card-soft p-3">
          <div class="text-[10.5px] uppercase tracking-[0.1em] faint font-semibold">Credit balance</div>
          <div id="usage-credit-balance" class="text-xl font-bold mt-1 mono">$0.00</div>
          <div id="usage-credit-warn" class="text-[11px] mt-0.5 hidden" style="color:#ef4444">⚠ overdrawn — top up to continue</div>
        </div>
        <div class="card-soft p-3">
          <div class="text-[10.5px] uppercase tracking-[0.1em] faint font-semibold">Top resource</div>
          <div id="usage-top-resource" class="text-sm font-semibold mt-1">—</div>
          <div id="usage-top-resource-amt" class="text-[11px] faint mono mt-0.5">$0.00</div>
        </div>
      </div>
      <div class="flex items-center justify-between mb-2">
        <h4 class="text-[12px] font-semibold tracking-tight">Where the money went</h4>
        <span id="usage-breakdown-count" class="text-[11px] faint">0 resources</span>
      </div>
      <p class="text-[11.5px] faint mb-2">Each row: <span class="muted">resource units &times; price per unit = cost</span>. Sorted biggest spend first.</p>
      <div id="usage-breakdown" class="space-y-1.5"></div>
    </section>

    <!-- HOW IT BREAKS DOWN -->
    <section class="card p-5 mb-5">
      <div class="flex items-center justify-between mb-3">
        <h3 class="text-sm font-semibold tracking-tight">What we're paying for right now (local estimate)</h3>
        <span id="usage-detail-count" class="text-[11px] faint">no items</span>
      </div>
      <p class="text-[12px] faint mb-3 leading-relaxed">
        Each row is one resource that has been running. <span class="muted">Hours &times; hourly rate = cost.</span>
        Stopping a stream or shutting down the sandbox stops the meter on that row.
      </p>
      <div id="usage-detail-rows" class="space-y-2">
        <div class="faint text-sm">loading…</div>
      </div>
    </section>

    <!-- INVOICES -->
    <section class="card p-5 mb-5">
      <h3 class="text-sm font-semibold tracking-tight mb-1">Recent activity (VideoDB invoices)</h3>
      <p class="text-[12px] faint mb-3">The most recent ten line-items VideoDB has billed for. This is the real number.</p>
      <div id="usage-invoices-pretty">
        <div class="faint text-sm">loading…</div>
      </div>
    </section>

    <!-- TECHNICAL DETAILS (collapsible) -->
    <details class="card p-5">
      <summary class="cursor-pointer text-sm font-medium select-none flex items-center justify-between">
        <span>Technical details</span>
        <span class="faint text-[11px]">raw SDK output</span>
      </summary>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
        <div>
          <div class="text-[11px] uppercase tracking-[0.12em] faint mb-1 font-semibold">SDK <code class="mono">check_usage()</code></div>
          <pre id="usage-raw" class="text-xs muted overflow-x-auto card-soft p-3 max-h-72">loading…</pre>
        </div>
        <div>
          <div class="text-[11px] uppercase tracking-[0.12em] faint mb-1 font-semibold">Invoices raw</div>
          <pre id="usage-invoices" class="text-xs muted overflow-x-auto card-soft p-3 max-h-72">loading…</pre>
        </div>
      </div>
      <p class="text-[11px] faint mt-3">Local estimate derived from <code class="mono">.state.json</code> start timestamps. Sandbox cost only counts the most-recent slot in state.</p>
      <div class="mt-3 pt-3 border-t divider flex flex-wrap gap-3 text-[11px]">
        <span class="faint">Dev endpoints:</span>
        <a class="link mono" href="/health" target="_blank" rel="noopener">/health</a>
        <a class="link mono" href="/api/stats" target="_blank" rel="noopener">/api/stats</a>
        <a class="link" href="https://docs.videodb.io" target="_blank" rel="noopener">VideoDB docs</a>
        <a class="link" href="https://github.com/skalkii/wildwatch" target="_blank" rel="noopener">GitHub</a>
      </div>
    </details>
  </main>

  <!-- ADD SOURCE MODAL -->
  <div id="add-modal" class="hidden fixed inset-0 modal-backdrop z-50 flex items-center justify-center p-4">
    <div class="modal-card p-6 w-full max-w-md">
      <h3 class="text-lg font-bold mb-4 tracking-tight">Add source</h3>
      <div class="flex gap-1 mb-4">
        <button data-modal-tab="upload" class="modal-tab-btn tab-active">File upload</button>
        <button data-modal-tab="url" class="modal-tab-btn">URL · YouTube/HLS</button>
        <button data-modal-tab="rtsp" class="modal-tab-btn">RTSP/RTMP</button>
      </div>
      <div class="space-y-3">
        <input id="modal-name" placeholder="Name (required)" class="input w-full px-3 py-2 text-sm">
        <div id="modal-pane-upload">
          <input id="modal-file" type="file" accept="video/*" class="w-full text-sm">
          <p class="text-[11px] faint mt-1">Max 500 MB</p>
        </div>
        <div id="modal-pane-url" class="hidden">
          <input id="modal-url" placeholder="https://www.youtube.com/watch?v=… OR https://x/y.m3u8" class="input w-full px-3 py-2 text-sm">
          <p class="text-[11px] faint mt-1">YouTube live URLs need bridge — paste RTSP from bore.pub instead.</p>
        </div>
        <div id="modal-pane-rtsp" class="hidden">
          <input id="modal-rtsp" placeholder="rtsp://host:port/path  or  rtmp://…" class="input w-full px-3 py-2 text-sm">
        </div>
      </div>
      <div class="flex justify-end gap-2 mt-6">
        <button id="modal-cancel" class="btn btn-ghost">Cancel</button>
        <button id="modal-submit" class="btn btn-primary">Add</button>
      </div>
      <p id="modal-error" class="text-[12px] mt-3 hidden" style="color:#ef4444"></p>
    </div>
  </div>

  <footer class="site-footer mt-auto">
    <div class="flex items-center gap-2">
      <span><strong class="muted">WildWatch</strong> · built on <a class="link" href="https://videodb.io" target="_blank" rel="noopener">VideoDB</a></span>
    </div>
    <div class="faint">v0.1.0</div>
  </footer>

<script>
const $ = (id) => document.getElementById(id);
const TIER_NAME = { 1: 'INFO', 2: 'NOTABLE', 3: 'URGENT' };

function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

// Reject any non-http(s) / non-relative URL — blocks `javascript:` and `data:` URIs
// that the SDK could theoretically return and that escapeHtml does NOT defang.
// Also explicitly rejects protocol-relative `//evil.com` which would otherwise
// pass the `startsWith('/')` branch and resolve to an attacker-controlled host
// (open-redirect / phishing vector).
function safeUrl(u) {
  if (u == null) return '';
  const s = String(u).trim();
  if (s === '') return '';
  if (s.startsWith('//')) return '';  // protocol-relative → drop
  if (s.startsWith('/') || s.startsWith('#') || s.startsWith('?')) return s;
  if (/^https?:[/]{2}/i.test(s)) return s;
  return ''; // anything else (javascript:, data:, vbscript:, file:) → drop
}

// ──── ID pills (click to copy) ───────────────────────────────────────────
// Every external identifier (video_id, rtstream_id, scene_index_id, event_id,
// sandbox id, source id) renders through _idPill. Click → clipboard, with a
// brief "copied" visual + toast confirmation.
function _idPill(id, opts) {
  if (id == null || id === '') return '';
  const s = String(id);
  const trunc = opts && opts.truncate;
  const max = (opts && opts.maxLen) || 18;
  const display = trunc && s.length > max ? s.slice(0, max) + '…' : s;
  const label = (opts && opts.label) ? `${escapeHtml(opts.label)} ` : '';
  return `<button type="button" class="id-pill" data-action="copy-id" data-id="${escapeHtml(s)}" title="Click to copy: ${escapeHtml(s)}">${label}<span class="id-pill-text">${escapeHtml(display)}</span><svg class="id-pill-icon" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg></button>`;
}

async function copyIdToClipboard(text, btn) {
  let ok = false;
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      ok = true;
    } else {
      // Fallback for non-secure contexts (LAN demo over http://).
      const ta = document.createElement('textarea');
      ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      ok = document.execCommand('copy');
      ta.remove();
    }
  } catch (e) { ok = false; }
  if (ok) {
    showToast(`Copied: ${text.slice(0, 36)}${text.length > 36 ? '…' : ''}`, { variant: 'success', duration: 1800 });
    if (btn) {
      btn.classList.add('copied');
      setTimeout(() => btn.classList.remove('copied'), 900);
    }
  } else {
    showToast('Copy failed — your browser blocked clipboard access.', { variant: 'error', duration: 4000 });
  }
}

// ──── Toast notifications (vanilla react-toastify-style) ─────────────────
// Reusable, dismissable, auto-fading cards stacked in the bottom-right.
// Replaces window.alert / window.confirm — those break tab focus and look
// like a phishing dialog. Variants: info | success | warn | error.

const _TOAST_VARIANT = {
  info:    { color: '#38bdf8', icon: 'i'  },
  success: { color: '#10b981', icon: '&check;'  },
  warn:    { color: '#f59e0b', icon: '!'  },
  error:   { color: '#ef4444', icon: '&times;'  },
};

function _ensureToastStack() {
  let stack = document.getElementById('toast-stack');
  if (!stack) {
    stack = document.createElement('div');
    stack.id = 'toast-stack';
    stack.style.cssText = 'position:fixed; right:1rem; bottom:1rem; display:flex; flex-direction:column; gap:0.5rem; z-index:9999; max-width:22rem;';
    document.body.appendChild(stack);
  }
  return stack;
}

function showToast(message, opts = {}) {
  const variant = _TOAST_VARIANT[opts.variant] || _TOAST_VARIANT.info;
  const stack = _ensureToastStack();
  const card = document.createElement('div');
  card.className = 'card-soft';
  card.style.cssText = `padding:0.6rem 0.8rem; border-left:3px solid ${variant.color}; box-shadow:0 8px 24px rgba(0,0,0,0.18); transform:translateY(6px); opacity:0; transition:transform 180ms ease, opacity 180ms ease;`;
  card.innerHTML = `<div class="flex items-start gap-2">
    <span class="pill mono" style="color:${variant.color}; background:color-mix(in oklab,${variant.color} 14%,transparent); border-color:color-mix(in oklab,${variant.color} 30%,transparent); flex-shrink:0;">${variant.icon}</span>
    <div class="text-[12.5px] flex-1">${escapeHtml(message)}</div>
    <button class="text-[11px] faint" style="background:none; border:none; cursor:pointer; padding:0 0.25rem; line-height:1;" aria-label="dismiss">&times;</button>
  </div>`;
  stack.appendChild(card);
  requestAnimationFrame(() => { card.style.transform = 'translateY(0)'; card.style.opacity = '1'; });
  const dismiss = () => {
    card.style.transform = 'translateY(6px)';
    card.style.opacity = '0';
    setTimeout(() => card.remove(), 200);
  };
  card.querySelector('button').addEventListener('click', dismiss);
  const ttl = opts.duration ?? 4000;
  if (ttl > 0) setTimeout(dismiss, ttl);
  return { dismiss, card };
}

// Replacement for window.confirm — returns a Promise<boolean>. Renders a
// styled card in the centre with Confirm / Cancel buttons. Keyboard:
// Enter confirms, Esc cancels.
function confirmToast(message, opts = {}) {
  return new Promise((resolve) => {
    const overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed; inset:0; background:rgba(0,0,0,0.4); display:flex; align-items:center; justify-content:center; z-index:10000;';
    const card = document.createElement('div');
    card.className = 'card-soft';
    card.style.cssText = 'padding:1rem 1.25rem; max-width:26rem; box-shadow:0 16px 48px rgba(0,0,0,0.4);';
    const title = opts.title || 'Are you sure?';
    const confirmLabel = opts.confirmLabel || 'Confirm';
    const cancelLabel = opts.cancelLabel || 'Cancel';
    const danger = opts.danger ? '#ef4444' : 'var(--accent)';
    card.innerHTML = `<div class="text-sm font-semibold mb-1">${escapeHtml(title)}</div>
      <div class="text-[12.5px] muted mb-3" style="white-space:pre-line;">${escapeHtml(message)}</div>
      <div class="flex justify-end gap-2">
        <button data-act="cancel" class="btn" style="padding:0.4rem 0.85rem; font-size:12px;">${escapeHtml(cancelLabel)}</button>
        <button data-act="ok" class="btn btn-primary" style="padding:0.4rem 0.85rem; font-size:12px; background:${danger}; border-color:${danger};">${escapeHtml(confirmLabel)}</button>
      </div>`;
    overlay.appendChild(card);
    const close = (result) => {
      document.removeEventListener('keydown', keyHandler);
      overlay.remove();
      resolve(result);
    };
    const keyHandler = (e) => {
      if (e.key === 'Escape') close(false);
      if (e.key === 'Enter')  close(true);
    };
    document.addEventListener('keydown', keyHandler);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(false); });
    card.querySelector('[data-act=cancel]').addEventListener('click', () => close(false));
    card.querySelector('[data-act=ok]').addEventListener('click', () => close(true));
    document.body.appendChild(overlay);
    card.querySelector('[data-act=ok]').focus();
  });
}

function renderEvent(ev) {
  const tier = ev.tier || 0;
  const streamUrl = safeUrl(ev.stream_url);
  const stream = streamUrl ? `<a class="link mono text-[11px]" href="${escapeHtml(streamUrl)}" target="_blank" rel="noopener">▶ play clip</a>` : '';
  const ts = ev.received_at ? new Date(ev.received_at * 1000).toLocaleTimeString() : '';
  return `<div class="ev ev-${tier} feed-enter" data-event-id="${escapeHtml(ev.event_id || '')}">
    <div class="flex justify-between items-center text-[11px] faint">
      <span class="inline-flex items-center gap-2">${TIER_NAME[tier] || tier} ${ev.event_id ? _idPill(ev.event_id, {truncate: true}) : ''}</span>
      <span class="mono">${ts}</span>
    </div>
    <div class="text-sm mono mt-1" style="color:var(--text)">${escapeHtml(ev.label || '')}</div>
    <div class="text-xs muted mt-1">${escapeHtml(ev.explanation || '')}</div>
    <div class="mt-1.5">${stream}</div>
  </div>`;
}

function formatUptime(sec) {
  sec = Math.max(0, Math.floor(sec || 0));
  if (sec < 60) return sec + 's';
  if (sec < 3600) return Math.floor(sec/60) + 'm ' + (sec%60) + 's';
  const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60);
  return h + 'h ' + m + 'm';
}

// Keep a Set of event_ids already in the DOM so we only inject new cards.
// Without this, every poll wipes + rebuilds the feed → flicker, lost scroll
// position, re-fires the fadeUp animation on every existing card.
const _renderedEventIds = new Set();

function applyStats(s) {
  $('stat-total').textContent = s.total || 0;
  $('stat-t1').textContent = (s.tier_counts && s.tier_counts['1']) || 0;
  $('stat-t2').textContent = (s.tier_counts && s.tier_counts['2']) || 0;
  $('stat-t3').textContent = (s.tier_counts && s.tier_counts['3']) || 0;
  $('uptime').textContent = formatUptime(s.uptime_s);
  $('subs').textContent = s.subscribers || 0;

  const feed = $('feed');
  const events = s.recent_events || [];
  if (!events.length) {
    if (_renderedEventIds.size) { _renderedEventIds.clear(); feed.innerHTML = ''; }
    return;
  }
  // First paint: render all once.
  if (_renderedEventIds.size === 0) {
    feed.innerHTML = events.map(renderEvent).join('');
    for (const ev of events) if (ev.event_id) _renderedEventIds.add(ev.event_id);
    return;
  }
  // Incremental: prepend only events we haven't seen.
  const fragments = [];
  for (const ev of events) {
    if (!ev.event_id) continue; // unkeyed events would re-add forever — skip
    if (_renderedEventIds.has(ev.event_id)) continue;
    fragments.push(renderEvent(ev));
    _renderedEventIds.add(ev.event_id);
  }
  if (fragments.length) feed.insertAdjacentHTML('afterbegin', fragments.join(''));

  // Trim DOM to mirror server-side cap (MAX_RECENT_EVENTS=50) to bound memory.
  const cards = feed.querySelectorAll('.ev');
  if (cards.length > 50) {
    for (let i = 50; i < cards.length; i++) {
      const id = cards[i].dataset.eventId;
      if (id) _renderedEventIds.delete(id);
      cards[i].remove();
    }
  }
}

function applyRtstreams(d) {
  const c = $('rtstreams');
  if (!d || !d.rtstreams) { c.innerHTML = '<span class="faint">n/a</span>'; return; }
  if (!d.rtstreams.length) { c.innerHTML = '<span class="faint">none</span>'; return; }
  c.innerHTML = d.rtstreams.map(r => {
    const ok = r.status === 'connected';
    const idBit = r.id ? `<span class="ml-2 inline-block align-middle">${_idPill(r.id, {truncate: true})}</span>` : '';
    return `<div class="flex items-center justify-between gap-2">
      <span class="truncate" title="${escapeHtml(r.name)}">
        <span class="live-dot ${ok ? 'live-on' : 'live-off'}" style="vertical-align:middle"></span>
        <span style="color:var(--text)">${escapeHtml(r.name)}</span>
        ${idBit}
      </span>
      <span class="faint mono text-[10.5px]">${escapeHtml(r.status)}</span>
    </div>`;
  }).join('');
}

function applySandboxes(d) {
  const c = $('sandboxes');
  if (!d || !d.sandboxes) { c.innerHTML = '<span class="faint">n/a</span>'; return; }
  if (!d.sandboxes.length) { c.innerHTML = '<span class="faint">none</span>'; return; }
  c.innerHTML = d.sandboxes.map(sb => `<div class="flex items-center justify-between gap-2">
    <span class="truncate inline-flex items-center gap-2">
      <span class="live-dot ${sb.is_active ? 'live-on' : 'live-off'}" style="vertical-align:middle"></span>
      ${_idPill(sb.id, {truncate: true})}
    </span>
    <span class="faint text-[10.5px]">${escapeHtml(sb.tier)}</span>
  </div>`).join('');
}

async function fetchStats() {
  try {
    const r = await fetch('/api/stats');
    applyStats(await r.json());
  } catch (e) { console.warn('stats fetch failed', e); }
}

async function fetchRemote() {
  try {
    const r = await fetch('/api/remote');
    const d = await r.json();
    applyRtstreams(d);
    applySandboxes(d);
  } catch (e) { console.warn('remote fetch failed', e); }
}

async function fireTest(tier) {
  await fetch(`/webhook/${tier}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      event_id: 'manual-' + Date.now(),
      label: 'manual_dashboard_test',
      explanation: `Manual tier-${tier} fire from dashboard at ` + new Date().toISOString(),
      stream_url: null
    })
  });
}

function startSSE() {
  const es = new EventSource('/events/stream');
  es.onopen = () => {
    $('conn-dot').className = 'live-dot live-on';
    $('conn-text').textContent = 'live';
  };
  es.onerror = () => {
    $('conn-dot').className = 'live-dot live-err';
    $('conn-text').textContent = 'reconnecting…';
  };
  es.onmessage = (m) => {
    let payload = null;
    try { payload = JSON.parse(m.data); } catch (e) { /* connect comment */ }
    if (payload && (payload.type === 'source_progress' || payload.type === 'source_deleted')) {
      fetchSources();  // refresh sources view on every progress/deletion event
    }
    fetchStats();  // always refresh alert feed (cheap)
  };
}

// ──── Sources tab ────
function renderSource(s) {
  const statusClass = `status-${s.status || 'queued'}`;
  const errMsg = s.error ? `<div class="text-[11px] mt-1.5" style="color:#ef4444">${escapeHtml(s.error)}</div>` : '';
  const stage = s.stage_msg ? `<div class="text-[11px] faint mt-1">${escapeHtml(s.stage_msg)}</div>` : '';
  const remote = s.video_id ? `video ${_idPill(s.video_id, {truncate: true})}` :
                 s.rtstream_id ? `rtstream ${_idPill(s.rtstream_id, {truncate: true})}` : '';
  const created = s.created_at ? new Date(s.created_at * 1000).toLocaleString() : '';
  // Action buttons depend on the source kind:
  //   - rtsp / rtmp: stream actually reconnects to a remote feed. Reconnect
  //     is useful when the upstream drops or the bridge restarts.
  //   - upload: source file is gone after first ingest. Re-running
  //     reconnect would just fail. Offer Re-index instead (re-runs the
  //     AI scene index on the existing video).
  //   - youtube / hls: reconnect would re-upload the same URL and create
  //     a duplicate VideoDB video. Offer Re-index instead.
  const isStreamKind = (s.kind === 'rtsp' || s.kind === 'rtmp');
  const reindexable = !!s.video_id && !isStreamKind;
  const reconnectBtn = isStreamKind
    ? `<button data-action="reconnect" data-id="${escapeHtml(s.id)}" class="btn btn-ghost text-[11px] !py-1 !px-2" title="Re-establish the live stream connection on VideoDB.">Reconnect</button>`
    : '';
  const reindexBtn = reindexable
    ? `<button data-action="reindex-video" data-id="${escapeHtml(s.video_id)}" class="btn btn-ghost text-[11px] !py-1 !px-2" title="Re-run the AI scene index on this video without re-uploading.">&#8634; Re-index</button>`
    : '';
  const disconnectBtn = isStreamKind
    ? `<button data-action="disconnect" data-id="${escapeHtml(s.id)}" class="btn btn-ghost text-[11px] !py-1 !px-2" title="Stop the live rtstream on VideoDB. Keeps the source row.">Disconnect</button>`
    : '';
  return `<div class="card p-4">
    <div class="flex justify-between items-start gap-3">
      <div class="min-w-0">
        <div class="font-semibold truncate" title="${escapeHtml(s.name)}">${escapeHtml(s.name)}</div>
        <div class="text-[11px] faint mt-1">${escapeHtml(s.kind)} &middot; ${created}</div>
      </div>
      <span class="pill ${statusClass}">${escapeHtml(s.status || 'queued')}</span>
    </div>
    <div class="text-[11px] muted mt-2 truncate mono" title="${escapeHtml(s.input || '')}">${escapeHtml(s.input || '')}</div>
    ${stage}
    ${errMsg}
    <div class="text-[11px] muted mt-2">${remote}</div>
    <div class="flex gap-2 mt-3 flex-wrap">
      ${reconnectBtn}
      ${reindexBtn}
      ${disconnectBtn}
      <button data-action="delete" data-id="${escapeHtml(s.id)}" class="btn text-[11px] !py-1 !px-2" style="background:color-mix(in oklab,#ef4444 14%,transparent); color:#ef4444; border:1px solid color-mix(in oklab,#ef4444 35%,transparent);">Delete</button>
    </div>
  </div>`;
}

async function fetchSources() {
  try {
    const r = await fetch('/api/sources');
    const d = await r.json();
    const sources = d.sources || [];
    // Update summary KPIs (guard — elements only exist on Sources tab markup)
    const totalEl = $('src-stat-total');
    if (totalEl) {
      const ready = sources.filter(s => s.status === 'ready').length;
      const error = sources.filter(s => s.status === 'error').length;
      const progress = sources.filter(s => ['queued','connecting','ingesting','indexing'].includes(s.status)).length;
      totalEl.textContent = sources.length;
      $('src-stat-ready').textContent = ready;
      $('src-stat-progress').textContent = progress;
      $('src-stat-error').textContent = error;
    }
    const grid = $('sources-grid');
    if (sources.length === 0) {
      grid.innerHTML = `<div class="col-span-full card p-8 text-center">
        <div class="mx-auto mb-3 w-12 h-12 rounded-xl flex items-center justify-center" style="background:color-mix(in oklab,var(--accent) 14%,transparent); color:var(--accent)">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>
        </div>
        <div class="text-sm font-semibold">No sources yet</div>
        <div class="text-xs faint mt-1 max-w-sm mx-auto">Add a video file, a YouTube link, or a live RTSP camera. WildWatch will start watching, indexing and alerting on it within seconds.</div>
        <button class="btn btn-primary mt-4" data-action="open-add-modal">+ Add your first source</button>
      </div>`;
      return;
    }
    grid.innerHTML = sources.map(renderSource).join('');
  } catch (e) { console.warn('sources fetch failed', e); }
}

async function deleteSource(id) {
  if (!confirm('Delete this source? Remote video/rtstream will be cleaned up.')) return;
  await fetch(`/api/sources/${id}`, { method: 'DELETE' });
  fetchSources();
}

async function disconnectSource(id) {
  await fetch(`/api/sources/${id}/disconnect`, { method: 'POST' });
  fetchSources();
}

async function reconnectSource(id) {
  await fetch(`/api/sources/${id}/reconnect`, { method: 'POST' });
  fetchSources();
}

// ──── Indexed Content tab ────
function formatDuration(sec) {
  sec = Number(sec) || 0;
  if (sec <= 0) return '—';
  if (sec < 60) return Math.round(sec) + 's';
  if (sec < 3600) {
    const m = Math.floor(sec/60); const s = Math.round(sec%60);
    return s ? `${m}m ${s}s` : `${m}m`;
  }
  const h = Math.floor(sec/3600); const m = Math.round((sec%3600)/60);
  return m ? `${h}h ${m}m` : `${h}h`;
}

function videoKindIcon(name) {
  const n = (name || '').toLowerCase();
  let label = 'Clip';
  let color = '#0ea5e9';
  if (n.includes('live') || n.includes('rtsp') || n.includes('segment')) { label = 'Stream snippet'; color = '#a78bfa'; }
  else if (n.includes('digest') || n.includes('reel') || n.includes('highlight')) { label = 'Highlight reel'; color = '#10b981'; }
  else if (n.includes('upload') || n.includes('sample')) { label = 'Uploaded clip'; color = '#f59e0b'; }
  return { label, color };
}

// Cached video list — filter/sort run client-side so toolbar changes don't
// hit the API. _libraryVids is the raw payload from /api/videos.
let _libraryVids = [];

function _libraryKindKey(name) {
  const n = (name || '').toLowerCase();
  if (n.includes('live') || n.includes('rtsp') || n.includes('segment')) return 'stream';
  if (n.includes('digest') || n.includes('reel') || n.includes('highlight')) return 'reel';
  if (n.includes('upload') || n.includes('sample')) return 'uploaded';
  return 'clip';
}

function _renderLibrary() {
  const el = $('videos-list');
  const countEl = $('library-count');
  if (!el) return;
  const filter = ($('library-filter')?.value || '').trim().toLowerCase();
  const sort = $('library-sort')?.value || 'length-desc';
  const kind = $('library-kind')?.value || 'all';
  let vids = (_libraryVids || []).slice();
  if (filter) {
    vids = vids.filter(v =>
      (v.name || '').toLowerCase().includes(filter) ||
      (v.id || '').toLowerCase().includes(filter)
    );
  }
  if (kind !== 'all') {
    vids = vids.filter(v => _libraryKindKey(v.name) === kind);
  }
  const cmp = {
    'name-asc':    (a,b) => (a.name||'').localeCompare(b.name||''),
    'name-desc':   (a,b) => (b.name||'').localeCompare(a.name||''),
    'length-desc': (a,b) => (Number(b.length)||0) - (Number(a.length)||0),
    'length-asc':  (a,b) => (Number(a.length)||0) - (Number(b.length)||0),
    'id-desc':     (a,b) => (b.id||'').localeCompare(a.id||''),
    'id-asc':      (a,b) => (a.id||'').localeCompare(b.id||''),
  }[sort];
  if (cmp) vids.sort(cmp);

  const total = _libraryVids.length;
  const shown = vids.length;
  const totalSec = vids.reduce((a,v) => a + (Number(v.length)||0), 0);
  if (countEl) {
    countEl.innerHTML = (shown === total)
      ? `${total} video${total===1?'':'s'} &middot; ${formatDuration(totalSec)}`
      : `${shown} / ${total} &middot; ${formatDuration(totalSec)}`;
  }

  if (total === 0) {
    el.innerHTML = `<div class="card-soft p-4 text-center">
      <div class="text-sm muted">No videos in the library yet.</div>
      <div class="text-[11px] faint mt-1">Upload a file or connect a live source from the <strong class="muted">Sources</strong> tab to get started.</div>
    </div>`;
    return;
  }
  if (shown === 0) {
    el.innerHTML = `<div class="card-soft p-4 text-center text-[12px] faint">No videos match the current filter.</div>`;
    return;
  }
  el.innerHTML = vids.map(v => {
    const dur = formatDuration(v.length);
    const kindMeta = videoKindIcon(v.name);
    const thumb = v.thumbnail_url
      ? `<img src="${escapeHtml(v.thumbnail_url)}" alt="" class="w-14 h-14 rounded-md object-cover shrink-0" loading="lazy">`
      : `<div class="w-14 h-14 rounded-md shrink-0 flex items-center justify-center" style="background:color-mix(in oklab,${kindMeta.color} 14%,transparent); color:${kindMeta.color}">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg>
        </div>`;
    const playUrl = safeUrl(v.stream_url);
    const playBtn = playUrl
      ? `<a href="${escapeHtml(playUrl)}" target="_blank" rel="noopener" data-stop-propagation class="link text-[11px] mono shrink-0">&#9658; play</a>`
      : '';
    const deleteBtn = `<button data-action="delete-video" data-id="${escapeHtml(v.id)}" data-name="${escapeHtml(v.name || v.id)}" data-stop-propagation class="text-[11px] shrink-0" style="background:none; border:1px solid var(--border); color:#ef4444; padding:0.15rem 0.45rem; border-radius:5px; cursor:pointer;" title="Delete this video from VideoDB">&times; delete</button>`;
    return `<div class="card-soft p-2.5 flex items-center gap-3 cursor-pointer hover:border-[var(--border-strong)] transition" style="border:1px solid var(--border)" data-action="show-video" data-id="${escapeHtml(v.id)}">
      ${thumb}
      <div class="min-w-0 flex-1">
        <div class="flex items-center gap-2 flex-wrap">
          <div class="text-sm font-medium truncate" title="${escapeHtml(v.name || 'untitled')}">${escapeHtml(v.name || 'Untitled clip')}</div>
          <span class="pill" style="color:${kindMeta.color}; background:color-mix(in oklab,${kindMeta.color} 14%,transparent);">${kindMeta.label}</span>
        </div>
        <div class="text-[11px] faint flex items-center gap-2 mt-0.5 flex-wrap" data-stop-propagation>
          <span class="mono">${dur}</span>
          <span>&middot;</span>
          ${_idPill(v.id, {truncate: true})}
        </div>
      </div>
      <div class="flex items-center gap-2 shrink-0" data-stop-propagation>
        ${playBtn}
        ${deleteBtn}
      </div>
    </div>`;
  }).join('');
}

async function fetchVideos() {
  try {
    const r = await fetch('/api/videos');
    const d = await r.json();
    _libraryVids = d.videos || [];
    _renderLibrary();
  } catch (e) { console.warn('videos fetch failed', e); }
}

async function deleteVideo(videoId, name) {
  const ok = await confirmToast(
    `This permanently removes "${name || videoId.slice(0, 18)}" from VideoDB. The video, its indexes, and any scenes will be gone.`,
    { title: 'Delete video?', confirmLabel: 'Delete', cancelLabel: 'Keep', danger: true }
  );
  if (!ok) return;
  const progress = showToast(`Deleting ${name || videoId.slice(0, 12)}&hellip;`, { variant: 'info', duration: 0 });
  try {
    const r = await fetch(`/api/videos/${encodeURIComponent(videoId)}`, { method: 'DELETE' });
    const d = await r.json();
    progress.dismiss();
    if (!r.ok) {
      showToast(`Delete failed: ${d.detail || 'unknown error'}`, { variant: 'error', duration: 5000 });
      return;
    }
    showToast('Video deleted.', { variant: 'success', duration: 3000 });
    // Optimistic local update + background re-fetch.
    _libraryVids = _libraryVids.filter(v => v.id !== videoId);
    _renderLibrary();
    // If the deleted video was the one in the detail panel, clear it.
    if ($('content-detail') && $('content-detail').dataset.videoId === videoId) {
      $('content-detail').innerHTML = '<span class="faint">Pick a video on the left to see its indexes and recent scenes.</span>';
      $('content-detail').dataset.videoId = '';
    }
    fetchVideos();
  } catch (e) {
    progress.dismiss();
    showToast(`Network error: ${e}`, { variant: 'error', duration: 5000 });
  }
}

// Infer index kind from its name so the dashboard can show "audio" vs
// "visual" pills without an extra API call. Naming convention used by
// `ingest.py:_kick_off_*` and `scripts/index_corpus.py`.
function _indexKindFromName(name) {
  const n = (name || '').toLowerCase();
  if (n.includes('audio')) return 'audio';
  if (n.includes('environment')) return 'environment';
  if (n.includes('behavior')) return 'behavior';
  if (n.includes('species') || n.includes('wildwatch-auto') || n.startsWith('wildwatch:')) return 'visual';
  return 'visual';
}
const _INDEX_KIND_PILL = {
  visual:      ['👁  Visual',     '#0ea5e9'],
  audio:       ['🎧 Audio',       '#10b981'],
  environment: ['🌿 Environment', '#a78bfa'],
  behavior:    ['🐾 Behavior',    '#f59e0b'],
};
function _indexKindPill(name) {
  const kind = _indexKindFromName(name);
  const [label, color] = _INDEX_KIND_PILL[kind] || ['Index', '#94a3b8'];
  return `<span class="pill" style="color:${color}; background:color-mix(in oklab,${color} 14%,transparent); border-color:color-mix(in oklab,${color} 30%,transparent);">${escapeHtml(label)}</span>`;
}

function _indexStatusPill(status) {
  const s = String(status || 'unknown').toLowerCase();
  const palette = {
    ready: ['Ready', 'status-ready'],
    indexed: ['Ready', 'status-ready'],
    complete: ['Ready', 'status-ready'],
    completed: ['Ready', 'status-ready'],
    processing: ['Processing', 'status-indexing'],
    queued: ['Queued', 'status-queued'],
    failed: ['Failed', 'status-error'],
    error: ['Failed', 'status-error'],
  };
  const [label, cls] = palette[s] || [s, 'status-queued'];
  return `<span class="pill ${cls}">${escapeHtml(label)}</span>`;
}

const _READY_INDEX_STATUSES = ['ready', 'indexed', 'complete', 'completed', 'done'];

async function showVideoDetail(videoId) {
  const el = $('content-detail');
  el.dataset.videoId = videoId;
  el.innerHTML = `<span class="faint">loading ${escapeHtml(videoId)} &hellip;</span>`;
  try {
    const r = await fetch(`/api/videos/${videoId}/indexes`);
    const d = await r.json();
    const idxs = d.indexes || [];
    const reindexBtn = `<button data-action="reindex-video" data-id="${escapeHtml(videoId)}" class="btn btn-ghost text-[11.5px]">↻ Re-index</button>`;
    if (idxs.length === 0) {
      el.innerHTML = `<div class="card-soft p-4">
        <div class="flex items-center justify-between gap-2 mb-1">
          <div class="text-sm font-medium">Scene index still being prepared</div>
          ${reindexBtn}
        </div>
        <div class="text-[11.5px] faint">VideoDB is reading this clip. Scene indexes show up here once the AI finishes its first pass — typically a minute or two per minute of footage. Refresh in a moment, or click <strong class="muted">Re-index</strong> to force a fresh pass.</div>
      </div>`;
      return;
    }
    el.innerHTML = `<div class="flex items-center justify-between gap-2 mb-2 flex-wrap">
        <div class="text-[11px] faint inline-flex items-center gap-2">Video ${_idPill(videoId, {truncate: true})}</div>
        ${reindexBtn}
      </div>
      <div class="space-y-2">
        ${idxs.map(i => {
          const idxId = i.scene_index_id || i.id || '';
          const name = i.name || `Scene Index ${idxId.slice(0, 8)}…`;
          const status = String(i.status || 'unknown').toLowerCase();
          const isReady = _READY_INDEX_STATUSES.includes(status);
          const btnClass = isReady ? 'btn btn-primary text-[11.5px]' : 'btn btn-ghost text-[11.5px]';
          const btnLabel = isReady ? '▶ View scenes' : `${status} — view anyway`;
          // Scene-pane lives INSIDE this card so scenes render right
          // below the index they belong to (not in a global pane at the
          // bottom of the panel). data-pane id pairs to data-idx via the
          // delegated handler.
          const paneId = `scenes-pane-${idxId}`;
          return `<div class="card-soft p-3">
            <div class="flex items-center justify-between gap-2 flex-wrap">
              <div class="min-w-0">
                <div class="flex items-center gap-2 flex-wrap">
                  ${_indexKindPill(name)}
                  <div class="text-sm font-medium truncate" title="${escapeHtml(name)}">${escapeHtml(name)}</div>
                </div>
                <div class="mt-1">${_idPill(idxId, {truncate: true})}</div>
              </div>
              ${_indexStatusPill(i.status)}
            </div>
            <div class="flex justify-end mt-2">
              <button data-action="show-scenes" data-id="${escapeHtml(videoId)}" data-idx="${escapeHtml(idxId)}" class="${btnClass}">${btnLabel}</button>
            </div>
            <div id="${paneId}" class="mt-3"></div>
          </div>`;
        }).join('')}
      </div>`;
    // Auto-load scenes for the first ready index inline so the panel
    // shows real content immediately. Users can still click other index
    // cards individually if there are multiple.
    const firstReady = idxs.find(i => _READY_INDEX_STATUSES.includes(String(i.status || '').toLowerCase()));
    if (firstReady) {
      showVideoScenes(videoId, firstReady.scene_index_id || firstReady.id);
    } else if (idxs.length > 0) {
      // No ready index — preview the first one's status inline so the
      // user sees the "still preparing" message in-place.
      const first = idxs[0];
      showVideoScenes(videoId, first.scene_index_id || first.id);
    }
  } catch (e) { el.innerHTML = `<span style="color:#ef4444">error: ${escapeHtml(String(e))}</span>`; }
}

async function reindexVideo(videoId) {
  const ok = await confirmToast(
    'VideoDB will re-read every frame + listen to the audio on this video. ' +
    'Telegram alerts will re-fire for every event detected (gunshot, ' +
    'alarm calls, rare species, etc.). Costs credits.',
    { title: 'Re-run analysis + alerts?', confirmLabel: 'Re-index + alert', cancelLabel: 'Cancel' }
  );
  if (!ok) return;
  const progress = showToast(`Requesting fresh scene index for ${videoId.slice(0, 12)}…`, { variant: 'info', duration: 0 });
  try {
    const r = await fetch(`/api/videos/${videoId}/reindex`, { method: 'POST' });
    const d = await r.json();
    progress.dismiss();
    if (!r.ok) {
      showToast(`Re-index failed: ${d.detail || JSON.stringify(d)}`, { variant: 'error', duration: 6000 });
      return;
    }
    showToast(d.message || 'Fresh scene index requested. Indexing runs in the background.', { variant: 'success', duration: 5000 });
    // Reload the detail panel so the new processing index appears.
    showVideoDetail(videoId);
  } catch (e) {
    progress.dismiss();
    showToast(`Network error: ${e}`, { variant: 'error', duration: 6000 });
  }
}

// ──── Scene parsing + rendering ─────────────────────────────────────────
// The species prompt produces bracket-tagged output:
//   [SCENE] light_mode=daylight; total=12; state=large_aggregation
//   [ANIMAL] species=oryx; count=12; age_sex=unknown; position=...
//   [NOTES] free-text observation
// Render it as friendly tagged cards rather than the raw raw text.

function _parseSceneText(raw) {
  // Returns a normalised structure covering BOTH index families:
  //   - visual species: [SCENE] [ANIMAL] [NOTES]
  //   - audio:          [SOUND] [SIGNAL] [SUMMARY]
  // The renderer downstream picks visual vs audio card layout based on
  // which fields are populated.
  const out = {scene: null, animals: [], notes: null, sounds: [], signals: [], summary: null};
  if (!raw) return out;
  const tokens = raw.split(/(\\[[A-Z_]+\\])/).map(s => s.trim()).filter(Boolean);
  for (let i = 0; i < tokens.length; i++) {
    const m = tokens[i].match(/^\\[([A-Z_]+)\\]$/);
    if (!m) continue;
    const tag = m[1];
    const body = (tokens[i+1] || '').replace(/^\\s*[:;,]\\s*/, '').trim();
    if (tag === 'SCENE')   out.scene = _parseKVList(body);
    if (tag === 'ANIMAL')  out.animals.push(_parseKVList(body));
    if (tag === 'NOTES')   out.notes = (out.notes ? out.notes + ' ' : '') + body;
    if (tag === 'SOUND')   out.sounds.push(_parseKVList(body));
    if (tag === 'SIGNAL')  out.signals.push(body);
    if (tag === 'SUMMARY') out.summary = body;
  }
  return out;
}

function _parseKVList(s) {
  // "key=value; key=value" → {key: value}. Tolerates the trailing
  // semicolons VideoDB sometimes emits and stripped values.
  const out = {};
  s.split(/[;\\n]/).forEach(part => {
    const ix = part.indexOf('=');
    if (ix < 0) return;
    const k = part.slice(0, ix).trim();
    const v = part.slice(ix + 1).trim();
    if (k) out[k] = v;
  });
  return out;
}

function _fmtSec(v) {
  // 10.01 → "10.0s", 100.1 → "1m 40s", 197.397 → "3m 17s".
  const n = Number(v);
  if (!isFinite(n)) return String(v ?? '');
  if (n < 60) return `${n.toFixed(1)}s`;
  const m = Math.floor(n / 60);
  const s = Math.round(n % 60);
  return `${m}m ${s}s`;
}

const _SCENE_STATE_PILL = {
  empty:               ['Empty',           '#94a3b8'],
  single_animal:       ['Single animal',   '#38bdf8'],
  small_group:         ['Small group',     '#10b981'],
  large_aggregation:   ['Large aggregation','#f59e0b'],
  mixed_species:       ['Mixed species',   '#a78bfa'],
};

const _LIGHT_MODE_PILL = {
  daylight:        ['☀️ Day',     '#f59e0b'],
  golden_hour:     ['🌅 Golden',  '#f59e0b'],
  dawn:            ['🌄 Dawn',    '#fb923c'],
  dusk:            ['🌆 Dusk',    '#fb923c'],
  ir_night:        ['🌙 IR night', '#60a5fa'],
  low_light:       ['🌑 Low light','#64748b'],
  low_light_color: ['🌑 Low light','#64748b'],
};

function _statePill(state) {
  const key = String(state || '').toLowerCase();
  const [label, color] = _SCENE_STATE_PILL[key] || [state || '—', '#94a3b8'];
  return `<span class="pill" style="color:${color}; background:color-mix(in oklab,${color} 14%,transparent); border-color:color-mix(in oklab,${color} 30%,transparent);">${escapeHtml(label)}</span>`;
}

function _lightPill(mode) {
  const key = String(mode || '').toLowerCase();
  const [label, color] = _LIGHT_MODE_PILL[key] || [mode || '—', '#94a3b8'];
  return `<span class="pill" style="color:${color}; background:color-mix(in oklab,${color} 14%,transparent); border-color:color-mix(in oklab,${color} 30%,transparent);">${escapeHtml(label)}</span>`;
}

function _renderAnimalRow(a) {
  const species = a.species ? a.species.replace(/_/g, ' ') : 'unknown';
  const count = a.count || '?';
  const ageSex = a.age_sex && a.age_sex !== 'unknown' ? a.age_sex : null;
  const pos = a.position || '';
  const isUncertain = /^possibly|unidentified/i.test(species);
  const speciesColor = isUncertain ? 'var(--text-faint)' : 'var(--text)';
  return `<div class="flex items-baseline gap-2 text-[12.5px]">
    <span class="font-medium" style="color:${speciesColor}">${escapeHtml(species)}</span>
    <span class="faint">&times;${escapeHtml(String(count))}</span>
    ${ageSex ? `<span class="faint">· ${escapeHtml(ageSex)}</span>` : ''}
    ${pos ? `<span class="faint truncate" title="${escapeHtml(pos)}">· ${escapeHtml(pos)}</span>` : ''}
  </div>`;
}

const _SOUND_CATEGORY_PILL = {
  biophony:       ['🦁 Biophony',       '#10b981'],
  geophony:       ['💨 Geophony',       '#60a5fa'],
  anthropophony:  ['⚠️ Anthropogenic',  '#ef4444'],
};
const _SIGNAL_PILL = {
  ALARM_CALL:           ['Alarm call',          '#f59e0b'],
  DISTRESS_CALL:        ['Distress call',       '#ef4444'],
  PREDATOR_VOCALIZATION:['Predator vocal',      '#ef4444'],
  ABNORMAL_SILENCE:     ['Abnormal silence',    '#a78bfa'],
};

function _renderSoundRow(s) {
  const cat = String(s.category || '').toLowerCase();
  const [catLabel, catColor] = _SOUND_CATEGORY_PILL[cat] || [cat || 'sound', '#94a3b8'];
  const type = s.type ? s.type.replace(/_/g, ' ') : 'unknown';
  const intensity = s.intensity ? s.intensity : null;
  const confidence = s.confidence ? s.confidence : null;
  const species = s.species && s.species !== 'unknown' ? s.species.replace(/_/g, ' ') : null;
  return `<div class="flex items-baseline gap-2 text-[12.5px] flex-wrap">
    <span class="pill" style="color:${catColor}; background:color-mix(in oklab,${catColor} 14%,transparent); border-color:color-mix(in oklab,${catColor} 30%,transparent);">${escapeHtml(catLabel)}</span>
    <span class="font-medium">${escapeHtml(type)}</span>
    ${species ? `<span class="faint">&middot; ${escapeHtml(species)}</span>` : ''}
    ${intensity ? `<span class="faint">&middot; ${escapeHtml(intensity)}</span>` : ''}
    ${confidence ? `<span class="faint">&middot; conf=${escapeHtml(confidence)}</span>` : ''}
  </div>`;
}

function _renderSignalPill(sig) {
  const key = String(sig || '').trim();
  const [label, color] = _SIGNAL_PILL[key] || [key || 'signal', '#a78bfa'];
  return `<span class="pill" style="color:${color}; background:color-mix(in oklab,${color} 14%,transparent); border-color:color-mix(in oklab,${color} 30%,transparent);">${escapeHtml(label)}</span>`;
}

function _renderSceneCard(sc, idx, videoId) {
  const start = sc.start ?? 0;
  const end = sc.end ?? 0;
  const text = (sc.description || sc.text || '').toString();
  const parsed = _parseSceneText(text);
  const isAudio = parsed.sounds.length > 0 || parsed.signals.length > 0 || !!parsed.summary;
  const isVisual = !!parsed.scene || parsed.animals.length > 0;

  const clickable = videoId && Number.isFinite(Number(start)) && Number.isFinite(Number(end)) && Number(end) > Number(start);
  const clickAttrs = (border) => clickable
    ? ` role="button" tabindex="0" data-action="play-scene-clip" data-video-id="${escapeHtml(videoId)}" data-start="${start}" data-end="${end}" style="cursor:pointer; border-left:3px solid ${border}" title="Click to play this scene"`
    : ` style="border-left:3px solid ${border}"`;
  const playHint = clickable
    ? '<span class="pill" style="color:var(--accent); background:color-mix(in oklab,var(--accent) 14%,transparent); border-color:color-mix(in oklab,var(--accent) 30%,transparent);">&#9658; Play</span>'
    : '';

  // ──── audio-flavoured card ────────────────────────────────────────────
  if (isAudio && !isVisual) {
    // Border color escalates to red if anthropogenic sound is present.
    const anthro = parsed.sounds.some(s => String(s.category || '').toLowerCase() === 'anthropophony');
    const border = anthro ? '#ef4444' : (parsed.signals.length ? '#f59e0b' : 'var(--border)');
    return `<div class="card-soft p-3 mb-2 scene-card hover:translate-y-[-1px] transition"${clickAttrs(border)}>
      <div class="flex items-center justify-between gap-2 flex-wrap">
        <div class="flex items-center gap-2">
          <span class="text-[11px] faint mono">#${idx}</span>
          <span class="text-sm font-semibold">Audio segment</span>
          <span class="text-[11px] faint mono">${_fmtSec(start)} &ndash; ${_fmtSec(end)}</span>
        </div>
        <div class="flex items-center gap-1.5 flex-wrap">
          ${parsed.signals.map(_renderSignalPill).join('')}
          ${playHint}
        </div>
      </div>
      ${parsed.sounds.length > 0 ? `<div class="mt-2 space-y-1">${parsed.sounds.map(_renderSoundRow).join('')}</div>` : ''}
      ${parsed.summary ? `<div class="text-[11.5px] muted mt-2 border-t divider pt-1.5"><span class="faint">Summary:</span> ${escapeHtml(parsed.summary)}</div>` : ''}
    </div>`;
  }

  // ──── visual-flavoured card (default + back-compat) ───────────────────
  const scene = parsed.scene || {};
  const state = scene.state || (parsed.animals.length === 0 ? 'empty' : '—');
  const total = scene.total || (parsed.animals.length || 0);
  const lightMode = scene.light_mode;
  const animals = parsed.animals;
  const isEmpty = state === 'empty' || animals.length === 0;
  const [, stateColor] = _SCENE_STATE_PILL[state] || ['—', 'var(--accent)'];
  const parsedNothing = !parsed.scene && animals.length === 0 && !parsed.notes;
  const fallback = parsedNothing && text
    ? `<div class="text-[12.5px] mt-2">${escapeHtml(text.slice(0, 600))}</div>`
    : '';
  const border = isEmpty ? 'var(--border)' : stateColor;
  return `<div class="card-soft p-3 mb-2 scene-card hover:translate-y-[-1px] transition"${clickAttrs(border)}>
    <div class="flex items-center justify-between gap-2 flex-wrap">
      <div class="flex items-center gap-2">
        <span class="text-[11px] faint mono">#${idx}</span>
        <span class="text-sm font-semibold">Scene</span>
        <span class="text-[11px] faint mono">${_fmtSec(start)} &ndash; ${_fmtSec(end)}</span>
      </div>
      <div class="flex items-center gap-1.5 flex-wrap">
        ${lightMode ? _lightPill(lightMode) : ''}
        ${_statePill(state)}
        ${total && total !== '0' ? `<span class="pill" style="color:var(--text); background:var(--bg-soft); border-color:var(--border);">${escapeHtml(String(total))} animals</span>` : ''}
        ${playHint}
      </div>
    </div>
    ${animals.length > 0 ? `<div class="mt-2 space-y-1">${animals.map(_renderAnimalRow).join('')}</div>` : ''}
    ${isEmpty && animals.length === 0 && !fallback ? '<div class="text-[12px] faint mt-1">No animals visible in this segment.</div>' : ''}
    ${fallback}
    ${parsed.notes ? `<div class="text-[11.5px] muted mt-2 border-t divider pt-1.5"><span class="faint">Notes:</span> ${escapeHtml(parsed.notes)}</div>` : ''}
  </div>`;
}

function _openClipPlayer(streamUrl, title) {
  // Modal HLS player. m3u8 manifests don't play in plain <video> tags on
  // most desktop browsers — Safari handles HLS natively, every other
  // browser needs hls.js to demux the manifest. Load both paths.
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed; inset:0; background:rgba(0,0,0,0.78); display:flex; align-items:center; justify-content:center; z-index:10000; padding:1rem;';
  const card = document.createElement('div');
  card.className = 'card-soft';
  card.style.cssText = 'padding:0.75rem; max-width:min(960px, 95vw); width:100%;';
  card.innerHTML = `<div class="flex items-center justify-between mb-2">
    <div class="text-sm font-semibold">${escapeHtml(title || 'Scene clip')}</div>
    <button data-act="close" class="text-[12px] faint" style="background:none; border:none; cursor:pointer;" aria-label="close">&times; close</button>
  </div>
  <video controls autoplay playsinline style="width:100%; max-height:70vh; background:#000; border-radius:6px;"></video>
  <div class="text-[11px] faint mt-2 mono break-all"><a class="link" href="${escapeHtml(streamUrl)}" target="_blank" rel="noopener">Open manifest in new tab &nearr;</a></div>`;
  overlay.appendChild(card);
  document.body.appendChild(card.parentElement === overlay ? overlay : overlay);
  const video = card.querySelector('video');
  let hls = null;
  const cleanup = () => {
    if (hls) try { hls.destroy(); } catch (e) {}
    video.pause();
    video.removeAttribute('src');
    video.load();
    overlay.remove();
    document.removeEventListener('keydown', keyHandler);
  };
  const keyHandler = (e) => { if (e.key === 'Escape') cleanup(); };
  document.addEventListener('keydown', keyHandler);
  card.querySelector('[data-act=close]').addEventListener('click', cleanup);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) cleanup(); });

  if (video.canPlayType('application/vnd.apple.mpegurl')) {
    // Safari native HLS.
    video.src = streamUrl;
  } else if (window.Hls && window.Hls.isSupported()) {
    hls = new window.Hls({ maxBufferLength: 30 });
    hls.loadSource(streamUrl);
    hls.attachMedia(video);
    hls.on(window.Hls.Events.ERROR, (_, data) => {
      if (data.fatal) {
        showToast(`Player error: ${data.type}/${data.details}`, { variant: 'error', duration: 5000 });
      }
    });
  } else {
    showToast('HLS not supported in this browser. Use the manifest link.', { variant: 'warn', duration: 6000 });
  }
}

async function playSceneClip(videoId, start, end) {
  const progress = showToast('Generating clip URL…', { variant: 'info', duration: 0 });
  try {
    const r = await fetch(`/api/videos/${videoId}/clip?start=${start}&end=${end}`);
    const d = await r.json();
    progress.dismiss();
    if (!r.ok || !d.stream_url) {
      showToast(`Clip failed: ${d.detail || 'no stream URL'}`, { variant: 'error', duration: 5000 });
      return;
    }
    _openClipPlayer(d.stream_url, `${videoId.slice(0, 12)}… · ${start}s &ndash; ${end}s`);
  } catch (e) {
    progress.dismiss();
    showToast(`Error: ${e}`, { variant: 'error', duration: 5000 });
  }
}

async function showVideoScenes(videoId, indexId) {
  // Per-index pane lives inside each index card (id="scenes-pane-<idxId>")
  // so scenes render directly below the index they belong to.
  const pane = $(`scenes-pane-${indexId}`);
  if (!pane) return;
  pane.innerHTML = '<div class="faint text-sm">loading scenes …</div>';
  try {
    const r = await fetch(`/api/videos/${videoId}/scenes/${indexId}?limit=20`);
    const d = await r.json();
    const status = String(d.status || 'unknown').toLowerCase();
    const scenes = d.scenes || [];
    const header = `<div class="flex items-center justify-between mb-2">
      <div class="text-[11.5px] faint">Index ${_indexStatusPill(status)} · ${scenes.length} scene${scenes.length===1?'':'s'} shown</div>
    </div>`;
    if (status === 'processing' || status === 'queued' || status === 'unknown') {
      pane.innerHTML = header + `<div class="card-soft p-3 text-sm">
        <strong class="muted">AI is still reading this video.</strong>
        <div class="faint text-[11.5px] mt-1">Scene index is in <code class="mono">${escapeHtml(status)}</code> state. VideoDB needs a minute or two per minute of footage. Refresh the dashboard or click the button again shortly.</div>
      </div>`;
      return;
    }
    if (status === 'not_found') {
      pane.innerHTML = header + `<div class="card-soft p-3 text-sm" style="color:#ef4444">Index not found on VideoDB. It may have been deleted.</div>`;
      return;
    }
    if (status === 'failed' || status === 'error') {
      pane.innerHTML = header + `<div class="card-soft p-3 text-sm" style="color:#ef4444">Index failed on VideoDB. Re-run the corpus indexer to retry.</div>`;
      return;
    }
    if (scenes.length === 0) {
      pane.innerHTML = header + `<div class="card-soft p-3 text-sm faint">No scenes returned. The video may be too short for the configured batch size.</div>`;
      return;
    }
    pane.innerHTML = header + scenes.map((sc, i) =>
      _renderSceneCard(sc, i + 1, videoId)
    ).join('');
  } catch (e) { pane.innerHTML = `<span style="color:#ef4444">error: ${escapeHtml(String(e))}</span>`; }
}

// search
// Library toolbar — client-side filter/sort/kind controls, re-render only.
['library-filter','library-sort','library-kind'].forEach(id => {
  const el = $(id);
  if (!el) return;
  const evt = el.tagName === 'INPUT' ? 'input' : 'change';
  el.addEventListener(evt, () => _renderLibrary());
});

$('search-scope').addEventListener('change', () => {
  const scope = $('search-scope').value;
  $('search-target-id').classList.toggle('hidden', scope === 'collection');
});

$('search-go').addEventListener('click', async () => {
  const q = $('search-q').value.trim();
  if (!q) return;
  const scope = $('search-scope').value;
  const target_id = $('search-target-id').value.trim() || null;
  $('search-results').innerHTML = '<span class="text-gray-500">searching...</span>';
  try {
    const r = await fetch('/api/search', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: q, scope, target_id })
    });
    const d = await r.json();
    if (!r.ok) { $('search-results').innerHTML = `<span style="color:#ef4444">${escapeHtml(d.detail || 'error')}</span>`; return; }
    const shots = d.shots || [];
    if (shots.length === 0) { $('search-results').innerHTML = '<span class="faint">no results</span>'; return; }
    const headerBits = [`${shots.length} shot(s) in <span class="muted">${escapeHtml(d.scope)}</span> scope`];
    if (d.videos_searched != null) headerBits.push(`across ${d.videos_searched} video(s)`);
    $('search-results').innerHTML = `<div class="text-[11px] faint mb-1">${headerBits.join(' &middot; ')}</div>` +
      shots.map((sh, i) => {
        // Reuse the scene parser/renderer so bracket-tagged text turns
        // into the same human-readable card used in the Indexed Content tab.
        const vid = sh.video_id;
        const synthetic = {
          start: sh.start,
          end: sh.end,
          description: sh.text || '',
        };
        const card = _renderSceneCard(synthetic, i + 1, vid);
        const srcName = sh.video_name || '';
        const srcId = sh.video_id || sh.rtstream_id || '';
        const idxId = sh.scene_index_id || '';
        const idxName = sh.scene_index_name || '';
        const parts = [`score=${sh.score?.toFixed?.(2) ?? '?'}`];
        if (srcName) parts.push(escapeHtml(srcName));
        if (srcId) parts.push(_idPill(srcId, {truncate: true}));
        if (idxName && !srcName) parts.push(escapeHtml(idxName));
        if (idxId) parts.push(_idPill(idxId, {truncate: true, label: 'idx'}));
        const meta = `<div class="text-[11px] faint mt-1 inline-flex items-center gap-2 flex-wrap" data-stop-propagation>${parts.join(' <span class=&quot;faint&quot;>&middot;</span> ')}</div>`;
        return `<div class="mb-2">${card}${meta}</div>`;
      }).join('');
  } catch (e) { $('search-results').innerHTML = `<span style="color:#ef4444">${escapeHtml(String(e))}</span>`; }
});

// ──── Usage tab ────
function formatHours(h) {
  if (h == null || isNaN(h)) return '—';
  if (h < 1/60) return '< 1 min';
  if (h < 1) return Math.round(h * 60) + ' min';
  if (h < 10) return h.toFixed(1) + ' h';
  return Math.round(h) + ' h';
}

function formatCurrency(n) {
  if (n == null || isNaN(n)) return '$0.00';
  return '$' + Number(n).toFixed(2);
}

function formatRelative(tsLike) {
  // Accept ISO string OR seconds-epoch
  let ms = null;
  if (typeof tsLike === 'number') ms = tsLike * (tsLike < 1e12 ? 1000 : 1);
  else if (typeof tsLike === 'string') { const t = Date.parse(tsLike); if (!isNaN(t)) ms = t; }
  if (ms == null) return '';
  const diff = (Date.now() - ms) / 1000;
  if (diff < 60) return Math.round(diff) + 's ago';
  if (diff < 3600) return Math.round(diff/60) + ' min ago';
  if (diff < 86400) return Math.round(diff/3600) + ' h ago';
  return Math.round(diff/86400) + ' d ago';
}

function renderUsageRow(x) {
  const isRT = x.kind === 'rtstream';
  const label = isRT ? 'Live stream' : 'AI brain (sandbox)';
  const sub = x.key || x.id || '';
  const tier = x.tier ? `<span class="pill status-ready">${escapeHtml(x.tier)}</span>` : '';
  const icon = isRT
    ? `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg>`
    : `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="M9 9h6v6H9z"/></svg>`;
  return `<div class="card-soft p-3 flex items-center gap-3">
    <div class="shrink-0 w-9 h-9 rounded-lg flex items-center justify-center" style="background:color-mix(in oklab,var(--accent) 15%,transparent); color:var(--accent)">${icon}</div>
    <div class="min-w-0 flex-1">
      <div class="flex items-center gap-2">
        <div class="text-sm font-medium">${label}</div>
        ${tier}
      </div>
      <div class="text-[11px] faint mono truncate" title="${escapeHtml(sub)}">${escapeHtml(sub)}</div>
    </div>
    <div class="text-right shrink-0">
      <div class="text-sm font-semibold mono">${formatCurrency(x.burn_usd)}</div>
      <div class="text-[11px] faint mono">${formatHours(x.hours)} &times; ${formatCurrency(x.rate_usd_per_h)}/h</div>
    </div>
  </div>`;
}

function renderInvoices(invoices) {
  if (!Array.isArray(invoices) || invoices.length === 0) {
    return '<div class="faint text-sm">No invoices yet — VideoDB hasn\\'t billed anything for this account.</div>';
  }
  // Each invoice shape varies; pull the most-likely fields gracefully.
  const rows = invoices.map(inv => {
    const amount = inv.amount ?? inv.total ?? inv.cost ?? inv.usd ?? null;
    const when = inv.created_at || inv.timestamp || inv.date || inv.invoice_date || null;
    const desc = inv.description || inv.kind || inv.type || inv.line_item || inv.product || '—';
    const id = inv.id || inv.invoice_id || '';
    return `<tr class="border-t divider">
      <td class="py-2 pr-3">
        <div class="text-sm">${escapeHtml(String(desc))}</div>
        ${id ? `<div class="text-[11px] faint mono">${escapeHtml(String(id))}</div>` : ''}
      </td>
      <td class="py-2 pr-3 text-[11px] faint">${when ? `<div>${escapeHtml(new Date(when).toLocaleString())}</div><div class="mono">${formatRelative(when)}</div>` : '—'}</td>
      <td class="py-2 text-right font-semibold mono">${amount != null ? formatCurrency(amount) : '—'}</td>
    </tr>`;
  }).join('');
  return `<table class="w-full text-sm">
    <thead><tr class="text-[10.5px] uppercase tracking-[0.1em] faint">
      <th class="text-left font-semibold pb-2">Item</th>
      <th class="text-left font-semibold pb-2">When</th>
      <th class="text-right font-semibold pb-2">Amount</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

// Friendly labels + simple groupings for raw VideoDB cost_metric keys.
const RESOURCE_LABELS = {
  rtstream_compute: 'Live stream watching',
  rtstream_storage: 'Live stream storage',
  scene: 'Visual scene reads',
  scene_index: 'Scene index',
  spoken_index: 'Spoken-word index',
  spoken_index_storage: 'Spoken-word storage',
  sandbox_medium: 'AI brain (medium sandbox)',
  sandbox_small: 'AI brain (small sandbox)',
  search_query: 'Search queries',
  llm_basic: 'LLM (basic)',
  llm_pro: 'LLM (pro)',
  llm_ultra: 'LLM (ultra)',
  llm_custom: 'LLM (custom)',
  llm: 'LLM (legacy)',
  file_upload: 'File uploads',
  media_storage: 'Media storage',
  streaming: 'HLS streaming',
  simple_stream: 'Simple stream',
  programmable_stream: 'Programmable stream (digest reels)',
  timeline_inline: 'Timeline inline (digest)',
  timeline_overlay: 'Timeline overlay (digest)',
  transcription: 'Transcription',
  translation: 'Translation',
  dubbing: 'Dubbing',
  music_generation: 'Generated music',
  voice_generation: 'Generated voice',
  image_generation: 'Generated images',
  video_generation: 'Generated video',
  generate_audio_url: 'Audio URL generation',
  generate_image_url: 'Image URL generation',
  meeting_recording: 'Meeting recording',
  transcoding: 'Transcoding',
  youtube_search: 'YouTube search',
};

function renderRealBilling(usage) {
  const wrap = $('usage-real');
  if (!usage || typeof usage !== 'object' || !usage.cost_metric) {
    wrap.classList.add('hidden');
    return;
  }
  wrap.classList.remove('hidden');

  const used = Number(usage.credit_used) || 0;
  const balance = Number(usage.credit_balance) || 0;
  $('usage-credit-used').textContent = formatCurrency(used);
  const balEl = $('usage-credit-balance');
  balEl.textContent = formatCurrency(balance);
  balEl.style.color = balance < 0 ? '#ef4444' : (balance < 5 ? '#f59e0b' : 'var(--text)');
  $('usage-credit-warn').classList.toggle('hidden', balance >= 0);
  $('usage-plan').textContent = usage.plan_id ? `plan: ${usage.plan_id}` : '';

  // Compute per-resource $: units (usage[key]) * price (cost_metric[key]).
  const priceCard = usage.cost_metric || {};
  const rows = [];
  for (const key of Object.keys(priceCard)) {
    const price = Number(priceCard[key]);
    const units = Number(usage[key]);
    if (!isFinite(price) || !isFinite(units) || units <= 0) continue;
    const cost = price * units;
    if (cost < 0.0001) continue; // hide rounding-floor noise
    rows.push({ key, label: RESOURCE_LABELS[key] || key.replace(/_/g, ' '), units, price, cost });
  }
  rows.sort((a, b) => b.cost - a.cost);

  $('usage-breakdown-count').textContent = `${rows.length} resource${rows.length===1?'':'s'} charged`;

  if (rows.length === 0) {
    $('usage-breakdown').innerHTML = '<div class="faint text-sm">No billable activity yet this period.</div>';
    $('usage-top-resource').textContent = '—';
    $('usage-top-resource-amt').textContent = formatCurrency(0);
    return;
  }

  $('usage-top-resource').textContent = rows[0].label;
  $('usage-top-resource-amt').textContent = formatCurrency(rows[0].cost);

  // Cost rows with proportional bar (relative to top spender) for at-a-glance.
  const topCost = rows[0].cost;
  // Format units intelligently — large counts get k/M, small price-per-unit
  // gets enough precision to be readable.
  const fmtUnits = (n) => {
    if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
    if (n >= 1000)    return (n/1000).toFixed(1) + 'k';
    if (n >= 10)      return n.toFixed(0);
    if (n >= 1)       return n.toFixed(2);
    return n.toFixed(3);
  };
  const fmtPrice = (n) => {
    if (n >= 1) return '$' + n.toFixed(2);
    if (n >= 0.01) return '$' + n.toFixed(3);
    return '$' + n.toFixed(4);
  };

  $('usage-breakdown').innerHTML = rows.map((r) => {
    const pct = Math.max(2, Math.round((r.cost / topCost) * 100));
    return `<div class="card-soft p-2.5">
      <div class="flex items-baseline justify-between gap-3">
        <div class="text-sm font-medium truncate" title="${escapeHtml(r.key)}">${escapeHtml(r.label)}</div>
        <div class="text-sm font-semibold mono shrink-0">${formatCurrency(r.cost)}</div>
      </div>
      <div class="mt-1.5 h-1.5 rounded-full overflow-hidden" style="background:color-mix(in oklab,var(--accent) 12%,transparent)">
        <div class="h-full rounded-full" style="width:${pct}%; background:var(--accent)"></div>
      </div>
      <div class="text-[10.5px] faint mono mt-1">${fmtUnits(r.units)} units &times; ${fmtPrice(r.price)}/unit</div>
    </div>`;
  }).join('');
}

async function fetchUsage() {
  try {
    const r = await fetch('/api/usage');
    const d = await r.json();
    const est = d.estimate || {};
    const details = est.details || [];
    const rtItems = details.filter(x => x.kind === 'rtstream');
    const sbItems = details.filter(x => x.kind === 'sandbox');

    const total = Number(est.total_usd) || 0;
    const totalEl = $('usage-total');
    totalEl.textContent = formatCurrency(total);
    // Amber implies "warning" — only paint amber when there's real spend.
    totalEl.style.color = total > 0 ? '#f59e0b' : 'var(--text-faint)';
    $('usage-rt').textContent = formatCurrency(est.rtstreams_usd || 0);
    $('usage-sb').textContent = formatCurrency(est.sandboxes_usd || 0);
    $('usage-rt-count').textContent = rtItems.length
      ? `${rtItems.length} stream${rtItems.length===1?'':'s'} running`
      : 'none running';
    $('usage-sb-count').textContent = sbItems.length
      ? `${sbItems[0].tier || 'active'} · running ${formatHours(sbItems[0].hours)}`
      : 'idle';

    // "Since start" — earliest hours value gives rough session length
    const longest = details.reduce((m, x) => Math.max(m, x.hours || 0), 0);
    $('usage-since').textContent = longest > 0 ? `over ${formatHours(longest)}` : 'no activity yet';

    const wrap = $('usage-detail-rows');
    $('usage-detail-count').textContent = details.length
      ? `${details.length} item${details.length===1?'':'s'} on the meter`
      : 'nothing running';
    if (details.length === 0) {
      wrap.innerHTML = `<div class="card-soft p-4 text-center">
        <div class="text-sm muted">Nothing on the meter.</div>
        <div class="text-[11px] faint mt-1">Add a source or connect a live stream to start using VideoDB credits.</div>
      </div>`;
    } else {
      wrap.innerHTML = details.map(renderUsageRow).join('');
    }

    // Real VideoDB billing (from check_usage): credit balance + per-resource breakdown.
    renderRealBilling(d.usage);

    // Invoices: pretty table + raw JSON
    const invoices = Array.isArray(d.invoices) ? d.invoices : [];
    $('usage-invoices-pretty').innerHTML = d.invoices_error
      ? `<div class="text-sm" style="color:#ef4444">Couldn't load invoices: ${escapeHtml(d.invoices_error)}</div>`
      : renderInvoices(invoices);

    // Tech section
    $('usage-raw').textContent = JSON.stringify(d.usage || d.usage_error || {}, null, 2);
    $('usage-invoices').textContent = JSON.stringify(d.invoices || d.invoices_error || [], null, 2);
  } catch (e) { console.warn('usage fetch failed', e); }
}

// Tab switching + persistence (localStorage + URL hash)
const VALID_TABS = ['alerts','sources','content','usage'];

function activateTab(tab, opts) {
  if (!VALID_TABS.includes(tab)) tab = 'alerts';
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('tab-active', b.dataset.tab === tab));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.add('hidden'));
  const pane = $('tab-' + tab);
  if (pane) pane.classList.remove('hidden');
  if (!opts || !opts.skipPersist) {
    try { localStorage.setItem('ww-tab', tab); } catch (e) {}
    if (history.replaceState) history.replaceState(null, '', '#' + tab);
    else location.hash = tab;
  }
  // Defer per-tab data fetch so we don't block the first paint — especially
  // important for `usage` which can take seconds when the SDK is slow.
  const dispatch = () => {
    if (tab === 'sources') fetchSources();
    if (tab === 'content') fetchVideos();
    if (tab === 'usage')   fetchUsage();
  };
  if (opts && opts.defer) {
    if ('requestIdleCallback' in window) requestIdleCallback(dispatch, { timeout: 500 });
    else setTimeout(dispatch, 0);
  } else {
    dispatch();
  }
}

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => activateTab(btn.dataset.tab));
});

window.addEventListener('hashchange', () => {
  const tab = (location.hash || '').replace(/^#/, '');
  if (VALID_TABS.includes(tab)) activateTab(tab, { skipPersist: true });
});

// Restore on load: URL hash wins over localStorage
(function restoreTab() {
  const hashTab = (location.hash || '').replace(/^#/, '');
  let saved = null;
  try { saved = localStorage.getItem('ww-tab'); } catch (e) {}
  const initial = VALID_TABS.includes(hashTab) ? hashTab : (VALID_TABS.includes(saved) ? saved : 'alerts');
  // `defer:true` → keep first paint fast even when restoring to `#usage`.
  activateTab(initial, { skipPersist: hashTab === initial, defer: true });
})();

// Add source modal
let modalKind = 'upload';
document.querySelectorAll('.modal-tab-btn').forEach(b => {
  b.addEventListener('click', () => {
    modalKind = b.dataset.modalTab;
    document.querySelectorAll('.modal-tab-btn').forEach(x => x.classList.remove('tab-active'));
    b.classList.add('tab-active');
    document.querySelectorAll('[id^="modal-pane-"]').forEach(p => p.classList.add('hidden'));
    $('modal-pane-' + modalKind).classList.remove('hidden');
  });
});

$('add-source-btn').addEventListener('click', () => { $('add-modal').classList.remove('hidden'); $('modal-error').classList.add('hidden'); });
$('modal-cancel').addEventListener('click', () => $('add-modal').classList.add('hidden'));

$('modal-submit').addEventListener('click', async () => {
  const name = $('modal-name').value.trim();
  if (!name) { showModalError('Name required'); return; }
  try {
    if (modalKind === 'upload') {
      const f = $('modal-file').files[0];
      if (!f) { showModalError('Pick a file'); return; }
      const fd = new FormData();
      fd.append('file', f);
      fd.append('name', name);
      const r = await fetch('/api/sources/upload', { method: 'POST', body: fd });
      if (!r.ok) { showModalError('upload failed: ' + (await r.text()).slice(0,200)); return; }
    } else if (modalKind === 'url') {
      const url = $('modal-url').value.trim();
      if (!url) { showModalError('URL required'); return; }
      const kind = url.includes('youtube.com') || url.includes('youtu.be') ? 'youtube' : 'hls';
      const r = await fetch('/api/sources', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ kind, input: url, name }) });
      if (!r.ok) { showModalError('add failed: ' + (await r.text()).slice(0,200)); return; }
    } else {
      const url = $('modal-rtsp').value.trim();
      if (!url) { showModalError('URL required'); return; }
      const kind = url.startsWith('rtmp://') ? 'rtmp' : 'rtsp';
      const r = await fetch('/api/sources', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ kind, input: url, name }) });
      if (!r.ok) { showModalError('add failed: ' + (await r.text()).slice(0,200)); return; }
    }
    $('add-modal').classList.add('hidden');
    $('modal-name').value = ''; $('modal-url').value = ''; $('modal-rtsp').value = ''; $('modal-file').value = '';
    fetchSources();
  } catch (e) { showModalError(e.message); }
});

function showModalError(msg) {
  const el = $('modal-error');
  el.textContent = msg;
  el.classList.remove('hidden');
}

// Delegated click handler for data-action buttons. Replaces inline onclick=
// attribute interpolation, which is XSS-prone — one apostrophe in an id
// would break the page or, in the worst case, execute arbitrary script.
document.addEventListener('click', (e) => {
  const stop = e.target.closest('[data-stop-propagation]');
  if (stop) { e.stopPropagation(); /* allow default <a> navigation */ }
  const t = e.target.closest('[data-action]');
  if (!t) return;
  const id = t.dataset.id;
  const idx = t.dataset.idx;
  switch (t.dataset.action) {
    case 'reconnect':       reconnectSource(id);     break;
    case 'disconnect':      disconnectSource(id);    break;
    case 'delete':          deleteSource(id);        break;
    case 'show-video':      showVideoDetail(id);     break;
    case 'show-scenes':     showVideoScenes(id, idx); break;
    case 'reindex-video':   reindexVideo(id); break;
    case 'delete-video':    deleteVideo(id, t.dataset.name); break;
    case 'open-add-modal':  $('add-source-btn').click(); break;
    case 'copy-id':         copyIdToClipboard(t.dataset.id, t); break;
    case 'play-scene-clip': {
      const v = t.dataset.videoId;
      const s = parseFloat(t.dataset.start);
      const e2 = parseFloat(t.dataset.end);
      if (v && isFinite(s) && isFinite(e2)) playSceneClip(v, s, e2);
      break;
    }
  }
});

// Theme toggle (null-guarded — if the toggle button isn't in the DOM yet,
// the rest of the boot script must still run).
function applyThemeIcons() {
  const dark = document.documentElement.classList.contains('dark');
  const sun = $('theme-icon-sun'), moon = $('theme-icon-moon'), lbl = $('theme-label');
  if (sun)  sun.classList.toggle('hidden', !dark);
  if (moon) moon.classList.toggle('hidden', dark);
  if (lbl)  lbl.textContent = dark ? 'Light' : 'Dark';
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', dark ? '#07100e' : '#f8fafc');
}
const _themeBtn = $('theme-toggle');
if (_themeBtn) {
  _themeBtn.addEventListener('click', () => {
    const next = !document.documentElement.classList.contains('dark');
    document.documentElement.classList.toggle('dark', next);
    try { localStorage.setItem('ww-theme', next ? 'dark' : 'light'); } catch (e) {}
    applyThemeIcons();
  });
}
applyThemeIcons();

// Boot
fetchStats();
fetchRemote();
fetchSources();
startSSE();
setInterval(fetchStats, 5000);
setInterval(fetchRemote, 15000);
setInterval(fetchSources, 10000);
</script>
</body>
</html>
"""


def get_dashboard_html() -> str:
    return _DASHBOARD_HTML
