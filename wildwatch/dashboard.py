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
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any

MAX_RECENT_EVENTS = 50

# Module-level state ------------------------------------------------------

_subscribers: list[asyncio.Queue] = []
_tier_counts: dict[int, int] = defaultdict(int)
_recent_events: list[dict] = []
_total: int = 0
_started_at: float = time.time()


def reset_state() -> None:
    """Test helper — clears all counters + subscribers."""
    global _total, _started_at
    _subscribers.clear()
    _tier_counts.clear()
    _recent_events.clear()
    _total = 0
    _started_at = time.time()


def broadcast(event: dict[str, Any]) -> None:
    """Record + fanout to every subscriber. Called from webhook handler."""
    global _total
    _total += 1
    tier = int(event.get("tier", 0))
    _tier_counts[tier] += 1
    _recent_events.append({**event, "received_at": event.get("received_at", time.time())})
    if len(_recent_events) > MAX_RECENT_EVENTS:
        _recent_events.pop(0)
    # Fanout to subscribers (sync put_nowait so dropped on overflow rather
    # than blocking the webhook response path).
    for q in list(_subscribers):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass


async def subscribe() -> AsyncIterator[dict]:
    """Yield each broadcast until subscriber drops."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subscribers.append(q)
    try:
        while True:
            ev = await q.get()
            yield ev
    finally:
        if q in _subscribers:
            _subscribers.remove(q)


def get_stats() -> dict[str, Any]:
    """Snapshot for polling endpoints (JSON-serialisable)."""
    return {
        "total": _total,
        "tier_counts": dict(_tier_counts),
        "recent_events": list(reversed(_recent_events)),
        "subscribers": len(_subscribers),
        "uptime_s": int(time.time() - _started_at),
    }


# ──── HTML template ────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>WildWatch — Live Dashboard</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; }
    .pulse { animation: pulse 1.2s ease-in-out infinite; }
    @keyframes pulse { 0%,100% { opacity: 1 } 50% { opacity: 0.4 } }
    .ev-1 { border-left-color: #16a34a; }
    .ev-2 { border-left-color: #ca8a04; }
    .ev-3 { border-left-color: #dc2626; }
    pre { font-size: 11px; }
  </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen">
  <header class="border-b border-gray-800 px-6 py-3 flex items-center justify-between">
    <div>
      <h1 class="text-xl font-bold">WildWatch</h1>
      <p class="text-xs text-gray-400">Real-time perception agent — live dashboard</p>
    </div>
    <div class="text-xs text-gray-400">
      <span id="conn-dot" class="inline-block w-2 h-2 rounded-full bg-gray-500 mr-1"></span>
      <span id="conn-text">connecting...</span>
      &nbsp; uptime <span id="uptime">0s</span>
      &nbsp; subscribers <span id="subs">0</span>
    </div>
  </header>

  <main class="p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
    <!-- Stats row across columns -->
    <section class="lg:col-span-3 grid grid-cols-4 gap-3">
      <div class="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <div class="text-xs text-gray-400">TOTAL ALERTS</div>
        <div class="text-3xl font-bold mt-1" id="stat-total">0</div>
      </div>
      <div class="bg-gray-900 rounded-lg p-4 border border-green-900">
        <div class="text-xs text-green-400">🟢 INFO (tier 1)</div>
        <div class="text-3xl font-bold mt-1 text-green-400" id="stat-t1">0</div>
      </div>
      <div class="bg-gray-900 rounded-lg p-4 border border-yellow-900">
        <div class="text-xs text-yellow-400">🟡 NOTABLE (tier 2)</div>
        <div class="text-3xl font-bold mt-1 text-yellow-400" id="stat-t2">0</div>
      </div>
      <div class="bg-gray-900 rounded-lg p-4 border border-red-900">
        <div class="text-xs text-red-400">🔴 URGENT (tier 3)</div>
        <div class="text-3xl font-bold mt-1 text-red-400" id="stat-t3">0</div>
      </div>
    </section>

    <!-- Live event feed (2 cols) -->
    <section class="lg:col-span-2 bg-gray-900 rounded-lg p-4 border border-gray-800">
      <h2 class="text-sm uppercase tracking-wider text-gray-400 mb-2">
        Live event feed
        <span id="feed-status" class="ml-2 text-xs text-green-500 pulse">●</span>
      </h2>
      <div id="feed" class="space-y-2 max-h-[600px] overflow-y-auto"></div>
    </section>

    <!-- Side panel: streams + sandboxes -->
    <aside class="space-y-4">
      <div class="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h2 class="text-sm uppercase tracking-wider text-gray-400 mb-2">RTStreams (VideoDB)</h2>
        <div id="rtstreams" class="text-xs space-y-1">loading...</div>
      </div>
      <div class="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h2 class="text-sm uppercase tracking-wider text-gray-400 mb-2">Sandboxes</h2>
        <div id="sandboxes" class="text-xs space-y-1">loading...</div>
      </div>
      <div class="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h2 class="text-sm uppercase tracking-wider text-gray-400 mb-2">Manual triggers</h2>
        <div class="flex gap-2">
          <button onclick="fireTest(1)" class="flex-1 bg-green-700 hover:bg-green-600 text-sm py-2 rounded">🟢 Fire T1</button>
          <button onclick="fireTest(2)" class="flex-1 bg-yellow-700 hover:bg-yellow-600 text-sm py-2 rounded">🟡 Fire T2</button>
          <button onclick="fireTest(3)" class="flex-1 bg-red-700 hover:bg-red-600 text-sm py-2 rounded">🔴 Fire T3</button>
        </div>
      </div>
    </aside>
  </main>

<script>
const $ = (id) => document.getElementById(id);
const TIER_NAME = { 1: 'INFO', 2: 'NOTABLE', 3: 'URGENT' };

function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function renderEvent(ev) {
  const tier = ev.tier || 0;
  const klass = `ev-${tier}`;
  const stream = ev.stream_url ? `<a class="underline text-blue-300" href="${escapeHtml(ev.stream_url)}" target="_blank">▶ play</a>` : '';
  const ts = ev.received_at ? new Date(ev.received_at * 1000).toLocaleTimeString() : '';
  return `<div class="border-l-4 ${klass} bg-gray-800 px-3 py-2 rounded">
    <div class="flex justify-between text-xs text-gray-400">
      <span>${TIER_NAME[tier] || tier} · ${escapeHtml(ev.event_id || '')}</span>
      <span>${ts}</span>
    </div>
    <div class="text-sm font-mono mt-1">${escapeHtml(ev.label || '')}</div>
    <div class="text-xs text-gray-300 mt-1">${escapeHtml(ev.explanation || '')}</div>
    <div class="mt-1">${stream}</div>
  </div>`;
}

function applyStats(s) {
  $('stat-total').textContent = s.total || 0;
  $('stat-t1').textContent = (s.tier_counts && s.tier_counts['1']) || 0;
  $('stat-t2').textContent = (s.tier_counts && s.tier_counts['2']) || 0;
  $('stat-t3').textContent = (s.tier_counts && s.tier_counts['3']) || 0;
  $('uptime').textContent = (s.uptime_s || 0) + 's';
  $('subs').textContent = s.subscribers || 0;
  const feed = $('feed');
  feed.innerHTML = (s.recent_events || []).map(renderEvent).join('');
}

function applyRtstreams(d) {
  const c = $('rtstreams');
  if (!d || !d.rtstreams) { c.innerHTML = '<span class="text-gray-500">n/a</span>'; return; }
  if (!d.rtstreams.length) { c.innerHTML = '<span class="text-gray-500">none</span>'; return; }
  c.innerHTML = d.rtstreams.map(r => `<div><span class="${r.status==='connected'?'text-green-400':'text-gray-500'}">●</span> ${escapeHtml(r.name)} <span class="text-gray-500">${escapeHtml(r.status)}</span></div>`).join('');
}

function applySandboxes(d) {
  const c = $('sandboxes');
  if (!d || !d.sandboxes) { c.innerHTML = '<span class="text-gray-500">n/a</span>'; return; }
  if (!d.sandboxes.length) { c.innerHTML = '<span class="text-gray-500">none</span>'; return; }
  c.innerHTML = d.sandboxes.map(sb => `<div><span class="${sb.is_active?'text-green-400':'text-gray-500'}">●</span> ${escapeHtml(sb.id)} <span class="text-gray-500">${escapeHtml(sb.tier)}</span></div>`).join('');
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
    $('conn-dot').className = 'inline-block w-2 h-2 rounded-full bg-green-500 mr-1';
    $('conn-text').textContent = 'live';
  };
  es.onerror = () => {
    $('conn-dot').className = 'inline-block w-2 h-2 rounded-full bg-red-500 mr-1';
    $('conn-text').textContent = 'reconnecting...';
  };
  es.onmessage = (m) => {
    fetchStats();  // re-render full feed (simpler than dom-prepend)
  };
}

// Boot
fetchStats();
fetchRemote();
startSSE();
setInterval(fetchStats, 5000);
setInterval(fetchRemote, 15000);
</script>
</body>
</html>
"""


def get_dashboard_html() -> str:
    return _DASHBOARD_HTML
