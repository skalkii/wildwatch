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
    .tab-active { background: #1f2937; border-bottom: 2px solid #3b82f6; }
    .status-queued { background: #6b7280; }
    .status-connecting { background: #3b82f6; }
    .status-ingesting { background: #8b5cf6; }
    .status-indexing { background: #ec4899; }
    .status-ready { background: #16a34a; }
    .status-error { background: #dc2626; }
    .status-disconnected { background: #6b7280; }
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

  <nav class="border-b border-gray-800 px-6 flex gap-1 text-sm">
    <button data-tab="alerts" class="tab-btn px-4 py-2 hover:bg-gray-800 tab-active">Alerts</button>
    <button data-tab="sources" class="tab-btn px-4 py-2 hover:bg-gray-800">Sources</button>
    <button data-tab="content" class="tab-btn px-4 py-2 hover:bg-gray-800">Indexed Content</button>
    <button data-tab="usage" class="tab-btn px-4 py-2 hover:bg-gray-800">Usage</button>
  </nav>

  <!-- ALERTS TAB -->
  <main id="tab-alerts" class="tab-pane p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
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
    <section class="lg:col-span-2 bg-gray-900 rounded-lg p-4 border border-gray-800">
      <h2 class="text-sm uppercase tracking-wider text-gray-400 mb-2">
        Live event feed
        <span id="feed-status" class="ml-2 text-xs text-green-500 pulse">●</span>
      </h2>
      <div id="feed" class="space-y-2 max-h-[600px] overflow-y-auto"></div>
    </section>
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

  <!-- SOURCES TAB -->
  <main id="tab-sources" class="tab-pane p-6 hidden">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-bold">Sources</h2>
      <button id="add-source-btn" class="bg-blue-600 hover:bg-blue-500 px-4 py-2 rounded text-sm">+ Add source</button>
    </div>
    <div id="sources-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
      loading...
    </div>
  </main>

  <!-- INDEXED CONTENT TAB -->
  <main id="tab-content" class="tab-pane p-6 hidden">
    <h2 class="text-lg font-bold mb-4">Indexed Content (TODO T-63)</h2>
    <p class="text-gray-400 text-sm">Wired in next ticket.</p>
  </main>

  <!-- USAGE TAB -->
  <main id="tab-usage" class="tab-pane p-6 hidden">
    <h2 class="text-lg font-bold mb-4">VideoDB Usage (TODO T-64)</h2>
    <p class="text-gray-400 text-sm">Wired in next ticket.</p>
  </main>

  <!-- ADD SOURCE MODAL -->
  <div id="add-modal" class="hidden fixed inset-0 bg-black/70 z-50 flex items-center justify-center">
    <div class="bg-gray-900 rounded-lg p-6 w-full max-w-md border border-gray-700">
      <h3 class="text-lg font-bold mb-4">Add source</h3>
      <div class="flex gap-1 mb-4 text-xs">
        <button data-modal-tab="upload" class="modal-tab-btn px-3 py-1 rounded bg-gray-800 tab-active">File upload</button>
        <button data-modal-tab="url" class="modal-tab-btn px-3 py-1 rounded">URL (YouTube/HLS)</button>
        <button data-modal-tab="rtsp" class="modal-tab-btn px-3 py-1 rounded">RTSP/RTMP</button>
      </div>
      <div class="space-y-3">
        <input id="modal-name" placeholder="Name (required)" class="w-full px-3 py-2 bg-gray-800 rounded text-sm">
        <div id="modal-pane-upload">
          <input id="modal-file" type="file" accept="video/*" class="w-full text-sm">
          <p class="text-xs text-gray-500 mt-1">Max 500 MB</p>
        </div>
        <div id="modal-pane-url" class="hidden">
          <input id="modal-url" placeholder="https://www.youtube.com/watch?v=... OR https://x/y.m3u8" class="w-full px-3 py-2 bg-gray-800 rounded text-sm">
          <p class="text-xs text-gray-500 mt-1">YouTube live URLs need bridge — paste RTSP from bore.pub instead</p>
        </div>
        <div id="modal-pane-rtsp" class="hidden">
          <input id="modal-rtsp" placeholder="rtsp://host:port/path  or  rtmp://..." class="w-full px-3 py-2 bg-gray-800 rounded text-sm">
        </div>
      </div>
      <div class="flex justify-end gap-2 mt-6">
        <button id="modal-cancel" class="px-4 py-2 text-sm bg-gray-700 hover:bg-gray-600 rounded">Cancel</button>
        <button id="modal-submit" class="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-500 rounded">Add</button>
      </div>
      <p id="modal-error" class="text-red-400 text-xs mt-3 hidden"></p>
    </div>
  </div>

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
    let payload = null;
    try { payload = JSON.parse(m.data); } catch (e) { /* connect comment */ }
    if (payload && payload.type === 'source_progress') {
      fetchSources();  // refresh sources view on every progress event
    }
    fetchStats();  // always refresh alert feed (cheap)
  };
}

// ──── Sources tab ────
function renderSource(s) {
  const statusClass = `status-${s.status || 'queued'}`;
  const errMsg = s.error ? `<div class="text-xs text-red-400 mt-1">${escapeHtml(s.error)}</div>` : '';
  const stage = s.stage_msg ? `<div class="text-xs text-gray-500 mt-1">${escapeHtml(s.stage_msg)}</div>` : '';
  const remote = s.video_id ? `video <code class="text-blue-300">${escapeHtml(s.video_id)}</code>` :
                 s.rtstream_id ? `rtstream <code class="text-blue-300">${escapeHtml(s.rtstream_id)}</code>` : '';
  const created = s.created_at ? new Date(s.created_at * 1000).toLocaleString() : '';
  return `<div class="bg-gray-900 rounded-lg p-4 border border-gray-800">
    <div class="flex justify-between items-start">
      <div>
        <div class="font-bold">${escapeHtml(s.name)}</div>
        <div class="text-xs text-gray-500 mt-1">${escapeHtml(s.kind)} · ${created}</div>
      </div>
      <span class="${statusClass} text-xs px-2 py-1 rounded">${escapeHtml(s.status || 'queued')}</span>
    </div>
    <div class="text-xs text-gray-400 mt-2 truncate" title="${escapeHtml(s.input || '')}">${escapeHtml(s.input || '')}</div>
    ${stage}
    ${errMsg}
    <div class="text-xs text-gray-300 mt-2">${remote}</div>
    <div class="flex gap-2 mt-3">
      <button onclick="reconnectSource('${s.id}')" class="text-xs bg-blue-700 hover:bg-blue-600 px-2 py-1 rounded">Reconnect</button>
      <button onclick="disconnectSource('${s.id}')" class="text-xs bg-yellow-700 hover:bg-yellow-600 px-2 py-1 rounded">Disconnect</button>
      <button onclick="deleteSource('${s.id}')" class="text-xs bg-red-700 hover:bg-red-600 px-2 py-1 rounded">Delete</button>
    </div>
  </div>`;
}

async function fetchSources() {
  try {
    const r = await fetch('/api/sources');
    const d = await r.json();
    const grid = $('sources-grid');
    if (!d.sources || d.sources.length === 0) {
      grid.innerHTML = '<div class="col-span-3 text-gray-500 text-sm">No sources yet. Click "+ Add source".</div>';
      return;
    }
    grid.innerHTML = d.sources.map(renderSource).join('');
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

// Tab switching
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('tab-active'));
    btn.classList.add('tab-active');
    document.querySelectorAll('.tab-pane').forEach(p => p.classList.add('hidden'));
    $('tab-' + tab).classList.remove('hidden');
    if (tab === 'sources') fetchSources();
  });
});

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
