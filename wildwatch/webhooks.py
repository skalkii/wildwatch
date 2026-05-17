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
import re
import tempfile
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import asynccontextmanager
from pathlib import Path as PPath
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypedDict
from urllib.parse import urlparse

if TYPE_CHECKING:
    # TYPE_CHECKING is False at runtime so this import never fires.
    # Combined with `from __future__ import annotations` above, type
    # annotations referencing `_videodb_types.Connection` etc. are stored
    # as strings and only resolved by static checkers. DO NOT "fix" this
    # into a runtime import — it would pull the heavy SDK on every test.
    import videodb as _videodb_types

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

# Upload rate limit — per-IP token bucket. Three concurrent attackers
# uploading 499 MB each would saturate the 4-worker SDK pool, fill disk,
# and DoS the dashboard. A leaky bucket (capacity=3, refill=1/min/IP)
# is enough to neutralise that without inconveniencing real operators
# who upload at most a few clips per session.
_UPLOAD_BUCKET_CAPACITY = 3
_UPLOAD_BUCKET_REFILL_PER_SEC = 1.0 / 60.0  # one new token per minute per IP
# Hard cap on the bucket dict to bound memory under attacker probing
# (especially when WILDWATCH_TRUSTED_PROXY=1 lets an attacker rotate
# X-Forwarded-For). OrderedDict gives O(1) LRU eviction via popitem(False).
_UPLOAD_BUCKETS_MAX = 50_000
# {ip: (tokens_float, last_refill_ts)}
_upload_buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
_upload_bucket_lock = threading.Lock()


_IPV6_BRACKET_RE = re.compile(r"^\[(.+)\](?::\d+)?$")


def _normalize_ip(raw: str) -> str:
    """Strip surrounding `[...]` brackets and `:port` from an XFF token.

    Without this, an attacker behind a trusted proxy could rotate the
    port suffix on `[::1]:1`, `[::1]:2` … to mint 65k distinct bucket
    keys from one host, defeating the per-IP limit. Returns "" for
    empty / unparsable input.

    Also defends against malformed bracket forms (`[::1`, no closing
    `]`) and multi-colon IPv4-with-suffix (`192.0.2.1:80:foo`) — both
    would otherwise pass through unnormalized and let an attacker mint
    distinct bucket keys from a single host by varying the suffix.
    """
    raw = raw.strip()
    if not raw:
        return ""
    m = _IPV6_BRACKET_RE.match(raw)
    if m:
        return m.group(1).strip()
    # Bracket present but missing closing `]` — strip what we can rather
    # than letting the raw malformed string be used as a fresh key.
    if raw.startswith("["):
        rest = raw[1:]
        # If there's a stray `]` further on, treat up to it as the addr.
        if "]" in rest:
            return rest.split("]", 1)[0].strip()
        # Bare `[::1` etc. — drop the bracket entirely.
        return rest.strip().split(":")[0] if rest.count(":") > 2 else rest.strip()
    # Strip a single trailing :port for plain IPv4 (one colon).
    if raw.count(":") == 1:
        return raw.split(":", 1)[0].strip()
    # Multi-colon non-IPv6 (e.g. `192.0.2.1:80:foo`) — reject as
    # malformed by returning the same fallback the empty-input path uses.
    if raw.count(":") > 1 and not _looks_like_bare_ipv6(raw):
        return ""
    return raw


def _looks_like_bare_ipv6(raw: str) -> bool:
    """Heuristic: bare IPv6 is hex chars + colons, no other punctuation.

    Used by _normalize_ip to distinguish a legitimate bare IPv6 address
    (`2001:db8::1`) from an attacker-malformed multi-colon string
    (`192.0.2.1:80:foo`). Not a full IPv6 parser — we just check that
    every char is hex or `:`. Anything with letters beyond a-f or
    other punctuation fails.
    """
    return all(c in "0123456789abcdefABCDEF:" for c in raw)


def _client_ip_from(request: Request) -> str:
    """Resolve the client IP, honouring X-Forwarded-For when configured.

    Behind a reverse proxy (nginx, Cloudflare, ALB) every upload would
    appear to come from the proxy, collapsing all real clients into a
    single shared rate-limit bucket — a self-DoS vector. Operators
    running behind a trusted proxy set ``WILDWATCH_TRUSTED_PROXY=1`` to
    use the FIRST IP in the X-Forwarded-For chain (the original client)
    instead of the TCP peer. Direct-exposure deployments leave the env
    unset so a remote attacker can't spoof XFF to bypass the limit.

    Result is normalised to strip `[...]` and `:port` so XFF token-
    rotation can't be used to dodge per-IP buckets.
    """
    if os.environ.get("WILDWATCH_TRUSTED_PROXY") == "1":
        xff = request.headers.get("x-forwarded-for")
        if xff:
            first = _normalize_ip(xff.split(",")[0])
            if first:
                return first
    peer = request.client.host if request.client else "unknown"
    return _normalize_ip(peer or "") or "unknown"


def _upload_rate_limit_check(client_ip: str) -> bool:
    """Return True if the IP has an upload token, else False (429).

    Token bucket per IP: starts full at capacity, refills at
    _UPLOAD_BUCKET_REFILL_PER_SEC tokens/second up to capacity. Each
    upload consumes one token. Lock-guarded so concurrent uploads from
    one IP can't both observe a full bucket and double-spend.

    Eviction: an IP that has refilled fully (idle long enough) is
    removed from the dict — the next request resets to default-full so
    the visible behaviour is identical. Plus a hard size cap with LRU
    pop on overflow to bound memory under attacker probing.
    """
    # Special case: when we can't identify the client (request.client is
    # None, e.g. some test harnesses or serverless deployments where the
    # platform strips peer info), bypass the bucket rather than collapsing
    # every unknown caller into a single shared 3-token pool. The shared
    # pool produces spurious 429s for legit traffic. Operators behind a
    # real proxy should set WILDWATCH_TRUSTED_PROXY=1 to read the actual
    # client IP from X-Forwarded-For; if they don't, the rate limiter is
    # ineffective by design — log a warning so it shows up.
    if client_ip == "unknown":
        logger.warning(
            "upload rate limit: client_ip is 'unknown' — bypassing bucket. "
            "Set WILDWATCH_TRUSTED_PROXY=1 if behind a reverse proxy."
        )
        return True

    now = time.time()
    with _upload_bucket_lock:
        tokens, last = _upload_buckets.get(client_ip, (float(_UPLOAD_BUCKET_CAPACITY), now))
        # Refill since last access, clamped to capacity.
        tokens = min(
            float(_UPLOAD_BUCKET_CAPACITY),
            tokens + (now - last) * _UPLOAD_BUCKET_REFILL_PER_SEC,
        )
        if tokens < 1.0:
            _upload_buckets[client_ip] = (tokens, now)
            _upload_buckets.move_to_end(client_ip)
            _evict_overflow_locked()
            return False
        new_tokens = tokens - 1.0
        # An IP whose bucket was AT CAPACITY before this request consumed
        # one token (i.e. fully idle long enough to refill) is worth
        # GC'ing — its state is the default and re-inserting it on every
        # request just pollutes the dict. Check the PRE-consume `tokens`
        # value, not the post-consume `new_tokens`. (Previous condition
        # `new_tokens >= capacity - 0.001` was unreachable arithmetic — a
        # full bucket minus 1.0 can never satisfy that.)
        if tokens >= float(_UPLOAD_BUCKET_CAPACITY) - 0.001:
            _upload_buckets.pop(client_ip, None)
        else:
            _upload_buckets[client_ip] = (new_tokens, now)
            _upload_buckets.move_to_end(client_ip)
            _evict_overflow_locked()
        return True


def _evict_overflow_locked() -> None:
    """Trim LRU entries when the bucket dict exceeds the hard cap.

    Caller MUST hold _upload_bucket_lock — name suffix `_locked` is the
    project convention. OrderedDict.popitem(last=False) drops the
    least-recently-touched entry in O(1).
    """
    while len(_upload_buckets) > _UPLOAD_BUCKETS_MAX:
        _upload_buckets.popitem(last=False)


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


# Per-cache TypedDicts so each cache's payload is type-narrowed at its
# call site. Sharing a single `_CacheEntry(data: Any)` let a future writer
# put `{"videos": ...}` into `_remote_cache` without a static error.

_RemoteData = dict[str, Any]  # {"rtstreams": list, "sandboxes": list, "error"?: str}
_VideosData = dict[str, Any]  # {"videos": list, "error"?: str}
# None as a sentinel here means "no usage payload fetched yet". The
# /api/usage TTL check uses `data is not None` to distinguish an empty-
# response cache hit (legitimate) from a never-populated cache (cold
# start). Renaming would obscure that intent.
_UsageData = dict[str, Any] | None


class _RemoteCacheEntry(TypedDict):
    at: float
    data: _RemoteData


class _VideosCacheEntry(TypedDict):
    at: float
    data: _VideosData


class _UsageCacheEntry(TypedDict):
    at: float
    data: _UsageData


_remote_cache: _RemoteCacheEntry = {"at": 0.0, "data": {"rtstreams": [], "sandboxes": []}}
_REMOTE_TTL_S = 10.0
_remote_lock = threading.Lock()

_videos_cache: _VideosCacheEntry = {"at": 0.0, "data": {"videos": []}}
_VIDEOS_TTL_S = 30.0
_videos_lock = threading.Lock()

# `data` defaults to None (not {}) so the `data is not None` check below is
# unambiguous — an empty-dict response would otherwise miss the cache and
# hammer the SDK on every poll.
_usage_cache: _UsageCacheEntry = {"at": 0.0, "data": None}
_USAGE_TTL_S = 60.0
_usage_lock = threading.Lock()

# Bounded executor for SDK calls that lack native timeouts (VideoDB SDK is
# blocking and offers no per-call deadline). Created LAZILY on first use so
# importing this module (e.g. from pytest) doesn't spawn non-daemon worker
# threads that hold up interpreter exit. atexit + lifespan shut it down
# cleanly when it has been created.
#
# IMPORTANT POOL SATURATION SEMANTICS — ThreadPoolExecutor workers cannot
# be interrupted. When `_async_sdk` times out via `asyncio.wait_for`, the
# underlying worker thread keeps running until the (hung) SDK call
# eventually returns. With max_workers=4, four simultaneous timeouts
# pin all four workers for the SDK's natural completion time — every
# subsequent call queues behind them. The saturation tripwire below
# returns 503 BEFORE enqueueing when the pool is already full, so the
# dashboard surfaces "VideoDB unreachable" instead of looking hung.
_SDK_EXECUTOR_MAX_WORKERS = 4
_SDK_EXECUTOR: ThreadPoolExecutor | None = None
_executor_lock = threading.Lock()
# Counter of futures we have submitted to the pool that have not yet
# resolved. Guarded by _executor_lock so the tripwire compare is atomic.
_sdk_in_flight: int = 0


class SDKPoolSaturated(RuntimeError):
    """Raised by _async_sdk when the SDK pool is already full of
    likely-hung calls. Callers in route handlers should catch this and
    return 503 so the dashboard can show a visible degraded state."""


def _get_executor() -> ThreadPoolExecutor:
    global _SDK_EXECUTOR
    if _SDK_EXECUTOR is None:
        with _executor_lock:
            if _SDK_EXECUTOR is None:
                _SDK_EXECUTOR = ThreadPoolExecutor(
                    max_workers=_SDK_EXECUTOR_MAX_WORKERS,
                    thread_name_prefix="sdk",
                )
                import atexit

                # Capture the executor reference in the closure so atexit
                # operates on the exact instance we just created — not a
                # later reassignment via the global. Idempotent shutdown
                # makes a double-call (atexit + lifespan) safe regardless.
                _exec_ref = _SDK_EXECUTOR
                atexit.register(lambda: _exec_ref.shutdown(wait=False, cancel_futures=True))
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
        # IMPORTANT: fut.cancel() is a NO-OP on an already-running future.
        # ThreadPoolExecutor workers cannot be interrupted from the outside.
        # A truly hung SDK call (e.g. a stuck network read) will keep the
        # worker thread occupied until the call eventually returns — that
        # consumes a slot in our 4-worker pool. The pool is sized assuming
        # most calls complete within timeout_s. If a deployment sees pool
        # exhaustion, switch the affected call sites to use a subprocess
        # or grow the pool. We still call cancel() for the case where the
        # future hasn't started yet (queued waiting for a worker).
        fut.cancel()
        fn_name = getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
        raise TimeoutError(f"SDK call {fn_name!r} timed out after {timeout_s}s") from e


async def _async_sdk(fn, *args, timeout_s: float = 5.0, **kwargs):
    """Async wrapper: dispatch a blocking SDK call to our SDK thread pool
    via ``loop.run_in_executor``, with a deadline enforced by
    ``asyncio.wait_for``. ONE worker thread per call.

    POOL SATURATION GUARD: before enqueueing, check if the pool is
    already full of likely-hung calls. If yes, raise ``SDKPoolSaturated``
    immediately so the caller can return 503 instead of queueing behind
    workers that cannot be interrupted. Without this, a single hung SDK
    call locks up the entire dashboard until the hung call resolves
    naturally.

    TIMEOUT SEMANTICS: ``asyncio.wait_for`` cancels the asyncio future
    but the underlying worker thread KEEPS RUNNING — ThreadPoolExecutor
    workers cannot be interrupted from the outside. We still call
    ``fut.cancel()`` for the case where the future was queued but not
    yet picked up by a worker (cancel succeeds in that case).
    """
    import functools

    global _sdk_in_flight
    # Tripwire — refuse to enqueue when every worker is occupied AND
    # the queue is forming. Use 2x worker count as the soft ceiling so
    # a brief burst still works; sustained saturation rejects.
    with _executor_lock:
        if _sdk_in_flight >= _SDK_EXECUTOR_MAX_WORKERS * 2:
            raise SDKPoolSaturated(
                f"SDK thread pool saturated ({_sdk_in_flight} in-flight, "
                f"workers={_SDK_EXECUTOR_MAX_WORKERS}) — VideoDB likely unreachable"
            )
        _sdk_in_flight += 1

    call = functools.partial(fn, *args, **kwargs)
    # Submit to the SDK executor directly and wrap the concurrent.futures
    # Future for asyncio. This split is important: `cf_fut` resolves when
    # the WORKER THREAD finishes; `aio_fut` resolves when asyncio.wait_for
    # cancels its wrapper (which happens on timeout BEFORE the worker is
    # done). The previous code used `loop.run_in_executor` which hides the
    # underlying CF future — `add_done_callback` on the wrapper fired
    # immediately on cancel, so `_release_in_flight_slot` ran before the
    # worker released its slot, leaking the saturation counter.
    cf_fut = _get_executor().submit(call)
    # asyncio.wrap_future's `loop` kwarg is deprecated since 3.10 and
    # removed in 3.14. Inside a coroutine the function resolves the
    # running loop automatically.
    aio_fut = asyncio.wrap_future(cf_fut)
    slot_deferred = False  # True means worker may still be pinned; defer release
    try:
        return await asyncio.wait_for(aio_fut, timeout=timeout_s)
    except TimeoutError as e:
        slot_deferred = True
        fn_name = getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
        with _executor_lock:
            in_flight_snapshot = _sdk_in_flight
        logger.warning(
            "_async_sdk: %r timed out after %ss; in_flight=%d (worker may still be running)",
            fn_name,
            timeout_s,
            in_flight_snapshot,
        )
        raise TimeoutError(f"SDK call {fn_name!r} timed out after {timeout_s}s") from e
    except asyncio.CancelledError:
        # Client disconnect (FastAPI cancels the coroutine) or shutdown.
        # asyncio.CancelledError is NOT a subclass of TimeoutError in 3.11+
        # so the success path would otherwise decrement immediately.
        slot_deferred = True
        raise
    finally:
        if slot_deferred:
            # Attach the callback to the CONCURRENT.FUTURES future (not
            # the asyncio wrapper) so it fires when the WORKER actually
            # completes, not when asyncio cancelled its view of it.
            cf_fut.add_done_callback(_release_in_flight_slot)
        else:
            with _executor_lock:
                _sdk_in_flight = max(0, _sdk_in_flight - 1)


def _release_in_flight_slot(_fut: Any) -> None:
    """Decrement _sdk_in_flight when a timed-out worker finally completes.

    Idempotent with floor at 0 so accidental over-decrement (e.g. if a
    future fires its callback after the success path already decremented)
    can never push the counter negative and trip the saturation guard
    falsely.
    """
    global _sdk_in_flight
    with _executor_lock:
        _sdk_in_flight = max(0, _sdk_in_flight - 1)


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

# `WILDWATCH_ALLOW_NO_ORIGIN=1` is a process-lifetime escape hatch for
# CLI clients. Read it ONCE at module import (not per request) so an
# operator who sets it for a one-off bootstrap can't accidentally leave
# it set and silently disable CSRF protection for the next deploy. Loud
# WARNING at startup so the log shows every operator that the guard is
# off — if you see this in prod logs, something is wrong.
_ALLOW_NO_ORIGIN = os.environ.get("WILDWATCH_ALLOW_NO_ORIGIN") == "1"
if _ALLOW_NO_ORIGIN:
    logger.warning(
        "CSRF Origin check DISABLED via WILDWATCH_ALLOW_NO_ORIGIN=1 — "
        "mutating /api/* requests no longer require an Origin header. "
        "Intended for trusted CLI use only; do not set in production."
    )


@app.middleware("http")
async def _csrf_origin_guard(request: Request, call_next):
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return await call_next(request)
    path = request.url.path
    if any(path.startswith(p) for p in _CSRF_EXEMPT_PATH_PREFIXES):
        return await call_next(request)

    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin:
        # DEFAULT-DENY on missing Origin for mutating requests. CLI users
        # can opt out via WILDWATCH_ALLOW_NO_ORIGIN=1 (read once at module
        # import so it shows in startup logs).
        if _ALLOW_NO_ORIGIN:
            return await call_next(request)
        return JSONResponse(
            status_code=403,
            content={
                "reason": "missing_origin",
                "detail": (
                    "missing Origin/Referer on mutating request. "
                    "Set WILDWATCH_ALLOW_NO_ORIGIN=1 for trusted CLI use."
                ),
            },
        )
    try:
        host = (urlparse(origin).hostname or "").lower()
    except Exception:
        return JSONResponse(
            status_code=403, content={"reason": "bad_origin", "detail": "bad Origin"}
        )
    if host not in _ALLOWED_ORIGIN_HOSTS:
        # `reason` lets a proxy-stripped-Origin case (missing_origin) be
        # distinguished from a real cross-origin attempt (disallowed_host)
        # in monitoring without parsing the detail string.
        return JSONResponse(
            status_code=403,
            content={"reason": "disallowed_host", "detail": f"Origin {host!r} not allowed"},
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
    record: dashboard.AlertEvent = {
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
def dashboard_index() -> HTMLResponse:
    # Disable browser caching of the SPA shell — every save during the
    # hackathon needs to reach the user on hard-refresh without the
    # stale-cache foot-gun.
    return HTMLResponse(
        content=dashboard.get_dashboard_html(),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


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
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        conn = await _async_sdk(_get_conn, timeout_s=10.0)
        rts = await _async_sdk(coll.list_rtstreams, timeout_s=5.0) or []
        sbs = await _async_sdk(conn.list_sandboxes, timeout_s=5.0) or []
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
class _ConnCache(TypedDict):
    # Annotated with the real SDK types when TYPE_CHECKING; runtime stays
    # `None | Any` so the videodb import isn't pulled in for callers who
    # only need the FastAPI app object.
    conn: _videodb_types.Connection | None
    coll: _videodb_types.Collection | None


_conn_cache: _ConnCache = {"conn": None, "coll": None}
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

    Both ``sources.add_source`` (disk IO + lock) and ``_get_coll`` (cold-
    start SDK auth) are blocking and must not run on the event loop.
    """
    try:
        s = await asyncio.to_thread(
            sources.add_source, kind=payload.kind, input=payload.input, name=payload.name
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    coll = await _async_sdk(_get_coll, timeout_s=10.0)
    _spawn_bg(ingest.dispatch(s.id, coll=coll), label=f"ingest.dispatch({s.id})")
    return s.__dict__


def _looks_like_video(head: bytes) -> bool:
    """Magic-byte sniff for the common video container formats.

    Without this the upload endpoint accepts any bytes, renames them to
    `.mp4`, and hands them to VideoDB. An attacker can upload HTML/script
    that any downstream player or VideoDB previewer might interpret.
    Cheap defence: require the first 32 bytes to match a known video
    container signature (mp4 / mov / webm / mkv / avi / mpeg-ps / flv).
    MPEG-TS is deliberately excluded — its sync-byte signature is too
    weak to verify without a real packet parser.
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
    # MPEG-TS deliberately NOT sniffed here. The sync-byte (0x47) check
    # is bypassable with any crafted prefix that includes a non-zero
    # second byte (e.g. b"\x47\x40 + padding" matches PUSI semantics).
    # Defeating that needs a real TS packet parser (PID, continuity
    # counter, adaptation-field flags) — out of scope for a sniff helper.
    # TS streams normally enter WildWatch via RTSP/RTMP/HLS (live stream
    # routes), not via this file-upload endpoint. Operators with a real
    # TS file should remux to MP4 first.
    # MPEG-PS / MPEG-1 / MPEG-2 video
    if head[:4] == b"\x00\x00\x01\xba" or head[:4] == b"\x00\x00\x01\xb3":
        return True
    # FLV
    if head[:3] == b"FLV":
        return True
    return False


@app.post("/api/sources/upload")
async def api_upload_source(
    request: Request,
    file: Annotated[UploadFile, File(...)],
    name: Annotated[str, Form(...)],
) -> dict:
    """Multipart upload path. Streams to a tempfile, then dispatches.

    Per-IP rate-limited (token bucket, capacity 3, refill 1/min) so a
    flood of 500 MB uploads can't saturate the SDK pool or fill disk.

    Bytes are streamed through aiofiles (already async). Everything else
    that touches .state.json or the SDK goes through asyncio.to_thread to
    keep the event loop unblocked. On 415 we DELETE the source row rather
    than leaving an orphan `status=error` entry.
    """
    client_ip = _client_ip_from(request)
    if not _upload_rate_limit_check(client_ip):
        # Don't echo the raw client_ip in the 429 detail — under
        # WILDWATCH_TRUSTED_PROXY=1 it came from X-Forwarded-For and
        # could contain attacker-controlled content reflected verbatim
        # into log aggregators and dashboards.
        raise HTTPException(
            status_code=429,
            detail=(
                f"upload rate limit exceeded "
                f"(bucket={_UPLOAD_BUCKET_CAPACITY}, refill 1/min); "
                "retry after one minute."
            ),
        )
    s = await asyncio.to_thread(sources.add_source, kind="upload", input="", name=name)

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
                if not sniff_done:
                    # 32 bytes covers ftyp/EBML/RIFF/FLV/MPEG-PS magic numbers.
                    if not _looks_like_video(chunk[:32]):
                        raise HTTPException(
                            status_code=415,
                            detail=(
                                "uploaded file does not look like a supported video "
                                "container (mp4/mov/webm/mkv/avi/mpeg/flv). "
                                "MPEG-TS is not accepted via upload — use the live-"
                                "stream RTSP/RTMP/HLS routes instead, or remux "
                                "to MP4 first. Refusing to forward to VideoDB."
                            ),
                        )
                    sniff_done = True
                written += len(chunk)
                # >= not > so the cap is a hard ceiling (a file of exactly
                # UPLOAD_MAX_BYTES bytes previously slipped through).
                if written >= UPLOAD_MAX_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"upload exceeds {UPLOAD_MAX_BYTES // (1024 * 1024)} MB cap",
                    )
                await out.write(chunk)
        await asyncio.to_thread(
            sources.update_source,
            s.id,
            input=str(tmp_path),
            stage_msg=f"received {written} bytes; queued for upload",
        )
    except HTTPException as e:
        tmp_path.unlink(missing_ok=True)
        # Both 415 (bad MIME) and 413 (oversize) mean we refused the file
        # entirely. Delete the orphan source row + broadcast source_deleted
        # so the dashboard card disappears immediately. Other statuses
        # leave the row in status=error for the operator to inspect.
        if e.status_code in (413, 415):
            reason = "rejected_mime" if e.status_code == 415 else "rejected_size"
            try:
                await asyncio.to_thread(sources.delete_source, s.id)
            except Exception as del_err:
                # Logging-only — don't mask the original 4xx with a 500.
                logger.warning(
                    "api_upload_source: cleanup delete failed for %s: %s",
                    s.id,
                    del_err,
                )
            try:
                dashboard.broadcast(
                    {
                        "type": "source_deleted",
                        "source_id": s.id,
                        "reason": reason,
                    }
                )
            except Exception:
                logger.warning(
                    "api_upload_source: dashboard.broadcast(source_deleted) failed",
                    exc_info=True,
                )
        else:
            await asyncio.to_thread(
                sources.update_source, s.id, status="error", error=str(e.detail)
            )
        raise
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        await asyncio.to_thread(sources.update_source, s.id, status="error", error=str(e))
        raise

    coll = await _async_sdk(_get_coll, timeout_s=10.0)
    _spawn_bg(ingest.dispatch(s.id, coll=coll), label=f"ingest.dispatch.upload({s.id})")
    return (await asyncio.to_thread(sources.get_source, s.id)).__dict__


@app.delete("/api/sources/{source_id}")
async def api_delete_source(source_id: str) -> dict:
    s = await asyncio.to_thread(sources.get_source, source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    # Best-effort remote cleanup (don't fail the local delete if remote fails)
    # but collect every failure so the caller can see remote resources that
    # may still be running (and burning credits). All blocking SDK calls go
    # through asyncio.to_thread so the event loop stays responsive.
    coll = await _async_sdk(_get_coll, timeout_s=10.0)
    warnings: list[str] = []
    if s.rtstream_id:
        try:
            rt = await _async_sdk(coll.get_rtstream, s.rtstream_id, timeout_s=5.0)
            await _async_sdk(rt.stop, timeout_s=10.0)
        except Exception as e:
            msg = f"rt.stop failed for {s.rtstream_id}: {e}"
            logger.warning("delete: %s", msg)
            warnings.append(msg)
    if s.video_id:
        try:
            await _async_sdk(coll.delete_video, s.video_id, timeout_s=10.0)
        except Exception as e:
            msg = f"coll.delete_video failed for {s.video_id}: {e}"
            logger.warning("delete: %s", msg)
            warnings.append(msg)
    await asyncio.to_thread(sources.delete_source, source_id)
    # Notify SSE subscribers so any open dashboard tab clears the card
    # immediately instead of waiting for the next /api/sources poll.
    try:
        dashboard.broadcast(
            {"type": "source_deleted", "source_id": source_id, "reason": "user_deleted"}
        )
    except Exception:
        logger.warning(
            "api_delete_source: dashboard.broadcast(source_deleted) failed",
            exc_info=True,
        )
    status = "deleted_with_warnings" if warnings else "deleted"
    return {"status": status, "id": source_id, "warnings": warnings}


@app.post("/api/sources/{source_id}/disconnect")
async def api_disconnect_source(source_id: str) -> dict:
    s = await asyncio.to_thread(sources.get_source, source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    if not s.rtstream_id:
        return {"status": "noop", "reason": "no rtstream attached"}
    coll = await _async_sdk(_get_coll, timeout_s=10.0)
    try:
        rt = await _async_sdk(coll.get_rtstream, s.rtstream_id, timeout_s=5.0)
        await _async_sdk(rt.stop, timeout_s=10.0)
        await asyncio.to_thread(
            sources.update_source, source_id, status="disconnected", stage_msg="rtstream stopped"
        )
    except Exception as e:
        await asyncio.to_thread(sources.update_source, source_id, status="error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
    return (await asyncio.to_thread(sources.get_source, source_id)).__dict__


@app.post("/api/sources/{source_id}/reconnect")
async def api_reconnect_source(source_id: str) -> dict:
    s = await asyncio.to_thread(sources.get_source, source_id)
    if s is None:
        raise HTTPException(status_code=404, detail="source not found")
    await asyncio.to_thread(
        sources.update_source,
        source_id,
        status="queued",
        error=None,
        stage_msg="reconnect requested",
    )
    coll = await _async_sdk(_get_coll, timeout_s=10.0)
    _spawn_bg(
        ingest.dispatch(source_id, coll=coll), label=f"ingest.dispatch.reconnect({source_id})"
    )
    return (await asyncio.to_thread(sources.get_source, source_id)).__dict__


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
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
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
    except Exception as exc:
        # Log the underlying failure so a non-iterable SDK return doesn't
        # look identical to a healthy "no results" — operators need to
        # see the contract violation.
        logger.warning("%s: coerce to list failed: %r — returning []", source, exc)
        return []


@app.get("/api/videos/{video_id}/indexes")
async def api_video_indexes(video_id: str) -> dict:
    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        video = await _async_sdk(coll.get_video, video_id, timeout_s=5.0)
        indexes = _coerce_to_list(
            await _async_sdk(video.list_scene_index, timeout_s=5.0),
            source=f"video({video_id}).list_scene_index",
        )
        return {"video_id": video_id, "indexes": indexes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/videos/{video_id}/scenes/{index_id}")
async def api_video_scenes(video_id: str, index_id: str, limit: int = 20) -> dict:
    """Return scenes for a video's scene index.

    BIG SUBTLETY: `video.get_scene_index(idx_id)` HANGS the SDK when the
    index is still in `processing` state — the SDK blocks waiting for
    backend completion (we've seen it sit >5 minutes). Solution: list
    the indexes first (fast), check the requested one's status. If not
    `ready`, return the status + an empty list rather than blocking.
    """
    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        video = await _async_sdk(coll.get_video, video_id, timeout_s=5.0)
        all_idxs = _coerce_to_list(
            await _async_sdk(video.list_scene_index, timeout_s=5.0),
            source=f"video({video_id}).list_scene_index",
        )
        meta = next(
            (i for i in all_idxs if (i.get("scene_index_id") or i.get("id")) == index_id),
            None,
        )
        if meta is None:
            return {
                "video_id": video_id,
                "index_id": index_id,
                "status": "not_found",
                "scenes": [],
            }
        status = str(meta.get("status", "unknown")).lower()
        if status not in ("ready", "complete", "completed", "indexed", "done"):
            # Don't block on a processing/failed index — surface the state
            # so the dashboard can render a friendly message.
            return {
                "video_id": video_id,
                "index_id": index_id,
                "status": status,
                "index_name": meta.get("name"),
                "scenes": [],
            }
        scenes = _coerce_to_list(
            await _async_sdk(video.get_scene_index, index_id, timeout_s=30.0),
            source=f"video({video_id}).get_scene_index({index_id})",
        )
        return {
            "video_id": video_id,
            "index_id": index_id,
            "status": "ready",
            "index_name": meta.get("name"),
            "scenes": scenes[:limit],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/videos/{video_id}/reindex")
async def api_video_reindex(video_id: str) -> dict:
    """Re-run the AI pass on a video AND re-fire Path-B alerts.

    Three things happen here, in order:
      1. If the video has no visual scene index, create one (species prompt).
         This handles backfill for old uploads from before auto-indexing.
      2. If the video has no audio index, create one (audio prompt).
         Idempotent — `kick_off_audio_index` skips if any audio-named
         index already exists.
      3. Spawn the post-upload analysis sweep against whatever indexes
         exist (newly-created or pre-existing). This re-runs the event
         queries and fires Telegram + dashboard SSE on hits.

    Why also run the sweep on already-indexed videos? Because the
    operator clicked "Re-index" — they want fresh alerts. If they only
    wanted to ensure the indexes exist, they wouldn't bother. The
    confirmToast on the dashboard explicitly warns "alerts may re-fire."
    """
    from wildwatch.ingest import _DEFAULT_SCENE_PROMPT_CONTEXT
    from wildwatch.post_upload_analysis import (
        kick_off_audio_index,
        run_post_upload_analysis,
    )
    from wildwatch.prompts import format_prompt

    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        video = await _async_sdk(coll.get_video, video_id, timeout_s=5.0)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"video not found: {e}") from e

    # Inspect existing indexes — only create what's missing.
    try:
        existing = await _async_sdk(video.list_scene_index, timeout_s=10.0) or []
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"list_scene_index failed: {e}") from e

    has_visual = any("audio" not in str(i.get("name", "")).lower() for i in existing)

    new_visual_id: str | None = None
    if not has_visual:
        try:
            prompt = format_prompt("species", **_DEFAULT_SCENE_PROMPT_CONTEXT)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"prompt format failed: {e}") from e
        try:
            new_visual_id = await _async_sdk(
                video.index_scenes,
                timeout_s=30.0,
                prompt=prompt,
                # Keep the "wildwatch-auto" prefix so post_upload_analysis's
                # poller picks this index up by substring match.
                name=f"wildwatch-auto-reindex-{int(time.time())}",
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"index_scenes failed: {e}") from e

    # Unique source_id per click so synthesised event_ids don't collide
    # if the operator re-indexes the same video twice in quick succession.
    source_id = f"reindex-{video_id[:8]}-{int(time.time())}"

    # Best-effort audio index kickoff (skips if one already exists).
    try:
        await _async_sdk(kick_off_audio_index, video, source_id, timeout_s=15.0)
    except Exception as e:
        logger.warning("reindex: audio index kickoff failed for video=%s: %r", video_id, e)

    # Spawn the sweep. Fire-and-forget — task tracked via local set so
    # the GC can't drop it mid-run.
    task = asyncio.create_task(run_post_upload_analysis(video, source_id))
    _reindex_sweep_tasks.add(task)
    task.add_done_callback(_reindex_sweep_tasks.discard)

    return {
        "video_id": video_id,
        "scene_index_id": str(new_visual_id) if new_visual_id else None,
        "status": "processing",
        "message": (
            "Re-running analysis. Telegram alerts will fire on every event "
            "the AI detects (gunshot, alarm calls, rare species, ...). Allow "
            "1-3 minutes if new indexes are still processing."
        ),
    }


# Strong refs to active re-index sweep tasks — same pattern as
# ingest._post_analysis_tasks. Prevents GC from dropping them mid-run.
_reindex_sweep_tasks: set[asyncio.Task[Any]] = set()


@app.delete("/api/videos/{video_id}")
async def api_delete_video(video_id: str) -> dict:
    """Delete a video from the VideoDB collection.

    Mirrors the Sources tab delete UX but for raw videos that may not
    have a corresponding Source row (e.g. corpus uploads bootstrapped
    via CLI). Surface SDK failures as 502 so the dashboard can toast.
    """
    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"connection failed: {e}") from e
    try:
        await _async_sdk(coll.delete_video, video_id, timeout_s=15.0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"delete_video failed: {e}") from e
    # Bust the videos cache so the next /api/videos call hits VideoDB.
    _videos_cache["at"] = 0.0
    _videos_cache["data"] = {"videos": []}
    try:
        dashboard.broadcast({"type": "video_deleted", "video_id": video_id})
    except Exception:
        logger.exception("video_deleted broadcast failed")
    return {"video_id": video_id, "status": "deleted"}


@app.get("/api/videos/{video_id}/clip")
async def api_video_clip(video_id: str, start: float, end: float) -> dict:
    """Return a playable stream URL for a [start, end] segment of a video.

    Used by the dashboard's scene cards — clicking a scene calls this
    and opens the returned URL in a new tab. The SDK call
    ``video.generate_stream(timeline=[(start, end)])`` returns a fresh
    m3u8 manifest scoped to that range.
    """
    if end <= start:
        raise HTTPException(status_code=400, detail="end must be > start")
    if end - start > 600:  # 10 min cap — scenes are seconds, not hours
        raise HTTPException(status_code=400, detail="segment too long")
    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        video = await _async_sdk(coll.get_video, video_id, timeout_s=5.0)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"video not found: {e}") from e
    try:
        url = await _async_sdk(
            video.generate_stream,
            timeout_s=15.0,
            timeline=[(float(start), float(end))],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"generate_stream failed: {e}") from e
    return {"video_id": video_id, "start": start, "end": end, "stream_url": url}


@app.get("/api/rtstreams/{rt_id}/indexes")
async def api_rtstream_indexes(rt_id: str) -> dict:
    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        rt = await _async_sdk(coll.get_rtstream, rt_id, timeout_s=5.0)
        raw = _coerce_to_list(
            await _async_sdk(rt.list_scene_indexes, timeout_s=5.0),
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
async def api_rtstream_scenes(rt_id: str, index_id: str, page_size: int = 20) -> dict:
    try:
        coll = await _async_sdk(_get_coll, timeout_s=10.0)
        rt = await _async_sdk(coll.get_rtstream, rt_id, timeout_s=5.0)
        idx = await _async_sdk(rt.get_scene_index, index_id, timeout_s=5.0)
        data = await _async_sdk(idx.get_scenes, page=1, page_size=page_size, timeout_s=5.0)
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
        conn = await _async_sdk(_get_conn, timeout_s=10.0)
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
async def api_search(req: SearchRequest) -> dict:
    """Search across collection/video/rtstream scopes.

    Skill conformance (video-db/skills · search-reference.md):
      - ``coll.search`` only hits the spoken-word index — wildlife clips
        have no transcripts so a naive ``coll.search`` returns nothing.
        Per the skill: "For keyword or scene search, use video.search()
        on individual videos instead." So collection-scope search
        FANS OUT across every video with a ready scene index and runs
        ``video.search(index_type=scene)`` on each, then merges by score.
      - Video/rtstream scene search needs ``index_type=IndexType.scene`` +
        a ``score_threshold`` so noise gets filtered.
      - The SDK raises a custom error on empty results (string contains
        "No results found"); catch it and return ``shots: []`` instead of
        a 500. That's what every skill example does.
    """
    coll = await _async_sdk(_get_coll, timeout_s=10.0)
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

    _READY = {"ready", "indexed", "complete", "completed", "done"}

    async def _video_scene_search(v: Any, query: str) -> list[dict]:
        """Search one video's scene index. Returns shots or empty list."""
        try:
            idxs = await _async_sdk(v.list_scene_index, timeout_s=8.0) or []
        except Exception as e:
            logger.debug("collection-search: list_scene_index failed for %s: %r", v.id, e)
            return []
        ready = [i for i in idxs if str(i.get("status", "")).lower() in _READY]
        if not ready:
            return []
        kwargs: dict[str, Any] = {"query": query, "score_threshold": 0.3}
        if IndexType is not None:
            kwargs["index_type"] = IndexType.scene
        if SearchType is not None:
            kwargs["search_type"] = SearchType.semantic
        try:
            result = await _async_sdk(v.search, timeout_s=10.0, **kwargs)
        except Exception as e:
            if "No results found" in str(e):
                return []
            logger.debug("collection-search: v.search failed for %s: %r", v.id, e)
            return []
        raw = getattr(result, "shots", None) or []
        out = []
        for s in raw:
            d = _shot_to_dict(s)
            # The SDK shot omits video_id; backfill so the dashboard can
            # link each hit to its source video.
            d.setdefault("video_id", v.id)
            if d.get("video_id") is None:
                d["video_id"] = v.id
            d["video_name"] = getattr(v, "name", None)
            out.append(d)
        return out

    try:
        if req.scope == "collection":
            videos = await _async_sdk(coll.get_videos, timeout_s=15.0) or []
            # Fan out — concurrent per-video scene search.
            tasks = [_video_scene_search(v, req.query) for v in videos]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            shots: list[dict] = [s for chunk in results for s in chunk]
            shots.sort(key=lambda s: s.get("score") or 0.0, reverse=True)
            cap = req.result_threshold or 50
            return {
                "scope": "collection",
                "shots": shots[:cap],
                "videos_searched": len(videos),
            }

        if req.scope == "video":
            if not req.target_id:
                raise HTTPException(status_code=400, detail="target_id required for video scope")
            v = await _async_sdk(coll.get_video, req.target_id, timeout_s=5.0)
            kwargs = {"query": req.query, "score_threshold": 0.3}
            if IndexType is not None:
                kwargs["index_type"] = IndexType.scene
            try:
                result = await _async_sdk(v.search, timeout_s=10.0, **kwargs)
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
            rt = await _async_sdk(coll.get_rtstream, req.target_id, timeout_s=5.0)
            kwargs = {"query": req.query, "score_threshold": 0.3}
            if IndexType is not None:
                kwargs["index_type"] = IndexType.scene
            if req.index_id:
                kwargs["index_id"] = req.index_id
            try:
                result = await _async_sdk(rt.search, timeout_s=10.0, **kwargs)
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
