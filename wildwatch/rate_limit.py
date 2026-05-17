"""Per-IP upload rate limiter.

Three concurrent attackers uploading 499 MB each would saturate the
4-worker SDK pool, fill disk, and DoS the dashboard. A leaky bucket
(capacity=3, refill=1/min/IP) neutralises that without inconveniencing
real operators.

Extracted from ``wildwatch/webhooks.py`` so the FastAPI route module
stays focused on routes. ``webhooks.py`` re-exports the symbols so
existing call sites + tests stay valid.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from collections import OrderedDict

from fastapi import Request

logger = logging.getLogger(__name__)

# Token bucket configuration.
_UPLOAD_BUCKET_CAPACITY = 3
_UPLOAD_BUCKET_REFILL_PER_SEC = 1.0 / 60.0  # one new token per minute per IP
# Hard cap on the bucket dict to bound memory under attacker probing.
# OrderedDict gives O(1) LRU eviction via popitem(False).
_UPLOAD_BUCKETS_MAX = 50_000
# {ip: (tokens_float, last_refill_ts)}
_upload_buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
_upload_bucket_lock = threading.Lock()


_IPV6_BRACKET_RE = re.compile(r"^\[(.+)\](?::\d+)?$")


def _normalize_ip(raw: str) -> str:
    """Strip surrounding ``[...]`` brackets and ``:port`` from an XFF token.

    Without this, an attacker behind a trusted proxy could rotate the
    port suffix on ``[::1]:1``, ``[::1]:2`` … to mint 65k distinct
    bucket keys from one host, defeating the per-IP limit. Returns ``""``
    for empty / unparsable input.

    Also defends against malformed bracket forms (``[::1`` with no
    closing ``]``) and multi-colon IPv4-with-suffix (``192.0.2.1:80:foo``).
    """
    raw = raw.strip()
    if not raw:
        return ""
    m = _IPV6_BRACKET_RE.match(raw)
    if m:
        return m.group(1).strip()
    # Bracket present but missing closing ``]`` — strip what we can.
    if raw.startswith("["):
        rest = raw[1:]
        if "]" in rest:
            return rest.split("]", 1)[0].strip()
        return rest.strip().split(":")[0] if rest.count(":") > 2 else rest.strip()
    # Strip a single trailing ``:port`` for plain IPv4 (one colon).
    if raw.count(":") == 1:
        return raw.split(":", 1)[0].strip()
    # Multi-colon non-IPv6 (e.g. ``192.0.2.1:80:foo``) → reject as malformed.
    if raw.count(":") > 1 and not _looks_like_bare_ipv6(raw):
        return ""
    return raw


def _looks_like_bare_ipv6(raw: str) -> bool:
    """Heuristic: bare IPv6 is hex chars + colons, no other punctuation."""
    return all(c in "0123456789abcdefABCDEF:" for c in raw)


def _client_ip_from(request: Request) -> str:
    """Resolve the client IP, honouring X-Forwarded-For when configured.

    Behind a reverse proxy (nginx, Cloudflare, ALB) every upload would
    appear to come from the proxy, collapsing all real clients into a
    single shared bucket — a self-DoS vector. Operators behind a
    trusted proxy set ``WILDWATCH_TRUSTED_PROXY=1`` to use the FIRST IP
    in X-Forwarded-For. Direct-exposure deployments leave the env unset
    so a remote attacker can't spoof XFF to bypass the limit.
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

    Token bucket per IP. Each upload consumes one token. Lock-guarded
    so concurrent uploads from one IP can't both observe a full bucket
    and double-spend. Idle IPs are GC'd from the dict; LRU overflow
    eviction bounds memory under attacker probing.
    """
    # Bypass when we can't identify the client. The shared-pool
    # fallback produces spurious 429s for legit traffic. Operators
    # behind a real proxy should set WILDWATCH_TRUSTED_PROXY=1.
    if client_ip == "unknown":
        logger.warning(
            "upload rate limit: client_ip is 'unknown' — bypassing bucket. "
            "Set WILDWATCH_TRUSTED_PROXY=1 if behind a reverse proxy."
        )
        return True

    now = time.time()
    with _upload_bucket_lock:
        tokens, last = _upload_buckets.get(client_ip, (float(_UPLOAD_BUCKET_CAPACITY), now))
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
        # GC entries whose bucket is at full capacity (fully idle) so
        # the dict doesn't pollute with default state.
        if tokens >= float(_UPLOAD_BUCKET_CAPACITY) - 0.001:
            _upload_buckets.pop(client_ip, None)
        else:
            _upload_buckets[client_ip] = (new_tokens, now)
            _upload_buckets.move_to_end(client_ip)
            _evict_overflow_locked()
        return True


def _evict_overflow_locked() -> None:
    """Trim LRU entries when the bucket dict exceeds the hard cap.

    Caller MUST hold ``_upload_bucket_lock``.
    """
    while len(_upload_buckets) > _UPLOAD_BUCKETS_MAX:
        _upload_buckets.popitem(last=False)
