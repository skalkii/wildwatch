"""Process-wide VideoDB connection cache.

``videodb.connect()`` performs an auth round-trip on every call —
recreating the connection per request would block on auth latency and
turn every dashboard tab into a slow disaster. The two functions in
this module cache the connection + collection for the life of the
process, gated by a re-entrant lock so a cold-cache burst doesn't
race two simultaneous ``videodb.connect()`` calls.

Extracted from ``wildwatch/webhooks.py`` to keep that module focused
on routes / middleware. The names ``_get_conn`` / ``_get_coll`` are
re-exported from ``webhooks`` so existing tests that monkeypatch
``wildwatch.webhooks._get_coll`` keep working.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from wildwatch._videodb_types import Collection, Connection
else:
    Collection = Connection = Any


class _ConnCache(TypedDict):
    """Module-level cache shape. The ``Any`` runtime alias keeps the
    videodb import lazy — callers that only need other helpers from the
    package don't pay the SDK import cost up front."""

    conn: Connection | None
    coll: Collection | None


_conn_cache: _ConnCache = {"conn": None, "coll": None}

# RLock not Lock — ``_get_coll`` acquires the lock and then calls
# ``_get_conn`` which acquires it again. A plain Lock would deadlock
# on the same thread; RLock allows recursive acquisition by the
# holder.
_conn_lock = threading.RLock()


def _get_conn() -> Any:
    """Return the process-cached VideoDB Connection, creating on first call."""
    if _conn_cache["conn"] is None:
        with _conn_lock:
            # Double-checked locking: the first caller pays the auth
            # cost; subsequent waiters get the cached handle without
            # re-running ``videodb.connect()``.
            if _conn_cache["conn"] is None:
                import videodb

                _conn_cache["conn"] = videodb.connect()
    return _conn_cache["conn"]


def _get_coll() -> Any:
    """Lazy-load + cache the default VideoDB Collection."""
    if _conn_cache["coll"] is None:
        with _conn_lock:
            if _conn_cache["coll"] is None:
                _conn_cache["coll"] = _get_conn().get_collection()
    return _conn_cache["coll"]


def reset_cache() -> None:
    """Test helper — drop the cached conn + coll so the next call rebuilds.

    NOT called from production code. Tests use it to ensure a
    monkeypatched ``videodb.connect`` actually fires on the next access.
    """
    with _conn_lock:
        _conn_cache["conn"] = None
        _conn_cache["coll"] = None
