"""Pytest fixtures shared across all tests.

Test environment hardening — applied via autouse session fixture so every
test in the suite gets a consistent setup without each file having to
remember it.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True, scope="session")
def _allow_no_origin_for_tests() -> None:
    """The CSRF/Origin guard rejects mutating requests without an Origin
    header. TestClient doesn't send one by default. Rather than monkey-
    patching every test to inject headers, set the explicit escape-hatch
    env var that the middleware honours, exactly as a CLI operator would."""
    os.environ.setdefault("WILDWATCH_ALLOW_NO_ORIGIN", "1")
    yield


@pytest.fixture(autouse=True)
def _reset_global_state() -> None:
    """Reset process-wide module globals between every test.

    Two state surfaces leak between tests by default:
      - `_conn_cache` — videodb.connect patches are sticky once the cache
        is warm; later tests inherit a previous test's Mock unless we clear.
      - `dashboard` counters (`_total`, `_tier_counts`, `_recent_events`,
        `_subscribers`, `_dropped_total`, `_started_at`) — tests that POST
        /webhook/* accumulate into these, polluting `/api/stats` assertions
        in later tests.
    """
    from wildwatch import dashboard as _dash
    from wildwatch import webhooks as _wh

    _wh._conn_cache["conn"] = None
    _wh._conn_cache["coll"] = None
    _dash.reset_state()
    yield
    _wh._conn_cache["conn"] = None
    _wh._conn_cache["coll"] = None
    _dash.reset_state()
