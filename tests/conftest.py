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
def _reset_conn_cache() -> None:
    """Reset the process-wide VideoDB connection cache before every test.

    Tests that patch ``videodb.connect`` rely on the patch actually being
    invoked. Since _get_conn() caches the connection for the life of the
    process, the first test's Mock would persist into later tests'
    patches, hiding the new side_effect/return_value behind a stale
    handle. Clearing the cache around each test makes each patch effective.
    """
    from wildwatch import webhooks as _wh

    _wh._conn_cache["conn"] = None
    _wh._conn_cache["coll"] = None
    yield
    _wh._conn_cache["conn"] = None
    _wh._conn_cache["coll"] = None
