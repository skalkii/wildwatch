"""Tests for wildwatch.wiring.wire_alerts — the (index x event) -> alert wirer.

Reproduces the silent-failure bug found in 1h pressure test: when a stream_key
is reused (e.g. orchestrator re-run with fresh rtstreams), the cached
alert_state dict made wire_alerts a no-op because the cache key
(stream_key, kind, ev_id_var) collided across rtstream incarnations.

The fix: include rtstream_id in the invalidation check so a fresh rtstream
forces re-wiring even though stream_key matches.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from wildwatch.events import EVENT_DEFINITIONS, INDEX_EVENT_MAP
from wildwatch.wiring import wire_alerts


def _make_indexes(create_alert_returns: str = "alert-X") -> dict[str, MagicMock]:
    """Fake per-kind index objects whose create_alert returns a fixed id."""
    indexes = {}
    for kind in INDEX_EVENT_MAP:
        idx = MagicMock(name=f"idx_{kind}")
        idx.create_alert.return_value = create_alert_returns
        indexes[kind] = idx
    return indexes


def _events_map() -> dict[str, str]:
    return {ev["id_var"]: f"event-{ev['id_var']}" for ev in EVENT_DEFINITIONS}


def test_empty_state_creates_all_alerts():
    indexes = _make_indexes()
    alert_state: dict = {}
    res = wire_alerts(
        rtstream_id="rt-001",
        indexes=indexes,
        events_map=_events_map(),
        base_url="https://hook.example.com",
        alert_state=alert_state,
    )
    expected = sum(len(v) for v in INDEX_EVENT_MAP.values())
    assert res.created == expected
    assert res.reused == 0
    assert res.replaced == 0
    assert len(alert_state) == expected
    # every entry persisted with rtstream_id stamp
    assert all(entry["rtstream_id"] == "rt-001" for entry in alert_state.values())


def test_same_rtstream_rerun_is_idempotent():
    indexes = _make_indexes()
    alert_state: dict = {}
    wire_alerts("rt-001", indexes, _events_map(), "https://h", alert_state)

    fresh_indexes = _make_indexes("alert-Y")
    res = wire_alerts("rt-001", fresh_indexes, _events_map(), "https://h", alert_state)

    assert res.created == 0
    assert res.replaced == 0
    expected = sum(len(v) for v in INDEX_EVENT_MAP.values())
    assert res.reused == expected
    # second pass must not have called create_alert at all
    for idx in fresh_indexes.values():
        idx.create_alert.assert_not_called()


def test_fresh_rtstream_same_stream_key_re_wires_all_alerts():
    """THE BUG REPRODUCER.

    Operator re-runs orchestrator. Same stream_key (e.g. 'mara_live') but a
    NEW rtstream was provisioned. The cached alert_state from the prior run
    must be invalidated so all alerts re-wire against the new index ids.
    Old code keyed only by (stream_key, kind, ev_id_var) so this silently
    skipped every create_alert call.
    """
    indexes_v1 = _make_indexes("alert-old")
    alert_state: dict = {}
    wire_alerts("rt-OLD", indexes_v1, _events_map(), "https://h", alert_state)

    indexes_v2 = _make_indexes("alert-new")
    res = wire_alerts("rt-NEW", indexes_v2, _events_map(), "https://h", alert_state)

    expected = sum(len(v) for v in INDEX_EVENT_MAP.values())
    assert res.replaced == expected, (
        "fresh rtstream must invalidate stale alert_state and re-create every alert"
    )
    assert res.created == 0  # bucket: 'replaced', not 'created'
    assert res.reused == 0
    # every persisted entry now points at rt-NEW
    assert all(entry["rtstream_id"] == "rt-NEW" for entry in alert_state.values())
    # and create_alert was called expected times across all indexes
    total_calls = sum(idx.create_alert.call_count for idx in indexes_v2.values())
    assert total_calls == expected


def test_callback_url_uses_correct_tier_per_event():
    indexes = _make_indexes()
    alert_state: dict = {}
    wire_alerts("rt-001", indexes, _events_map(), "https://h", alert_state)

    tier_by_id = {ev["id_var"]: ev["tier"] for ev in EVENT_DEFINITIONS}
    for kind, event_id_vars in INDEX_EVENT_MAP.items():
        for ev_id_var in event_id_vars:
            entry = alert_state[f"{kind}.{ev_id_var}"]
            expected_url = f"https://h/webhook/{tier_by_id[ev_id_var]}"
            assert entry["callback_url"] == expected_url


def test_migration_entry_without_rtstream_id_is_treated_as_stale():
    """Old .state.json files (pre-fix) have no rtstream_id field. Treat as
    stale and re-wire so operators don't have to manually clear state."""
    indexes = _make_indexes()
    # Simulate pre-fix state shape: missing rtstream_id field
    alert_state = {
        "species.rare_species": {
            "alert_id": "legacy",
            "event_id": "event-rare_species",
            "label": "rare_species_sighting",
            "tier": 1,
            "callback_url": "https://h/webhook/1",
        }
    }
    res = wire_alerts("rt-001", indexes, _events_map(), "https://h", alert_state)
    expected = sum(len(v) for v in INDEX_EVENT_MAP.values())
    # the one stale entry replaced + the other 17 created
    assert res.replaced == 1
    assert res.created == expected - 1
