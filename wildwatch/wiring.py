"""Shared (index x event) -> alert wiring helper.

Both ``scripts/bootstrap.py`` and ``scripts/start_live_test.py`` used to
inline near-identical ``wire_alerts`` functions whose idempotency key was
``(stream_key, kind, ev_id_var)``. That key collides across rtstream
incarnations: when a new rtstream got the same ``stream_key`` (e.g.
``mara_live``) the cached entries silently caused every ``create_alert``
call to be skipped, so the new rtstream booted with zero alerts wired.

This module centralises the wiring logic and uses ``rtstream_id`` as the
real invalidation key. The persisted ``alert_state`` dict shape stays
human-readable (``f"{kind}.{ev_id_var}"``) but each entry now records the
``rtstream_id`` it was created for. A mismatch (or a pre-fix entry with no
``rtstream_id`` field) is treated as stale and the alert is re-created.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from wildwatch.events import EVENT_DEFINITIONS, INDEX_EVENT_MAP

logger = logging.getLogger(__name__)


class WireFailure(NamedTuple):
    """One row of WireResult.failures — named so callers don't rely on
    positional access."""

    kind: str
    event_id_var: str
    error_repr: str


@dataclass
class WireResult:
    created: int = 0
    reused: int = 0
    replaced: int = 0
    failed: int = 0
    failures: list[WireFailure] = field(default_factory=list)


def wire_alerts(
    rtstream_id: str,
    indexes: dict[str, Any],
    events_map: dict[str, str],
    base_url: str,
    alert_state: dict[str, dict],
    ws_connection_id: str | None = None,
) -> WireResult:
    """Wire one alert per (index_kind, event) pair in ``INDEX_EVENT_MAP``.

    Mutates ``alert_state`` in place; caller is responsible for persisting
    the surrounding state dict to disk.

    Args:
        rtstream_id: id of the rtstream these indexes belong to. Used as
            the cache invalidation key — a fresh rtstream forces a re-wire
            even if ``alert_state`` already has entries.
        indexes: ``{kind: index_obj}`` where ``index_obj.create_alert(
            event_id, callback_url=...)`` returns an alert id.
        events_map: ``{id_var: event_id}`` mapping (output of bootstrap's
            event-ensuring step).
        base_url: webhook receiver base URL; tier suffix gets appended.
        alert_state: per-stream alert cache, keyed by ``f"{kind}.{ev_id_var}"``.
    """
    tier_by_id = {ev["id_var"]: ev["tier"] for ev in EVENT_DEFINITIONS}
    label_by_id = {ev["id_var"]: ev["label"] for ev in EVENT_DEFINITIONS}

    result = WireResult()
    for kind, event_id_vars in INDEX_EVENT_MAP.items():
        # Guard the lookup — a stream connected without audio support, or
        # a misspelled key in INDEX_EVENT_MAP, used to raise KeyError here
        # and bypass the per-alert failure tracking entirely.
        idx = indexes.get(kind)
        if idx is None:
            # Mirror the per-alert idempotency check (line ~140 of the inner
            # loop). On re-run with the same rtstream_id, an existing
            # sentinel means we already logged + recorded this failure;
            # count it as `reused` and move on so logs don't storm.
            already_recorded_count = 0
            for ev_id_var in event_id_vars:
                existing = alert_state.get(f"{kind}.{ev_id_var}")
                if (
                    existing is not None
                    and existing.get("rtstream_id") == rtstream_id
                    and existing.get("error") == "index_missing"
                ):
                    already_recorded_count += 1
                    result.reused += 1
                    continue
                result.failed += 1
                result.failures.append(
                    WireFailure(kind, ev_id_var, "KeyError: index missing from indexes dict")
                )
                alert_state[f"{kind}.{ev_id_var}"] = {
                    "alert_id": None,
                    "rtstream_id": rtstream_id,
                    "event_id": None,
                    "label": label_by_id.get(ev_id_var, ev_id_var),
                    "tier": tier_by_id.get(ev_id_var),
                    "callback_url": None,
                    "ws_connection_id": ws_connection_id,
                    "error": "index_missing",
                }
            # Only log on FRESH failures — re-runs that hit only sentinels
            # are silent.
            if already_recorded_count < len(event_id_vars):
                logger.error(
                    "wire_alerts: index %r missing from indexes dict; "
                    "marking %d new + %d already-recorded events as failed",
                    kind,
                    len(event_id_vars) - already_recorded_count,
                    already_recorded_count,
                )
            continue
        for ev_id_var in event_id_vars:
            # Guard events_map too — if the event registration failed
            # during bootstrap and didn't populate this id_var, we used
            # to KeyError before the per-alert try/except.
            event_id = events_map.get(ev_id_var)
            if event_id is None:
                # Mirror the missing-index retry-storm guard: if this
                # (kind, ev_id_var) already has an event_id_missing
                # sentinel for THIS rtstream, count it as reused and
                # don't re-log.
                existing = alert_state.get(f"{kind}.{ev_id_var}")
                if (
                    existing is not None
                    and existing.get("rtstream_id") == rtstream_id
                    and existing.get("error") == "event_id_missing"
                ):
                    result.reused += 1
                    continue
                result.failed += 1
                result.failures.append(
                    WireFailure(kind, ev_id_var, "KeyError: event_id missing from events_map")
                )
                alert_state[f"{kind}.{ev_id_var}"] = {
                    "alert_id": None,
                    "rtstream_id": rtstream_id,
                    "event_id": None,
                    "label": label_by_id.get(ev_id_var, ev_id_var),
                    "tier": tier_by_id.get(ev_id_var),
                    "callback_url": None,
                    "ws_connection_id": ws_connection_id,
                    "error": "event_id_missing",
                }
                logger.error(
                    "wire_alerts: event %r not in events_map; skipping (sentinel persisted)",
                    ev_id_var,
                )
                continue
            tier = tier_by_id[ev_id_var]
            cb = f"{base_url}/webhook/{tier}"
            key = f"{kind}.{ev_id_var}"

            existing = alert_state.get(key)
            if existing is not None and existing.get("rtstream_id") == rtstream_id:
                result.reused += 1
                continue

            # Dual-delivery: forward ws_connection_id when available so the
            # alert fires through BOTH the webhook callback AND the WebSocket
            # channel (skill's rtstream-reference.md, Alert Delivery section).
            create_kwargs: dict[str, Any] = {"callback_url": cb}
            if ws_connection_id:
                create_kwargs["ws_connection_id"] = ws_connection_id

            # Per-alert isolation: a single create_alert failure (transient
            # SDK error, bad event_id, quota) used to abort every remaining
            # alert for the stream because the exception propagated. Now
            # each failure is logged + recorded + we keep going.
            try:
                alert_id = idx.create_alert(event_id, **create_kwargs)
            except Exception as e:
                result.failed += 1
                result.failures.append(WireFailure(kind, ev_id_var, repr(e)))
                logger.error(
                    "wire_alerts: create_alert failed for kind=%s event=%s "
                    "(label=%s tier=%s rtstream=%s): %s",
                    kind,
                    ev_id_var,
                    label_by_id[ev_id_var],
                    tier,
                    rtstream_id,
                    e,
                    exc_info=True,
                )
                continue

            alert_state[key] = {
                "alert_id": alert_id,
                "rtstream_id": rtstream_id,
                "event_id": event_id,
                "label": label_by_id[ev_id_var],
                "tier": tier,
                "callback_url": cb,
                "ws_connection_id": ws_connection_id,
            }
            if existing is None:
                result.created += 1
            else:
                result.replaced += 1

    return result
