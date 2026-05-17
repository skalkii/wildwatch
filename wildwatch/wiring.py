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

from dataclasses import dataclass
from typing import Any

from wildwatch.events import EVENT_DEFINITIONS, INDEX_EVENT_MAP


@dataclass
class WireResult:
    created: int
    reused: int
    replaced: int


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

    created = reused = replaced = 0
    for kind, event_id_vars in INDEX_EVENT_MAP.items():
        idx = indexes[kind]
        for ev_id_var in event_id_vars:
            event_id = events_map[ev_id_var]
            tier = tier_by_id[ev_id_var]
            cb = f"{base_url}/webhook/{tier}"
            key = f"{kind}.{ev_id_var}"

            existing = alert_state.get(key)
            if existing is not None and existing.get("rtstream_id") == rtstream_id:
                reused += 1
                continue

            # Dual-delivery: forward ws_connection_id when available so the
            # alert fires through BOTH the webhook callback AND the WebSocket
            # channel (skill's rtstream-reference.md, Alert Delivery section).
            create_kwargs: dict[str, Any] = {"callback_url": cb}
            if ws_connection_id:
                create_kwargs["ws_connection_id"] = ws_connection_id
            alert_id = idx.create_alert(event_id, **create_kwargs)
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
                created += 1
            else:
                replaced += 1

    return WireResult(created=created, reused=reused, replaced=replaced)
