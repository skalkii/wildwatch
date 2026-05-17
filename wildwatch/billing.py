"""Local credit-burn estimator for the dashboard's Usage tab.

Back-of-napkin estimate from ``.state.json`` start timestamps + a small
rate table. Cross-checked against ``coll.list_rtstreams()`` /
``conn.get_sandbox()`` so we only meter resources VideoDB confirms are
still alive.

Extracted from ``wildwatch/webhooks.py`` during the size-split refactor.
``webhooks.py`` re-exports ``_estimate_credit_burn_usd`` so existing
route handlers + tests stay valid without any call-site changes.

The single public entry point is ``_estimate_credit_burn_usd``. It
takes a ``coll_getter`` + ``conn_getter`` callable so the function
stays decoupled from the FastAPI app — a CLI script can call this
directly with the wildwatch.sdk_pool helpers.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Rates (from PARALLEL_STREAM_HANDOVER analysis):
#   - Live RTStream visual+audio indexing at RELAXED batch_config: ~$5/h
#   - Sandbox Medium tier: $3.50/h flat
#   - Sandbox Small tier: $1/h flat
_RT_RATE_USD_PER_H = 5.0
_SB_RATE_MEDIUM = 3.5
_SB_RATE_SMALL = 1.0

# Statuses VideoDB exposes that we treat as "billing is happening right now".
_RT_LIVE_STATUSES = frozenset({"connected", "running", "ingesting", "indexing", "ready"})


def _parse_started_at(raw: Any) -> float | None:
    """Convert a started_at field (ISO string or numeric epoch) to seconds.

    Returns None on parse failure so the caller can decide whether to
    skip or surface the error.
    """
    try:
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        return float(raw)
    except Exception:
        return None


def _estimate_credit_burn_usd(
    *,
    coll_getter: Callable[[], Any],
    conn_getter: Callable[[], Any],
    with_timeout: Callable[..., Any],
    coerce_to_list: Callable[..., list],
    state_file: Path | None = None,
) -> dict:
    """Build the local credit-burn dict the Usage tab renders.

    ``coll_getter`` / ``conn_getter`` are the cached SDK accessors
    (``wildwatch.sdk_pool._get_coll`` etc.). ``with_timeout`` is the
    blocking-call wrapper from ``webhooks._with_timeout``. ``coerce_to_list``
    is the SDK-contract-tolerant cast helper. Injecting them keeps this
    module decoupled from the FastAPI app — a CLI script could call
    this with its own variants.
    """
    if state_file is None:
        state_file = Path(__file__).resolve().parent.parent / ".state.json"
    if not state_file.exists():
        return {
            "total_usd": 0.0,
            "rtstreams_usd": 0.0,
            "sandboxes_usd": 0.0,
            "details": [],
        }
    try:
        state = json.loads(state_file.read_text())
    except Exception as e:
        logger.warning("_estimate_credit_burn_usd: %s unreadable: %r", state_file, e)
        return {"total_usd": 0.0, "error": f"state unreadable: {e}"}

    now = time.time()
    rt_total = 0.0
    sb_total = 0.0
    details: list[dict] = []

    # Live VideoDB-side status — only meter rtstreams actually running.
    # If the SDK call fails (network blip, auth glitch), we DO NOT
    # silently zero the estimate. Flag the result as
    # ``live_status_unknown`` and fall back to the legacy upper-bound.
    live_status_by_id: dict[str, str] = {}
    live_status_available = False
    sdk_warning: str | None = None
    try:
        coll = coll_getter()
        rts = coerce_to_list(
            with_timeout(coll.list_rtstreams, timeout_s=5.0),
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

    for key, rt_state in state.get("rtstreams", {}).items():
        started_iso = rt_state.get("started_at") or rt_state.get("created_at")
        if not started_iso:
            continue
        rt_id = rt_state.get("rtstream_id") or rt_state.get("id")
        live_status = live_status_by_id.get(str(rt_id), "")

        if live_status_available:
            if not rt_id or live_status not in _RT_LIVE_STATUSES:
                continue
            displayed_status = live_status
        else:
            displayed_status = "unknown_sdk_unreachable"
        started_ts = _parse_started_at(started_iso)
        if started_ts is None:
            logger.warning(
                "_estimate_credit_burn_usd: rtstream %s has unparseable started_at=%r",
                key,
                started_iso,
            )
            continue
        hours = max(0.0, (now - started_ts) / 3600.0)
        burn = hours * _RT_RATE_USD_PER_H
        rt_total += burn
        details.append(
            {
                "kind": "rtstream",
                "key": key,
                "rtstream_id": rt_id,
                "status": displayed_status,
                "hours": round(hours, 2),
                "rate_usd_per_h": _RT_RATE_USD_PER_H,
                "burn_usd": round(burn, 2),
            }
        )

    sb = state.get("sandbox")
    if sb and sb.get("created_at") and sb.get("id"):
        # Default to "assume active" on probe failure — masking real
        # burn is worse than showing an unverified row.
        sb_active = True
        sb_status = "unverified"
        try:
            conn = conn_getter()
            if hasattr(conn, "get_sandbox"):
                sb_obj = with_timeout(conn.get_sandbox, sb["id"], timeout_s=5.0)
                if sb_obj is not None:
                    status = str(
                        getattr(sb_obj, "status", None)
                        or (sb_obj.get("status") if isinstance(sb_obj, dict) else None)
                        or ""
                    ).lower()
                    sb_status = status or "unknown"
                    sb_active = status in ("active", "running", "ready")
                else:
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
            started_ts = _parse_started_at(sb["created_at"])
            if started_ts is not None:
                hours = max(0.0, (now - started_ts) / 3600.0)
                rate = (
                    _SB_RATE_MEDIUM
                    if str(sb.get("tier", "")).endswith("medium")
                    else _SB_RATE_SMALL
                )
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
            else:
                logger.warning("_estimate_credit_burn_usd: sandbox created_at unparseable: %r", sb)

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
