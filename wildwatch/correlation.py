"""Cross-modal correlation engine — the perception agent's reasoning layer.

The Tier 2/3 "this isn't just one signal, it's a confirmed pattern" alerts
come from here. Each rule says "if I see X in index A AND Y in index B
within W seconds, fire a synthesised event."

Searches happen via the SDK's ``rt.search(query, index_id=...)`` — that call
has NO ``time_range`` kwarg (verified live in T-13). So we filter shots
client-side against ``rule.window_seconds`` relative to ``now``.

Rules are pure data; ``evaluate_rule`` is a pure function that takes a
``search_fn`` callable so unit tests can mock the SDK entirely.

CORRELATION_RULES verbatim from CLAUDE.md sec 10.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


CORRELATION_RULES: list[dict] = [
    {
        "name": "confirmed_predator_event",
        "tier": 3,
        "window_seconds": 90,
        "queries": [
            ("audio", "alarm_call OR predator_vocalization OR predator vocal"),
            ("behavior", "fleeing OR alarm_response OR frozen OR threat_display"),
        ],
        "synthesis_label": "CONFIRMED_PREDATOR_EVENT",
    },
    {
        "name": "confirmed_human_intrusion",
        "tier": 3,
        "window_seconds": 120,
        "queries": [
            ("audio", "vehicle_engine OR voices_human OR gunshot OR chainsaw"),
            ("environment", "human_made_object_visible"),
        ],
        "synthesis_label": "CONFIRMED_HUMAN_INTRUSION",
    },
    {
        "name": "stress_then_silence",
        "tier": 2,
        "window_seconds": 180,
        "queries": [
            ("behavior", "alarm_response OR alert_posture"),
            ("audio", "ABNORMAL_SILENCE"),
        ],
        "synthesis_label": "PREDATOR_APPROACH_PATTERN",
    },
]


SEARCH_ERROR = object()  # sentinel: SDK error during search, distinct from no-match


@dataclass
class CorrelationHit:
    rule_name: str
    synthesis_label: str
    tier: int
    # evidence keyed by (index_kind, query) tuple so rules with multiple
    # queries on the same index_kind don't overwrite each other's shots.
    evidence: dict[tuple[str, str], list[dict]]
    fired_at: float


@dataclass
class CorrelationState:
    """Tracks per-rule last-fire time so we don't spam the same correlation."""

    last_fired: dict[str, float] = field(default_factory=dict)

    def should_fire(self, rule_name: str, now_ts: float, cooldown: float = 300.0) -> bool:
        last = self.last_fired.get(rule_name)
        if last is None:
            return True
        return (now_ts - last) > cooldown

    def mark_fired(self, rule_name: str, at: float) -> None:
        self.last_fired[rule_name] = at


def shots_within_window(
    shots: list[dict],
    window_start_ts: float,
    now_ts: float,
) -> list[dict]:
    """Return shots whose ``start`` timestamp falls inside [window_start_ts, now_ts]."""
    out = []
    for s in shots:
        start = s.get("start") if isinstance(s, dict) else getattr(s, "start", None)
        if start is None:
            continue
        if window_start_ts <= start <= now_ts:
            out.append(s)
    return out


SearchFn = Callable[[str, str], list[dict]]


def evaluate_rule(
    rule: dict,
    search_fn: SearchFn,
    now_ts: float,
) -> CorrelationHit | None:
    """Evaluate one rule against the current search state.

    ``search_fn(index_kind, query) -> list[shot-dict]`` is the swap point;
    tests pass a mock, production passes a closure over ``rtstream.search``
    that resolves ``index_kind`` to ``index_id`` from ``.state.json``.

    Returns a ``CorrelationHit`` only when EVERY query in the rule yields
    at least one shot whose start is in the rule's time window. All-or-
    nothing keeps the synthesised events precision-over-recall.
    """
    window_start = now_ts - rule["window_seconds"]
    evidence: dict[tuple[str, str], list[dict]] = {}
    for index_kind, query in rule["queries"]:
        try:
            shots = search_fn(index_kind, query) or []
        except Exception as e:
            # Network/SDK error — don't fire on partial truth.
            logger.warning(
                "search failed for rule=%s index=%s query=%r: %s",
                rule["name"],
                index_kind,
                query,
                e,
            )
            return SEARCH_ERROR  # type: ignore[return-value]
        in_window = shots_within_window(shots, window_start, now_ts)
        if not in_window:
            return None
        evidence[(index_kind, query)] = in_window
    return CorrelationHit(
        rule_name=rule["name"],
        synthesis_label=rule["synthesis_label"],
        tier=rule["tier"],
        evidence=evidence,
        fired_at=now_ts,
    )
