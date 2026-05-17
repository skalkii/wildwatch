"""Tests for cross-modal correlation rule evaluator (pure logic)."""

from __future__ import annotations

from wildwatch.correlation import (
    CORRELATION_RULES,
    CorrelationHit,
    CorrelationState,
    evaluate_rule,
    shots_within_window,
)


def _shot(start: float, end: float, text: str = "") -> dict:
    return {"start": start, "end": end, "text": text, "score": 1.0}


def test_correlation_rules_are_non_empty_and_well_formed() -> None:
    assert isinstance(CORRELATION_RULES, list)
    assert len(CORRELATION_RULES) >= 1
    for rule in CORRELATION_RULES:
        for field in ("name", "tier", "window_seconds", "queries", "synthesis_label"):
            assert field in rule, f"{rule.get('name', '?')} missing {field}"
        assert rule["tier"] in (1, 2, 3)
        assert rule["window_seconds"] > 0
        assert len(rule["queries"]) >= 2, "correlation needs >=2 queries to cross modes"
        for kind, query in rule["queries"]:
            assert kind in ("species", "behavior", "environment", "audio")
            assert isinstance(query, str) and query


def test_shots_within_window_filters_correctly() -> None:
    shots = [
        _shot(1000.0, 1005.0),
        _shot(1050.0, 1055.0),
        _shot(1100.0, 1105.0),
    ]
    # Window [1040, 1080] — only the middle one
    out = shots_within_window(shots, window_start_ts=1040, now_ts=1080)
    assert len(out) == 1
    assert out[0]["start"] == 1050.0


def test_shots_within_window_empty() -> None:
    assert shots_within_window([], 1000, 2000) == []


def test_evaluate_rule_fires_when_all_queries_hit_in_window() -> None:
    rule = {
        "name": "test_rule",
        "tier": 3,
        "window_seconds": 60,
        "queries": [
            ("audio", "alarm_call"),
            ("behavior", "fleeing"),
        ],
        "synthesis_label": "TEST_CONFIRMED",
    }
    now = 2000.0
    # Both queries return shots within last 60s
    search_results = {
        ("audio", "alarm_call"): [_shot(1980, 1985)],
        ("behavior", "fleeing"): [_shot(1990, 1995)],
    }
    hit = evaluate_rule(rule, lambda kind, q: search_results[(kind, q)], now)
    assert isinstance(hit, CorrelationHit)
    assert hit.rule_name == "test_rule"
    assert hit.synthesis_label == "TEST_CONFIRMED"
    assert hit.tier == 3
    # evidence keyed by (kind, query) tuple
    assert set(hit.evidence.keys()) == {("audio", "alarm_call"), ("behavior", "fleeing")}


def test_evaluate_rule_no_fire_when_one_query_empty() -> None:
    rule = {
        "name": "t",
        "tier": 2,
        "window_seconds": 60,
        "queries": [("audio", "x"), ("behavior", "y")],
        "synthesis_label": "T",
    }
    results = {
        ("audio", "x"): [_shot(1980, 1985)],
        ("behavior", "y"): [],
    }
    assert evaluate_rule(rule, lambda k, q: results[(k, q)], 2000.0) is None


def test_evaluate_rule_no_fire_when_hit_outside_window() -> None:
    rule = {
        "name": "t",
        "tier": 2,
        "window_seconds": 60,
        "queries": [("audio", "x"), ("behavior", "y")],
        "synthesis_label": "T",
    }
    # audio shot is 200s old, way outside the 60s window
    results = {
        ("audio", "x"): [_shot(1800, 1805)],
        ("behavior", "y"): [_shot(1990, 1995)],
    }
    assert evaluate_rule(rule, lambda k, q: results[(k, q)], 2000.0) is None


def test_correlation_state_enforces_cooldown() -> None:
    s = CorrelationState()
    assert s.should_fire("rule_a", now_ts=1000.0, cooldown=300) is True
    s.mark_fired("rule_a", at=1000.0)
    # Inside cooldown -> no
    assert s.should_fire("rule_a", now_ts=1100.0, cooldown=300) is False
    # After cooldown -> yes
    assert s.should_fire("rule_a", now_ts=1400.0, cooldown=300) is True


def test_correlation_state_cooldown_per_rule_independent() -> None:
    s = CorrelationState()
    s.mark_fired("rule_a", at=1000.0)
    # rule_b never fired -> ok
    assert s.should_fire("rule_b", now_ts=1100.0, cooldown=300) is True


def test_evaluate_rule_returns_none_on_search_exception() -> None:
    """SDK errors collapse to None — same outcome as no-match, just with a
    WARNING log so ops can tell a quiet stream from a broken SDK."""
    rule = {
        "name": "t",
        "tier": 2,
        "window_seconds": 60,
        "queries": [("audio", "x"), ("behavior", "y")],
        "synthesis_label": "T",
    }

    def boom(kind: str, query: str) -> list[dict]:
        raise RuntimeError("SDK exploded")

    result = evaluate_rule(rule, boom, 2000.0)
    assert result is None
