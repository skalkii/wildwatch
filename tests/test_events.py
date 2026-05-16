"""Schema tests for EVENT_DEFINITIONS + INDEX_EVENT_MAP."""

from __future__ import annotations

import pytest

from wildwatch.events import EVENT_DEFINITIONS, INDEX_EVENT_MAP

REQUIRED_EVENT_KEYS = {"id_var", "label", "tier", "prompt"}
VALID_INDEX_KINDS = {"species", "behavior", "environment", "audio"}


def test_events_list_non_empty() -> None:
    assert isinstance(EVENT_DEFINITIONS, list)
    assert len(EVENT_DEFINITIONS) > 0


@pytest.mark.parametrize("ev", EVENT_DEFINITIONS, ids=lambda e: e["id_var"])
def test_event_has_required_keys(ev: dict) -> None:
    missing = REQUIRED_EVENT_KEYS - ev.keys()
    assert not missing, f"missing: {missing}"


@pytest.mark.parametrize("ev", EVENT_DEFINITIONS, ids=lambda e: e["id_var"])
def test_event_tier_in_range(ev: dict) -> None:
    assert ev["tier"] in (1, 2, 3)


def test_event_id_vars_unique() -> None:
    ids = [ev["id_var"] for ev in EVENT_DEFINITIONS]
    assert len(ids) == len(set(ids)), f"duplicates: {[i for i in ids if ids.count(i) > 1]}"


def test_event_labels_unique() -> None:
    labels = [ev["label"] for ev in EVENT_DEFINITIONS]
    assert len(labels) == len(set(labels))


def test_event_prompts_non_trivial() -> None:
    for ev in EVENT_DEFINITIONS:
        assert isinstance(ev["prompt"], str)
        assert len(ev["prompt"]) > 20


def test_index_event_map_keys_are_valid_index_kinds() -> None:
    assert set(INDEX_EVENT_MAP.keys()) == VALID_INDEX_KINDS


def test_every_id_var_in_map_exists_in_definitions() -> None:
    known_ids = {ev["id_var"] for ev in EVENT_DEFINITIONS}
    for kind, id_list in INDEX_EVENT_MAP.items():
        for id_var in id_list:
            assert id_var in known_ids, f"{kind} references unknown id_var: {id_var}"


def test_every_event_definition_is_wired_to_at_least_one_index() -> None:
    wired = {id_var for ids in INDEX_EVENT_MAP.values() for id_var in ids}
    defined = {ev["id_var"] for ev in EVENT_DEFINITIONS}
    orphans = defined - wired
    assert not orphans, f"events defined but not wired: {orphans}"


def test_wireup_counts_match_documented() -> None:
    # Documents the current invariant: 18 event defs, 18 wire-up slots
    # (each event wired exactly once to one index kind).
    assert len(EVENT_DEFINITIONS) == 18
    total_wireups = sum(len(v) for v in INDEX_EVENT_MAP.values())
    assert total_wireups == 18
