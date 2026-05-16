"""Tests for prompt loader + formatter."""

from __future__ import annotations

import pytest

from wildwatch.prompts import format_prompt, load_prompt

ALL_NAMES = ("species", "behavior", "environment", "audio")


@pytest.mark.parametrize("name", ALL_NAMES)
def test_load_prompt_returns_non_trivial_string(name: str) -> None:
    text = load_prompt(name)
    assert isinstance(text, str)
    assert len(text) > 100


def test_load_prompt_missing_raises_file_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_prompt("does_not_exist")


def test_format_species_substitutes_both_placeholders() -> None:
    out = format_prompt("species", location_context="Etosha", species_list="oryx, lion")
    assert "Etosha" in out
    assert "oryx, lion" in out
    assert "{location_context}" not in out
    assert "{species_list}" not in out


def test_format_behavior_substitutes_only_location() -> None:
    out = format_prompt("behavior", location_context="Mara")
    assert "Mara" in out
    assert "{location_context}" not in out


def test_format_environment_takes_no_kwargs() -> None:
    out = format_prompt("environment")
    # Static prompt — should be identical to raw load
    assert out == load_prompt("environment")


def test_format_audio_substitutes_both_placeholders() -> None:
    out = format_prompt(
        "audio",
        location_context="Kruger",
        expected_sounds="lion roars, hippo bellows",
    )
    assert "Kruger" in out
    assert "lion roars, hippo bellows" in out
    assert "{expected_sounds}" not in out


def test_format_missing_required_placeholder_raises_key_error() -> None:
    with pytest.raises(KeyError):
        format_prompt("species", location_context="X")  # species_list missing
