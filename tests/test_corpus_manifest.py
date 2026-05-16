"""Schema + coverage tests for samples/triggers/manifest.json."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from config import STREAMS
from wildwatch.events import EVENT_DEFINITIONS

MANIFEST_PATH = Path(__file__).resolve().parent.parent / "samples" / "triggers" / "manifest.json"

REQUIRED_CLIP_KEYS = {
    "slug",
    "source",
    "source_search_query",
    "duration_s",
    "events_expected",
    "license",
}
VALID_SOURCES = {"youtube", "fsd50k", "synthesized"}
SLUG_RE = re.compile(r"^[a-z][a-z0-9_]+$")


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text())


def test_manifest_has_version_and_clips(manifest: dict) -> None:
    assert manifest.get("version") == 1
    assert isinstance(manifest.get("clips"), list)
    assert len(manifest["clips"]) >= 1


@pytest.mark.parametrize("clip_idx", range(10))
def test_each_clip_has_required_keys(manifest: dict, clip_idx: int) -> None:
    if clip_idx >= len(manifest["clips"]):
        pytest.skip("fewer than 10 clips")
    clip = manifest["clips"][clip_idx]
    missing = REQUIRED_CLIP_KEYS - clip.keys()
    assert not missing, f"clip[{clip_idx}] missing {missing}"


def test_clip_slugs_unique_and_snake_case(manifest: dict) -> None:
    slugs = [c["slug"] for c in manifest["clips"]]
    assert len(slugs) == len(set(slugs)), "duplicate slugs"
    bad = [s for s in slugs if not SLUG_RE.match(s)]
    assert not bad, f"non-snake_case slugs: {bad}"


def test_each_clip_source_is_valid(manifest: dict) -> None:
    for clip in manifest["clips"]:
        assert clip["source"] in VALID_SOURCES, f"{clip['slug']}: bad source {clip['source']!r}"


def test_each_clip_duration_positive_int(manifest: dict) -> None:
    for clip in manifest["clips"]:
        d = clip["duration_s"]
        assert isinstance(d, int)
        assert d > 0, f"{clip['slug']}: duration_s must be > 0"


def test_events_expected_reference_real_event_ids(manifest: dict) -> None:
    known_ids = {ev["id_var"] for ev in EVENT_DEFINITIONS}
    for clip in manifest["clips"]:
        for ev_id in clip["events_expected"]:
            assert ev_id in known_ids, f"{clip['slug']}: unknown event id_var {ev_id!r}"


def test_stream_context_when_set_is_valid_stream_key(manifest: dict) -> None:
    for clip in manifest["clips"]:
        ctx = clip.get("stream_context")
        if ctx is not None:
            assert ctx in STREAMS, f"{clip['slug']}: unknown stream_context {ctx!r}"


def test_coverage_every_event_appears_in_at_least_one_clip(manifest: dict) -> None:
    covered: set[str] = set()
    for clip in manifest["clips"]:
        covered.update(clip["events_expected"])
    defined = {ev["id_var"] for ev in EVENT_DEFINITIONS}
    missing = defined - covered
    assert not missing, f"events with no clip in manifest: {sorted(missing)}"


def test_synthesized_clips_have_overlay_or_documented_recipe(manifest: dict) -> None:
    # synthesized clips that wrap an audio overlay must record the mix_filter;
    # purely video-synthesis clips (e.g. camera_failure_synth) may have
    # audio_overlay=null but should document recipe in notes/search_query.
    for clip in manifest["clips"]:
        if clip["source"] != "synthesized":
            continue
        overlay = clip.get("audio_overlay")
        if overlay is not None:
            assert "mix_filter" in overlay, f"{clip['slug']}: missing audio_overlay.mix_filter"
        else:
            # Document recipe via notes or search query
            recipe = (clip.get("notes") or "") + (clip.get("source_search_query") or "")
            assert "ffmpeg" in recipe.lower(), (
                f"{clip['slug']}: synthesized clip lacks ffmpeg recipe in notes/query"
            )
