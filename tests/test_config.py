"""Schema tests for STREAMS registry and FALLBACK_RTSP constant."""

from __future__ import annotations

import pytest

from config import FALLBACK_RTSP, STREAMS

REQUIRED_KEYS = {
    "name",
    "rtsp_url",
    "youtube_url",
    "use_bridge",
    "location_context",
    "species_list",
    "expected_sounds",
}


def test_streams_is_non_empty_dict() -> None:
    assert isinstance(STREAMS, dict)
    assert len(STREAMS) >= 1


def test_stream_keys_are_unique_slugs() -> None:
    for key in STREAMS:
        assert isinstance(key, str)
        assert key
        assert key == key.lower().replace(" ", "_")


@pytest.mark.parametrize("stream_key", list(STREAMS.keys()))
def test_each_stream_has_required_keys(stream_key: str) -> None:
    stream = STREAMS[stream_key]
    missing = REQUIRED_KEYS - stream.keys()
    assert not missing, f"{stream_key} missing keys: {missing}"


@pytest.mark.parametrize("stream_key", list(STREAMS.keys()))
def test_each_stream_field_types(stream_key: str) -> None:
    stream = STREAMS[stream_key]
    assert isinstance(stream["name"], str) and stream["name"]
    assert stream["rtsp_url"] is None or isinstance(stream["rtsp_url"], str)
    assert isinstance(stream["youtube_url"], str)
    assert isinstance(stream["use_bridge"], bool)
    assert isinstance(stream["location_context"], str) and stream["location_context"]
    assert isinstance(stream["species_list"], str) and stream["species_list"]
    assert isinstance(stream["expected_sounds"], str) and stream["expected_sounds"]


def test_fallback_rtsp_is_rtsp_url() -> None:
    assert isinstance(FALLBACK_RTSP, str)
    assert FALLBACK_RTSP.startswith("rtsp://")
