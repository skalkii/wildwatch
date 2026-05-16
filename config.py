"""Streams registry + fallback URL.

Single source of truth for which RTSP/YouTube feeds we ingest and the
ecological context (species_list, expected_sounds) we inject into the
four index prompts.
"""

from __future__ import annotations

STREAMS: dict[str, dict[str, object]] = {
    "namibia_waterhole": {
        "name": "Namibia Waterhole (HDOnTap)",
        # Fill rtsp_url from HDOnTap embed page when available;
        # otherwise the YouTube URL is bridged via mediamtx.
        "rtsp_url": None,
        "youtube_url": "https://www.youtube.com/watch?v=AeMUdOPFcXI",
        "use_bridge": True,
        "location_context": "Namib Desert waterhole, Gondwana Namibia Park",
        "species_list": (
            "oryx, springbok, gemsbok, zebra, kudu, elephant, giraffe, "
            "black-backed jackal, brown hyena, leopard, lion, "
            "various birds (ostrich, vulture, francolin)"
        ),
        "expected_sounds": (
            "wind, drinking splashes, hooves, oryx snorts, jackal calls, "
            "bird songs, occasional lion roars, hyena whoops at night"
        ),
    },
    "wild_africa_live": {
        "name": "Wild Africa Live (multi-cam)",
        "rtsp_url": None,
        "youtube_url": "https://www.youtube.com/watch?v=vr4o_AsrU1k",
        "use_bridge": True,
        "location_context": (
            "Rotating African reserves: South Africa, Kenya, Botswana, Namibia, Zimbabwe"
        ),
        "species_list": (
            "Big Five (lion, leopard, elephant, rhino, buffalo), "
            "zebra, giraffe, wildebeest, impala, kudu, warthog, "
            "hippo, crocodile, hyena, wild dog, baboon, vervet monkey, "
            "vultures, eagles, hornbills, fish eagles"
        ),
        "expected_sounds": (
            "fish eagle calls, baboon barks, lion roars, "
            "elephant trumpets, hippo bellows, various antelope alarm snorts"
        ),
    },
}

# Guaranteed-working sample stream from VideoDB — used for smoke tests
# and as a demo fallback if upstream wildlife streams are flaky.
FALLBACK_RTSP = "rtsp://samples.rts.videodb.io:8554/intruder"
