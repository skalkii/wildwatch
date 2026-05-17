"""Streams registry + fallback URL.

Single source of truth for which RTSP/YouTube feeds we ingest and the
ecological context (species_list, expected_sounds) we inject into the
four index prompts.
"""

from __future__ import annotations

STREAMS: dict[str, dict[str, object]] = {
    "namibia_waterhole": {
        "name": "Namibia Waterhole (HDOnTap)",
        # bore.pub:13005 — current docker bridge session. Remote port
        # changes on every `docker compose down/up`; update here OR
        # override via WILDWATCH_RTSP_NAMIBIA env at bootstrap time.
        "rtsp_url": "rtsp://bore.pub:13005/namibia",
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
        "name": "Hwange Waterhole (Wilderness Linkwasha)",
        "rtsp_url": "rtsp://bore.pub:13005/hwange",
        # Old vr4o_AsrU1k went offline 2026-05-16; swapped to Africam's Hwange
        # Linkwasha 24/7 cam which is reliably live and matches the waterhole prompt
        # orientation. Zimbabwe reserve, still inside the rotating-reserves context.
        "youtube_url": "https://www.youtube.com/watch?v=-rXriX4SiQk",
        "use_bridge": True,
        "location_context": ("Hwange National Park, Zimbabwe — Wilderness Linkwasha waterhole cam"),
        "species_list": (
            "elephant, lion, leopard, buffalo, zebra, giraffe, wildebeest, "
            "impala, kudu, sable antelope, roan antelope, warthog, baboon, "
            "vervet monkey, wild dog, hyena, jackal, vultures, eagles, hornbills"
        ),
        "expected_sounds": (
            "elephant trumpets and rumbles, lion roars at dusk and dawn, "
            "baboon barks, kudu barks, impala alarm snorts, hyena whoops at night, "
            "various bird calls, hornbill cackles"
        ),
    },
}

# Guaranteed-working sample stream from VideoDB — used for smoke tests
# and as a demo fallback if upstream wildlife streams are flaky.
FALLBACK_RTSP = "rtsp://samples.rts.videodb.io:8554/intruder"
