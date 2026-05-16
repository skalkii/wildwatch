"""1-hour live pressure test orchestrator.

For each of 3 live wildlife streams (mara, hwange, amboseli):
- coll.connect_rtstream pointed at the bore-public RTSP URL
- 4 indexes (species, behavior, environment visual + audio)
  with RELAXED batch_config (cost containment)
- 18 alerts wired per INDEX_EVENT_MAP via shared event ids

Events created idempotently (reused from prior bootstrap).
All state persisted to .state.json under rtstreams.<stream_key>.

Usage:
    BORE_PUBLIC=bore.pub:25224 python scripts/start_live_test.py \\
        --duration 3600
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from wildwatch.events import EVENT_DEFINITIONS  # noqa: E402
from wildwatch.prompts import format_prompt  # noqa: E402
from wildwatch.wiring import wire_alerts as _wire_alerts  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"

# Per-stream profile inlined (vs config.py) so the bore URL changes per run
# without polluting committed config.
LIVE_STREAMS = {
    "mara_live": {
        "name": "Mara River Crossing (Africam)",
        "path": "mara",
        "location_context": (
            "Mara River, Masai Mara National Reserve, Kenya — wildebeest migration "
            "crossing point, May peak season"
        ),
        "species_list": (
            "wildebeest (hundreds at peak), zebra, Nile crocodile, hippopotamus, "
            "lion, leopard, cheetah, hyena, jackal, vultures, marabou stork, "
            "fish eagle, plovers"
        ),
        "expected_sounds": (
            "wildebeest grunts and stampede thunder, water splashes during crossings, "
            "lion roars, hyena whoops, vulture squabbles at carcasses"
        ),
    },
    "hwange_live": {
        "name": "Hwange Waterhole (Wilderness Linkwasha)",
        "path": "hwange",
        "location_context": ("Hwange National Park, Zimbabwe — Wilderness Linkwasha waterhole cam"),
        "species_list": (
            "elephant, lion, leopard, buffalo, zebra, giraffe, wildebeest, "
            "impala, kudu, sable antelope, roan antelope, warthog, baboon, "
            "vervet monkey, wild dog, hyena, jackal, vultures, eagles, hornbills"
        ),
        "expected_sounds": (
            "elephant trumpets and rumbles, lion roars at dusk and dawn, "
            "baboon barks, kudu barks, impala alarm snorts, hyena whoops at night"
        ),
    },
    "amboseli_live": {
        "name": "Amboseli Waterhole (Tortilis Camp)",
        "path": "amboseli",
        "location_context": (
            "Amboseli National Park, Kenya — waterhole with Mount Kilimanjaro backdrop"
        ),
        "species_list": (
            "African elephant (large herds with calves), Maasai giraffe, "
            "Burchell's zebra, wildebeest, Thomson's gazelle, Grant's gazelle, "
            "impala, warthog, lion (occasional), spotted hyena, jackal, "
            "secretary bird, vultures"
        ),
        "expected_sounds": (
            "elephant rumbles and trumpets, zebra alarm calls, giraffe hoof beats, "
            "lion roars at dusk/dawn, hyena whoops at night, various birds"
        ),
    },
}

# Cost-containment: 30s visual batches x 1 frame, 60s audio
BATCH_CONFIG_VISUAL = {"type": "time", "value": 30, "frame_count": 1}
BATCH_CONFIG_AUDIO = {"type": "time", "value": 60}


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2))
        tmp.replace(STATE_FILE)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _event_attr(ev, name):
    if isinstance(ev, dict):
        return ev.get(name)
    return getattr(ev, name, None)


def ensure_events(conn, state: dict) -> dict:
    events_state = state.setdefault("events", {})
    existing = {_event_attr(ev, "label"): _event_attr(ev, "event_id") for ev in conn.list_events()}
    for ev_def in EVENT_DEFINITIONS:
        if existing.get(ev_def["label"]):
            events_state[ev_def["id_var"]] = existing[ev_def["label"]]
        else:
            events_state[ev_def["id_var"]] = conn.create_event(
                event_prompt=ev_def["prompt"], label=ev_def["label"]
            )
    return events_state


def bootstrap_stream(coll, stream_key, cfg, rtsp_url):
    print(f"[{stream_key}] connect_rtstream {rtsp_url}")
    rt = coll.connect_rtstream(
        url=rtsp_url, name=stream_key, media_types=["video", "audio"], store=True
    )
    print(f"  rt.id={rt.id} status={rt.status}")

    prompt_ctx = {
        "location_context": cfg["location_context"],
        "species_list": cfg["species_list"],
        "expected_sounds": cfg["expected_sounds"],
    }
    indexes = {}
    for kind in ("species", "behavior", "environment"):
        idx = rt.index_visuals(
            prompt=format_prompt(kind, **prompt_ctx),
            batch_config=BATCH_CONFIG_VISUAL,
            name=f"{stream_key}_{kind}",
        )
        indexes[kind] = idx
        print(f"  index {kind:11s} -> {getattr(idx, 'rtstream_index_id', '?')}")
    audio_idx = rt.index_audio(
        prompt=format_prompt("audio", **prompt_ctx),
        batch_config=BATCH_CONFIG_AUDIO,
        name=f"{stream_key}_audio",
    )
    indexes["audio"] = audio_idx
    print(f"  index audio       -> {getattr(audio_idx, 'rtstream_index_id', '?')}")
    return rt, indexes


def wire_alerts(rt, indexes, events_map, base_url, state, stream_key):
    """Wire alerts via the shared helper; rt.id keys invalidation."""
    alert_state = state.setdefault("alerts", {}).setdefault(stream_key, {})
    res = _wire_alerts(
        rtstream_id=rt.id,
        indexes=indexes,
        events_map=events_map,
        base_url=base_url,
        alert_state=alert_state,
    )
    print(
        f"  alerts: created={res.created} reused={res.reused} "
        f"replaced={res.replaced} (rtstream={rt.id})"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=3600, help="seconds to keep streams running")
    ap.add_argument("--no-stop", action="store_true", help="leave running on exit")
    args = ap.parse_args()

    load_dotenv()
    state = _load_state()
    base_url = state.get("webhook_base_url")
    if not base_url:
        sys.exit("ERROR: webhook_base_url unset")

    bore_pub = os.environ.get("BORE_PUBLIC")
    if not bore_pub:
        sys.exit("ERROR: BORE_PUBLIC env unset (e.g. bore.pub:25224)")
    print(f"webhook = {base_url}")
    print(f"bore    = {bore_pub}")
    print()

    conn = videodb.connect()
    coll = conn.get_collection()

    print("[events] ensuring 18 events ...")
    events_map = ensure_events(conn, state)
    _save_state(state)
    print(f"  events ready: {len(events_map)}\n")

    rtstreams = []
    for stream_key, cfg in LIVE_STREAMS.items():
        rtsp_url = f"rtsp://{bore_pub}/{cfg['path']}"
        try:
            rt, indexes = bootstrap_stream(coll, stream_key, cfg, rtsp_url)
            state.setdefault("rtstreams", {})[stream_key] = {
                "id": rt.id,
                "url": rtsp_url,
                "indexes": {
                    k: getattr(v, "rtstream_index_id", None) or getattr(v, "id", "?")
                    for k, v in indexes.items()
                },
                "started_at": datetime.now(UTC).isoformat(),
            }
            _save_state(state)
            wire_alerts(rt, indexes, events_map, base_url, state, stream_key)
            _save_state(state)
            rtstreams.append(rt)
            print()
        except Exception as e:
            print(f"  FAILED to bootstrap {stream_key}: {type(e).__name__}: {e}\n")

    print(f"\n=== {len(rtstreams)} streams running ===")
    print(f"   observing for {args.duration}s ({args.duration / 60:.1f} min) ...")
    print("   dashboard: http://localhost:8000/")
    print("   Ctrl+C to stop early\n")

    try:
        slept = 0
        while slept < args.duration:
            time.sleep(min(60, args.duration - slept))
            slept += 60
            print(f"   ... t+{slept}s")
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        if not args.no_stop:
            print("\nstopping rtstreams ...")
            for rt in rtstreams:
                try:
                    rt.stop()
                    print(f"  stopped {rt.id}")
                except Exception as e:
                    print(f"  WARN stop {rt.id} failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
