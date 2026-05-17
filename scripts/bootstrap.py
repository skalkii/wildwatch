"""T-26 bootstrap: wire one rtstream with 4 indexes + 18 events + 18 alerts.

Path B: runs against FALLBACK_RTSP (VideoDB sample intruder cam). Wildlife
prompts won't organically match this content — the goal here is to prove
the wire-up shape end-to-end (events created, alerts wired with the right
callback URL, state persisted, idempotent on re-run).

Idempotency:
  - Events: lookup by label via conn.list_events() before create.
  - Alerts: re-create if state has no record; otherwise skip (we don't
    bother trying to detect existing alert objects on remote — cheap to
    over-create on a clean state file).
  - RTStream + indexes: created fresh each run unless --resume is passed
    AND state has matching ids. (For hackathon speed, default is fresh.)

Cost: ~$0.20 for a 5-min bootstrap + observation. Stops rtstream at exit.

Usage:
    python scripts/bootstrap.py [--observe SECS] [--no-stop]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from config import FALLBACK_RTSP  # noqa: E402
from wildwatch.events import EVENT_DEFINITIONS  # noqa: E402
from wildwatch.prompts import format_prompt  # noqa: E402
from wildwatch.wiring import wire_alerts  # noqa: E402

STATE_FILE = REPO_ROOT / ".state.json"

# B-path stream uses a synthetic context; FALLBACK is a security cam, not
# wildlife. Prompts will still be valid Python strings; just won't match.
FALLBACK_CONTEXT = {
    "location_context": "VideoDB sample intruder cam (test feed)",
    "species_list": "n/a (security cam, no wildlife expected)",
    "expected_sounds": "n/a (typically silent feed)",
}

# Relaxed batch_config -> per-handover cost analysis ~$5/h vs aggressive ~$30/h.
BATCH_CONFIG_VISUAL = {"type": "time", "value": 30, "frame_count": 1}
BATCH_CONFIG_AUDIO = {"type": "time", "value": 60}


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        print(f"WARN: {STATE_FILE} corrupt; starting fresh")
        return {}


def _save_state(state: dict) -> None:
    from wildwatch.state_io import atomic_write_json

    atomic_write_json(STATE_FILE, state)


def _event_attr(ev, name: str):
    """list_events may return dicts OR objects; read either shape."""
    if isinstance(ev, dict):
        return ev.get(name)
    return getattr(ev, name, None)


def _ensure_events(conn, state: dict) -> dict[str, str]:
    """Create all events idempotently. Returns id_var -> event_id mapping."""
    events_state: dict[str, str] = state.setdefault("events", {})
    existing = {_event_attr(ev, "label"): _event_attr(ev, "event_id") for ev in conn.list_events()}

    for ev_def in EVENT_DEFINITIONS:
        label = ev_def["label"]
        id_var = ev_def["id_var"]
        if existing.get(label):
            events_state[id_var] = existing[label]
            print(f"  reuse  event {label:35s} -> {existing[label]}")
        else:
            event_id = conn.create_event(
                event_prompt=ev_def["prompt"],
                label=label,
            )
            events_state[id_var] = event_id
            print(f"  create event {label:35s} -> {event_id}")
    return events_state


def _read_ws_connection_id() -> str | None:
    """Read ws_connection_id written by wildwatch/ws_listener.py.

    Per VideoDB skills' rtstream-reference.md, alerts and indexes should
    forward ``ws_connection_id`` so events also flow through the WebSocket
    channel (dual delivery: callback + ws). The listener writes the id to
    ``$VIDEODB_EVENTS_DIR/videodb_ws_id`` (default ``/tmp/videodb_ws_id``).
    """
    import os

    base = os.environ.get("VIDEODB_EVENTS_DIR", "/tmp")
    p = Path(base) / "videodb_ws_id"
    if not p.exists():
        return None
    try:
        return p.read_text().strip() or None
    except Exception:
        return None


def _bootstrap_stream(
    coll,
    stream_key: str,
    rtsp_url: str,
    prompt_ctx: dict,
    ws_connection_id: str | None = None,
) -> tuple:
    """Connect rtstream + create 4 indexes (3 visual + 1 audio).

    When ``ws_connection_id`` is provided, every index call attaches it so
    events flow through the WebSocket too — the dual-delivery pattern the
    skill prescribes.
    """
    print(f"\n[stream {stream_key}] connect_rtstream {rtsp_url}")
    rt = coll.connect_rtstream(
        url=rtsp_url,
        name=stream_key,
        media_types=["video", "audio"],
        store=True,
    )
    print(f"  rtstream.id = {rt.id}  status={rt.status}")

    # Optional speech-to-text transcript on the live stream. Skill ships this
    # as a one-liner — biophony alone won't yield English transcripts but
    # if the demo includes voiceover or any human speech in audio, this
    # captures it without an extra index call.
    if ws_connection_id:
        try:
            rt.start_transcript(ws_connection_id=ws_connection_id)
            print(f"  start_transcript ws={ws_connection_id[:10]}…")
        except Exception as e:
            print(f"  WARN start_transcript failed: {type(e).__name__}: {e}")

    def _ws_kwargs() -> dict:
        return {"ws_connection_id": ws_connection_id} if ws_connection_id else {}

    indexes: dict[str, object] = {}
    for kind in ("species", "behavior", "environment"):
        prompt = format_prompt(kind, **prompt_ctx)
        idx = rt.index_visuals(
            prompt=prompt,
            batch_config=BATCH_CONFIG_VISUAL,
            name=f"{stream_key}_{kind}",
            **_ws_kwargs(),
        )
        idx_id = getattr(idx, "rtstream_index_id", None) or getattr(idx, "id", "?")
        indexes[kind] = idx
        print(f"  index {kind:11s} -> {idx_id}")

    audio_prompt = format_prompt("audio", **prompt_ctx)
    audio_idx = rt.index_audio(
        prompt=audio_prompt,
        batch_config=BATCH_CONFIG_AUDIO,
        name=f"{stream_key}_audio",
        **_ws_kwargs(),
    )
    audio_idx_id = getattr(audio_idx, "rtstream_index_id", None) or getattr(audio_idx, "id", "?")
    indexes["audio"] = audio_idx
    print(f"  index audio       -> {audio_idx_id}")

    return rt, indexes


def _wire_alerts(
    rt,
    indexes: dict[str, object],
    events_map: dict[str, str],
    base_url: str,
    state: dict,
    stream_key: str,
    ws_connection_id: str | None = None,
) -> None:
    """Per INDEX_EVENT_MAP, create alert for each (kind, event) pair.

    Idempotency key includes ``rt.id`` so a fresh rtstream re-wires even
    when ``stream_key`` matches a prior run's cache.
    """
    print("\n[alerts] wiring (index, event) -> callback")
    alert_state = state.setdefault("alerts", {}).setdefault(stream_key, {})
    res = wire_alerts(
        rtstream_id=rt.id,
        indexes=indexes,
        events_map=events_map,
        base_url=base_url,
        alert_state=alert_state,
        ws_connection_id=ws_connection_id,
    )
    print(
        f"  alerts: created={res.created} reused={res.reused} "
        f"replaced={res.replaced} failed={res.failed} (rtstream={rt.id})"
    )
    for kind, ev_id_var, err in res.failures:
        print(f"    FAIL {kind}.{ev_id_var}: {err[:120]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--observe", type=int, default=60, help="seconds to keep stream alive")
    ap.add_argument("--no-stop", action="store_true", help="leave rtstream running after observe")
    ap.add_argument(
        "--ws",
        action="store_true",
        help="forward ws_connection_id from wildwatch/ws_listener.py to indexes + alerts",
    )
    args = ap.parse_args()

    load_dotenv()
    state = _load_state()
    base_url = state.get("webhook_base_url")
    if not base_url:
        sys.exit("ERROR: state.webhook_base_url unset; run T-25 first to set tunnel URL")
    # Refuse plain-HTTP webhook URLs — VideoDB will POST alert callbacks
    # cross-internet to whatever URL we register. http:// is sniffable +
    # tamperable. The two exceptions are localhost / 127.0.0.1 for dev.
    if not (
        base_url.startswith("https://")
        or base_url.startswith("http://localhost")
        or base_url.startswith("http://127.0.0.1")
    ):
        sys.exit(
            f"ERROR: webhook_base_url={base_url!r} must be https:// (or http://localhost). "
            "Plain HTTP exposes the alert payload to MITM."
        )
    print(f"webhook base = {base_url}\n")

    conn = videodb.connect()
    coll = conn.get_collection()

    ws_id: str | None = None
    if args.ws:
        ws_id = _read_ws_connection_id()
        if ws_id:
            print(f"[ws] using ws_connection_id = {ws_id[:10]}…")
        else:
            print(
                "[ws] no ws_connection_id found at "
                "$VIDEODB_EVENTS_DIR/videodb_ws_id — start wildwatch/ws_listener.py first"
            )

    # 1. Events — idempotent on label.
    print("[events] creating/reusing 18 events ...")
    events_map = _ensure_events(conn, state)
    _save_state(state)

    # 2. One stream (Path B = FALLBACK_RTSP intruder cam).
    # rt declared OUTSIDE the try so it's visible to finally even if
    # _bootstrap_stream raises midway (e.g. one index_visuals fails after
    # connect_rtstream succeeded -- without this guard we'd leak ingest).
    stream_key = "fallback_intruder"
    rt = None
    # Hoist `indexes` outside the try-block so a partial failure in the
    # comprehension at line ~272 (e.g. `get_scene_index` raises on the third
    # of four kinds) doesn't leave `indexes` unbound — which would have
    # raised UnboundLocalError downstream at line ~289 instead of falling
    # through to the fresh-provision path.
    indexes: dict[str, object] = {}
    try:
        # Re-run idempotency: if state has an active rtstream for this key,
        # reconnect instead of provisioning a new one (avoids leaking the
        # previous stream when the prior run crashed mid-wire).
        cached_rt = state.get("rtstreams", {}).get(stream_key, {}).get("id")
        if cached_rt:
            try:
                existing = coll.get_rtstream(cached_rt)
                if getattr(existing, "status", None) != "stopped":
                    print(f"[stream {stream_key}] reusing cached rtstream {cached_rt}")
                    # Build indexes dict explicitly so a mid-iteration raise
                    # leaves indexes={} + rt=None and we drop to fresh-provision.
                    rebuilt: dict[str, object] = {}
                    for kind, idx_id in state["rtstreams"][stream_key].get("indexes", {}).items():
                        rebuilt[kind] = existing.get_scene_index(idx_id)
                    rt = existing
                    indexes = rebuilt
            except Exception as e:
                print(f"[stream {stream_key}] cached rtstream gone ({e}); will provision new")
                rt = None  # ensure we fall through
                indexes = {}
        if rt is None:
            rt, indexes = _bootstrap_stream(
                coll, stream_key, FALLBACK_RTSP, FALLBACK_CONTEXT, ws_connection_id=ws_id
            )
        state.setdefault("rtstreams", {})[stream_key] = {
            "id": rt.id,
            "url": FALLBACK_RTSP,
            "indexes": {
                kind: getattr(idx, "rtstream_index_id", None) or getattr(idx, "id", "?")
                for kind, idx in indexes.items()
            },
            "started_at": datetime.now(UTC).isoformat(),
        }
        _save_state(state)

        # 3. Wire 18 alerts (one per INDEX_EVENT_MAP entry).
        _wire_alerts(rt, indexes, events_map, base_url, state, stream_key, ws_connection_id=ws_id)
        _save_state(state)

        if args.observe > 0:
            print(f"\nobserving {args.observe}s for any alerts to fire ...")
            time.sleep(args.observe)
    finally:
        if rt is not None and not args.no_stop:
            try:
                rt.stop()
                try:
                    rt.refresh()
                except Exception:
                    pass
                print(f"\nrtstream stopped  status={rt.status}")
            except Exception as e:
                print(f"\nWARN stop failed: {type(e).__name__}: {e}")

    print("\nbootstrap complete.")
    print(f"  events in state:  {len(state.get('events', {}))}")
    print(f"  alerts in state:  {sum(len(v) for v in state.get('alerts', {}).values())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
