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
from wildwatch.events import EVENT_DEFINITIONS, INDEX_EVENT_MAP  # noqa: E402
from wildwatch.prompts import format_prompt  # noqa: E402

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
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2))
        tmp.replace(STATE_FILE)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


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


def _bootstrap_stream(coll, stream_key: str, rtsp_url: str, prompt_ctx: dict) -> tuple:
    """Connect rtstream + create 4 indexes (3 visual + 1 audio)."""
    print(f"\n[stream {stream_key}] connect_rtstream {rtsp_url}")
    rt = coll.connect_rtstream(
        url=rtsp_url,
        name=stream_key,
        media_types=["video", "audio"],
        store=True,
    )
    print(f"  rtstream.id = {rt.id}  status={rt.status}")

    indexes: dict[str, object] = {}
    for kind in ("species", "behavior", "environment"):
        prompt = format_prompt(kind, **prompt_ctx)
        idx = rt.index_visuals(
            prompt=prompt,
            batch_config=BATCH_CONFIG_VISUAL,
            name=f"{stream_key}_{kind}",
        )
        idx_id = getattr(idx, "rtstream_index_id", None) or getattr(idx, "id", "?")
        indexes[kind] = idx
        print(f"  index {kind:11s} -> {idx_id}")

    audio_prompt = format_prompt("audio", **prompt_ctx)
    audio_idx = rt.index_audio(
        prompt=audio_prompt,
        batch_config=BATCH_CONFIG_AUDIO,
        name=f"{stream_key}_audio",
    )
    audio_idx_id = getattr(audio_idx, "rtstream_index_id", None) or getattr(audio_idx, "id", "?")
    indexes["audio"] = audio_idx
    print(f"  index audio       -> {audio_idx_id}")

    return rt, indexes


def _wire_alerts(
    indexes: dict[str, object],
    events_map: dict[str, str],
    base_url: str,
    state: dict,
    stream_key: str,
) -> None:
    """Per INDEX_EVENT_MAP, create alert for each (kind, event) pair."""
    print("\n[alerts] wiring (index, event) -> callback")
    alert_state = state.setdefault("alerts", {}).setdefault(stream_key, {})
    tier_by_id = {ev["id_var"]: ev["tier"] for ev in EVENT_DEFINITIONS}
    label_by_id = {ev["id_var"]: ev["label"] for ev in EVENT_DEFINITIONS}

    n_created = 0
    n_existing = 0
    for kind, event_id_vars in INDEX_EVENT_MAP.items():
        idx = indexes[kind]
        for ev_id_var in event_id_vars:
            event_id = events_map[ev_id_var]
            tier = tier_by_id[ev_id_var]
            cb = f"{base_url}/webhook/{tier}"
            key = f"{kind}.{ev_id_var}"
            if alert_state.get(key):
                n_existing += 1
                continue
            alert_id = idx.create_alert(event_id, callback_url=cb)
            alert_state[key] = {
                "alert_id": alert_id,
                "event_id": event_id,
                "label": label_by_id[ev_id_var],
                "tier": tier,
                "callback_url": cb,
            }
            n_created += 1
            print(f"  wire {kind:11s}/{ev_id_var:25s} tier={tier} -> {alert_id}")
    print(f"\n  alerts: created={n_created} pre-existing={n_existing}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--observe", type=int, default=60, help="seconds to keep stream alive")
    ap.add_argument("--no-stop", action="store_true", help="leave rtstream running after observe")
    args = ap.parse_args()

    load_dotenv()
    state = _load_state()
    base_url = state.get("webhook_base_url")
    if not base_url:
        sys.exit("ERROR: state.webhook_base_url unset; run T-25 first to set tunnel URL")
    print(f"webhook base = {base_url}\n")

    conn = videodb.connect()
    coll = conn.get_collection()

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
                    rt, indexes = (
                        existing,
                        {
                            kind: existing.get_scene_index(idx_id)
                            for kind, idx_id in state["rtstreams"][stream_key]
                            .get("indexes", {})
                            .items()
                        },
                    )
            except Exception as e:
                print(f"[stream {stream_key}] cached rtstream gone ({e}); will provision new")
        if rt is None:
            rt, indexes = _bootstrap_stream(coll, stream_key, FALLBACK_RTSP, FALLBACK_CONTEXT)
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
        _wire_alerts(indexes, events_map, base_url, state, stream_key)
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
