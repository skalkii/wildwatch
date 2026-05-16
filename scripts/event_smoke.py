"""T-21 RTStream event + alert smoke.

Wires:
  rtstream -> visual index -> event -> alert -> webhook.site

Stays connected for OBSERVE_SECONDS so the alert can fire at least once
on the intruder sample (which shows a person walking around a yard).

Cost: ~$0.20-0.40 for ingest + index over ~3-5 min.
"""

from __future__ import annotations

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

STATE_FILE = REPO_ROOT / ".state.json"

WEBHOOK_URL = "https://webhook.site/a9c8dc08-07ec-4a92-881f-5bd72ed240e8"
EVENT_PROMPT = "Detect if a person is visible in the scene at any point during this video chunk."
EVENT_LABEL = "t21_person_present"
SIMPLE_INDEX_PROMPT = "Describe what is visible in this video chunk in 1-2 sentences."
BATCH_CONFIG = {"type": "time", "value": 5, "frame_count": 2}
OBSERVE_SECONDS = 240  # 4 min — gives a few batches a chance to fire the event


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


def main() -> int:
    load_dotenv()
    conn = videodb.connect()
    coll = conn.get_collection()
    print(f"collection.id = {coll.id}\n")

    rt = None
    try:
        print("connecting rtstream ...")
        rt = coll.connect_rtstream(
            url=FALLBACK_RTSP,
            name="smoke_event",
            media_types=["video"],
            store=True,
        )
        print(f"  rtstream.id = {rt.id}  status={rt.status}\n")

        print("creating visual index ...")
        idx = rt.index_visuals(
            prompt=SIMPLE_INDEX_PROMPT,
            batch_config=BATCH_CONFIG,
            name="smoke_event_visual",
        )
        idx_id = getattr(idx, "rtstream_index_id", None) or getattr(idx, "id", None)
        print(f"  index_id = {idx_id}\n")

        print("creating event ...")
        event_id = conn.create_event(
            event_prompt=EVENT_PROMPT,
            label=EVENT_LABEL,
        )
        print(f"  event_id = {event_id}\n")

        print(f"creating alert with callback_url={WEBHOOK_URL}")
        alert_id = idx.create_alert(event_id, callback_url=WEBHOOK_URL)
        print(f"  alert_id = {alert_id}\n")

        # Persist for cleanup + debugging
        state = _load_state()
        state.setdefault("rtstreams", {})["smoke_event"] = {
            "id": rt.id,
            "index_id": idx_id,
            "event_id": event_id,
            "alert_id": alert_id,
            "webhook_url": WEBHOOK_URL,
            "created_at": datetime.now(UTC).isoformat(),
        }
        _save_state(state)

        print(f"observing {OBSERVE_SECONDS}s for alerts to fire ...")
        print(f"  open {WEBHOOK_URL} in browser to watch incoming POSTs")
        time.sleep(OBSERVE_SECONDS)

        print("\nlisting alerts on the index (for sanity) ...")
        try:
            alerts = idx.list_alerts() if hasattr(idx, "list_alerts") else []
            for a in alerts:
                print(f"  alert {a}")
        except Exception as e:
            print(f"  list_alerts failed: {type(e).__name__}: {e}")
    finally:
        if rt is not None:
            print("\nstopping rtstream ...")
            try:
                rt.stop()
                try:
                    rt.refresh()
                except Exception:
                    pass
                print(f"  stopped  status={rt.status}")
            except Exception as e:
                print(f"  WARN stop failed: {type(e).__name__}: {e}")

    print("\nNow paste the webhook.site payload back here to lock AlertPayload shape.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
