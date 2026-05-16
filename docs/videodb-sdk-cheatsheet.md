# VideoDB SDK Cheatsheet

Source-of-truth: sandbox guide (https://hackday.videodb.io/sandbox.html) +
direct inspection of the installed hackathon-branch SDK
(`videodb==0.4.5`, ref `hackathon`).

Use this doc before writing any SDK call. Update if signatures drift.

---

## Install

```bash
pip install "git+https://github.com/Video-DB/videodb-python.git@hackathon"
```

## Imports

```python
from videodb import connect, SandboxTier
from videodb._constants import SceneExtractionType
from videodb.editor import Timeline, Track, Clip, ImageAsset, AudioAsset, Fit
```

---

## Connection

```python
conn = videodb.connect(api_key=None, session_token=None, base_url="https://api.videodb.io")
# api_key falls back to env VIDEO_DB_API_KEY if not passed
coll = conn.get_collection()             # default collection
coll = conn.get_collection(collection_id="c-...")  # explicit
```

## WebSocket

```python
ws = conn.connect_websocket(collection_id="default")  # returns WebSocketConnection
```

---

## Sandbox lifecycle

```python
sb = conn.create_sandbox(tier=SandboxTier.medium, idle_timeout=600)
sb.wait_for_ready(timeout=300, interval=5)
assert sb.is_active                 # mandatory gate per sandbox guide
sb.refresh()                        # re-poll server-side state
print(sb.id, sb.status, sb.tier)
sb.stop()
sb.wait_for_stop(timeout=120)

conn.list_sandboxes()               # all sandboxes for this account
conn.get_sandbox(sandbox_id)        # rehydrate by id
```

Tiers: `SandboxTier.small` ($1/h, 4 concurrent), `SandboxTier.medium` ($3.50/h, 2 concurrent).

---

## Upload (recorded video / audio)

```python
video = coll.upload(url="https://example.com/video.m3u8")     # YouTube/HLS
audio = coll.upload(url="...", media_type="audio")            # audio-only
```

## Video scene indexing (recorded)

```python
index_id = video.index_scenes(
    extraction_type=SceneExtractionType.time_based,
    extraction_config={"time": 10, "select_frames": ["first"], "frame_count": 1},
    model_name="google/gemma-4-31B-it",
    prompt="...",
    sandbox_id=sb.id,
)
idx = video.get_scene_index(index_id)   # list of scenes with .description
scenes = video.get_scenes()             # all scenes across indexes
```

---

## RTStream (live)

### Connect + start

```python
rtstream = coll.connect_rtstream(
    url="rtsp://...",
    name="My Stream",
    media_types=["video"],   # or ["video", "audio"]
    store=True,              # persist scenes for later search
)
rtstream.start()             # MANDATORY — otherwise no ingest
rtstream.stop()              # stops ingest + indexing
```

### Visual index

```python
visual_idx = rtstream.index_visuals(
    prompt="...",
    batch_config={"type": "time", "value": 5, "frame_count": 3},
    model_name="google/gemma-4-31B-it",
    sandbox_id=sb.id,
    name="my_visual_index",
)
```

### Audio index

```python
audio_idx = rtstream.index_audio(
    prompt="...",
    batch_config={"type": "time", "value": 30},
    model_name="Qwen/Qwen3.5-9B",
    sandbox_id=sb.id,
    name="my_audio_index",
)
```

### Retrieve scenes

```python
scenes = rtstream.get_scenes(start=None, end=None, page=1, page_size=100)
idx_obj = rtstream.get_scene_index(index_id)   # RTStreamSceneIndex handle
```

### Search

```python
result = rtstream.search(
    query="lion OR roar",
    index_id=None,                  # optional: scope to one scene index
    result_threshold=None,          # default server-side
    score_threshold=None,           # min similarity
    dynamic_score_percentage=None,
    filter=None,                    # metadata filter list[dict]
)
# result is RTStreamSearchResult
for shot in result.shots:
    # shot.start, shot.end (Unix timestamps),
    # shot.text, shot.search_score, shot.scene_index_id,
    # shot.scene_index_name, shot.metadata
    ...
```

**WARNING:** `search()` has NO `time_range` param. The correlation engine (T-33)
must filter `result.shots` client-side by `shot.start` / `shot.end` against the
desired window. Cache `last_seen_ts` per rule.

### Generate playable clip URL

```python
player_url = rtstream.generate_stream(
    start=1736000000,     # int Unix timestamp
    end=1736000060,       # int Unix timestamp
    player_config={       # optional
        "title": "Predator vocalization",
        "description": "Alarm call window",
        "slug": "wildwatch",
    },
)
# Also sets rtstream.stream_url (raw HLS) and rtstream.player_url.
```

---

## Events + Alerts

### Create event (once per connection)

```python
event_id = conn.create_event(
    event_prompt="Detect when SIGNAL contains ALARM_CALL",
    label="alarm_call_detected",
)
conn.list_events()  # [{event_id, label, event_prompt, ...}]
```

### Wire alert (per index)

```python
alert_id = visual_idx.create_alert(
    event_id=event_id,
    callback_url="https://your-tunnel.example/webhook/2",
    ws_connection_id=None,   # optional websocket push
)
visual_idx.list_alerts()
visual_idx.enable_alert(alert_id)
visual_idx.disable_alert(alert_id)
```

Same pattern for `audio_idx.create_alert(...)`.

### Webhook payload shape

(Per VideoDB intrusion-detection tutorial — verify on first real callback.)

```json
{
  "event_id": "...",
  "label": "POACHING_ALERT_GUNSHOT",
  "confidence": 0.92,
  "explanation": "...",
  "timestamp": "...",
  "start_time": "...",
  "end_time": "...",
  "stream_url": "https://rt.stream.videodb.io/manifests/.../...m3u8"
}
```

---

## Programmable editing

```python
from videodb.editor import Timeline, Track, Clip, ImageAsset, AudioAsset, Fit
# Plus VideoAsset, TextAsset (verify import path on first use).

t = Timeline(conn)
t.resolution = "1280x720"
t.background = "#000000"

vtrack = Track()
vtrack.add_clip(0, Clip(asset=ImageAsset(id=image.id), duration=10, fit=Fit.crop))

atrack = Track()
atrack.add_clip(0, Clip(asset=AudioAsset(id=audio.id), duration=10))

t.add_track(vtrack)
t.add_track(atrack)

stream_url = t.generate_stream()
player_url = f"https://console.videodb.io/player?url={stream_url}"
```

---

## Models (cost / tier matrix)

| Model                              | Tier   | Use                          |
|------------------------------------|--------|------------------------------|
| `google/gemma-4-31B-it`            | Medium | Visual VLM (our default)     |
| `google/gemma-4-26B-A4B-it`        | Medium | Visual VLM (MoE variant)     |
| `google/gemma-4-E2B-it`            | Small  | Lightweight visual fallback  |
| `Qwen/Qwen3.5-9B`                  | Small/Med | Audio + text reasoning    |
| `Qwen/Qwen3.5-27B`                 | Medium | Larger text reasoning        |
| `openai/whisper-large-v3-turbo`    | Small  | Speech-to-text               |
| `k2-fsa/OmniVoice`                 | Small  | TTS (not used)               |
| `black-forest-labs/FLUX.1-dev`     | Medium | Image gen (not used)         |
| `stabilityai/stable-audio-open-1.0`| Small  | Audio gen (not used)         |

WildWatch uses gemma-4-31B-it (visual) + Qwen3.5-9B (audio) on one shared Medium sandbox.

---

## Critical guide instructions (verbatim)

- "Install the hackathon branch"
- "Only run jobs after `sandbox.status == 'active'`"
- "Use the smallest tier that supports your selected model"
- "Always wait for the sandbox to be active before submitting jobs"
- "Pass `sandbox_id=sandbox.id` explicitly for sandbox-backed jobs"
- "Stop the sandbox after use to avoid unnecessary runtime billing"

Don'ts:
- Don't omit `sandbox_id` (auto-resolve not guaranteed)
- Don't skip waiting for sandbox readiness
- Don't leave sandboxes running unnecessarily

---

## Player URL pattern

For attaching playable links to Telegram alerts:

```
https://console.videodb.io/player?url={stream_url}
```

---

## Support

- Discord (fastest unblock): https://discord.gg/CqkZcEh3P
- Email: team@videodb.io
- Docs: https://docs.videodb.io
- LLM-friendly doc index: https://docs.videodb.io/llms.txt
