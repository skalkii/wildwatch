# Repo Map — Where everything lives and what it does

> **Audience:** anyone joining the project — engineers, conservation partners, product reviewers, judges. You don't need a Python background to follow this. The technical labels are explained inline.

This map walks through every folder in `wildwatch/` and explains, in plain English, what each file does, what technology it uses, and who reads or runs it. Read it once and you'll know exactly where to look for anything in the codebase.

---

## 1. Project at a glance

**What WildWatch is:** a 24/7 wildlife monitor that watches livestreams from protected areas, listens to their audio, and uses AI to flag anything worth a ranger's attention — a rare species, an alarm call, a gunshot, an injured animal. It then ships those alerts to a phone (Telegram) and a live web dashboard, and at the end of each day stitches the highlights into a short video reel.

**How it's built:** Python on the back end, browser-rendered HTML on the front end, [VideoDB](https://videodb.io) as the "AI eyes and ears" that actually look at the video and audio. No machine-learning training of our own — VideoDB's vision-and-language model does the perception, we wire it up with carefully written prompts.

**Why it's split into folders:** each folder has one job. Configuration in one place, AI prompts in another, the live perception pipeline in another, helper scripts in a fourth, tests in a fifth. You can dip into any one folder without needing to read the rest.

---

## 2. Top-level tree

```
wildwatch/
├── README.md                  # Project pitch, quickstart, links
├── CLAUDE.md                  # Detailed handover for coding-agent collaborators
├── LICENSE                    # MIT
├── pyproject.toml             # Python package definition + dependencies
├── .env.example               # Template for the secrets file (.env)
├── docker-compose.yml         # One-command "run everything" setup
├── Dockerfile                 # Container recipe for the FastAPI server
│
├── config.py                  # Stream registry + fallback URLs (the only place URLs live)
│
├── wildwatch/                 # The Python package — the actual app
│   ├── webhooks.py            # FastAPI server: dashboard, API, alert receiver
│   ├── dashboard.py           # The single-page HTML dashboard + live event broadcaster
│   ├── sources.py             # "Source" = anything we're watching; CRUD layer
│   ├── ingest.py              # Pulls a Source into VideoDB (file / URL / RTSP)
│   ├── events.py              # 18 alert definitions + which index gets which event
│   ├── wiring.py              # Connects an index to an event with a callback URL
│   ├── correlation.py         # Cross-modal reasoning ("audio + visual = confirmed event")
│   ├── digest.py              # Daily highlight-reel builder
│   ├── prompts.py             # Loads the 4 AI prompts from prompts/
│   ├── sandbox.py             # Lifecycle helper for the VideoDB AI sandbox
│   ├── event_log.py           # Append-only log of every alert that fired
│   ├── state_io.py            # Crash-safe JSON file writes
│   ├── telegram.py            # Sends alerts to a Telegram bot
│   ├── post_upload_analysis.py# Path-B: post-upload audio+visual sweep → synthesised webhooks
│   └── ws_listener.py         # Optional WebSocket listener (skill's verbatim drop-in)
│
├── prompts/                   # The four AI prompts that drive every observation
│   ├── species.txt            # "What animals do you see?"
│   ├── behavior.txt           # "What are they doing?"
│   ├── environment.txt        # "What's the scene like? Weather? Water? Hazards?"
│   └── audio.txt              # "What do you hear?"
│
├── scripts/                   # CLI tools you run from the terminal
│   ├── bootstrap.py           # The big "wire it all up" script (events, indexes, alerts)
│   ├── build_digest.py        # Build today's highlight reel
│   ├── run_correlation.py     # Run the cross-modal reasoning loop
│   ├── build_corpus.py        # Pull sample clips into VideoDB for offline iteration
│   ├── upload_corpus.py       # Helper to push corpus clips
│   ├── index_corpus.py        # Bulk scene-index every corpus video + smoke-test search
│   ├── iterate_prompt.py      # Test a single prompt against one clip (no rtstream cost)
│   ├── start_live_test.py     # Run a live waterhole stream end-to-end
│   ├── event_smoke.py         # Smoke test: events get created
│   ├── audio_chain_smoke.py   # Smoke test: audio prompt → alert
│   ├── rtstream_smoke.py      # Smoke test: rtstream connection works
│   ├── rtstream_index_smoke.py# Smoke test: visual + audio indexes attach
│   ├── rtstream_audio_smoke.py# Smoke test: audio-only index works
│   ├── sdk_smoke.py           # Smoke test: VideoDB SDK reachable
│   ├── sdk_full_smoke.py      # Full SDK round-trip (sandbox, index, alert)
│   └── sdk_integration_smoke.py # End-to-end VideoDB integration check
│
├── bridge/                    # YouTube → RTSP bridging (so VideoDB can read a YouTube live)
│   ├── docker-compose.yml     # Spins up mediamtx (RTSP relay)
│   ├── mediamtx.yml           # mediamtx config
│   ├── watch_bore.sh          # Watches the bore.pub tunnel
│   └── watch_bridges.sh       # Watches the bridge container
│
├── samples/                   # Curated reference clips
│   └── triggers/              # 29 Africam YouTube URLs grouped by what alert they should trigger
│       ├── manifest.json      # Machine-readable map: which clip should fire which event
│       ├── README.md          # Curator notes
│       └── CURATION_HANDOFF.md# Notes for the corpus build
│
├── demo/                      # Demo storyboard + recording notes (filled in pre-submission)
├── docs/                      # This map + flow diagrams + VideoDB SDK cheatsheet
├── data/                      # Runtime artefacts (event log JSONL, etc.) — gitignored
├── logs/                      # Process logs (uvicorn, listeners) — gitignored
├── tests/                     # pytest test suite — covers prompts, events, ingest, sources,
│                              # dashboard, digest, correlation, state IO, sandbox, telegram
│
└── .state.json                # Live state: which streams are connected, which events exist,
                               # which alerts are wired, where the sandbox is. Atomically
                               # written by state_io.py. Re-readable on restart.
```

---

## 3. `wildwatch/` — the actual application

This is the Python package. Everything the FastAPI server, dashboard, and CLI scripts import lives here. Each file is small and does one job.

### 3.1 `webhooks.py` — the FastAPI server

| What it is | The single HTTP server that exposes everything: the dashboard at `/`, the API under `/api/...`, the alert-receiving webhook at `/webhook/{tier}`. |
| --- | --- |
| Why it exists | VideoDB needs a public URL to POST alerts to. The same server also powers the dashboard so we don't run two processes. |
| Key technology | FastAPI (Python web framework), `aiofiles` (async file uploads), `videodb` Python SDK. |
| Who runs it | `uvicorn wildwatch.webhooks:app --port 8000` — what `docker-compose up` and the quickstart `Path B` both invoke. |
| Notable bits | `_get_conn()` / `_get_coll()` cache the VideoDB connection (process-wide, RLock-guarded). `_async_sdk()` wraps every blocking SDK call in a 4-worker thread pool + per-call `asyncio.wait_for` deadline. Pool tracks `_sdk_in_flight`; at 2× saturation new calls raise `SDKPoolSaturated → 503` rather than queueing forever. CancelledError (client disconnect) defers the slot release via `cf_fut.add_done_callback` so the counter doesn't leak. Origin/CSRF middleware blocks cross-origin mutating requests. Upload route adds magic-byte sniff + per-IP rate-limit + 413/415 `source_deleted` broadcast. `/` dashboard route serves with `Cache-Control: no-store` so a hard-refresh always picks up fresh JS. **Scene endpoints:** `GET /api/videos/{id}/scenes/{index_id}` lists indexes first and short-circuits when status ≠ `done`, dodging the SDK hang on a still-processing index. `POST /api/videos/{id}/reindex` triggers a fresh `index_scenes` without deleting prior indexes. `GET /api/videos/{id}/clip?start=&end=` returns a playable HLS manifest via `video.generate_stream(timeline=[(start,end)])` for the dashboard's scene-card click-to-play. `DELETE /api/videos/{id}` calls `coll.delete_video` (removes the video + all its indexes from VideoDB), busts the videos cache, and broadcasts a `video_deleted` SSE so every open dashboard refreshes. **Search fan-out:** `POST /api/search` with `scope=collection` no longer calls `coll.search` (which only hits the spoken-word index and returns 0 for transcript-less wildlife clips). Instead it enumerates `coll.get_videos()`, filters to videos with a `done` scene index, and fans out concurrent per-video `v.search(index_type=scene, score_threshold=0.3)` calls — merging + ranking by score. |

### 3.2 `dashboard.py` — the live single-page UI

| What it is | The entire HTML, CSS, and JavaScript for the operator dashboard, served as one big string from a single endpoint. |
| --- | --- |
| Why it exists | Real-time operators (rangers, ecologists, judges) need a window into what the AI is seeing right now. A single-page app means no build step, no separate front-end repo. |
| Key technology | Tailwind CSS via CDN, **hls.js** for in-modal HLS playback, vanilla JavaScript, Server-Sent Events (SSE) for live push. Inline SVG favicon. Dark/light theme with CSS variables. |
| Who reads it | Anyone who opens `http://localhost:8000/`. |
| Notable bits | Four tabs (Alerts, Sources, Indexed Content, Usage). Tab state persists in `localStorage` + URL hash. Every label is rewritten in plain English for non-tech viewers. The Usage tab does the live `cost_metric × usage` math so you can see exactly where credits went. **Scene card renderer (visual + audio):** `_parseSceneText` understands BOTH bracket-tag families — visuals (`[SCENE] [ANIMAL] [NOTES]`) and audio (`[SOUND] [SIGNAL] [SUMMARY]`). `_renderSceneCard` picks the right layout per parsed structure: visual cards show light-mode pill + scene-state pill + per-animal rows, audio cards show category pills (🦁 Biophony / 💨 Geophony / ⚠️ Anthropogenic) + signal pills (Alarm call / Distress call / Predator vocal / Abnormal silence) + per-sound rows. Border colour escalates for anthropogenic audio events. Same renderer is reused inside the Indexed Content tab AND inside the search results so search hits look identical. Every scene card is clickable → opens a modal HLS player via `_openClipPlayer(url)` that uses Safari-native HLS or falls back to hls.js. **Index kind pill:** every index card shows its kind (Visual / Audio / Environment / Behavior) inferred from the index name so operators can see at a glance which AI lens fired. **Library toolbar:** the Indexed Content tab's Library panel has a sticky header with a name/id filter, a sort dropdown (name/length/id × asc/desc), and a kind filter (all/clip/uploaded/stream/reel). Filter + sort run client-side against `_libraryVids` so toolbar changes don't hit the API. The list scrolls inside the card with the header pinned at the top. **Per-source-kind actions:** `renderSource` only shows Reconnect/Disconnect for `rtsp`/`rtmp` (where reconnect actually re-establishes a live feed). Uploads and URL sources show Re-index instead (re-runs the AI scene index on the existing video — re-uploading would duplicate the file). Delete is always available. **Toast system:** `showToast(msg, {variant})` (info/success/warn/error) and `confirmToast(msg, {title, danger})` replace `window.alert`/`window.confirm` — used by re-index, delete, clip-fetch, and any future async UX. |

### 3.3 `sources.py` — the source registry

| What it is | A small in-memory + on-disk registry of every "Source" the user has added (an uploaded file, a YouTube link, an RTSP stream). |
| --- | --- |
| Why it exists | The dashboard needs to remember what you added across restarts. Persistence is in `.state.json["sources"]`. |
| Key technology | Plain dataclasses + JSON serialisation via `state_io.py`. |
| Who reads it | `webhooks.py` (the API), `ingest.py` (the pipeline). |
| Notable bits | Each Source has a status (`queued → connecting → ingesting → ready` or `error`). The dashboard reads that status to colour the card. |

### 3.4 `ingest.py` — the source-to-VideoDB pipeline

| What it is | The dispatcher that takes a freshly added Source and actually pulls it into VideoDB. |
| --- | --- |
| Why it exists | YouTube needs `yt-dlp` to grab a downloadable URL. RTSP can be handed to `coll.connect_rtstream()` directly. Local files use `coll.upload(file_path=)`. Each path has different failure modes that the dashboard needs to surface. |
| Key technology | `yt-dlp` (YouTube), `httpx` (URL probe), `videodb` SDK. |
| Who runs it | A background task spawned by the `/api/sources` endpoint when you add a source. |
| Notable bits | Every progress transition is broadcast to the dashboard via Server-Sent Events so the card animates in real time. **Auto scene-index + audio-index on upload:** after `coll.upload` succeeds, `_kick_off_scene_index(video, source_id)` fires `video.index_scenes(prompt=species)` AND `_kick_off_audio_index_async` fires `video.index_audio(prompt=audio)`. Both are idempotent (skip if matching index already exists) and best-effort (failure is logged but never propagates). Source status pulses through `connecting → ingesting → indexing → ready` so the dashboard explains what's happening. **Post-upload auto-analysis:** `_spawn_post_upload_analysis(video, source_id)` kicks `wildwatch/post_upload_analysis.py:run_post_upload_analysis` as a tracked asyncio task. That task polls until both indexes finish, then searches each one for event-of-interest queries (gunshot, chainsaw, rare species, etc.) and POSTs synthesised webhooks to `/webhook/{tier}` on hit — making Telegram alerts work on archive URL uploads even though VideoDB's native event/alert system is rtstream-only. |

### 3.5 `events.py` — the 18 alert definitions

| What it is | One big Python list, `EVENT_DEFINITIONS`, plus a small dictionary, `INDEX_EVENT_MAP`, that says which AI prompt's index should be wired to which event. |
| --- | --- |
| Why it exists | VideoDB events are server-side rules ("fire when the AI says X"). We define them once and reuse across multiple streams — that's the design pattern the skill rewards. |
| Key technology | Pure Python data — no SDK calls at import time. |
| Who reads it | `bootstrap.py` (to create events on a fresh run), `wiring.py` (to attach them to indexes). |
| Notable bits | Each event has a tier (1=info, 2=notable, 3=urgent), a label, and a prompt that VideoDB's event engine evaluates against incoming scene/audio descriptions. |

### 3.6 `wiring.py` — index ↔ event connector

| What it is | A single helper, `wire_alerts(...)`, that for each `(index, event)` pair creates an alert on VideoDB pointing at our `/webhook/{tier}` URL. |
| --- | --- |
| Why it exists | When the AI's index emits a description that matches an event prompt, VideoDB fires an alert. That alert needs a callback URL to land somewhere — that's us. |
| Key technology | `videodb` SDK (`idx.create_alert(event_id, callback_url=...)`). |
| Who calls it | `scripts/bootstrap.py`. |
| Notable bits | Idempotency is keyed on `rtstream_id` so a fresh rtstream re-wires alerts even if the local state cache has stale entries. When `--ws` is set, the helper also forwards `ws_connection_id` for dual-delivery (callback + WebSocket). |

### 3.7 `correlation.py` — cross-modal reasoning

| What it is | The "perception agent" reasoning layer — every 30 seconds it asks "did I see fleeing animals AND hear an alarm call in the last 90 seconds?" If yes, it fires a synthesised Tier-3 event. |
| --- | --- |
| Why it exists | Single-signal alerts are noisy (one bird alarm call might just be a passing kite). Multi-signal alerts (alarm + fleeing + frozen behaviour) are confirmation. This is the differentiator the demo storyboard sells. |
| Key technology | `videodb` SDK search across multiple indexes within a time window. |
| Who calls it | `scripts/run_correlation.py` runs the loop. The loop POSTs synthesised events to our own webhook so they flow through the same downstream pipeline as VideoDB-fired ones. |
| Notable bits | Each correlation rule has a cooldown so we don't fire the same synthesised event 100 times. The window is configurable per-rule. |

### 3.8 `digest.py` — daily highlight reel

| What it is | At the end of a day, picks the top-N events from the log, maps each to a corpus clip, and stitches them into a 90-second video using VideoDB's programmable editor. |
| --- | --- |
| Why it exists | A real ranger crew can't watch 24 hours of footage. A 90-second reel of the day's highlights is the operational deliverable. |
| Key technology | VideoDB's `Timeline`, `Track`, `Clip`, `VideoAsset`, `TextAsset`, `AudioAsset`, `Transition` — the editor model. Optional `coll.generate_music()` for soundtrack and `coll.generate_text()` for a natural-language summary card. |
| Who runs it | `python scripts/build_digest.py [--music] [--no-overlays]`. |
| Notable bits | Multi-track timeline (video + tier-label overlays + optional music) so the reel reads cleanly. Falls back to a synthesised montage if the event log is empty. |

### 3.9 `prompts.py` — the prompt loader

| What it is | A tiny helper that reads the four `.txt` files from `prompts/` and substitutes per-stream context (location name, expected species, expected sounds). |
| --- | --- |
| Why it exists | The prompts themselves are long and worth version-controlling as text files. Python code shouldn't carry kilobyte-strings around. |
| Key technology | `str.format`. |
| Who calls it | `bootstrap.py` and any script that creates an index. |

### 3.10 `sandbox.py` — the VideoDB sandbox lifecycle

| What it is | A helper that creates or reuses one shared "sandbox" — the dedicated GPU compute slot that VideoDB charges hourly for. Enforces the rule "one sandbox, status-gated, context-managed teardown." |
| --- | --- |
| Why it exists | Sandboxes are the expensive bit (e.g. $3.50/h for `sandbox_medium`). Leaking one across a session is the #1 way to burn credits. |
| Key technology | `videodb` SDK + Python context manager. |
| Who calls it | Anything that needs to run an index, generation, or programmable-editing call — that's `ingest.py`, `bootstrap.py`, the digest builder, smoke scripts. |
| Notable bits | `wait_for_ready` blocks until the sandbox is `active`. `is_active` short-circuits subsequent calls. Idle-timeout is 600 s server-side. |

### 3.11 `event_log.py` — append-only alert log

| What it is | Every alert the webhook receives is written as one JSON line to `data/live_event_log.jsonl`. |
| --- | --- |
| Why it exists | The digest builder needs a tamper-evident, restart-safe record. Append-only JSONL is the simplest format that survives crashes. |
| Key technology | Plain Python file IO with `aiofiles` for async writes. |
| Who reads it | `digest.py` (to pick the top-N events). |
| Notable bits | `read_since(min_ts)` tolerates malformed `received_at` values rather than crashing the whole digest — a bug-fix from earlier in the build. |

### 3.12 `state_io.py` — durable JSON writes

| What it is | A 30-line helper that does `atomic_write_json(path, obj)` correctly: write to `.tmp`, fsync, rename, fsync the parent directory. |
| --- | --- |
| Why it exists | `Path.write_text(json.dumps(...))` is not crash-safe — a power loss between the write and the rename leaves you with an empty file. We learned this the hard way. |
| Key technology | Python `os.fsync`, `os.replace`. |
| Who calls it | Anywhere that persists to `.state.json` — `bootstrap.py`, `webhooks.py` source CRUD, `sandbox.py`. |

### 3.13a `post_upload_analysis.py` — Path B (synthesised alerts on uploads)

| What it is | The post-upload event-detection sweep that makes Telegram alerts work on **uploaded videos and URLs**, not just live rtstreams. |
| --- | --- |
| Why it exists | VideoDB's `create_alert` only attaches to **rtstream** scene indexes — the SDK does not expose alerts on an uploaded video's scene index. Without this module, archive URL uploads would never trigger a Telegram (the bracket-tagged AI output would sit in the index waiting for someone to search it). |
| Key technology | `videodb` SDK (`video.list_scene_index` polling, `video.search` per event, `video.generate_stream` for clip URLs), `httpx` (POST synthesised payloads to our own webhook). |
| Who calls it | `wildwatch/ingest.py:_spawn_post_upload_analysis(video, source_id)` after every successful upload or URL ingest. Fire-and-forget asyncio task, tracked in a module-level Set so the GC can't drop it. |
| Flow | Wait until species index reaches `done` (cap 20 min) AND audio index reaches `done` (cap 8 min). For each event in the `_EVENT_QUERY` map (gunshot, chainsaw, rare_species, etc.), run `video.search(query, index_type=scene, scene_index_id=…, score_threshold=0.35)` and synthesise a `/webhook/{tier}` POST for the top hit. |
| Notable bits | Hardcoded `_EVENT_QUERY` map of search queries per event id_var (events.py prompts are written for the event engine, not natural-language search, so a separate query string is needed). `_MAX_FIRES_PER_UPLOAD = 12` caps Telegram spam from over-permissive prompts. All SDK errors are logged but never propagated — a failed sweep leaves the upload otherwise usable. `LOCAL_WEBHOOK_URL` env var overrides the default `http://localhost:8000` POST target for non-default deployments. |

### 3.13 `telegram.py` — Telegram alert sender

| What it is | `send_alert(...)` posts a Markdown-formatted message to your Telegram chat with the alert label, explanation, and a tappable clip URL. |
| --- | --- |
| Why it exists | A live demo where the presenter's phone buzzes mid-presentation is what sells "real-time perception." |
| Key technology | Telegram Bot API via `httpx`. Bot token + chat id come from `.env`. |
| Who calls it | `webhooks.py` after every alert lands. |

### 3.14 `ws_listener.py` — optional WebSocket subscriber

| What it is | A standalone script (verbatim from the official `video-db/skills` plugin) that connects to VideoDB's WebSocket channel, writes the connection id to a file, and logs every event it receives as JSONL. |
| --- | --- |
| Why it exists | The VideoDB skill prescribes dual-delivery: alerts should fire via both the webhook callback AND the WebSocket. That's higher reliability and the "depth of SDK usage" axis the judges weight. |
| Key technology | `videodb` SDK `conn.connect_websocket()`, asyncio with retry/backoff. |
| Who runs it | `python wildwatch/ws_listener.py --cwd /Users/kal/Desktop/wildwatch &` then `python scripts/bootstrap.py --ws`. |

---

## 4. `prompts/` — the four AI prompts

These four `.txt` files are the single most important piece of intellectual property in the project. They turn a generic VideoDB visual-language model into a wildlife perception system.

| File | Length | What it asks the AI to do |
| --- | --- | --- |
| `species.txt` | ~70 lines | "List every animal you can see. Use 'unidentified' if you're unsure. Distinguish day vs IR-night footage." |
| `behavior.txt` | ~90 lines | "Pick from this controlled vocabulary: drinking, fleeing, alarm_posture, courtship_display, fighting, … Plus interactions (predator_prey, parent_offspring) and anomalies (limping, isolated)." |
| `environment.txt` | ~65 lines | "Time of day, light mode (daylight/IR), weather, water level, vegetation, ground features. Flag carcasses, smoke, vehicles, broken camera." |
| `audio.txt` | ~95 lines | "Classify into biophony (animal), geophony (wind/rain), or anthropophony (gunshot/chainsaw/vehicle). Flag alarm calls and abnormal silence." |

Each prompt is loaded by `wildwatch/prompts.py` and formatted with stream-specific context (e.g. "Etosha waterhole, oryx and gemsbok expected") before being sent to VideoDB.

---

## 5. `scripts/` — the CLI surface

These are the executable entry points. You run them, they do one thing, they exit.

### Production scripts (used in the demo)

- **`bootstrap.py`** — Reads `.env` and `.state.json`, makes sure all 18 events exist on VideoDB, connects one rtstream, creates the four indexes against it, wires alerts. The `--ws` flag enables WebSocket dual-delivery. The `--observe N` flag keeps the stream alive for N seconds before stopping it.
- **`build_digest.py`** — Reads the event log, builds today's highlight reel, prints the playable URL. `--music` adds a generated soundtrack; `--no-overlays` skips the tier-label burn-ins.
- **`run_correlation.py`** — Runs the cross-modal correlation loop against an already-bootstrapped rtstream. Every 30 s it searches across the indexes and fires synthesised events when a rule matches.
- **`start_live_test.py`** — End-to-end test: bootstrap + run for 15 minutes + tear down. Used for credit-burn sanity checks.

### Build / curation scripts

- **`build_corpus.py`** — Walks `samples/triggers/manifest.json` and uploads every clip to VideoDB so the digest builder has a pool to draw from.
- **`upload_corpus.py`** — One-off uploader for arbitrary clips.
- **`index_corpus.py`** — Bulk re-indexer for every video in `state["corpus"]`. Lists each video's existing scene indexes, creates a fresh one with the species prompt if none exists, polls until `done`, then runs a `v.search(query="any animal", index_type=scene)` smoke test so you can verify the index actually answers queries. Idempotent — re-running skips already-indexed videos. Useful for backfilling videos uploaded before auto-indexing existed, or when an auto-index kickoff failed silently. Per-slug filter via `--slug <name>`.
- **`iterate_prompt.py`** — The cheapest dev loop: runs a single prompt against a recorded clip (no rtstream cost). Used during prompt iteration.

### Smoke tests (verify one thing at a time)

- `sdk_smoke.py` — VideoDB SDK is reachable, auth works.
- `sdk_full_smoke.py` — Sandbox lifecycle works end-to-end.
- `sdk_integration_smoke.py` — Full upload → index → search → alert.
- `event_smoke.py` — Events get created and listed.
- `rtstream_smoke.py` — A stream can be connected.
- `rtstream_index_smoke.py` — Visual + audio indexes attach to a stream.
- `rtstream_audio_smoke.py` — Audio-only index works.
- `audio_chain_smoke.py` — Audio prompt → event → alert end-to-end.

---

## 6. `tests/` — automated checks

`pytest`-based. Run them all with `pytest`. Per-module coverage:

| Test file | What it locks down |
| --- | --- |
| `conftest.py` | Two autouse session fixtures: (1) sets `WILDWATCH_ALLOW_NO_ORIGIN=1` so the CSRF middleware lets TestClient through, (2) resets the process-wide `_conn_cache` before/after every test so `videodb.connect` patches actually take effect. |
| `test_config.py` | `config.py` constants are well-formed. |
| `test_prompts.py` | The four prompt files exist, load, format. |
| `test_events.py` | `EVENT_DEFINITIONS` is well-typed and `INDEX_EVENT_MAP` covers it. |
| `test_sources.py` | Source CRUD survives restart. |
| `test_source_routes.py` | API endpoints for sources accept/reject the right inputs. |
| `test_ingest.py` | The three ingest paths (file, URL, RTSP) handle success and failure. |
| `test_dashboard.py` | SSE broadcaster, stats, and event-feed limits. |
| `test_content_routes.py` | Search and video API endpoints. |
| `test_usage_route.py` | `/api/usage` shape. |
| `test_digest.py` | Timeline construction, tier-clip selection, fallback montage. |
| `test_correlation.py` | Rule evaluation, cooldown, evidence collation. |
| `test_event_log.py` | Append, read, malformed-line tolerance. |
| `test_state_io.py` | Atomic write + crash semantics. |
| `test_sandbox.py` | Sandbox lifecycle, idempotency. |
| `test_bg_tasks.py` | Background task tracking + error propagation. |
| `test_cache_on_failure.py` | Outage caches prevent hammering the SDK. |
| `test_corpus_manifest.py` | Sample-trigger manifest is well-formed. |
| `test_telegram.py` | Bot message formatting. |

---

## 7. Config + state

- `config.py` — every stream URL + their per-stream prompt context lives here. The only place you edit when adding a new stream.
- `.state.json` — runtime persisted state: connected rtstreams, events, alerts, sources, sandbox id, webhook base URL. **Atomically rewritten** by `state_io.py` after every change.
- `.env` — secrets and optional config:
  - `VIDEO_DB_API_KEY` — required, from https://console.videodb.io
  - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — required for Telegram alerts
  - `WILDWATCH_ALLOWED_ORIGINS` — optional comma-separated hosts whitelisted by the CSRF/Origin guard (in addition to `localhost`/`127.0.0.1`/`0.0.0.0`).
  - `WILDWATCH_ALLOW_NO_ORIGIN=1` — optional escape hatch for trusted CLI clients (curl/scripts) that don't send an `Origin` header. Read once at module import + WARNING-logged at startup so it shows up in every log rotation.
  - `WILDWATCH_TRUSTED_PROXY=1` — set when the server runs behind nginx / Cloudflare / ALB. The upload rate limiter then reads the first IP from `X-Forwarded-For` instead of the TCP peer (which would be the proxy itself, collapsing all clients into one shared bucket). NEVER set in a direct-exposure deployment — an attacker can spoof `X-Forwarded-For` to bypass per-IP limits.
  - `VIDEODB_EVENTS_DIR` — optional directory where `wildwatch/ws_listener.py` writes the `videodb_ws_id` file (defaults to `/tmp`). Files there are created with `0o600` + `O_NOFOLLOW`.
- `.env.example` — template; copy to `.env` and fill in.

### Built-in upload protections

`POST /api/sources/upload` carries three defences that fire BEFORE bytes touch disk:

1. **Per-IP rate limit** — token bucket, capacity 3, refill 1 token/min/IP, OrderedDict-LRU capped at 50k entries. Returns `429` when exceeded. IPv6/port suffixes are normalised so `[::1]:1234` and `[::1]:9999` share one bucket.
2. **MIME magic-byte sniff** — first 32 bytes must match a video container (`mp4`/`mov`/`webm`/`mkv`/`avi`/`mpeg-ps`/`flv`). Returns `415` on miss. MPEG-TS is deliberately excluded (sync-byte heuristic isn't bypass-resistant without a real packet parser).
3. **Size cap** — `UPLOAD_MAX_BYTES = 500 MB`. Returns `413` when exceeded.

`413` and `415` paths both delete the orphan Source row and emit a `source_deleted` SSE event so the dashboard card disappears immediately.

---

## 8. Glossary for non-technical readers

| Term | Meaning in this project |
| --- | --- |
| **RTSP / RTMP** | Live-video streaming protocols. Wildlife cameras speak RTSP; gaming/social services speak RTMP. We can read either. |
| **HLS** | The streaming format VideoDB hands back when it wants you to play a clip in a browser. |
| **VideoDB sandbox** | A dedicated cloud GPU slot VideoDB rents you while it runs perception models on your stream. Per-hour billed. |
| **Index** | One AI "lens" pointed at a stream — e.g. our **species index** asks the AI "what animals do you see?" every 5 seconds. We run 4 indexes per stream (species, behavior, environment, audio). |
| **Event** | A server-side rule that says "fire if any index outputs match this prompt." We define 18 events, ranging from "rare species seen" to "POACHING_ALERT_GUNSHOT." |
| **Alert** | The actual webhook POST that VideoDB sends when an event fires. Our server receives these at `/webhook/{tier}` and forwards them to Telegram and the dashboard. |
| **Tier** | Severity. 1 = info (green), 2 = notable (yellow), 3 = urgent (red). |
| **Correlation** | Cross-modal reasoning. "Audio says alarm call AND visual says fleeing animals within 90 s → upgrade to confirmed tier-3 predator event." |
| **Digest** | A 90-second auto-edited highlight reel of the day's most notable events. |
| **Sandbox / VideoDB SDK / VideoDB Skill** | VideoDB is the third-party platform we build on. Their **SDK** is the Python library we call. Their **Skill** is the official Claude Code plugin that ships best-practice patterns for the SDK — we follow its conventions. |

---

## 9. Where to start reading code

- **Curious about the AI prompts?** → `prompts/species.txt` (and its three siblings).
- **Curious about the alert pipeline?** → `wildwatch/webhooks.py` (the `/webhook/{tier}` endpoint), then `wildwatch/telegram.py` and `wildwatch/event_log.py`.
- **Curious about the dashboard?** → `wildwatch/dashboard.py` (the entire UI in one file).
- **Curious about cross-modal reasoning?** → `wildwatch/correlation.py` and the `CORRELATION_RULES` list inside `scripts/run_correlation.py`.
- **Curious about the digest reel?** → `wildwatch/digest.py`.
- **Want to add a new stream?** → edit `config.py`, then run `python scripts/bootstrap.py`.
- **Want to test a single prompt change cheaply?** → `python scripts/iterate_prompt.py prompts/species.txt samples/triggers/<clip>.mp4`.

See `docs/FEATURE_FLOWS.md` next for the visual end-to-end flow charts.
