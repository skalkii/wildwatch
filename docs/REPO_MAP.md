# Repo Map

Every folder + file in one page. Plain English. Tech labels inline.

## 1. At a glance

24/7 wildlife monitor. Watches livestreams from protected areas, listens to audio, uses AI to flag noteworthy moments — rare species, alarm calls, gunshots, injured animals. Ships alerts to Telegram + live dashboard. Daily highlight reel.

Python back-end, browser HTML front-end, [VideoDB](https://videodb.io) as the AI eyes/ears. No ML training of our own — VideoDB's VLM does perception; we wire it up with carefully written prompts.

**Why this exists:** 286k rangers manage 20M+ km² — one per 72 km² (IUCN target: one per 5 km²). See [README → Why this matters](../README.md#why-this-matters).

---

## 2. Top-level tree

```
wildwatch/
├── README.md, CLAUDE.md, LICENSE, pyproject.toml, .env.example
├── docker-compose.yml, Dockerfile
├── config.py                 # Stream registry + fallback URLs
│
├── wildwatch/                # Python package
│   ├── webhooks.py           # FastAPI app: dashboard, /api/*, /webhook/{tier}, /api/digest/build
│   ├── dashboard.py          # SSE broadcaster + HTML loader (loads static/dashboard.html)
│   ├── sources.py            # Source CRUD + status state machine (lock-guarded)
│   ├── ingest.py             # File / URL / RTSP → VideoDB; async _emit; broadcast circuit-breaker
│   ├── events.py             # 18 event definitions + INDEX_EVENT_MAP
│   ├── wiring.py             # index ↔ event ↔ webhook connector
│   ├── correlation.py        # Cross-modal reasoning loop
│   ├── digest.py             # Daily reel + compute_analytics + length-sync (loop / tail-music)
│   ├── prompts.py            # Prompt loader + DEFAULT_UPLOAD_PROMPT_CONTEXT
│   ├── sandbox.py            # Shared sandbox lifecycle
│   ├── sdk_pool.py           # Process-wide VideoDB conn cache (RLock-guarded)
│   ├── rate_limit.py         # Per-IP upload token bucket
│   ├── billing.py            # Credit-burn estimator (DI'd)
│   ├── event_log.py          # Append-only streaming JSONL
│   ├── state_io.py           # Atomic .state.json writes (O_NOFOLLOW + fsync)
│   ├── telegram.py           # send_alert + send_digest (QuickChart album)
│   ├── post_upload_analysis.py  # Path-B sweep — synthesised alerts for uploads
│   └── static/dashboard.html # Whole single-page UI (HTML+CSS+JS, importlib.resources)
│
├── prompts/                  # The four AI prompts
│   ├── species.txt           # "What animals do you see?"
│   ├── behavior.txt          # "What are they doing?"
│   ├── environment.txt       # "Scene? Weather? Water? Hazards?"
│   └── audio.txt             # "What do you hear?"
│
├── scripts/                  # CLI entry points
│   ├── bootstrap.py          # Wire events + indexes + alerts (idempotent)
│   ├── build_digest.py       # CLI fallback for daily reel
│   ├── run_correlation.py    # Cross-modal correlation runner
│   ├── ws_listener.py        # Optional WebSocket dual-delivery
│   ├── iterate_prompt.py     # Test a single prompt against one clip
│   ├── build_corpus.py / upload_corpus.py / index_corpus.py
│   ├── start_live_test.py    # 15-min end-to-end smoke
│   └── *_smoke.py            # Per-primitive verification
│
├── bridge/                   # YouTube → RTSP bridge (mediamtx + bore + streamlink + ffmpeg)
├── samples/triggers/         # 29 Africam URLs grouped by intended trigger + manifest.json
├── demo/                     # Storyboard + recording notes
├── docs/                     # This map, FEATURE_FLOWS, GENAI_ROADMAP, SDK cheatsheet
├── data/, logs/              # Runtime artefacts (gitignored)
├── tests/                    # pytest suite
└── .state.json               # Live state (streams, events, alerts, sandbox)
```

---

## 3. `wildwatch/` modules

### 3.1 `webhooks.py` — FastAPI server

Single HTTP server: dashboard at `/`, API under `/api/*`, alert receiver at `/webhook/{tier}`.

| | |
|---|---|
| Tech | FastAPI, `aiofiles`, `videodb` SDK |
| Run | `uvicorn wildwatch.webhooks:app --port 8000` |

Notable bits:
- **SDK pool** — `_get_conn` / `_get_coll` re-exported from `wildwatch.sdk_pool`. `_async_sdk()` wraps every blocking SDK call in 4-worker thread pool + per-call `asyncio.wait_for`. `_sdk_in_flight` tracked under `_executor_lock`; 2× saturation → `SDKPoolSaturated → 503`. `CancelledError` defers slot release via `cf_fut.add_done_callback`.
- **`_require_coll(timeout_s=10.0)`** — canonical "acquire coll or raise HTTPException" helper. Translates `SDKPoolSaturated → 503`, `TimeoutError → 504` in one place.
- **Lifespan reset** — `_lifespan` sets `_SDK_EXECUTOR = None` after shutdown so `uvicorn --reload` rebuilds pool on next request.
- **CSRF / SSRF** — Origin middleware blocks cross-origin mutating; `_validate_source_input` enforces per-kind URL schemes; `_host_is_private(host)` blocks loopback/private/link-local in textual AND IPv4-mapped IPv6.
- **Webhook auth** — `WILDWATCH_WEBHOOK_SECRET` env enables `X-WildWatch-Secret` (`hmac.compare_digest`). Unset → loud WARNING + open access.
- **Upload route** — magic-byte sniff + per-IP rate-limit + 413/415 `source_deleted` broadcast; `.partial` → `.mp4` only after sniff.
- **Video endpoints** — `GET /api/videos/{id}/scenes/{index_id}`, `POST /api/videos/{id}/reindex`, `GET /api/videos/{id}/clip?start=&end=` (via `video.generate_stream`), `DELETE /api/videos/{id}` (cache bust + `video_deleted` SSE).
- **Search fan-out** — `POST /api/search` scope=collection enumerates videos with `done` scene index, concurrent `v.search` calls.
- **Digest** — `POST /api/digest/build` wraps sync `digest.build_digest` in `asyncio.to_thread` (~30–90s); calls `telegram.send_digest` on success when `notify_telegram=true`.
- **Local credit-burn meter** — `_estimate_credit_burn_usd()` thin wrapper around `wildwatch.billing._estimate_credit_burn_usd`.

### 3.2 `dashboard.py` — SSE broadcaster + HTML loader

Server-side half: SSE broadcaster (`broadcast` + `subscribe`), counters (`_total`, `_tier_counts`, `_recent_events`), static HTML loader via `importlib.resources` + `@lru_cache`. UI string lives in `static/dashboard.html` (~200 lines of Python).

Tech: `asyncio.Queue` fan-out for SSE. Static asset = Tailwind via CDN, `hls.js` for in-modal HLS, `Chart.js` for digest charts, vanilla JS. Inline SVG favicon. Dark/light via CSS vars on `:root`.

Notable bits:
- **Four tabs** — Alerts, Sources, Indexed Content, Usage. State in `localStorage` + URL hash.
- **Scene card renderer** — `_parseSceneText` understands BOTH bracket-tag families (visual: `[SCENE] [ANIMAL] [NOTES]`, audio: `[SOUND] [SIGNAL] [SUMMARY]`). `_renderSceneCard` picks layout per parsed structure. Border escalates for anthropogenic audio. Reused in Indexed Content + search results. Every card → modal HLS player via `_openClipPlayer(url)`.
- **Index kind pill** — Visual / Audio / Environment / Behavior inferred from index name.
- **Library toolbar** — sticky header, filter + sort + kind. Runs client-side against `_libraryVids`. Inner-scroll.
- **Library = active rtstreams only** — cross-references `coll.list_rtstreams()` with `/api/sources` rows. Both signals together = "actively ingesting now". VideoDB sometimes reports stale `connected`.
- **Per-index collapse** — toggle so long visual scenes don't hide the audio index.
- **Audio-blocked surface** — `audio_blocked: 'no_speech'` annotation → amber "no speech — skipped" pill + Remove button (calls `DELETE /api/videos/{id}/indexes/{idx_id}`).
- **Per-source-kind actions** — `renderSource` shows Reconnect/Disconnect only for `rtsp`/`rtmp`. Uploads/URLs show Re-index instead.
- **Toast system** — `showToast(msg, {variant})` (info/success/warn/error) + `confirmToast(msg, {danger})` replace `alert`/`confirm`.
- **Daily summary modal** — `_openDigestModal(d)` full-screen overlay. `_digestTheme()` reads CSS vars from `:root` (tier colours fixed). 4-up KPI strip + 2×2 charts + inline HLS + transcript. Chart instances tracked + destroyed on close.

### 3.3 `sources.py` — source registry

| | |
|---|---|
| What | Small in-memory + on-disk registry of every "Source" the user added. |
| Why | Dashboard needs to remember across restarts. Persists to `.state.json["sources"]`. |
| Tech | Dataclasses + `state_io.py`. |
| Status | `queued → connecting → ingesting → ready` or `error`. |

### 3.4 `ingest.py` — source → VideoDB pipeline

| | |
|---|---|
| What | Dispatcher per source kind: upload / youtube / hls / rtstream. |
| Tech | `yt-dlp`, `httpx` (URL probe), `videodb` SDK. |
| Runs | Background task spawned by `/api/sources` endpoint. |

Notable bits:
- **Async `_emit`** — every status transition wraps the atomic JSON write in `asyncio.to_thread`. Loop stays hot under concurrent ingests (was parking on disk I/O per transition).
- **Auto scene + audio index** — `_kick_off_scene_index(video, source_id)` + `_kick_off_audio_index_async`. Idempotent + best-effort. Generic-upload prompt context = `wildwatch.prompts.DEFAULT_UPLOAD_PROMPT_CONTEXT`.
- **Broadcast log-flood circuit-breaker** — `_BROADCAST_FAIL_COUNTER` logs one full traceback per failure burst + short WARNING every 50th. Successful broadcast resets counter.
- **Post-upload sweep** — `_spawn_post_upload_analysis(video, source_id)` kicks `wildwatch/post_upload_analysis.py:run_post_upload_analysis`.
- **Live-YouTube handling** — `_ingest_youtube` probes via `yt-dlp --print is_live`. Live URLs → clear error pointing at `bridge/README.md`.

### 3.5 `events.py` — 18 alert definitions

`EVENT_DEFINITIONS` list + `INDEX_EVENT_MAP` dict. Pure Python data (no SDK at import). Read by `bootstrap.py` + `wiring.py`. Each event: tier (1=info, 2=notable, 3=urgent), label, prompt evaluated by VideoDB's event engine.

### 3.6 `wiring.py` — index ↔ event connector

`wire_alerts(...)` creates one alert per `(index, event)` pair pointing at `/webhook/{tier}`. Idempotency keyed on `rtstream_id`. `--ws` forwards `ws_connection_id` for dual-delivery.

### 3.7 `correlation.py` — cross-modal reasoning

30s sweep. "Audio says alarm-call AND visual says fleeing within 90s" → fire synthesised Tier-3 event. Each rule has cooldown (default 300s). Runs from `scripts/run_correlation.py`. POSTs synthesised events to own webhook.

### 3.8 `digest.py` — reel + analytics aggregator

| | |
|---|---|
| What | Reel builder + chart-data aggregator. Reads last 24h, dedupes, picks top-N, builds narrated reel, returns chart-ready analytics. |
| Tech | VideoDB Timeline / Track / Clip / VideoAsset(`volume=0`) / TextAsset / AudioAsset(`volume=1.5`) / Transition. `coll.generate_text` (narrator), `coll.generate_voice(voice_name="George", config={speed:0.85, stability:0.75})`, `coll.generate_music` (background + tail outro). |
| Run | Dashboard → Daily summary → Build → `POST /api/digest/build`. CLI: `python scripts/build_digest.py [--music] [--no-overlays]`. |

Notable bits:
- `dedupe_events` collapses `(label, source-first-48-chars, 60s bucket)`.
- `compute_analytics(events)` = pure aggregator. One pass → tier counts + top_labels + top_species + 24-bucket hourly + light_modes + overlapping categories (visual/audio/behaviour/environment/threat).
- `build_timeline` returns `(timeline, n_clips, reel_seconds, ctx)` so caller can extend/pad without re-probing.
- **Per-clip waterfall** — (1) event's own `video_id` + `start_time` (Path-B uploads carry these — shows real triggering scene); (2) `pick_corpus_video_id` with skip-list for `_video_has_info` rejects; (3) `_discover_collection_fallback` scans `coll.get_videos()`.
- **Length sync** — `audio.length > reel_seconds` → `_extend_reel_with_loop(ctx, audio_len)` appends clips. `reel_seconds > audio.length` → `_add_tail_music(timeline, conn, start=audio_len, duration=gap)`.

### 3.9 `prompts.py` — prompt loader

Reads four `.txt` files from `prompts/`, substitutes per-stream context. `str.format` only. `DEFAULT_UPLOAD_PROMPT_CONTEXT` shared dict for uploads.

### 3.9b `sdk_pool.py` — process-wide conn cache

`_get_conn()` + `_get_coll()` + `reset_cache()` test helper. `threading.RLock` double-checked locking (recursive). Lazy `import videodb` so non-SDK callers don't pay import cost. Extracted from `webhooks.py` during SoC refactor.

### 3.9c `rate_limit.py` — per-IP upload token bucket

Token bucket for `POST /api/sources/upload`. Owns state dict + lock + helpers (`_normalize_ip`, `_looks_like_bare_ipv6`, `_client_ip_from`, `_upload_rate_limit_check`, `_evict_overflow_locked`). `OrderedDict` LRU, `threading.Lock`. Capacity 3, refill 1/min/IP. Hard cap 50k entries with LRU pop. `WILDWATCH_TRUSTED_PROXY=1` reads first IP in `X-Forwarded-For`. `_normalize_ip` strips `[…]` + `:port` so XFF rotation can't mint new keys.

### 3.9d `billing.py` — credit-burn estimator

Local upper-bound for "what are we burning right now?" — built from `.state.json` start timestamps cross-checked against `coll.list_rtstreams()` + `conn.get_sandbox()`. Dependency-injected helpers (`coll_getter`, `conn_getter`, `with_timeout`, `coerce_to_list`) keep it decoupled from FastAPI. Live-status SDK failure → fallback + `live_status_unknown` flag (not silent zero).

### 3.10 `sandbox.py` — VideoDB sandbox lifecycle

Creates or reuses ONE shared sandbox. Status-gated. Context-managed teardown. `wait_for_ready` blocks until `is_active=True`. Server-side idle timeout 600s.

### 3.11 `event_log.py` — append-only JSONL

`data/live_event_log.jsonl`. **Streaming reads** — `_iter_records()` yields one dict per line (no slurp; safe for unbounded growth on 24/7 deploy). `read_all()` lists for existing contract; `read_since()` filters inline. Tolerant parse — corrupt lines counted in aggregate WARNING. `WILDWATCH_LOG_FILE` env redirects path.

### 3.12 `state_io.py` — durable JSON writes

`atomic_write_json(path, obj)` — write `.tmp` → `os.fsync` → `os.replace` → fsync parent dir. `Path.write_text(json.dumps(...))` is NOT crash-safe (learned the hard way).

### 3.13 `post_upload_analysis.py` — Path B

`create_alert` is rtstream-only. This makes Telegram fire on uploads.

| | |
|---|---|
| Tech | `video.list_scene_index` polling, `video.search` per event, `video.generate_stream`, `httpx`. |
| Runs | `wildwatch/ingest.py:_spawn_post_upload_analysis(video, source_id)` post-ingest. Tracked asyncio Task. |
| Flow | Wait for scene index `done` (20m cap) + audio `done` (8m cap). For each event in `_EVENT_QUERY` → `video.search(query, index_type=scene, scene_index_id=…, score_threshold=0.35)`. Synthesise `/webhook/{tier}` POST for top hit. |

Notable bits:
- **`_EVENT_QUERY`** — hand-curated search query per event id_var (events.py prompts written for event engine, not natural-language search).
- **Audio fallback queries** include visual cues (e.g. `gunshot OR weapon OR muzzle flash OR person aiming firearm`) so sweep produces hits when running audio queries against visual index.
- **`_MAX_FIRES_PER_UPLOAD = 12`** — Telegram flood guard.
- **Transcript gating** — `_has_transcript(video)` probes `video.get_transcript()` → `video.generate_transcript(force=False)`. `kick_off_audio_index` returns `"created" / "existing_ready" / "no_transcript_skipped" / "failed" / "prompt_failed"` so dashboard toast can explain.
- **Stuck-index purge** — `purge_stuck_audio_indexes` deletes any audio index in `processing/queued/pending/initiated` (VideoDB has no cancel-job API). `kick_off_audio_index(force=True)` purges ALL audio indexes for the explicit "Re-index audio" CTA.
- **All SDK errors logged but never propagated.** `LOCAL_WEBHOOK_URL` overrides default `http://localhost:8000`.

### 3.13b `telegram.py` — alert + digest sender

Two delivery surfaces: `send_alert` per-event tier-coloured, `send_digest` daily album.

| | |
|---|---|
| Tech | Telegram Bot API via `httpx`. `parse_mode=HTML`. `sendMediaGroup` for charts. **QuickChart.io** renders Chart.js JSON → PNG (no auth, no local deps). |

Notable bits:
- `genai_friendly_explanation(coll, tier, label, raw)` — `coll.generate_text(model_name='basic')` rewrites event-engine prose into one ranger-friendly sentence. Fail-soft → `humanise_explanation` bracket-parser.
- `friendly_label(label)` — title-cases snake_case preserving acronyms (HLS / RTSP / AI / …).
- `_COLL_GETTER` wired via `configure_coll_getter(_get_coll)` so rewriter reuses cached connection.
- `send_digest` does TWO API calls: `sendMediaGroup` (up to 4 QuickChart PNGs with HTML caption + 🔵🟡🔴 KPI line) + `sendMessage` (narration + reel link).
- `_digest_chart_urls(analytics)` builds Chart.js configs matching modal palette + URL-encodes into `quickchart.io/chart?…&c=…`.
- `build_digest_message` — Unicode-block ASCII-bar fallback (`<pre>` blocks) when QuickChart unreachable.
- **Token-leak guard** — `httpx.TransportError`/`HTTPStatusError` paths re-raise WITHOUT `from e` so chained traceback doesn't print the URL (token-bearing).

---

## 4. `prompts/`

Four `.txt` files = single most important IP. Generic VideoDB VLM → wildlife perception system.

| File | Lines | Asks the AI to |
|---|---|---|
| `species.txt` | ~70 | List every animal. Use "unidentified" if unsure. Distinguish day vs IR-night. |
| `behavior.txt` | ~90 | Pick from controlled vocab (drinking, fleeing, alarm_posture, courtship, fighting, …) + interactions + anomalies (limping, isolated). |
| `environment.txt` | ~65 | Time, light, weather, water level, vegetation, ground, hazards (carcass, smoke, vehicle, broken camera). |
| `audio.txt` | ~95 | Classify biophony / geophony / anthropophony. Flag alarm calls + abnormal silence. |

Loaded by `wildwatch/prompts.py`, formatted with stream context.

---

## 5. `scripts/`

### Production
- **`bootstrap.py`** — Reads `.env` + `.state.json`. Ensures 18 events. Iterates every `config.STREAMS` entry with non-null `rtsp_url` + `fallback_intruder`. Idempotent. Operator-added streams left running on exit; only `fallback_intruder` stopped. `--ws` enables WebSocket dual-delivery. `--observe N` keeps alive N seconds.
- **`build_digest.py`** — Reads event log → builds reel → prints URL. `--music` adds soundtrack; `--no-overlays` skips tier-label burn-ins.
- **`run_correlation.py`** — Cross-modal correlation loop against already-bootstrapped rtstream.
- **`start_live_test.py`** — Bootstrap + run 15 min + tear down. Credit-burn sanity check.

### Curation
- **`build_corpus.py`** — Walks `samples/triggers/manifest.json`, uploads all clips.
- **`upload_corpus.py`** — One-off uploader for arbitrary clips.
- **`index_corpus.py`** — Bulk re-indexer. Lists scene indexes, creates fresh species-prompt index if none, polls until `done`, runs `v.search` smoke test. Idempotent. `--slug <name>` filter.
- **`iterate_prompt.py`** — Cheapest dev loop: one prompt vs one clip (no rtstream cost).

### Smoke
- `sdk_smoke.py` — SDK reachable + auth works.
- `sdk_full_smoke.py` — Sandbox lifecycle end-to-end.
- `sdk_integration_smoke.py` — Upload → index → search → alert.
- `event_smoke.py` — Events created + listed.
- `rtstream_smoke.py` / `rtstream_index_smoke.py` / `rtstream_audio_smoke.py` — stream connect + indexes attach.
- `audio_chain_smoke.py` — Audio prompt → event → alert end-to-end.

---

## 6. `tests/`

`pytest`-based. Run all: `pytest`.

| Test file | What it locks down |
|---|---|
| `conftest.py` | Autouse fixtures: `WILDWATCH_ALLOW_NO_ORIGIN=1` for TestClient + reset `_conn_cache` per test. |
| `test_config.py` | `config.py` constants well-formed. |
| `test_prompts.py` | Prompt files load + format. |
| `test_events.py` | `EVENT_DEFINITIONS` typed + `INDEX_EVENT_MAP` covers it. |
| `test_sources.py` | Source CRUD survives restart. |
| `test_source_routes.py` | API accept/reject. |
| `test_ingest.py` | Three ingest paths handle success + failure. |
| `test_dashboard.py` | SSE broadcaster, stats, event-feed limits. |
| `test_content_routes.py` | Search + video API. |
| `test_usage_route.py` | `/api/usage` shape. |
| `test_digest.py` | Timeline construction, tier-clip selection, fallback montage. |
| `test_correlation.py` | Rule evaluation, cooldown, evidence collation. |
| `test_event_log.py` | Append, read, malformed-line tolerance. |
| `test_state_io.py` | Atomic write + crash semantics. |
| `test_sandbox.py` | Sandbox lifecycle, idempotency. |
| `test_bg_tasks.py` | Background task tracking + error propagation. |
| `test_cache_on_failure.py` | Outage caches prevent hammering SDK. |
| `test_corpus_manifest.py` | Sample-trigger manifest well-formed. |
| `test_telegram.py` | Bot message formatting. |

---

## 7. Config + state

- **`config.py`** — every stream URL + per-stream prompt context. Only place to edit when adding a new stream.
- **`.state.json`** — runtime: connected rtstreams, events, alerts, sources, sandbox id, webhook base URL. Atomically rewritten by `state_io.py`.
- **`.env`** — secrets:
  - `VIDEO_DB_API_KEY` — required.
  - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — required for Telegram.
  - `WILDWATCH_ALLOWED_ORIGINS` — optional comma-list whitelisted by CSRF/Origin guard (in addition to localhost/127.0.0.1/0.0.0.0).
  - `WILDWATCH_ALLOW_NO_ORIGIN=1` — escape hatch for trusted CLI clients. Read once at import, WARNING-logged at startup.
  - `WILDWATCH_TRUSTED_PROXY=1` — behind nginx / Cloudflare / ALB. Rate limiter reads first IP from `X-Forwarded-For`. **NEVER set in direct-exposure deployment** — attacker can spoof XFF.
  - `WILDWATCH_WEBHOOK_SECRET` — optional shared secret for `/webhook/{tier}` (header `X-WildWatch-Secret`, verified via `hmac.compare_digest`).
  - `VIDEODB_EVENTS_DIR` — optional dir for `scripts/ws_listener.py` to write `videodb_ws_id` (default `/tmp`). Files created `0o600 + O_NOFOLLOW`.
- **`.env.example`** — template.

### Upload protections (`POST /api/sources/upload`)

Three defences before bytes hit disk:
1. **Per-IP rate limit** — token bucket, capacity 3, refill 1/min/IP, LRU 50k cap. 429 over cap. IPv6 + `:port` normalised so `[::1]:1234` and `[::1]:9999` share one bucket.
2. **MIME magic-byte sniff** — first 32 bytes must match `mp4 / mov / webm / mkv / avi / mpeg-ps / flv`. 415 on miss. MPEG-TS excluded (sync-byte not bypass-resistant).
3. **Size cap** — `UPLOAD_MAX_BYTES = 500 MB`. 413 over.

413 and 415 paths both delete orphan Source row + emit `source_deleted` SSE.

---

## 8. Glossary

| Term | Meaning |
|---|---|
| **RTSP / RTMP** | Live-video protocols. Wildlife cameras = RTSP; gaming/social = RTMP. We read either. |
| **HLS** | Streaming format VideoDB hands back for browser playback. |
| **VideoDB sandbox** | Dedicated cloud GPU slot, hourly-billed, runs perception models. |
| **Index** | One AI "lens" pointed at a stream — e.g. species index runs every 5s. We run 4 per stream. |
| **Event** | Server-side rule: "fire if index outputs match this prompt." We define 18. |
| **Alert** | Webhook POST VideoDB sends when an event fires. Received at `/webhook/{tier}`. |
| **Tier** | Severity: 1=info (🟢), 2=notable (🟡), 3=urgent (🔴). |
| **Correlation** | "Audio says alarm call AND visual says fleeing within 90s → confirmed tier-3." |
| **Digest** | 90s auto-edited highlight reel + analytics + Telegram album. |
| **Path B** | Synthesised webhook flow for uploads (VideoDB `create_alert` is rtstream-only). |
| **Sandbox / SDK / Skill** | VideoDB = platform. SDK = Python lib. Skill = Claude Code plugin for SDK patterns. |

---

## 9. Where to start reading

- **AI prompts?** → `prompts/species.txt` + siblings.
- **Alert pipeline?** → `wildwatch/webhooks.py:receive_alert` → `telegram.py` + `event_log.py`.
- **Dashboard?** → `wildwatch/static/dashboard.html` + `wildwatch/dashboard.py`.
- **Cross-modal?** → `wildwatch/correlation.py` + `scripts/run_correlation.py:CORRELATION_RULES`.
- **Digest reel?** → `wildwatch/digest.py`.
- **Add new stream?** → edit `config.py`, run `python scripts/bootstrap.py`.
- **Test one prompt cheap?** → `python scripts/iterate_prompt.py prompts/species.txt samples/triggers/<clip>.mp4`.

Next: `docs/FEATURE_FLOWS.md` for visual end-to-end flows.
