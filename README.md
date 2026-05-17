# WildWatch

**Real-time perception agent for protected-area wildlife monitoring.**

WildWatch turns continuous wildlife livestreams (and uploaded clips) into structured ecological observations — species, behavior, environment, threats — with tiered alerts, cross-modal reasoning, and a one-click daily summary reel narrated by AI. Built end-to-end on the [VideoDB](https://videodb.io) SDK for the **Eyes & Ears** hackathon.

---

## What it does in 60 seconds

A single ranger trying to protect a 100 km² reserve with three or four wildlife cameras streaming day and night can't possibly watch every frame — yet the meaningful moments (leopard at the waterhole, herd in distress, chainsaw at 2 a.m.) are exactly the moments they need to know about.

**WildWatch is the always-on observer that watches and listens for them.** Four AI "lenses" (species, behaviour, environment, audio) sit on top of every livestream or uploaded clip. When any lens spots something noteworthy, an alert lands on Telegram and the dashboard within seconds — colour-coded by urgency, with a tappable clip of the actual moment.

**Cross-modal reasoning** stops single-signal noise: *"an alarm call AND fleeing animals within 90 seconds"* escalates to red, where either signal alone would not. **Daily summary** stitches deduped highlights into a narrated 90-second reel via VideoDB's `generate_text` + `generate_voice` + Timeline editor.

No in-house ML. The whole project leans on VideoDB's prompt-driven VLM indexing — drop a new prompt file in to add a new lens.

**Read these next:**
- 📁 [`docs/REPO_MAP.md`](docs/REPO_MAP.md) — every folder and file explained.
- 🔀 [`docs/FEATURE_FLOWS.md`](docs/FEATURE_FLOWS.md) — diagrams of every feature.
- ⚠️ [`docs/GENAI_ROADMAP.md`](docs/GENAI_ROADMAP.md) — what's wired, what's not, and the one real platform limitation.

---

## The problem

Existing conservation AI (SpeciesNet, Wildlife Insights, MegaDetector) processes **single camera-trap images** for **species classification only**. WildWatch tackles four gaps in the literature simultaneously:

1. **Continuous stream processing** — 24/7 livestreams in real time, not snapshots.
2. **Behavioral classification** — not just "what species" but "what is it doing".
3. **Multimodal reasoning** — audio + visual co-witnessing in one stack.
4. **Anthropogenic threat detection** — gunshots, chainsaws, vehicles in protected areas.

---

## Architecture

```
┌────────────────────────┐
│  Stream sources        │
│  - RTSP / RTMP camera  │
│  - YouTube Live (via   │
│    mediamtx bridge)    │
│  - Uploaded file / URL │
└──────────┬─────────────┘
           │
           ▼
┌─────────────────────────┐
│ VideoDB RTStream OR     │
│ Video (uploaded)        │
└──────────┬──────────────┘
           │
   ┌───────┼───────┬─────────────┐
   ▼       ▼       ▼             ▼
┌─────┐ ┌─────┐ ┌─────┐       ┌─────┐
│SPEC.│ │BEHV.│ │ENV. │       │AUDIO│  ← 4 parallel indexes
└──┬──┘ └──┬──┘ └──┬──┘       └──┬──┘
   │       │       │             │
   └───────┼───────┼─────────────┘
           ▼
┌─────────────────────────┐
│ Events + Alerts         │
│ (rtstreams)             │
│ Path-B sweep (uploads)  │
└──────────┬──────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌──────────────┐
│Telegram │ │ Dashboard    │
│ (bot)   │ │ (SSE live)   │
└─────────┘ └──────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Daily summary (manual)      │
│ generate_text + generate_   │
│ voice + Timeline reel       │
└─────────────────────────────┘
```

---

## Depth of VideoDB SDK usage

| Layer | Primitive | How WildWatch uses it |
|---|---|---|
| See | `coll.connect_rtstream()` | Live RTSP feeds (direct cameras, or YouTube-bridged via mediamtx + bore). |
| See | `coll.upload()` | Uploaded clips + URL ingest for offline iteration and the demo trigger flow. |
| Understand | `rtstream.index_visuals()` / `video.index_scenes()` | Three visual lenses (species, behaviour, environment) — separate indexes, not one prompt. |
| Understand | `rtstream.index_audio()` / `video.index_audio()` | Audio lens (biophony + anthropophony). See limitation below. |
| Understand | `rtstream.search()` / `video.search()` | Cross-index queries in the correlation loop + Path-B sweep. |
| Act | `conn.create_event()` | 18 events defined ONCE on the connection, reused across streams. |
| Act | `index.create_alert()` | Webhooks → FastAPI → Telegram with playable clip URLs. |
| Act | `conn.connect_websocket()` | Optional dual-delivery channel (skill convention). |
| Act | `rtstream.generate_stream()` / `video.generate_stream(timeline=…)` | Playable clip URLs attached to every alert. |
| Act | `coll.generate_text()` | Telegram alert rewriter + daily summary paragraph. |
| Act | `coll.generate_voice()` | Daily summary narration (`AudioAsset` on the reel timeline). |
| Act | `coll.generate_music()` | Optional reel soundtrack. |
| Act | Programmable editor (`Timeline`, `Track`, `Clip`, `VideoAsset`, `TextAsset`, `AudioAsset`, `Transition`) | Daily summary reel composition. |

One shared Medium `SandboxTier` for every index/generation call, status-gated, idle-timeout 600s — so credit burn is bounded.

Built with the official [VideoDB Skills plugin](https://github.com/video-db/skills). Installed via `/plugin install videodb@videodb-skills` in Claude Code.

---

## Local setup (free-tier, no paid services)

Tested on macOS 14+ (Apple Silicon and Intel) and Ubuntu 22.04.

### Prerequisites

| Tool | Why | Install |
|---|---|---|
| Python 3.12 | the app | `brew install python@3.12` / `apt install python3.12` |
| Docker Desktop | RTSP relay + tunnel (live feeds only) | https://docs.docker.com/desktop/ |
| ffmpeg + streamlink | YouTube → RTSP bridge (live feeds only) | `brew install ffmpeg streamlink` |
| `cloudflared` | public webhook URL (so VideoDB can call back) | `brew install cloudflare/cloudflare/cloudflared` |
| A VideoDB account | the AI brain | https://console.videodb.io — free credits on signup |
| A Telegram bot | alerts | message [@BotFather](https://t.me/BotFather), `/newbot`, copy the token |

### 1. Clone + Python env

```bash
git clone https://github.com/skalkii/wildwatch.git
cd wildwatch
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Configure `.env`

```bash
cp .env.example .env
```

Fill in:
- `VIDEO_DB_API_KEY` — from https://console.videodb.io → API Keys.
- `TELEGRAM_BOT_TOKEN` — from BotFather.
- `TELEGRAM_CHAT_ID` — send any message to your bot, then `curl https://api.telegram.org/bot<TOKEN>/getUpdates` → copy the `chat.id`.
- `WEBHOOK_BASE_URL` — set in step 4.

### 3. Start the server

```bash
uvicorn wildwatch.webhooks:app --host 127.0.0.1 --port 8000 --reload
```

Open http://localhost:8000/ — the dashboard.

### 4. Public webhook URL (for live alerts to reach you)

In a separate terminal:

```bash
cloudflared tunnel --url http://localhost:8000
```

Copy the printed `https://*.trycloudflare.com` URL into `.env` as `WEBHOOK_BASE_URL`, then restart uvicorn. (Skip this step for the upload-only demo flow below — Path-B sweep delivers locally.)

---

## Demo flow (no live feed needed)

The point of the demo is to show the full pipeline. Live wildlife feeds are stale most of the day and unreliable for scheduled demos — so this flow uses uploaded sample clips.

1. **Open the dashboard.** Sources tab → **+ Add source** → **File upload** or paste a YouTube URL.
2. **Wait for `ready`.** The card pulses through `queued → connecting → ingesting → indexing → ready` (1-3 min depending on length). Auto scene + audio indexing kicks off the moment upload finishes.
3. **Watch the Alerts feed.** Path-B sweep searches each index for gunshot / chainsaw / rare-species / alarm-call / human-intrusion patterns and fires synthesised webhooks. Telegram buzzes; the dashboard's Alerts tab fills in.
4. **(Optional) Fire test alerts.** Alerts tab → "Test the alert system" panel → 🟢 / 🟡 / 🔴 buttons. Useful for sanity-checking Telegram setup.
5. **Build the daily summary.** Alerts tab → **Daily summary → Build**. The backend:
   - reads the last 24h of events from `data/live_event_log.jsonl`;
   - dedupes by `(label, source[:48], 60s bucket)` so one scene contributes one shot;
   - picks the top 10 by tier + recency;
   - composes a Timeline reel (one VideoAsset per event + tier-label TextAsset overlays);
   - calls `coll.generate_text` to write a 45-65 word ranger-friendly paragraph;
   - calls `coll.generate_voice` to narrate that paragraph and attach the audio as an `AudioAsset` track;
   - returns the player URL + summary text. UI shows both.
6. **Click "▶ Play reel"** to watch the narrated compilation.

Total demo length: ~3 minutes once a clip is uploaded.

---

## Live feeds (optional, hacky free-tier)

Live YouTube wildlife streams work — but the path involves a bridge container because VideoDB only accepts `rtsp://` / `rtmp://` for live, and YouTube serves HLS.

```bash
# 1. Bring up mediamtx (RTSP relay on :8554) + bore (public TCP tunnel)
docker compose -f bridge/docker-compose.yml up -d
docker compose -f bridge/docker-compose.yml logs bore | grep "listening at"
# → "listening at bore.pub:<remote_port>"  — copy that port

# 2. Pump a YouTube live URL into the relay (one terminal per stream)
./bridge/start_bridge.sh "https://www.youtube.com/watch?v=8J9USywkGmw" madikwe

# 3. In the dashboard: + Add source → RTSP → rtsp://bore.pub:<remote_port>/madikwe
```

Known caveats:

- **bore.pub rotates the remote port on every reconnect.** VideoDB rtstreams point at a fixed URL, so a bore disconnect silently stales the live feed. Re-wire manually with a re-bootstrap. (A paid tunnel like Cloudflare Spectrum or ngrok reserved would fix this.)
- **Video re-encode is needed.** `bridge/start_bridge.sh` re-encodes to H.264 Main@720p — VideoDB's rtstream segmenter drops video from High@1080p YouTube feeds and produces audio-only `.ts` segments otherwise.
- Live feeds are mostly empty (waterholes at night, sleepy daytime). Expect long stretches with no alerts. The demo uses uploads precisely because of this.

---

## Known limitations

See [`docs/GENAI_ROADMAP.md`](docs/GENAI_ROADMAP.md) for the full discussion. Summary:

- **VideoDB has no native non-speech audio classification.** `video.index_audio(prompt=…)` is transcript-based and hangs `processing` forever on silent / SFX-only clips. The dashboard surfaces this with an amber "no speech — skipped" pill. Path-B sweep falls back to running audio-event queries against the visual index. A future VideoDB `index_type=audio_event` would fix this; alternatively a PANNs / YAMNet sidecar (out of scope here to keep SDK depth high).
- **bore.pub rotates ports** — see above.
- **macOS Docker Desktop** doesn't expose `network_mode: host` ports — `bridge/docker-compose.yml` uses explicit port mappings instead.

---

## Security defences (applied automatically)

- **CSRF / Origin guard** — every mutating `/api/*` request needs an `Origin`/`Referer` matching `localhost` / `127.0.0.1` / `0.0.0.0` (or a host in `WILDWATCH_ALLOWED_ORIGINS`). `/webhook/*` is exempt. CLI clients can set `WILDWATCH_ALLOW_NO_ORIGIN=1`.
- **Upload rate limit** — `POST /api/sources/upload` is token-bucketed per client IP (capacity 3, refill 1/min). Set `WILDWATCH_TRUSTED_PROXY=1` behind nginx / Cloudflare / ALB so the bucket reads `X-Forwarded-For`.
- **Upload MIME sniff** — first 32 bytes must match a known video container. Rejected uploads get deleted + a `source_deleted` SSE so the dashboard card disappears.
- **SDK pool saturation** — blocking SDK calls run through a 4-worker pool. At 2× saturation new calls raise `SDKPoolSaturated → 503` instead of queueing.
- **State file perms** — `.state.json` written `0o600` atomically (`.tmp` + fsync + rename + parent fsync). Same for `/tmp/videodb_*` files.

---

## State

Everything persists to `.state.json` (atomic write, single-process safe). `data/live_event_log.jsonl` is the append-only alert log used by the digest builder.

---

## Tests

```bash
pip install pytest pytest-asyncio pytest-mock respx httpx
pytest
```

Per-module coverage is documented in [`docs/REPO_MAP.md`](docs/REPO_MAP.md) §6.

---

## Repo layout

```
wildwatch/
├── prompts/             # The four index prompts (species, behavior, environment, audio)
├── wildwatch/           # Python package
│   ├── webhooks.py      #   FastAPI app: dashboard, /api/*, /webhook/{tier}, /api/digest/build
│   ├── dashboard.py     #   Single-page UI (HTML+CSS+JS in one file)
│   ├── sources.py       #   Source CRUD + status machine
│   ├── ingest.py        #   File / URL / RTSP → VideoDB
│   ├── events.py        #   18 event definitions + INDEX_EVENT_MAP
│   ├── wiring.py        #   index ↔ event ↔ webhook connector
│   ├── correlation.py   #   Cross-modal reasoning loop
│   ├── digest.py        #   Daily summary reel (Timeline + generate_text + generate_voice)
│   ├── telegram.py      #   Bot API send_alert with GenAI rewriter
│   ├── post_upload_analysis.py  # Path-B sweep (Telegram on uploaded clips)
│   ├── event_log.py     #   Append-only JSONL alert log
│   ├── state_io.py      #   Atomic .state.json writes
│   ├── sandbox.py       #   Shared sandbox lifecycle
│   └── prompts.py       #   Prompt loader + per-stream context
├── bridge/              # mediamtx + bore + streamlink/ffmpeg YouTube → RTSP
├── scripts/             # bootstrap.py, build_digest.py, run_correlation.py, ws_listener.py, smoke tests
├── docs/                # REPO_MAP.md, FEATURE_FLOWS.md, GENAI_ROADMAP.md, videodb-sdk-cheatsheet.md
├── samples/             # Curated reference clips + trigger manifest
└── tests/               # pytest suite
```

---

## License

MIT. See [`LICENSE`](LICENSE).
