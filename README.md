# WildWatch

**Real-time perception agent for protected-area wildlife monitoring.**

WildWatch turns continuous wildlife livestreams into structured ecological observations — species, behavior, environment, threats — with tiered alerts, cross-modal reasoning, and auto-generated daily highlight reels. Built on the [VideoDB](https://videodb.io) SDK for the **Eyes & Ears** 48-hour hackathon (May 16–18, 2026).

> Submission target: GitHub repo + 60–180s demo video + 200-word writeup at https://hackday.videodb.io.

---

## WildWatch in 60 seconds — for everyone

Imagine a single ranger trying to protect a 100 km² reserve with three or four wildlife cameras streaming day and night. There's no human way to watch every frame, and yet the meaningful moments — a leopard at the waterhole, a herd of elephants in distress, the sound of a chainsaw at 2 a.m. — are exactly the moments a ranger needs to know about right away.

**WildWatch is the always-on observer that watches and listens for them.** A team of four AI "lenses" (one for species, one for behaviour, one for the surrounding environment, one for audio) sits on top of every livestream we plug in. The moment any of them spots something noteworthy, an alert lands on the ranger's phone within seconds — colour-coded by urgency, with a tappable clip of the actual moment. At the end of every day a 90-second highlight reel summarises what happened.

The smartest part is **cross-modal reasoning**: instead of firing on a single signal (which is often noise), the system waits for two independent signals to agree. *"An alarm call AND fleeing animals within 90 seconds"* is far more likely to be a real predator event than either signal alone, so that's what gets escalated to red.

It works because we don't train any of our own AI — we use carefully written prompts to steer VideoDB's general-purpose perception model into a wildlife specialist. That makes the project cheap to run, easy to extend (drop a new prompt file in to add a new "lens"), and accurate enough for real-world conservation work.

**Two diagrams that explain everything visually:**

- 📁 [`docs/REPO_MAP.md`](docs/REPO_MAP.md) — every folder and file in plain English. Read this first if you're new.
- 🔀 [`docs/FEATURE_FLOWS.md`](docs/FEATURE_FLOWS.md) — step-by-step diagrams of every feature, from "AI sees a leopard" to "phone buzzes" to "daily reel is built."

If you're a non-technical reader, **start with the two docs above** — they avoid jargon and explain each step with file references for anyone who wants to follow up in code.

---

## The problem

Existing conservation AI (SpeciesNet, Wildlife Insights, MegaDetector) processes **single camera-trap images** for **species classification only**. The unsolved problems in the conservation tech literature:

1. **Continuous stream processing** — nobody runs these models against 24/7 livestreams in real time.
2. **Behavioral classification** — current tools stop at "what species"; they don't say "what is the animal doing."
3. **Multimodal reasoning** — bioacoustic tools (BirdNET) and visual tools are separate stacks today.
4. **Anthropogenic threat detection** — gunshots, chainsaws, vehicles in protected areas; some products exist (Rainforest Connection) but they're audio-only.

WildWatch attacks all four simultaneously using VideoDB's prompt-driven VLM indexing.

---

## Architecture

```
┌────────────────────────┐
│  Stream sources        │
│  - HDOnTap direct RTSP │
│  - YouTube Live (via   │
│    mediamtx bridge)    │
└──────────┬─────────────┘
           │
           ▼
┌─────────────────────────┐
│ VideoDB RTStream        │
│ coll.connect_rtstream() │
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
│ Events (reusable across │
│ streams) + Alerts       │
└──────────┬──────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌──────────────┐
│Webhooks │ │ WebSocket    │
│→Telegram│ │ → live UI    │
└─────────┘ └──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Correlation engine      │
│ (cross-modal reasoning) │
│ Search every 30s,       │
│ fire confirmed events   │
└──────────┬──────────────┘
           ▼
┌─────────────────────────┐
│ Daily digest reel       │
│ (programmable editing)  │
└─────────────────────────┘
```

---

## Depth of VideoDB SDK usage

WildWatch exercises **all 10 VideoDB primitives** across the See / Understand / Act layers — most submissions stop at 4–5.

| Layer | Primitive | How WildWatch uses it |
|---|---|---|
| See | `coll.connect_rtstream()` | Two streams: direct RTSP + YouTube-bridged. Demonstrates production portability. |
| See | `coll.upload()` | Recorded clips for offline prompt iteration and the digest reel source pool. |
| Understand | `rtstream.index_visuals()` | THREE separate visual indexes (species, behavior, environment) — not one omnibus prompt. |
| Understand | `rtstream.index_audio()` | One audio index covering biophony + anthropophony. The differentiator. |
| Understand | `rtstream.search()` | Cross-index queries inside the correlation engine. |
| Act | `conn.create_event()` | Events defined ONCE, reused across both streams (the design intent). |
| Act | `index.create_alert()` | Webhooks → FastAPI → Telegram with playable clip URLs. |
| Act | `conn.connect_websocket()` | Live channel for the dashboard demo. |
| Act | `rtstream.generate_stream()` | Generates playable clip URLs attached to alerts. |
| Act | Programmable editing (`Timeline`, `VideoAsset`, `TextAsset`) | Auto-generated daily highlight reel. |

**Sandbox-aware:** every index/generation call passes `sandbox_id` to a single shared Medium `SandboxTier` (gemma-4-31B-it for visual, Qwen3.5-9B for audio). One sandbox lifecycle, idle-timeout 600s, status-gated before submitting jobs.

**Built with the official [VideoDB Skills plugin](https://github.com/video-db/skills).** Installed in this repo's Claude Code session via `/plugin install videodb@videodb-skills`. The skill surfaces server-side perception primitives (See / Understand / Act) directly to the coding agent, keeping SDK shape and best-practice prompts in lockstep with `docs.videodb.io`. WildWatch is a real-world build of that pattern: continuous wildlife streams → indexed perception → tiered alerts → auto-edited reels.

---

## Quickstart

### Path A — Docker compose (recommended)

```bash
git clone https://github.com/skalkii/wildwatch.git && cd wildwatch
cp .env.example .env
# Edit .env: VIDEO_DB_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

docker compose up                  # starts mediamtx + wildwatch + bore
# (or)
docker compose --profile tunnel up # also brings up cloudflared (needs CLOUDFLARED_TUNNEL_TOKEN in .env)
```

Then open **http://localhost:8000/** — the live dashboard.

Services spun:
- `wildwatch` — FastAPI app on `:8000` (dashboard + webhook + REST API)
- `mediamtx` — RTSP relay on `:8554` for YouTube/HLS-bridged streams
- `bore` — TCP tunnel exposing `mediamtx:8554` to `bore.pub:<remote_port>` (so VideoDB can reach your local RTSP)
- `cloudflared` *(optional)* — public HTTPS tunnel for the webhook receiver

### Path B — Local dev (faster iteration)

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env  # fill creds

uvicorn wildwatch.webhooks:app --host 127.0.0.1 --port 8000 &  # dashboard
mediamtx bridge/mediamtx.yml &                                  # RTSP relay (brew install mediamtx)
bore local 8554 --to bore.pub &                                 # public RTSP tunnel
cloudflared tunnel --url http://localhost:8000 &                # public webhook URL
```

> **Security note**: the server is bound to `127.0.0.1` because every `POST/PUT/PATCH/DELETE /api/*` route is guarded by an Origin/Referer middleware that defaults to allowing `localhost` only. Browsers send `Origin` automatically; CLI clients (curl, scripts) without an `Origin` header are rejected with `403` unless `WILDWATCH_ALLOW_NO_ORIGIN=1` is set in the env. Add LAN hosts via `WILDWATCH_ALLOWED_ORIGINS=hostA,hostB`. `POST /webhook/*` is exempt — VideoDB calls it cross-origin.

---

## Use the dashboard

Open `http://localhost:8000/`. Four tabs:

| Tab | What it does |
|---|---|
| **Alerts** | Live SSE feed of every event the webhook receives. Per-tier counters. Manual 🟢🟡🔴 fire buttons. RTStream + Sandbox state panels. |
| **Sources** | Add any source: file upload (≤500 MB), URL (YouTube / HLS), or RTSP/RTMP. Each card shows live status (`queued` → `connecting` → `ingesting` → `ready`). Per-card Reconnect / Disconnect / Delete. |
| **Indexed Content** | Browse every uploaded video, its scene indexes, and recent scene records. Cross-scope search (collection / video / rtstream) with score-ranked results. |
| **Usage** | Local upper-bound credit-burn estimate (hours × rate) + raw `conn.check_usage()` SDK output + recent invoices. |

### Add a source from the UI

1. Sources tab → **+ Add source**
2. Pick file / URL / RTSP tab
3. Name it, paste/select, submit
4. Watch the card progress in real-time

### Manual API examples

```bash
# RTSP from sample stream
curl -X POST http://localhost:8000/api/sources \
  -H 'Content-Type: application/json' \
  -d '{"kind":"rtsp","input":"rtsp://samples.rts.videodb.io:8554/intruder","name":"sample"}'

# YouTube archive video (live URLs need bridge — paste the bore.pub RTSP)
curl -X POST http://localhost:8000/api/sources \
  -H 'Content-Type: application/json' \
  -d '{"kind":"youtube","input":"https://www.youtube.com/watch?v=...","name":"my-clip"}'

# Search across collection
curl -X POST http://localhost:8000/api/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"elephant OR oryx","scope":"collection"}'

# Usage snapshot
curl http://localhost:8000/api/usage | jq
```

---

## State

Everything persists to `.state.json` (atomic .tmp + rename, single-process safe). `data/live_event_log.jsonl` is the append-only alert log used by the digest builder. Re-running `bootstrap.py` is idempotent.

---

## Demo

<!-- TODO(T-39): embed final demo video link -->
*Demo video coming soon.*

---

## Repo layout

```
wildwatch/
├── prompts/             # The four index prompts (see CLAUDE.md §6)
├── wildwatch/           # Python package
│   ├── sandbox.py       # Lifecycle: ensure / managed / stop
│   ├── pipeline.py      # Stream connect, index creation, event wiring
│   ├── events.py        # 17 event definitions, INDEX_EVENT_MAP
│   ├── correlation.py   # Cross-modal reasoning loop
│   ├── webhooks.py      # FastAPI webhook receiver
│   ├── telegram.py      # Bot API send_alert
│   └── digest.py        # Daily highlight reel via programmable editing
├── bridge/              # mediamtx + streamlink/ffmpeg YouTube → RTSP
├── scripts/             # bootstrap.py, iterate_prompt.py, smoke tests
├── docs/                # SDK cheatsheet, budget, programmable-editing recipes
└── demo/                # storyboard, recording notes, final video
```

---

## Writeup

<!-- TODO(T-40): 200-word writeup for hackathon submission -->
*Writeup coming with submission.*

---

## License

MIT — see [LICENSE](LICENSE).
