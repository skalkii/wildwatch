# WildWatch

**Real-time perception agent for protected-area wildlife monitoring.**

WildWatch turns continuous wildlife livestreams into structured ecological observations — species, behavior, environment, threats — with tiered alerts, cross-modal reasoning, and auto-generated daily highlight reels. Built on the [VideoDB](https://videodb.io) SDK for the **Eyes & Ears** 48-hour hackathon (May 16–18, 2026).

> Submission target: GitHub repo + 60–180s demo video + 200-word writeup at https://hackday.videodb.io.

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

---

## Quickstart

```bash
# 1. Clone
git clone https://github.com/skalkii/wildwatch.git && cd wildwatch

# 2. Python 3.12 venv + install (videodb pinned to hackathon branch)
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 3. Configure
cp .env.example .env
# Edit .env: set VIDEO_DB_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# 4. (Live demo) Start mediamtx bridge for YouTube → RTSP
docker compose -f bridge/docker-compose.yml up -d
./bridge/start_bridge.sh "<youtube_url>" wildafrica

# 5. (Live demo) Expose webhook receiver via tunnel
uvicorn wildwatch.webhooks:app --reload &
cloudflared tunnel --url http://localhost:8000  # capture public URL → WEBHOOK_BASE_URL

# 6. Bootstrap: connect streams, create events, wire alerts
python scripts/bootstrap.py
```

State persists to `.state.json`. Re-running `bootstrap.py` is idempotent.

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
