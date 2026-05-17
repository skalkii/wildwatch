# CLAUDE.md — WildWatch Hackathon Handover

> **For Claude Code**: This file is your primary context for working on this project. Read it fully before touching any code. The user is participating in the VideoDB "Eyes & Ears" 48-hour hackathon (May 16-18, 2026, 10:00 IST kickoff). They are a solo builder.

---

## 0. TL;DR for humans (read this first if you're new)

WildWatch is an "always-on observer" for protected-area wildlife cameras. It plugs into any live video stream (RTSP camera, YouTube live, MP4 file) and runs four AI prompts continuously — one watching for species, one for behaviour, one for environment / threats, one listening to audio. When something matches a 18-rule library of "things worth alerting on," it pushes a colour-coded notification (green/yellow/red) to a Telegram bot and a live web dashboard, with a tappable clip of the moment. A nightly script stitches the day's highlights into a 90-second video reel.

The differentiator is **cross-modal reasoning**: instead of firing on single noisy signals, the system waits for two independent signals (audio + visual, behaviour + environment) to agree before escalating to "urgent." That's the demo-video gold moment.

There's no in-house ML training. The project leans entirely on VideoDB's vision-and-language perception model — we steer it with prompt engineering. That's a deliberate choice that maps to the hackathon's "depth of VideoDB SDK usage" scoring axis (30% of the score).

**Where to look first:**
- 📁 [`docs/REPO_MAP.md`](docs/REPO_MAP.md) — every file in the repo explained in plain English with tech labels.
- 🔀 [`docs/FEATURE_FLOWS.md`](docs/FEATURE_FLOWS.md) — Mermaid diagrams + step-by-step walkthroughs of every feature.
- 🟢 [`README.md`](README.md) — pitch + quickstart + architecture diagram.

If you're an LLM coding agent picking up this project, the rest of this file (sections 1–19) is your detailed brief. Sections 17–18 are particularly load-bearing: they encode "what NOT to do" lessons accumulated during the build.

---

## 1. Project identity

**Name:** WildWatch
**One-liner:** A real-time perception agent for protected-area wildlife monitoring. Turns continuous wildlife livestreams into structured ecological observations — species, behavior, environment, threats — with tiered alerts, cross-modal reasoning, and auto-generated daily highlight reels.
**Built on:** VideoDB SDK (Python). https://docs.videodb.io
**Submission target:** GitHub repo + 60-180 second demo video + 200-word writeup. Submit at https://hackday.videodb.io before Mon 18 May 10:00 IST.

**Judging criteria (this governs every decision):**
- Technical execution — 40%
- Creativity & originality — 30%
- Depth of VideoDB SDK usage — 30%

**The "depth of VideoDB usage" 30% is where most submissions will be thin.** Every architectural choice should ask: am I using a VideoDB primitive shallowly, or am I using its full design intent? See section 5.

---

## 2. The problem we're solving (use this framing in code comments and README)

Existing conservation AI (SpeciesNet, Wildlife Insights, MegaDetector) processes **single camera-trap images** for **species classification only**. The unsolved problems in the conservation tech literature are:

1. **Continuous stream processing** — nobody runs these models against 24/7 livestreams in real time
2. **Behavioral classification** — current tools stop at "what species"; they don't say "what is the animal doing"
3. **Multimodal reasoning** — bioacoustic tools (BirdNET) and visual tools are separate stacks today
4. **Anthropogenic threat detection** — gunshots, chainsaws, vehicles in protected areas; some products exist (Rainforest Connection) but they're audio-only

WildWatch attacks all four simultaneously using VideoDB's prompt-driven VLM indexing. This framing is **the moat for the creativity score**. Do not water it down.

---

## 3. Architecture

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
│ Search-over-index every │
│ 30s, fire confirmed     │
│ events                  │
└──────────┬──────────────┘
           ▼
┌─────────────────────────┐
│ Daily digest reel       │
│ (programmable editing)  │
└─────────────────────────┘
```

---

## 4. Repo layout

```
wildwatch/
├── README.md              # Pitch, quickstart, architecture, demo embed
├── CLAUDE.md              # This file
├── .env.example           # VIDEO_DB_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
├── pyproject.toml         # or requirements.txt
├── config.py              # Streams registry, prompt strings, event defs
├── bridge/
│   ├── docker-compose.yml # mediamtx + streamlink+ffmpeg for YouTube→RTSP
│   └── README.md          # Bridge setup notes
├── prompts/
│   ├── species.txt        # See section 6 — Index 1
│   ├── behavior.txt       # See section 6 — Index 2
│   ├── environment.txt    # See section 6 — Index 3
│   └── audio.txt          # See section 6 — Index 4
├── wildwatch/
│   ├── __init__.py
│   ├── pipeline.py        # Stream connect, index creation, event wiring
│   ├── events.py          # Event/alert definitions (matches section 6)
│   ├── correlation.py     # Cross-modal reasoning loop
│   ├── webhooks.py        # FastAPI app receiving alerts → Telegram
│   ├── telegram.py        # Telegram bot send_message + send_video
│   ├── digest.py          # Daily highlight reel via programmable editing
│   └── ui.py              # Optional: minimal live dashboard
├── scripts/
│   ├── bootstrap.py       # One-shot: connect streams, create indexes/events
│   ├── iterate_prompt.py  # Run a prompt against sample_clip.mp4, print output
│   └── replay.py          # Replay event log for demo
├── samples/
│   └── sample_clip.mp4    # 30-min recorded clip for offline prompt iteration
└── demo/
    ├── storyboard.md      # 90-second demo storyboard
    └── recording.md       # Notes on what to capture
```

---

## 5. VideoDB primitives — use ALL of them, deeply

The judges built VideoDB around three layers: **See, Understand, Act**. Most submissions will skip Act entirely. Don't.

| Layer | Primitive | How WildWatch uses it |
|---|---|---|
| See | `coll.connect_rtstream(rtsp_url=...)` | Two streams: one direct RTSP, one bridged from YouTube. Demonstrates production portability. |
| See | `coll.upload(url=youtube_url)` | Recorded clips for offline prompt iteration and the digest reel source pool. |
| Understand | `rtstream.index_visuals(prompt=...)` | THREE separate visual indexes (species, behavior, environment) — not one omnibus prompt. Demonstrates index composability. |
| Understand | `rtstream.index_audio(prompt=...)` | One audio index covering biophony + anthropophony. The differentiator. |
| Understand | `rtstream.search(query, index_id, time_range=...)` | Used in correlation engine to detect multi-modal patterns. |
| Act | `conn.create_event(event_prompt=..., label=...)` | Defined ONCE, reused across both streams (this is the design intent). |
| Act | `index.create_alert(event_id, callback_url=...)` | Webhooks → FastAPI → Telegram with clip URL. |
| Act | `conn.connect_websocket()` | Live dashboard channel for the demo. |
| Act | `rtstream.generate_stream(start, end)` | Generate playable clip URLs to attach to alerts. |
| Act | Programmable editing | Auto-generate daily highlight reel from flagged shots. |

**If your code only uses 4–5 of these, you're at "shallow" depth. WildWatch uses all 10.**

---

## 6. The four index prompts

These are not drafts. They are the production prompts the user worked through. Use them verbatim or improve them with explicit reasoning — never water them down. Put each in `prompts/<name>.txt` and load in `config.py`.

### prompts/species.txt

```
You are a wildlife observer monitoring a continuous video feed from a protected area. Your job is to identify and count every animal visible in this scene chunk.

CONTEXT
The feed is from {location_context}. Expected species include: {species_list}. Time of day and infrared mode may vary. Treat IR/grayscale footage with extra caution on species identification.

FOR EACH ANIMAL VISIBLE, REPORT:
- SPECIES: state the species. Use "possibly [species]" if not certain. Use "unidentified [taxonomic group]" (e.g., "unidentified antelope", "unidentified raptor") if species cannot be determined.
- COUNT: how many of this species are visible. If a group, give a precise count up to 10 and an estimate beyond ("approximately 15-20").
- AGE/SEX if clearly visible: adult/juvenile/calf; male/female only when dimorphism is unambiguous (e.g., male lion mane, bull elephant tusks). Otherwise omit.
- POSITION: brief — "at water edge", "background", "approaching from left", "drinking", etc.

ALSO REPORT:
- TOTAL_ANIMALS_VISIBLE: integer
- SCENE_STATE: one of [empty, single_animal, small_group, large_aggregation, mixed_species]
- LIGHT_MODE: [daylight, dusk, dawn, ir_night, low_light]

FORMAT
Use this structure exactly:
[SCENE] light_mode=<mode>; total=<n>; state=<state>
[ANIMAL] species=<name>; count=<n>; age_sex=<descriptor or unknown>; position=<brief>
[ANIMAL] species=<name>; count=<n>; age_sex=<descriptor or unknown>; position=<brief>
[NOTES] <any one-line observation worth flagging>

WHAT TO SKIP
- Do not describe vegetation, weather, or ground conditions here (separate index handles that).
- Do not infer behavior beyond position (separate index handles that).
- Do not guess species when uncertain — "unidentified" is correct and useful.
- If the scene is empty, output: [SCENE] light_mode=<mode>; total=0; state=empty
```

### prompts/behavior.txt

```
You are an ethologist annotating animal behavior from a continuous wildlife feed. Your job is to describe what animals are doing and how they interact with each other.

CONTEXT
This is a {location_context} feed. You are observing behavior, not identifying species (a separate index handles species). Use generic terms ("the larger animal", "the adult", "the group") if needed.

FOR EACH ANIMAL OR GROUP, REPORT BEHAVIOR FROM THIS VOCABULARY:
- Maintenance: drinking, foraging, grazing, browsing, resting, sleeping, grooming, dust_bathing, mud_bathing, wallowing
- Locomotion: walking, running, fleeing, approaching, retreating, swimming
- Social: nursing, mating, courtship_display, dominance_display, submission, allogrooming, play
- Vigilance & stress: alert_posture, scanning, frozen, alarm_response, agitated
- Aggression: chasing, fighting, threat_display, biting, kicking
- Parental: feeding_young, leading_young, defending_young
- Other: investigating, marking_territory, vocalizing, drinking_interrupted

FOR INTERACTIONS BETWEEN ANIMALS, REPORT:
- TYPE: one of [predator_prey, dominance, mating, parent_offspring, conspecific_aggression, interspecific_tension, peaceful_coexistence]
- PARTICIPANTS: brief descriptors (e.g., "large adult and smaller juvenile")
- INTENSITY: low / medium / high

ALSO FLAG:
- DOMINANT_BEHAVIOR: what is the most common behavior in this scene
- BEHAVIORAL_ANOMALY: anything unusual — limping, isolation from group, abnormal posture, apparent injury, repeated interrupted_drinking (a known predator-presence proxy)

FORMAT
[BEHAVIOR] subject=<descriptor>; action=<term from vocabulary>; intensity=<low/med/high or n/a>
[INTERACTION] type=<type>; participants=<brief>; intensity=<level>
[DOMINANT] <single behavior term>
[ANOMALY] <description, or "none">

EXAMPLES
[BEHAVIOR] subject=large_adult_left; action=drinking; intensity=n/a
[BEHAVIOR] subject=juvenile_center; action=play; intensity=med
[INTERACTION] type=parent_offspring; participants=adult and juvenile; intensity=low
[DOMINANT] drinking
[ANOMALY] one adult shows interrupted_drinking pattern, raising head repeatedly

WHAT TO SKIP
- Don't identify species (handled elsewhere).
- Don't describe weather or vegetation (handled elsewhere).
- Don't speculate on intent beyond the vocabulary — "the lion looked angry" is not useful; "threat_display, high intensity" is.
- If no animals are visible, output only: [DOMINANT] none; [ANOMALY] none
```

### prompts/environment.txt

```
You are an environmental monitor describing scene conditions at a wildlife observation site. Your job is to capture the physical and ecological context that gives behavior its meaning.

REPORT THE FOLLOWING:

TIME_OF_DAY: [pre_dawn, dawn, morning, midday, afternoon, dusk, early_night, late_night]
  Infer from light quality. If IR mode is on, time is likely night unless otherwise indicated.

LIGHT_MODE: [daylight, golden_hour, dusk, ir_night, low_light_color]

WEATHER: [clear, overcast, light_rain, heavy_rain, storm, fog, dust]; mention wind if visible in vegetation

WATER_STATE (if waterhole visible): [full, normal, low, very_low_drying, dry, flooded]; mention turbidity if relevant

VEGETATION_STATE: [lush_green, mixed, dry_yellow, scorched_burnt, sparse]; flag recent fires or grazing pressure

GROUND_FEATURES: any of [tracks_visible, dung_visible, mud, dust, sand, rock, leaf_litter]; carcass_present if applicable

ENVIRONMENTAL_FLAGS — flag any of these explicitly:
- carcass_or_remains_visible
- recent_kill_evidence (blood, vultures, drag marks)
- environmental_hazard (smoke, flood, fallen tree blocking access)
- camera_obstruction (lens dirty, vegetation in frame, partial occlusion)
- camera_failure (frozen frame, severe artifacts, complete dark not explained by night)
- human_made_object_visible (vehicle, structure, fence, litter)

FORMAT
[TIME] <slot>
[LIGHT] <mode>
[WEATHER] <state>; wind=<none/light/strong>
[WATER] <state>  (omit line if no water in frame)
[VEG] <state>
[GROUND] <comma-separated features>
[FLAGS] <comma-separated flags, or "none">

WHAT TO SKIP
- Animals — handled by other indexes.
- Sounds — handled by audio index.
- Don't speculate about long-term trends from one frame; just report current state.
```

### prompts/audio.txt

```
You are a bioacoustic monitor analyzing audio from a continuous wildlife stream. Your job is to identify what is being heard — both natural sounds and human-origin sounds — and flag anything that warrants attention.

CONTEXT
The stream is from {location_context}. Expected biophony includes: {expected_sounds}.

CLASSIFY ALL DISTINCT SOUNDS HEARD INTO THESE CATEGORIES:

BIOPHONY — animal sounds:
- Mammal vocalizations: roars, growls, barks, snorts, trumpets, chuffs, whoops, sawing_calls, distress_calls, alarm_calls. Identify species when distinctive (lion_roar, hyena_whoop, elephant_trumpet, baboon_bark, leopard_sawing). Otherwise "unidentified_mammal_<type>".
- Bird vocalizations: songs, calls, alarm_calls. Identify species when distinctive (fish_eagle, hornbill, francolin). Otherwise "unidentified_bird_<type>".
- Insects/amphibians: cicada_chorus, frog_chorus, cricket_chorus
- Physical sounds: drinking_splash, hooves, footsteps_heavy, branch_break

GEOPHONY — non-anthropogenic environmental sounds:
- Wind, rain_light, rain_heavy, thunder, water_flow, leaf_rustle

ANTHROPOPHONY — human-origin sounds. FLAG THESE WITH HIGH PRIORITY:
- gunshot, chainsaw, vehicle_engine, motorcycle, aircraft_overhead, voices_human, machinery, dog_bark_domestic

SPECIAL ATTENTION SIGNALS — report explicitly if heard:
- ALARM_CALL: any species producing what sounds like a predator-warning vocalization
- DISTRESS_CALL: high-pitched, repeating, urgent vocalizations indicating an animal in trouble
- PREDATOR_VOCALIZATION: roar, growl, sawing call, hunting bark
- ABNORMAL_SILENCE: scene is conspicuously quiet given the expected biophony — often a predator presence indicator

FORMAT
[SOUND] category=<biophony/geophony/anthropophony>; type=<term>; species=<if known, else unknown>; intensity=<faint/clear/loud>; confidence=<low/med/high>
[SIGNAL] <ALARM_CALL/DISTRESS_CALL/PREDATOR_VOCALIZATION/ABNORMAL_SILENCE if applicable>
[SUMMARY] <one line on overall acoustic scene>

EXAMPLES
[SOUND] category=biophony; type=alarm_call; species=baboon; intensity=loud; confidence=high
[SOUND] category=anthropophony; type=vehicle_engine; species=unknown; intensity=faint; confidence=med
[SIGNAL] ALARM_CALL
[SUMMARY] Multiple baboon alarm calls; vehicle audible in distance.

WHAT TO SKIP
- Don't describe the visual scene.
- Don't speculate about what caused a sound — just report what you hear.
- If audio is silent or only background hum, output: [SOUND] category=geophony; type=ambient_only; intensity=faint; confidence=high; [SUMMARY] no notable acoustic events
- If audio is unavailable or unintelligible, output: [SUMMARY] audio_unavailable
```

---

## 7. Events — define once, reuse across streams

Events are server-side rules. Per VideoDB design intent, they should be **created once on the connection** and **alerts wired separately per index per stream**. Demonstrate this pattern in code.

```python
# wildwatch/events.py

EVENT_DEFINITIONS = [
    # ──── Species index events ────
    {
        "id_var": "rare_species",
        "label": "rare_species_sighting",
        "tier": 1,
        "prompt": "Detect when a rare or flagship species is reported: leopard, rhino, wild dog, cheetah, pangolin, or any species marked as IUCN endangered.",
    },
    {
        "id_var": "mixed_aggregation",
        "label": "mixed_aggregation",
        "tier": 1,
        "prompt": "Detect when scene_state is mixed_species and total animals is greater than 5 — multi-species aggregation event.",
    },
    {
        "id_var": "juvenile_present",
        "label": "juvenile_present",
        "tier": 1,
        "prompt": "Detect when any reported animal has age_sex containing 'juvenile', 'calf', 'cub', 'chick', or 'foal'.",
    },
    {
        "id_var": "large_aggregation",
        "label": "large_aggregation",
        "tier": 2,
        "prompt": "Detect when total animals visible is 15 or more — large aggregation event.",
    },
    # ──── Behavior index events ────
    {
        "id_var": "predator_activity",
        "label": "predator_activity",
        "tier": 2,
        "prompt": "Detect when behavior includes any of: fleeing, alarm_response, chasing, threat_display, frozen — or when interaction type is predator_prey.",
    },
    {
        "id_var": "parental_care",
        "label": "parental_care",
        "tier": 1,
        "prompt": "Detect when behavior contains nursing, feeding_young, leading_young, defending_young, or interaction type is parent_offspring.",
    },
    {
        "id_var": "welfare_concern",
        "label": "welfare_concern",
        "tier": 3,
        "prompt": "Detect when an animal is reported with limping, apparent injury, abnormal posture, or isolation_from_group in the anomaly field.",
    },
    {
        "id_var": "notable_social",
        "label": "notable_social_behavior",
        "tier": 1,
        "prompt": "Detect courtship_display, mating, or play behavior.",
    },
    # ──── Environment index events ────
    {
        "id_var": "mortality_event",
        "label": "mortality_event",
        "tier": 3,
        "prompt": "Detect when flags contain carcass_or_remains_visible or recent_kill_evidence.",
    },
    {
        "id_var": "human_intrusion_visual",
        "label": "potential_human_intrusion_visual",
        "tier": 3,
        "prompt": "Detect when flags contain human_made_object_visible AND the time is night, or when human-made objects are flagged in a context where they should not be present.",
    },
    {
        "id_var": "camera_health",
        "label": "camera_health_issue",
        "tier": 1,
        "prompt": "Detect when flags contain camera_obstruction or camera_failure.",
    },
    {
        "id_var": "water_critical",
        "label": "water_critical",
        "tier": 2,
        "prompt": "Detect when water state is very_low_drying or dry — flag for drought monitoring.",
    },
    # ──── Audio index events ────
    {
        "id_var": "gunshot",
        "label": "POACHING_ALERT_GUNSHOT",
        "tier": 3,
        "prompt": "Detect when any sound is classified as gunshot in anthropophony, with confidence medium or high.",
    },
    {
        "id_var": "chainsaw",
        "label": "ILLEGAL_LOGGING_ALERT",
        "tier": 3,
        "prompt": "Detect when any sound is classified as chainsaw with confidence medium or high.",
    },
    {
        "id_var": "human_intrusion_audio",
        "label": "human_intrusion_audio",
        "tier": 3,
        "prompt": "Detect when anthropophony sounds (vehicle_engine, motorcycle, voices_human) are reported with intensity clear or loud, at any time, or with any intensity during night hours.",
    },
    {
        "id_var": "alarm_call",
        "label": "alarm_call_detected",
        "tier": 2,
        "prompt": "Detect when SIGNAL contains ALARM_CALL or when any alarm_call sound type is reported with confidence medium or high.",
    },
    {
        "id_var": "predator_vocal",
        "label": "predator_vocalization",
        "tier": 2,
        "prompt": "Detect when SIGNAL contains PREDATOR_VOCALIZATION, or when sound type contains lion_roar, leopard_sawing, hyena_whoop, or any term tagged as a predator vocalization.",
    },
    {
        "id_var": "acoustic_silence",
        "label": "acoustic_anomaly_silence",
        "tier": 2,
        "prompt": "Detect when SIGNAL contains ABNORMAL_SILENCE.",
    },
]

# Wire-up matrix: which events attach to which index
INDEX_EVENT_MAP = {
    "species":     ["rare_species", "mixed_aggregation", "juvenile_present", "large_aggregation"],
    "behavior":    ["predator_activity", "parental_care", "welfare_concern", "notable_social"],
    "environment": ["mortality_event", "human_intrusion_visual", "camera_health", "water_critical"],
    "audio":       ["gunshot", "chainsaw", "human_intrusion_audio", "alarm_call",
                    "predator_vocal", "acoustic_silence"],
}
```

---

## 8. Streams

```python
# config.py — streams registry

STREAMS = {
    "namibia_waterhole": {
        "name": "Namibia Waterhole (HDOnTap)",
        "rtsp_url": None,  # Fill from HDOnTap; check their embed page for direct stream URL
        "youtube_url": "https://www.youtube.com/watch?v=AeMUdOPFcXI",  # Etosha Okaukuejo fallback
        "use_bridge": True,  # True if YouTube, needs mediamtx
        "location_context": "Namib Desert waterhole, Gondwana Namibia Park",
        "species_list": "oryx, springbok, gemsbok, zebra, kudu, elephant, giraffe, "
                        "black-backed jackal, brown hyena, leopard, lion, "
                        "various birds (ostrich, vulture, francolin)",
        "expected_sounds": "wind, drinking splashes, hooves, oryx snorts, jackal calls, "
                           "bird songs, occasional lion roars, hyena whoops at night",
    },
    "wild_africa_live": {
        "name": "Wild Africa Live (multi-cam)",
        "rtsp_url": None,
        "youtube_url": "https://www.youtube.com/watch?v=vr4o_AsrU1k",
        "use_bridge": True,
        "location_context": "Rotating African reserves: South Africa, Kenya, Botswana, Namibia, Zimbabwe",
        "species_list": "Big Five (lion, leopard, elephant, rhino, buffalo), "
                        "zebra, giraffe, wildebeest, impala, kudu, warthog, "
                        "hippo, crocodile, hyena, wild dog, baboon, vervet monkey, "
                        "vultures, eagles, hornbills, fish eagles",
        "expected_sounds": "fish eagle calls, baboon barks, lion roars, "
                           "elephant trumpets, hippo bellows, various antelope alarm snorts",
    },
}

# VideoDB sample stream — guaranteed-working fallback for demos
FALLBACK_RTSP = "rtsp://samples.rts.videodb.io:8554/intruder"
```

---

## 9. The YouTube → RTSP bridge

```yaml
# bridge/docker-compose.yml
version: "3.8"
services:
  mediamtx:
    image: bluenviron/mediamtx:latest
    container_name: wildwatch-mediamtx
    network_mode: host  # easier on dev laptops
    environment:
      - MTX_PROTOCOLS=tcp
    ports:
      - "8554:8554"   # RTSP
      - "1935:1935"   # RTMP
      - "8888:8888"   # HLS (optional)
```

```bash
# bridge/start_bridge.sh
#!/usr/bin/env bash
# Usage: ./start_bridge.sh <youtube_url> <stream_name>
# Example: ./start_bridge.sh "https://www.youtube.com/watch?v=vr4o_AsrU1k" wildafrica
set -euo pipefail
YOUTUBE_URL="$1"
STREAM_NAME="$2"
streamlink "$YOUTUBE_URL" best -O \
  | ffmpeg -re -i pipe:0 -c copy -f rtsp "rtsp://localhost:8554/${STREAM_NAME}"
```

Verify with VLC: `vlc rtsp://localhost:8554/wildafrica` should show the stream.

---

## 10. The cross-modal correlation engine

This is the "perception agent" pitch in action. Most submissions won't have anything like this. Do not skip it.

```python
# wildwatch/correlation.py — pseudocode-level sketch, fill in real SDK calls

import asyncio
from datetime import datetime, timedelta

CORRELATION_RULES = [
    {
        "name": "confirmed_predator_event",
        "tier": 3,
        "window_seconds": 90,
        "queries": [
            ("audio",    "alarm_call OR predator_vocalization OR predator vocal"),
            ("behavior", "fleeing OR alarm_response OR frozen OR threat_display"),
        ],
        "synthesis_label": "CONFIRMED_PREDATOR_EVENT",
    },
    {
        "name": "confirmed_human_intrusion",
        "tier": 3,
        "window_seconds": 120,
        "queries": [
            ("audio",       "vehicle_engine OR voices_human OR gunshot OR chainsaw"),
            ("environment", "human_made_object_visible"),
        ],
        "synthesis_label": "CONFIRMED_HUMAN_INTRUSION",
    },
    {
        "name": "stress_then_silence",
        "tier": 2,
        "window_seconds": 180,
        "queries": [
            ("behavior", "alarm_response OR alert_posture"),
            ("audio",    "ABNORMAL_SILENCE"),
        ],
        "synthesis_label": "PREDATOR_APPROACH_PATTERN",
    },
]

async def correlation_loop(rtstream, indexes_by_kind, interval_s=30):
    """
    Every interval_s, for each rule, search across indexes within the rule's
    time window. If both queries return hits, fire a synthesized event with
    evidence pointing to the underlying shots.
    """
    while True:
        now = datetime.utcnow()
        for rule in CORRELATION_RULES:
            window_start = now - timedelta(seconds=rule["window_seconds"])
            hits = {}
            for index_kind, query in rule["queries"]:
                index = indexes_by_kind[index_kind]
                # NOTE: confirm exact search API shape with VideoDB docs
                results = rtstream.search(
                    query=query,
                    index_id=index.id,
                    # time_range arg name may vary — verify
                )
                hits[index_kind] = results.shots if results else []
            if all(hits.values()):
                await fire_synthesized_event(rule, hits)
        await asyncio.sleep(interval_s)

async def fire_synthesized_event(rule, hits):
    # Post to Telegram with both pieces of evidence
    # This shows "the agent reasoned across modalities" — the demo gold moment
    ...
```

⚠️ **IMPORTANT for Claude Code**: verify VideoDB's exact search API for time-windowed search over an RTStream index before implementing. The docs sketch this but the precise argument names should be checked against the actual SDK. Look at `docs.videodb.io/llms.txt` for the full doc index.

---

## 11. Webhooks → Telegram

The killer demo move is "phone buzzes, clip plays." Make this work first; everything else can fail and you still have a demo.

```python
# wildwatch/webhooks.py

from fastapi import FastAPI, Request
from .telegram import send_alert

app = FastAPI()
TIER_EMOJI = {1: "🟢", 2: "🟡", 3: "🔴"}

@app.post("/webhook/{tier}")
async def receive_alert(tier: int, request: Request):
    payload = await request.json()
    # Sample payload from VideoDB intrusion-detection tutorial:
    # {
    #   "event_id": "...",
    #   "label": "POACHING_ALERT_GUNSHOT",
    #   "confidence": 0.92,
    #   "explanation": "...",
    #   "timestamp": "...",
    #   "start_time": "...", "end_time": "...",
    #   "stream_url": "https://rt.stream.videodb.io/manifests/.../...m3u8"
    # }
    await send_alert(
        tier=tier,
        emoji=TIER_EMOJI.get(tier, "⚪"),
        label=payload["label"],
        explanation=payload.get("explanation", ""),
        confidence=payload.get("confidence"),
        stream_url=payload.get("stream_url"),
        timestamp=payload.get("timestamp"),
    )
    return {"status": "received"}
```

Telegram setup: BotFather → create bot → get token → start a chat with your bot → get chat_id from `https://api.telegram.org/bot<TOKEN>/getUpdates`.

---

## 12. The daily highlight reel

```python
# wildwatch/digest.py — uses VideoDB programmable editing
# Goal: pull top-N flagged shots from last 24h, stitch into a ~90s reel

def build_daily_digest(conn, rtstream, top_n=10):
    # Gather flagged shots from the alert log (Telegram/local DB)
    # For each shot, fetch its playable URL via rtstream.generate_stream(start, end)
    # Use VideoDB programmable editing to concatenate clips with title overlays
    # See docs.videodb.io/pages/core-concepts/programmable-editing
    ...
```

Verify the exact programmable-editing API surface before implementing — Claude Code, this is one of the less stable parts of VideoDB's surface area, so read the latest docs before writing it.

---

## 13. Bootstrap script

The one-shot to wire everything up:

```python
# scripts/bootstrap.py
"""
Run once at start of hackathon to:
1. Connect both streams via VideoDB
2. Create the four indexes per stream
3. Create all events (reusable, defined ONCE on conn)
4. Wire alerts for each (index × event) pair
5. Save IDs to .state.json for restart resilience
"""
import json
import os
import videodb
from pathlib import Path
from wildwatch.events import EVENT_DEFINITIONS, INDEX_EVENT_MAP
from config import STREAMS

STATE_FILE = Path(".state.json")
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE_URL", "http://localhost:8000")

def load_prompt(name):
    return Path(f"prompts/{name}.txt").read_text()

def main():
    conn = videodb.connect(api_key=os.environ["VIDEO_DB_API_KEY"])
    coll = conn.get_collection()
    state = {"streams": {}, "events": {}}

    # 1. Create events ONCE (the design pattern judges will recognize)
    for ev in EVENT_DEFINITIONS:
        eid = conn.create_event(event_prompt=ev["prompt"], label=ev["label"])
        state["events"][ev["id_var"]] = eid
        print(f"Created event: {ev['label']}")

    # 2. For each stream, connect + create indexes + wire alerts
    for stream_key, stream_cfg in STREAMS.items():
        rtsp_url = stream_cfg["rtsp_url"]
        if rtsp_url is None:
            print(f"⚠️ {stream_key} needs RTSP URL — fill in config.py or run bridge")
            continue

        rtstream = coll.connect_rtstream(
            name=stream_cfg["name"],
            url=rtsp_url,
        )
        stream_state = {"rtstream_id": rtstream.id, "indexes": {}, "alerts": {}}

        # Format prompts with this stream's context
        prompts = {
            "species":     load_prompt("species").format(**stream_cfg),
            "behavior":    load_prompt("behavior").format(**stream_cfg),
            "environment": load_prompt("environment").format(**stream_cfg),
            "audio":       load_prompt("audio").format(**stream_cfg),
        }

        # Visual indexes
        for kind in ("species", "behavior"):
            idx = rtstream.index_visuals(
                prompt=prompts[kind],
                name=f"{stream_key}_{kind}",
                batch_config={"type": "time", "value": 5, "frame_count": 3},
            )
            stream_state["indexes"][kind] = idx.id

        # Environment can be slower
        env_idx = rtstream.index_visuals(
            prompt=prompts["environment"],
            name=f"{stream_key}_environment",
            batch_config={"type": "time", "value": 60, "frame_count": 1},
        )
        stream_state["indexes"]["environment"] = env_idx.id

        # Audio index
        audio_idx = rtstream.index_audio(prompt=prompts["audio"])
        stream_state["indexes"]["audio"] = audio_idx.id

        # Wire alerts
        for index_kind, event_var_list in INDEX_EVENT_MAP.items():
            # Re-fetch the index object — or store handles in a dict during creation
            ...  # Fill in: for each event, call index.create_alert(event_id, callback_url=...)

        state["streams"][stream_key] = stream_state

    STATE_FILE.write_text(json.dumps(state, indent=2))
    print(f"State saved to {STATE_FILE}")

if __name__ == "__main__":
    main()
```

---

## 14. The 48-hour schedule (anchored to wall-clock)

Hours measured from kickoff (Sat 16 May 10:00 IST).

| When | Hours | What |
|---|---|---|
| Sat 10–11 | 0–1 | Demo storyboard. **Don't start coding yet.** |
| Sat 11–13 | 1–3 | End-to-end skeleton: 1 stream → 1 index → 1 event → webhook fires |
| Sat 13–16 | 3–6 | Real prompts via `scripts/iterate_prompt.py` against `sample_clip.mp4` |
| Sat 16–19 | 6–9 | Switch to live RTSP; create all 4 indexes + all events + all alerts |
| Sat 19–21 | 9–11 | Dinner + cross-modal correlation engine |
| Sat 21 | 11 | **MVP CHECKPOINT** — if not all green, cut second stream + digest |
| Sat 21–24 | 11–14 | Buffer, push mvp-day1 tag to GitHub, sleep prep |
| **Sat 00–07** | **14–21** | **SLEEP. 7 hours. Non-negotiable.** |
| Sun 07–08 | 21–22 | Wake, review overnight event log, tune |
| Sun 08–11 | 22–25 | Second stream via YouTube bridge; demonstrate event reuse |
| Sun 11 | 25 | Mid-point check-in on VideoDB Discord |
| Sun 11–15 | 25–29 | Daily digest reel (programmable editing) |
| Sun 15–16 | 29–30 | Late lunch + buffer |
| Sun 16–20 | 30–34 | Demo video — 4–6 takes, pre-staged dramatic moment |
| Sun 20–21 | 34–35 | README + 200-word writeup |
| Sun 21–01 | 35–39 | First submission tonight, polish |
| **Mon 01–07** | **39–45** | **Sleep** |
| Mon 07–10 | 45–48 | Final pass, last re-submission |
| Mon 10:00 IST | 48 | Submissions close. |

---

## 15. Failure-mode contingency

| What breaks | Fallback |
|---|---|
| YouTube→RTSP bridge dies mid-demo | Fall back to HDOnTap direct stream or `FALLBACK_RTSP` |
| Live stream "boring" during demo (empty waterhole) | Pre-indexed 24h replay window for most of demo; live for closing 20s |
| VideoDB credits run out | Drop second live stream; use recorded YouTube upload for stream B |
| Audio prompt produces nothing | Drop to 3 indexes; emphasize visual cross-correlation in writeup |
| Programmable editing for digest fails | Just timestamp-link top 5 clips in README; skip from video |
| Telegram bot fails | Discord webhook (5-min swap) |
| Sunday morning: feels too ambitious | The MVP at hour 11 Saturday is submittable on its own. Submit it. |

---

## 16. Demo video storyboard (the most important artifact)

```
[0–10s]  Title card. "WildWatch — perception for protected areas."
         Sub: "Built on VideoDB for the Eyes & Ears hackathon."

[10–25s] Cut to a Wildlife Insights / SpeciesNet screenshot.
         VO: "Conservation AI today processes single camera-trap images
              for species ID. The world is continuous. Behavior matters.
              Threats matter. Audio matters."

[25–40s] Cut to architecture diagram (the one in section 3).
         VO: "WildWatch ingests live wildlife streams, runs four parallel
              prompt-driven indexes — species, behavior, environment, audio —
              and fires tiered alerts with playable evidence."

[40–60s] Live demo. Show terminal with bootstrap.py output.
         Show Telegram. THREE alerts fire in sequence (pre-staged timing
         so they land in 20 seconds, not over an hour of real footage):
         🟢 "Mixed species aggregation at waterhole"
         🟡 "Predator vocalization detected — alarm call confirmed"
         🔴 "POACHING ALERT — Gunshot detected" (use a staged audio injection;
            be honest about this in the README)
         Each alert has a clip URL — tap one, clip plays.

[60–80s] Show terminal: correlation engine logs "CONFIRMED_PREDATOR_EVENT"
         after audio + behavior both fired within 90s.
         VO: "The agent reasons across modalities — weak signals from audio
              and visual indexes upgrade to confirmed events."

[80–90s] Auto-generated daily reel plays (5-8 second teaser).
         End card: GitHub link + "Same pipeline, two streams, real-time."
```

---

## 17. Doing-work checklist for Claude Code

When you (Claude Code) start a session on this repo, do this in order:

1. Read this file fully. Do not skip sections.
2. Check `.state.json` — if it exists, the user has already bootstrapped. Resume from there.
3. Check `prompts/` — confirm all four prompt files exist and match section 6.
4. Run `git log --oneline -20` to see what's been done.
5. Ask the user: "what's the current blocker / next thing to build?" — don't assume; the 48-hour timeline means priorities shift hour-by-hour.
6. **Never** unilaterally widen scope. If you finish a task fast, ask before starting the next one. The user's biggest risk is scope creep, not idle hands.
7. When writing VideoDB SDK calls, **verify the exact API shape against the live docs**, not your memory. The SDK evolves; doc fetch beats memory. Start at https://docs.videodb.io/llms.txt for the full index.
8. Prefer small commits with clear messages — easy to roll back when something breaks at hour 30 with a tired user.
9. If a stream is dead or rate-limiting you, fall back to `FALLBACK_RTSP` and keep moving. Don't burn an hour debugging an upstream YouTube outage.
10. The demo video is the deliverable judges actually watch. Anything that doesn't serve the storyboard in section 16 is a deprioritization candidate.

---

## 18. Things to NOT do

- Don't rewrite the prompts to be "cleaner" or "more concise." They are this length on purpose.
- Don't add a fifth index. Four is the depth play; five is scope creep.
- Don't build a fancy frontend dashboard. Telegram + a terminal log is enough for the demo. A web UI is hour-44 polish, not hour-10 work.
- Don't get distracted by VideoDB's `Director` framework, MCP server, or n8n integrations — they're cool but irrelevant for this scope.
- Don't fine-tune anything. The whole point of this submission is that prompt engineering on a VLM substitutes for ML training.
- Don't claim things in the README that aren't in the code. Honest engineering writeup > marketing copy. Judges are technical builders.

---

## 19. Quick reference — links

- VideoDB docs entry: https://docs.videodb.io/pages/getting-started/welcome
- Docs index for LLM consumption: https://docs.videodb.io/llms.txt
- Intrusion detection tutorial (template pattern): https://docs.videodb.io/examples-and-tutorials/live-intelligence/intrusion-detection
- Events & realtime: https://docs.videodb.io/pages/core-concepts/events-and-realtime
- Indexes & search: https://docs.videodb.io/pages/core-concepts/indexes-and-search
- Hackathon page: https://hackday.videodb.io/
- VideoDB GitHub: https://github.com/video-db
- Console (API key + credits): https://console.videodb.io/
- Submission form: https://hackday.videodb.io/#submit
- Discord (real-time help during hackathon): joined via Luma RSVP confirmation

---

*End of handover. Build sharp, sleep enough, demo well.*
