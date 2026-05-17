# VideoDB GenAI usage + known limitations

This file documents what WildWatch built on top of VideoDB's GenAI
surfaces, and the **one** real limitation that shaped scope.

## What's wired

| Surface | Where | What it does |
| --- | --- | --- |
| `coll.generate_text` | `wildwatch/telegram.py:genai_friendly_explanation` | Rewrites bracket-tagged AI explanations into one-sentence ranger-friendly prose. Cached by `sha256(label + raw)`. Used by every alert (Telegram + dashboard feed). |
| `coll.generate_text` | `wildwatch/digest.py:build_digest` | Documentary-narrator prompt produces a 130-170 word paragraph for the daily reel — opens with the day's most urgent finding and closes with a "watch for tonight" beat. |
| `coll.generate_voice` | `wildwatch/digest.py:build_digest` | Turns the summary into narration via `voice_name="George"` (deep ElevenLabs voice) + `config={"speed":0.85, "stability":0.75}` for slow documentary pacing. Returned `audio.length` drives the reel-↔-voice length sync. |
| `coll.generate_music` | `wildwatch/digest.py:build_timeline` + `_add_tail_music` | Background score on the reel (opt-in via `add_music=True`), AND short tail outro generated automatically when narration is shorter than the reel — `_add_tail_music(start=audio_len, duration=gap)` fills the silent end so the reel doesn't dead-air. |
| `Timeline / Track / Clip / VideoAsset / AudioAsset / TextAsset / Transition / Font / Background` | `wildwatch/digest.py` | Stitches deduped tier-1/2/3 clips into the reel with `VideoAsset(volume=0)` (clip audio muted so narration sits clean), `AudioAsset(volume=1.5)` (narration boosted), and tier-label TextAsset overlays. `build_timeline` returns reel context that drives the per-clip looping in `_extend_reel_with_loop`. |
| `coll.get_video(vid).length` | `wildwatch/digest.py:_video_has_info` | Probe to filter unusable corpus videos (audio-only / deleted / no `video_info`) before adding to the reel — the failure path that previously crashed `timeline.generate_stream` is now caught upfront. |
| `coll.get_videos()` | `wildwatch/digest.py:_discover_collection_fallback` | Last-resort fallback when every state-corpus entry has been rejected — scans the live collection for any video that passes the `video_info` probe. |
| External: **QuickChart.io** | `wildwatch/telegram.py:_digest_chart_urls` | Free public Chart.js → PNG renderer. URL-encodes Chart.js JSON configs (palette matched to dashboard modal); Telegram fetches each URL itself as a `sendMediaGroup` photo. No local image-gen deps. |
| Telegram `sendMediaGroup` + `sendMessage` | `wildwatch/telegram.py:send_digest` | Two-step delivery: album of 1-4 colour charts + HTML caption with KPIs, then narration paragraph + reel link. ASCII-bar fallback via `build_digest_message` if QuickChart unreachable. |

The dashboard's **Daily summary → Build** button on the Alerts tab fires
`POST /api/digest/build`. The endpoint runs the full chain
synchronously (~30-90s), opens an in-app modal with the analytics
charts + inline HLS reel, and (when `notify_telegram=true`,
default) ships the same content to the configured Telegram chat.

## What is deliberately NOT in this repo

We considered (and explicitly cut from hackathon scope):

- **`generate_image` cover frames** — adds another async call to the
  digest path; not visible in a 90s demo video. Skip.
- **`dub_video` for multilingual reels** — needs per-recipient language
  config + per-language Telegram chat fan-out. Cool, not load-bearing
  for the demo. Skip.
- **`generate_video` "what-if" footage** — generated content is not the
  pitch. Skip.
- **Voice-driven alert acknowledgement** — out of demo path. Skip.
- **Server-side chart image generation (Pillow / matplotlib)** —
  QuickChart.io covers Telegram chart delivery without adding a
  dep. Reconsider only if QuickChart's quota becomes an issue at
  scale.

## The one real limitation: non-speech audio

**VideoDB has no native non-speech sound-event detection.** Confirmed
via SDK source + live probes + full doc search:

- `video.index_audio(prompt=...)` calls `POST /index/scene` with
  `extraction_type=SceneExtractionType.transcript`. It segments the
  existing transcript and runs the prompt over each segment via an
  LLM. **It does not analyse the raw audio waveform.**
- When a clip has no speech, `generate_transcript` raises
  `InvalidRequestError: Failed to detect the language, no spoken data
  found`. `index_audio` still accepts the call, returns a scene_index_id,
  and then sits permanently in `processing` because there's nothing
  to segment.
- Live probe of three uploaded clips (`dry_waterhole`,
  `logging_synth.mp4`, `zebra_waterhole`) — all hit the same error.
- Full doc search (`docs.videodb.io/llms-full.txt`) returns zero hits
  for `audio event`, `sound event`, `non-speech`, `gunshot detection`,
  `chainsaw`, `bird sound`, `audio classifier`.

### How we work around it

- `wildwatch/post_upload_analysis.py:_has_transcript` probes the
  transcript layer before calling `index_audio`. On "no spoken data
  found", the audio index is skipped and audio-event queries fall back
  to the **visual** scene index (visual cues like "weapon visible" /
  "smoke" still match poaching / fire scenes).
- The dashboard's Indexed Content tab annotates audio-named indexes
  stuck in `processing` with `audio_blocked: 'no_speech'` and surfaces
  an amber "no speech — skipped" pill + a Remove button.

### If you want real non-speech audio events

Two paths, both **out of scope for the hackathon submission** (judges
score on depth of VideoDB SDK usage; bringing a second AI provider
dilutes that axis).

- **Option A — wait for VideoDB.** Watch
  `docs.videodb.io/api-reference/videos/indexing/` for a new
  `index_type` value (e.g. `audio_event`). Swap the transcript-gate
  for the new index type, ~1h refactor.
- **Option B — sidecar PANNs (CNN14)** or YAMNet. Wraps a 527-class
  AudioSet model in a small FastAPI service that takes
  `(signed_url, start, end)` and returns `[(class, confidence)]`.
  Path-B sweep posts audio-event queries through the sidecar instead
  of the visual fallback. ~6-8h end-to-end.

## Other free-tier limitations we hit (not VideoDB-specific)

- **bore.pub rotates the remote port on every reconnect.** RTStreams
  registered with VideoDB point at a fixed URL, so bore disconnects
  silently stale the live feed. Worked around by manual re-wire; a
  paid tunnel (Cloudflare Spectrum / ngrok reserved) would fix this.
- **macOS Docker Desktop doesn't expose `network_mode: host` ports**
  to the host — switched mediamtx + bore containers to explicit port
  mapping.
- **VideoDB rtstream segmenter dropped video** from `H.264 High@1080p`
  YouTube feeds, producing audio-only `.ts` segments. Fixed by
  re-encoding to `H.264 Main@720p` in the streamlink → ffmpeg → RTSP
  bridge (`bridge/start_bridge.sh`).

## Cost (rough)

| Per-day usage | Cost |
| --- | --- |
| ~100 alerts × `generate_text` (basic) for rewrite | $0.03 |
| 1 × `generate_text` digest summary (130-170 words) | $0.01 |
| 1 × `generate_voice` digest narration (~60-90s slow voice) | $0.08 |
| 1 × `generate_music` background score (when `add_music=True`) | $0.10 |
| Conditional `generate_music` tail when narration < reel | $0.04 |
| 1 × Timeline reel compile | $0.10 |
| QuickChart.io chart PNGs (×4) | $0 (free public tier) |
| Telegram `sendMediaGroup` + `sendMessage` | $0 |
| **Daily GenAI total** | **≈ $0.36** |

Negligible vs the perception pipeline (live RTStream visual + audio
indexing is ~$5/h per stream).
