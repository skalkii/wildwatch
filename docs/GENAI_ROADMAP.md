# VideoDB GenAI usage + known limitations

This file documents what WildWatch built on top of VideoDB's GenAI
surfaces, and the **one** real limitation that shaped scope.

## What's wired

| Surface | Where | What it does |
| --- | --- | --- |
| `coll.generate_text` | `wildwatch/telegram.py:genai_friendly_explanation` | Rewrites bracket-tagged AI explanations into one-sentence ranger-friendly prose. Cached by `sha256(label + raw)`. Used by every alert (Telegram + dashboard feed). |
| `coll.generate_text` | `wildwatch/digest.py:build_digest` | Summarises the last 24h of events into a 45-65 word paragraph for the daily reel. |
| `coll.generate_voice` | `wildwatch/digest.py:build_digest` | Turns the summary paragraph into a narration `AudioAsset` attached to the reel timeline. |
| `coll.generate_music` | `wildwatch/digest.py:build_timeline` | Background score on the reel (opt-in via `add_music=True`). |
| `Timeline / Track / VideoAsset / AudioAsset / TextAsset / Transition` | `wildwatch/digest.py` | Stitches deduped tier-1/2/3 clips into the reel with overlays and (optional) music + voiceover. |

The dashboard's **Daily summary → Build** button on the Alerts tab fires
`POST /api/digest/build` which runs the full chain synchronously.

## What is deliberately NOT in this repo

We considered (and explicitly cut from hackathon scope):

- **`generate_image` cover frames** — adds a third async call to the
  digest path; not visible in a 90s demo video. Skip.
- **`dub_video` for multilingual reels** — needs per-recipient language
  config + per-language Telegram chat fan-out. Cool, not load-bearing
  for the demo. Skip.
- **`generate_video` "what-if" footage** — generated content is not the
  pitch. Skip.
- **Voice-driven alert acknowledgement** — out of demo path. Skip.

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
| 1 × `generate_text` digest summary | $0.005 |
| 1 × `generate_voice` digest narration | $0.05 |
| 1 × `generate_music` background score | $0.10 |
| 1 × Timeline reel compile | $0.10 |
| **Daily GenAI total** | **≈ $0.29** |

Negligible vs the perception pipeline (live RTStream visual + audio
indexing is ~$5/h per stream).
