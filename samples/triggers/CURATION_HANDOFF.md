# Corpus Curation Handoff — for Claude.ai (web)

Paste this entire document into a fresh Claude.ai conversation. The goal: return a filled-in JSON patch for `samples/triggers/manifest.json` so the local Claude Code agent can resume building the test corpus.

---

## Context

We're building **WildWatch**, a real-time perception agent for protected-area wildlife monitoring, submitted to the **VideoDB "Eyes & Ears" 48-hour hackathon** (16–18 May 2026). Built on the VideoDB Python SDK.

We have 18 detection events spanning species ID, behavior, environment, and bioacoustic threats. To test all of them before live demo, we need a curated corpus of 10 short clips — each targeting one or more events.

The manifest at `samples/triggers/manifest.json` is already authored with slugs, expected events, durations, search queries, and licensing posture — but `source_url` is `null` for every clip because URL selection requires web search the CLI agent can't reliably do.

**Your job:** pick the actual URLs, validate they exist + are accessible, and return a JSON patch the local agent can apply.

---

## Constraints

1. **Licensing**:
   - Prefer **CC0 / CC-BY / Pexels License / Pixabay License** wherever possible.
   - YouTube clips fall back to **educational fair-use**, but only for short excerpts (≤5 min) as **local test fixtures** — we never republish.
   - Audio overlays MUST be CC0 (FSD50K) or equivalently permissive.

2. **Duration discipline**:
   - Don't suggest clips dramatically longer than the manifest spec — we extract a specific `section` like `"0:00-5:00"`.
   - Shorter is fine; longer should still have a usable in-range window.

3. **Geographic / ecological plausibility**:
   - `stream_context` field on each clip maps to one of two stream profiles: `namibia_waterhole` (Namib Desert oryx/elephant/hyena) or `wild_africa_live` (rotating East/Southern African reserves with Big Five). Pick clips that ecologically fit.

4. **No clickbait, no graphic / disturbing content** — we're showing this to hackathon judges. Aim for documentary-quality, calm scenes.

5. **Stable URLs only** — no shorts, no livestreams (livestream URLs rot daily). Prefer permanent YouTube video IDs (the `?v=XXXX` form), Pexels stable IDs, FSD50K Zenodo records.

---

## The 10 clips to curate

For each clip below, return:
- `source_url`: the chosen URL (or `null` if you can't find a good match)
- `section`: time range like `"02:15-07:15"` if the picked clip is longer than `duration_s`
- `notes_addendum`: brief comment on why you picked this clip
- For synthesized clips (`source: synthesized`): the URL of the **base** footage you'd use AND the chosen FSD50K record / Freesound clip URL

### 1. `waterhole_rich_scene`
- **Targets:** juvenile_present, mixed_aggregation, parental_care, notable_social, large_aggregation
- **Stream context:** `namibia_waterhole`
- **Duration:** 5 minutes
- **Search hint:** Tony Lewis Wildlife channel on YouTube, Namibia waterhole compilations. Or any reputable wildlife channel showing a multi-species daytime waterhole gathering with calves/young visible.
- **Must show:** mixed herds (≥2 species), juveniles at water edge, mothers leading young, herd size ≥15 at peak.

### 2. `predator_hunt`
- **Targets:** predator_activity, predator_vocal, alarm_call, rare_species
- **Stream context:** `wild_africa_live`
- **Duration:** 2 minutes
- **Search hint:** SafariLive / WildEarth YouTube archives — lion or leopard stalk/chase sequences with prey alarm calls audible.
- **Must show:** big cat (lion/leopard/cheetah qualifies as `rare_species`), prey fleeing/frozen, audible vocalization (lion roar, leopard sawing call, alarm snorts from impala/baboon).

### 3. `vehicle_intrusion`
- **Targets:** human_intrusion_visual, human_intrusion_audio
- **Stream context:** `wild_africa_live`
- **Duration:** 1 minute
- **Search hint:** safari game drive footage with vehicle visible in frame AND audible engine + tourist voices.
- **Must show:** clearly visible safari vehicle + human voices OR engine clearly audible. Bonus if night.

### 4. `carcass_aftermath`
- **Targets:** mortality_event
- **Stream context:** `wild_africa_live`
- **Duration:** 30 seconds
- **Search hint:** vultures around a carcass, post-predation scene. Documentary-style, not gore-focused.
- **Must show:** visible remains or vultures circling / feeding at a kill site.

### 5. `injured_wildlife`
- **Targets:** welfare_concern
- **Stream context:** `wild_africa_live`
- **Duration:** 30 seconds
- **Search hint:** wildlife documentary footage of a limping elephant, antelope with leg injury, or an animal visibly isolated from its herd.
- **Must show:** observable abnormal gait, posture, or social isolation.

### 6. `dry_waterhole`
- **Targets:** water_critical
- **Stream context:** `namibia_waterhole`
- **Duration:** 20 seconds
- **Search hint:** climate / drought documentary footage of a dry or very-low-water waterhole.
- **Must show:** cracked mud, near-empty waterhole, or animals walking in dry bed.

### 7. `pre_storm_silence`
- **Targets:** acoustic_silence
- **Stream context:** `wild_africa_live`
- **Duration:** 30 seconds
- **Search hint:** ambient savanna footage where biophony is notably absent (often before a thunderstorm, or in mid-day heat-stress quiet).
- **Must:** be quiet in the audio track. Visual content less important.
- **Alternative:** if no good YouTube match, suggest an ambient savanna soundscape from Freesound.org with CC0 license + a static or slow-pan video stitch.

### 8. `poaching_synth` (SYNTHESIZED)
- **Targets:** gunshot
- **Base footage needs:** ~10 s of quiet wildlife (waterhole, savanna, forest — minimal pre-existing audio). **Strongly prefer Pexels CC0 nature stock** to avoid fair-use ambiguity for a synthesized fixture.
- **Audio overlay needs:** a single distinct gunshot ≤2 s. Prefer **FSD50K** record from https://zenodo.org/records/4060432 (CC0), or a Freesound.org CC0 gunshot clip with stable URL.
- **Return:** both the base footage URL and the audio clip URL/record-ID.

### 9. `logging_synth` (SYNTHESIZED)
- **Targets:** chainsaw
- **Base footage needs:** ~10 s of forest / tree cover. Pexels CC0 preferred.
- **Audio overlay needs:** chainsaw running for ≥5 s. FSD50K or Freesound CC0.
- **Return:** base URL + audio URL.

### 10. `camera_failure_synth` (SYNTHESIZED, NO EXTERNAL ASSETS)
- **Targets:** camera_health
- **Recipe:** purely ffmpeg-generated black frame + low audio noise. No external sources needed.
- **Return:** nothing to curate — confirm you understood, no URL needed.

---

## FSD50K access notes (for clips 8 + 9)

The full FSD50K dataset is ~24 GB (the bottleneck). We don't need the whole thing — just two specific clips. If you can return:
1. The Zenodo record URL: https://zenodo.org/records/4060432
2. The **specific clip IDs** within FSD50K that correspond to a clean gunshot and a clean chainsaw (the dev/eval split CSV is downloadable from the same record)

…that's ideal. If FSD50K browsing is too slow, fall back to **Freesound.org** CC0 search:
- https://freesound.org/search/?q=gunshot&f=license%3A%22Creative+Commons+0%22
- https://freesound.org/search/?q=chainsaw&f=license%3A%22Creative+Commons+0%22

Return Freesound stable URLs (the `/people/USER/sounds/ID/` form).

---

## Output format expected

Return one fenced JSON block in this shape:

```json
{
  "patches": [
    {
      "slug": "waterhole_rich_scene",
      "source_url": "https://www.youtube.com/watch?v=...",
      "section": "0:00-5:00",
      "notes_addendum": "Channel: ..., creator: ..., picked because ..."
    },
    {
      "slug": "predator_hunt",
      "source_url": "...",
      "section": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "vehicle_intrusion",
      "source_url": "...",
      "section": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "carcass_aftermath",
      "source_url": "...",
      "section": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "injured_wildlife",
      "source_url": "...",
      "section": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "dry_waterhole",
      "source_url": "...",
      "section": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "pre_storm_silence",
      "source_url": "...",
      "section": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "poaching_synth",
      "base_url": "https://www.pexels.com/video/...",
      "overlay_url": "https://freesound.org/people/.../sounds/.../",
      "notes_addendum": "Base: ..., overlay: ..."
    },
    {
      "slug": "logging_synth",
      "base_url": "...",
      "overlay_url": "...",
      "notes_addendum": "..."
    },
    {
      "slug": "camera_failure_synth",
      "notes_addendum": "No external assets — pure ffmpeg synthesis confirmed."
    }
  ],
  "fsd50k_overall": {
    "decision": "use FSD50K dev split CSV at ... | falling back to Freesound CC0",
    "notes": "..."
  }
}
```

---

## Verification before returning

For each YouTube URL, please double-check:
- Video is **public**, not "unlisted" / age-restricted / region-locked.
- The `section` time range you picked actually contains the described content.
- The video is at least the suggested duration (so the section fits).

For Pexels: that the URL goes to a video page (not a search result).

For Freesound/FSD50K: that the clip is downloadable as a single file (not part of a paid pack).

---

## What happens after you return the JSON

The local Claude Code agent will:
1. Apply your patches into `manifest.json` (merging `source_url`, `section`, `notes_addendum` into each clip entry; for synth clips: into `audio_overlay`).
2. Run `python scripts/build_corpus.py` which calls `yt-dlp` + `ffmpeg` to materialise every clip locally under `samples/triggers/*.mp4`.
3. Re-run `pytest tests/test_corpus_manifest.py` to confirm nothing broke.
4. Bulk-upload to VideoDB and start the prompt sweep.

---

## If you can't find a good match for a clip

Set its `source_url` to `null` in your patch and add a `notes_addendum` explaining what's missing. The local agent will skip that clip and the operator can revisit. Coverage tests will still pass as long as **another** clip in the corpus targets the same event — but every event in `EVENT_DEFINITIONS` must appear in at least one clip with a real URL. If a clip slot stays empty, suggest an alternate clip we should add to compensate.

---

End of handoff. Return the JSON when ready.
