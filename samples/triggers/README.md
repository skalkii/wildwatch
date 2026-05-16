# Trigger corpus

Curated short clips that exercise every event in `wildwatch.events.EVENT_DEFINITIONS`. Drives the prompt-iteration loop (T-17a/b) and the golden-file regression tests (T-17c).

**Schema:** see `manifest.json`. **Coverage tested by:** `tests/test_corpus_manifest.py` (T-15c).

---

## Contents

10 clips, ~150 MB total. All `.mp4` files are gitignored — fetch via `scripts/build_corpus.py` (T-15b).

| Slug | Source | Duration | Events targeted |
|---|---|---|---|
| `waterhole_rich_scene` | YouTube | 5 min | juvenile_present, mixed_aggregation, parental_care, notable_social, large_aggregation |
| `predator_hunt` | YouTube | 2 min | predator_activity, predator_vocal, alarm_call, rare_species |
| `vehicle_intrusion` | YouTube | 1 min | human_intrusion_visual, human_intrusion_audio |
| `carcass_aftermath` | YouTube | 30 s | mortality_event |
| `injured_wildlife` | YouTube | 30 s | welfare_concern |
| `dry_waterhole` | YouTube | 20 s | water_critical |
| `pre_storm_silence` | YouTube | 30 s | acoustic_silence |
| `poaching_synth` | Synthesized | 10 s | gunshot |
| `logging_synth` | Synthesized | 10 s | chainsaw |
| `camera_failure_synth` | Synthesized | 5 s | camera_health |

Coverage: all 18 events in `EVENT_DEFINITIONS` appear in at least one clip's `events_expected`.

---

## Sourcing rules

- **YouTube clips** use `yt-dlp --download-sections "*<section>"` against URLs the operator picks via the manifest's `source_search_query`. Used under educational fair-use; original creator attribution lives in the manifest. We never republish — clips are local fixtures only.
- **Audio overlays** come from [FSD50K](https://zenodo.org/records/4060432) (CC0). Download the dataset once locally; the build script picks specific clip IDs (recorded in manifest after T-15b).
- **Synthesized clips** are deterministic ffmpeg pipelines. The `audio_overlay.mix_filter` and `source_search_query` in the manifest document the exact recipe.

---

## Operator workflow (T-15b)

1. Pre-flight: have `samples/triggers/_fsd50k/` populated with FSD50K WAVs (or fall back to a single CC0 gunshot/chainsaw sample on disk).
2. For each clip in manifest with `source: youtube`:
   - Search YouTube using `source_search_query`
   - Pick a clip; copy URL into `manifest.json[i].source_url`
3. Run `python scripts/build_corpus.py` — downloads + synthesizes everything per manifest.
4. Inspect output via `ffprobe`. Re-run is idempotent (skips existing files unless `--force`).

---

## License notes

Per CLAUDE.md sec 18 ("don't claim things in the README that aren't in the code"):

- We do not ship clip content in this repo.
- Synthesized clips use CC0 audio overlaid on small fair-use video excerpts purely as detection-pipeline test fixtures, not as artistic content.
- The demo video (T-39) discloses that anthropogenic-threat alerts in the demo are triggered by these synthesized fixtures, not real poaching.
