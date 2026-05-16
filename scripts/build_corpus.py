"""Materialise the trigger corpus from samples/triggers/manifest.json.

Pipeline per clip:
- source: youtube      -> yt-dlp --download-sections '*START-END'
- source: synthesized  -> download base + overlay, ffmpeg amix
                          (camera_failure_synth: pure ffmpeg lavfi recipe)

Idempotent: skips clips whose output already exists (use --force to rebuild).
Skips clips lacking required URLs (warns + continues). After build, ffprobe
validates duration into a per-clip metadata sidecar.

Usage:
    python scripts/build_corpus.py [--force] [--only SLUG ...]
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = REPO_ROOT / "samples" / "triggers" / "manifest.json"
SAMPLES_DIR = REPO_ROOT / "samples" / "triggers"
WORK_DIR = SAMPLES_DIR / "_work"
RESULTS_PATH = SAMPLES_DIR / "build_results.json"


def _check_tools() -> None:
    for tool in ("yt-dlp", "ffmpeg", "ffprobe"):
        if shutil.which(tool) is None:
            sys.exit(f"missing required binary: {tool}")


def _run(cmd: list[str]) -> bool:
    print(f"  $ {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"  ! exit {res.returncode}")
        if res.stderr:
            print(f"  stderr: {res.stderr.strip()[:500]}")
        return False
    return True


def _ffprobe_duration(path: Path) -> float | None:
    res = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return None
    try:
        return float(res.stdout.strip())
    except ValueError:
        return None


def _download_url(url: str, dest: Path) -> bool:
    """Try yt-dlp first (handles YouTube + Pexels + many others), fall back to urllib."""
    if _run(["yt-dlp", "-o", str(dest), url]):
        return True
    print(f"  yt-dlp failed; trying urllib for {url}")
    try:
        urllib.request.urlretrieve(url, dest)
        return True
    except Exception as e:
        print(f"  urllib failed: {e}")
        return False


def _build_youtube(clip: dict, out: Path) -> bool:
    url = clip.get("source_url")
    if not url:
        print(f"  skip {clip['slug']}: source_url unset")
        return False
    section = clip.get("section") or f"0:00-{clip['duration_s']}"
    return _run(
        [
            "yt-dlp",
            "--download-sections",
            f"*{section}",
            "--no-playlist",
            "-f",
            "mp4/best[ext=mp4]/best",
            "-o",
            str(out),
            url,
        ]
    )


def _build_camera_failure(clip: dict, out: Path) -> bool:
    """Pure ffmpeg: black frame + low-volume noise for clip['duration_s'] seconds."""
    dur = clip["duration_s"]
    return _run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"color=black:s=1280x720:d={dur}:r=30",
            "-f",
            "lavfi",
            "-i",
            f"anoisesrc=d={dur}:a=0.005:c=brown",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            str(out),
        ]
    )


def _build_synthesized_overlay(clip: dict, out: Path) -> bool:
    """Download base + overlay, run ffmpeg amix per the manifest mix_filter."""
    overlay = clip.get("audio_overlay") or {}
    base_url = overlay.get("base_url")
    overlay_url = overlay.get("overlay_url")
    if not (base_url and overlay_url):
        print(f"  skip {clip['slug']}: audio_overlay needs base_url + overlay_url")
        return False

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    base_path = WORK_DIR / f"{clip['slug']}__base.mp4"
    overlay_path = WORK_DIR / f"{clip['slug']}__overlay.wav"

    if not base_path.exists() and not _download_url(base_url, base_path):
        return False
    if not overlay_path.exists() and not _download_url(overlay_url, overlay_path):
        return False

    # Trim base to duration_s, mix overlay at moderate volume
    dur = clip["duration_s"]
    return _run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(base_path),
            "-i",
            str(overlay_path),
            "-filter_complex",
            "[1:a]volume=0.6[a1];[0:a][a1]amix=inputs=2:duration=first[a]",
            "-map",
            "0:v",
            "-map",
            "[a]",
            "-t",
            str(dur),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            str(out),
        ]
    )


def _build_synthesized(clip: dict, out: Path) -> bool:
    if clip.get("audio_overlay") is None:
        return _build_camera_failure(clip, out)
    return _build_synthesized_overlay(clip, out)


def build_clip(clip: dict, force: bool = False) -> dict:
    slug = clip["slug"]
    out = SAMPLES_DIR / f"{slug}.mp4"
    result = {"slug": slug, "path": str(out), "status": "pending", "duration_s": None}

    if out.exists() and not force:
        dur = _ffprobe_duration(out)
        result["status"] = "skipped_exists"
        result["duration_s"] = dur
        print(f"  skip {slug} (exists, {dur}s)")
        return result

    print(f"--> {slug} ({clip['source']})")
    ok = False
    source = clip["source"]
    if source == "youtube":
        ok = _build_youtube(clip, out)
    elif source == "synthesized":
        ok = _build_synthesized(clip, out)
    else:
        print(f"  unknown source: {source}")

    if ok and out.exists():
        dur = _ffprobe_duration(out)
        result["status"] = "ok"
        result["duration_s"] = dur
        size_mb = out.stat().st_size / 1024 / 1024
        print(f"  ok  {slug}  {dur}s  {size_mb:.1f} MB")
    else:
        result["status"] = "failed"

    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="rebuild even if output exists")
    ap.add_argument("--only", nargs="*", help="only process these slugs")
    args = ap.parse_args()

    _check_tools()
    manifest = json.loads(MANIFEST.read_text())
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for clip in manifest["clips"]:
        if args.only and clip["slug"] not in args.only:
            continue
        results.append(build_clip(clip, force=args.force))

    RESULTS_PATH.write_text(json.dumps({"results": results}, indent=2))
    print(f"\nresults -> {RESULTS_PATH}")
    summary = {}
    for r in results:
        summary[r["status"]] = summary.get(r["status"], 0) + 1
    print(f"summary: {summary}")
    return 0 if all(r["status"] in ("ok", "skipped_exists") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
