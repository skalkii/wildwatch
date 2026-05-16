"""Materialise the trigger corpus from samples/triggers/manifest.json.

Pipeline per clip:
- source: youtube      -> yt-dlp --download-sections '*START-END'
- source: synthesized  -> fetch base_url (Pexels) + overlay_url (Freesound),
                          run ffmpeg amix per per-clip audio_overlay recipe
                          (camera_failure_synth: pure ffmpeg lavfi)

Hostname dispatch on download:
- youtube.com / youtu.be    -> yt-dlp
- pexels.com                -> Pexels page scrape for direct mp4 URL
- freesound.org             -> Freesound page scrape for public preview MP3
- anything else             -> yt-dlp first, then urllib fallback

Idempotent: skips clips whose output already exists (use --force to rebuild).
After build, ffprobe validates duration into samples/triggers/build_results.json.

Usage:
    python scripts/build_corpus.py [--force] [--only SLUG ...]
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = REPO_ROOT / "samples" / "triggers" / "manifest.json"
SAMPLES_DIR = REPO_ROOT / "samples" / "triggers"
WORK_DIR = SAMPLES_DIR / "_work"
RESULTS_PATH = SAMPLES_DIR / "build_results.json"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


# ──── tool / helper ────────────────────────────────────────────────────────


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


def _http_get(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _http_download(url: str, dest: Path, timeout: int = 180) -> bool:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r, open(dest, "wb") as f:
            shutil.copyfileobj(r, f)
        return True
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        print(f"  http_download failed: {e}")
        return False


# ──── source-specific fetchers ─────────────────────────────────────────────


def _download_pexels(page_url: str, dest: Path) -> bool:
    """Pexels pages embed direct mp4 URLs in HTML. Pick highest resolution."""
    try:
        html = _http_get(page_url).decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  pexels page fetch failed: {e}")
        return False
    candidates = re.findall(r'https://videos\.pexels\.com/video-files/[^"\s]+\.mp4', html)
    if not candidates:
        print(f"  pexels: no mp4 sources on {page_url}")
        return False

    def _resolution(u: str) -> int:
        m = re.search(r"_(\d+)_(\d+)_", u)
        return int(m.group(1)) * int(m.group(2)) if m else 0

    best = max(set(candidates), key=_resolution)
    print(f"  pexels mp4 -> {best.split('/')[-1]}")
    return _http_download(best, dest)


def _download_freesound(page_url: str, dest: Path) -> bool:
    """Preview MP3 is on cdn.freesound.org/previews/{prefix}/{id}_{uploader}-hq.mp3."""
    try:
        html = _http_get(page_url).decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  freesound page fetch failed: {e}")
        return False
    # Prefer -hq.mp3, fall back to -lq.mp3 if hq not present.
    m = re.search(r"https://cdn\.freesound\.org/previews/\d+/\d+_\d+-hq\.mp3", html)
    if not m:
        m = re.search(r"https://cdn\.freesound\.org/previews/\d+/\d+_\d+-lq\.mp3", html)
    if not m:
        print(f"  freesound: no preview URL on {page_url}")
        return False
    print(f"  freesound preview -> {m.group(0).split('/')[-1]}")
    return _http_download(m.group(0), dest)


def _download_yt_dlp_plain(url: str, dest: Path) -> bool:
    """yt-dlp for sources without section trimming (used for synth base downloads)."""
    return _run(["yt-dlp", "--no-playlist", "-o", str(dest), url])


def _dispatch_download(url: str, dest: Path) -> bool:
    """Pick fetcher by hostname.

    Pexels: yt-dlp handles their CDN reliably and avoids Cloudflare 403 on the
    page-scrape route. Page-scrape (_download_pexels) is kept as a fallback.
    Freesound: page scrape for the public preview MP3 (no OAuth needed).
    """
    host = urllib.parse.urlparse(url).netloc.lower()
    if "pexels.com" in host:
        return _download_yt_dlp_plain(url, dest) or _download_pexels(url, dest)
    if "freesound.org" in host:
        return _download_freesound(url, dest)
    if "youtube.com" in host or "youtu.be" in host:
        return _download_yt_dlp_plain(url, dest)
    return _download_yt_dlp_plain(url, dest) or _http_download(url, dest)


# ──── per-clip builders ────────────────────────────────────────────────────


def _capture_live_stream(url: str, dur: int, out: Path) -> bool:
    """Stream `url` for `dur` seconds via streamlink->ffmpeg using subprocess
    pipes (no shell=True — defends against command-injection from manifest URLs).
    """
    streamlink_cmd = [
        "streamlink",
        "--stream-segment-timeout",
        "30",
        url,
        "best",
        "-O",
    ]
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-i",
        "pipe:0",
        "-t",
        str(dur),
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        str(out),
    ]
    print(f"  $ {' '.join(streamlink_cmd)} | {' '.join(ffmpeg_cmd)}")
    sl = subprocess.Popen(streamlink_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        ff = subprocess.Popen(ffmpeg_cmd, stdin=sl.stdout, stderr=subprocess.PIPE)
        # Allow SIGPIPE propagation when ffmpeg closes early on -t cap.
        if sl.stdout is not None:
            sl.stdout.close()
        ff_err = ff.communicate()[1]
        ff_rc = ff.returncode
        sl.terminate()
        sl_err = sl.communicate()[1] if sl.stderr else b""
        if ff_rc != 0:
            print(f"  ! ffmpeg exit {ff_rc}")
            if ff_err:
                print(f"  ffmpeg stderr: {ff_err.decode('utf-8', 'ignore').strip()[-400:]}")
            if sl_err:
                print(f"  streamlink stderr: {sl_err.decode('utf-8', 'ignore').strip()[-400:]}")
            return False
        return True
    except Exception as e:
        print(f"  ! pipeline error: {e}")
        sl.kill()
        return False


def _build_live_youtube(clip: dict, out: Path) -> bool:
    """Capture N seconds from CURRENT live position of a YouTube live stream."""
    url = clip.get("source_url")
    if not url:
        print(f"  skip {clip['slug']}: source_url unset")
        return False
    dur = clip["duration_s"]
    if _capture_live_stream(url, dur, out):
        return True
    fallback = clip.get("fallback_url")
    if fallback:
        print(f"  primary live failed; retrying fallback_url for {clip['slug']}")
        return _capture_live_stream(fallback, dur, out)
    return False


def _build_youtube(clip: dict, out: Path) -> bool:
    url = clip.get("source_url")
    if not url:
        print(f"  skip {clip['slug']}: source_url unset")
        return False
    section = clip.get("section") or f"0:00-{clip['duration_s']}"
    ok = _run(
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
    if ok or not clip.get("fallback_url"):
        return ok
    print(f"  primary failed; retrying fallback_url for {clip['slug']}")
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
            clip["fallback_url"],
        ]
    )


def _build_camera_failure(clip: dict, out: Path) -> bool:
    """Pure ffmpeg: black frame + low-volume pink noise for duration_s seconds."""
    dur = clip["duration_s"]
    return _run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"color=black:s=1280x720:r=25:d={dur}",
            "-f",
            "lavfi",
            "-i",
            f"anoisesrc=color=pink:amplitude=0.01:d={dur}",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            str(out),
        ]
    )


def _build_synthesized_overlay(clip: dict, out: Path) -> bool:
    """Download base (Pexels) + overlay (Freesound), ffmpeg amix per recipe."""
    overlay = clip.get("audio_overlay") or {}
    base_url = overlay.get("base_url")
    overlay_url = overlay.get("overlay_url")
    if not (base_url and overlay_url):
        print(f"  skip {clip['slug']}: audio_overlay needs base_url + overlay_url")
        return False

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    base_path = WORK_DIR / f"{clip['slug']}__base.mp4"
    overlay_path = WORK_DIR / f"{clip['slug']}__overlay.mp3"

    if not base_path.exists():
        if not _dispatch_download(base_url, base_path):
            return False
    if not overlay_path.exists():
        if not _dispatch_download(overlay_url, overlay_path):
            return False

    # Build the audio filter graph from per-clip parameters.
    dur = clip["duration_s"]
    base_vol = overlay.get("base_volume", 0.3)
    over_vol = overlay.get("overlay_volume", 0.8)
    delay_ms = overlay.get("overlay_delay_ms", 2000)
    trim = overlay.get("overlay_trim")  # e.g. "10-17" or None

    over_chain = []
    if trim:
        a, b = trim.split("-")
        over_chain.append(f"atrim={a}:{b},asetpts=PTS-STARTPTS")
    over_chain.append(f"adelay={delay_ms}|{delay_ms}")
    over_chain.append(f"volume={over_vol}")
    over_filter = ",".join(over_chain)

    filter_complex = (
        f"[1:a]{over_filter}[over];"
        f"[0:a]volume={base_vol}[base];"
        f"[base][over]amix=inputs=2:duration=first[mixed]"
    )

    return _run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(base_path),
            "-i",
            str(overlay_path),
            "-filter_complex",
            filter_complex,
            "-map",
            "0:v",
            "-map",
            "[mixed]",
            "-t",
            str(dur),
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
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

    # Intentionally-dropped clip (manifest marks these source_url=null + duration=0).
    if clip["source"] in ("youtube", "live_youtube") and clip.get("source_url") is None:
        result["status"] = "dropped"
        print(f"  drop {slug} (intentionally uncovered; source_url=null)")
        return result

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
    elif source == "live_youtube":
        ok = _build_live_youtube(clip, out)
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
        # Don't silently swallow; flag loudly so operator sees coverage gap.
        print(f"  FAILED: {slug} -- coverage may break for events {clip['events_expected']}")

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
    summary: dict[str, int] = {}
    for r in results:
        summary[r["status"]] = summary.get(r["status"], 0) + 1
    print(f"summary: {summary}")
    return 0 if all(r["status"] in ("ok", "skipped_exists", "dropped") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
