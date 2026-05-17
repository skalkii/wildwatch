# bridge/ — YouTube live → RTSP for VideoDB

VideoDB's live-stream ingestion (`coll.connect_rtstream`) accepts **`rtsp://` or `rtmp://`** URLs only. YouTube serves HLS. This module solves that gap by:

1. Pulling the YouTube HLS feed locally via `streamlink`
2. Re-encoding + republishing as RTSP through `mediamtx`
3. Exposing that local RTSP to the public internet through `bore.pub` so VideoDB's cloud workers can reach it

Everything bridge-related lives **in this folder**. Don't scatter bridge config elsewhere.

---

## What's in this directory

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Spins up `mediamtx` (RTSP relay :8554) + `bore` (public TCP tunnel) as two containers. |
| `mediamtx.yml` | mediamtx config — `MTX_PROTOCOLS=tcp` forces TCP-only RTSP transport. |
| `start_bridge.sh` | Pulls one YouTube live URL, re-encodes via ffmpeg, publishes to the local mediamtx. Run once per stream. |
| `watch_bore.sh` | Prints the rotating bore.pub remote port from container logs. |
| `watch_bridges.sh` | Lightweight health probe across running bridges. |

---

## When you need this

- ✅ Live YouTube channel (e.g. an Africam wildlife cam)
- ✅ Any HLS/`.m3u8` live source that isn't `rtsp://` / `rtmp://`
- ❌ Pre-recorded clip → upload directly via dashboard (`+ Add source → File upload` or `+ Add URL`) — no bridge needed
- ❌ A camera that natively speaks `rtsp://` — add the URL directly

If you only need the upload-sample-video demo flow (no live feed), **skip this whole folder**.

---

## Prerequisites

| Tool | Why | Install |
| --- | --- | --- |
| Docker Desktop | runs `mediamtx` + `bore` containers | https://docs.docker.com/desktop/ |
| `streamlink` | pulls the YouTube HLS feed | `brew install streamlink` / `pipx install streamlink` |
| `ffmpeg` | re-encodes to H.264 Main@720p (required — see "Codec caveat" below) | `brew install ffmpeg` / `apt install ffmpeg` |

---

## Setup — one time per laptop

```bash
docker compose -f bridge/docker-compose.yml up -d
```

Spins up two containers:
- `wildwatch-mediamtx` — RTSP relay listening on `localhost:8554`
- `wildwatch-bore` — connects to `bore.pub` and tunnels port `8554`

Read the public port that `bore` was assigned (rotates on every container restart):

```bash
./bridge/watch_bore.sh
# → listening at bore.pub:19327
```

That URL — `bore.pub:<port>` — is what VideoDB's cloud workers will hit.

---

## Per-stream pump

Run `start_bridge.sh` once for each YouTube URL you want to ingest. Each call runs in the foreground; use a separate terminal (or `nohup … &`) per stream.

```bash
./bridge/start_bridge.sh "https://www.youtube.com/watch?v=8J9USywkGmw" madikwe
./bridge/start_bridge.sh "https://www.youtube.com/watch?v=AeMUdOPFcXI" namibia
./bridge/start_bridge.sh "https://www.youtube.com/watch?v=-rXriX4SiQk" hwange
./bridge/start_bridge.sh "https://www.youtube.com/watch?v=0P_LBKqVbfs" tembe
```

Second argument is the **stream slug** — becomes the RTSP path segment. Slugs are arbitrary; pick anything URL-safe.

What it does:
1. `streamlink "<url>" 720p,720p60,480p,best -O` pipes the HLS feed to stdout
2. `ffmpeg` re-encodes to H.264 Main@720p + AAC at 96kbps
3. Pushes via TCP RTSP to `rtsp://localhost:8554/<slug>`

You can verify the local feed works before exposing it:

```bash
ffprobe -v error -rtsp_transport tcp \
        -show_entries stream=codec_name,codec_type,width,height \
        rtsp://localhost:8554/madikwe
# Expected: H.264 + AAC, both tracks present
```

---

## Wiring the public URL into VideoDB

In the dashboard at http://localhost:8000 :

1. **+ Add source** → **RTSP / RTMP**
2. URL: `rtsp://bore.pub:<bore_port>/<slug>` (port from `watch_bore.sh`, slug matches `start_bridge.sh`)
3. Name: anything; this is your label

The card pulses through `queued → connecting → ingesting → indexing → ready`. Once ready, `scripts/bootstrap.py` (or the inline wiring script under `/tmp/wire_*.py` used during demos) attaches the four AI indexes + 18 alerts.

---

## Codec caveat (important — don't skip)

The ffmpeg re-encode in `start_bridge.sh` is **mandatory**, not optional polish:

```
-c:v libx264 -profile:v main -level 4.0 -vf scale=1280:720 \
-pix_fmt yuv420p -g 60 -keyint_min 60 \
-b:v 1500k -maxrate 1800k -bufsize 3000k \
-c:a aac -b:a 96k -ar 44100
```

Why: VideoDB's rtstream segmenter has been observed to drop video tracks from `H.264 High@1080p` YouTube feeds, producing `.ts` segments that contain only AAC audio. Re-encoding to `H.264 Main@720p` produces segments that consistently carry both tracks.

If you bypass the re-encode (`-c:v copy`), clips from `rt.generate_stream()` are likely to play audio-only. The visual perception path still works because VideoDB ingests frames separately for the visual indexes — but the per-event clip URLs attached to alerts will be silent video or audio-only.

---

## Caveats — known limitations

- **bore.pub port rotates** on every reconnect. VideoDB rtstreams pin a fixed URL at `connect_rtstream` time, so a bore-container restart silently stales every registered rtstream until you re-wire. There's no programmatic fix on the free tier — you'd need Cloudflare Spectrum (paid) or ngrok reserved (paid) for a stable public URL.
- **macOS Docker Desktop** doesn't expose `network_mode: host` ports through to the macOS host. `docker-compose.yml` uses explicit port mapping (`8554:8554`, `1935:1935`, `8889:8889`) instead.
- **mediamtx fragments RTP packets > 1440 bytes** automatically. The default H.264 NALU sizes from a 720p ffmpeg encode stay under that threshold; if you push higher bitrates, manage fragmentation explicitly.
- **streamlink occasionally drops** the HLS source mid-stream. `start_bridge.sh` doesn't auto-restart; wrap in `while true; do …; done` if you need 24/7 uptime.

---

## Teardown

```bash
docker compose -f bridge/docker-compose.yml down
```

Stops both containers. The `start_bridge.sh` foreground processes (one per stream) terminate on Ctrl+C.

---

## Architecture diagram

```
   YouTube live HLS
         │
         ▼
   streamlink ──► ffmpeg (re-encode H.264 Main@720p + AAC)
                       │
                       ▼ RTSP push (TCP)
                  mediamtx :8554 ◄── localhost
                       │
                       ▼ TCP tunnel
                  bore.pub:<remote_port>
                       │
                       ▼  rtsp://bore.pub:<port>/<slug>
                  VideoDB connect_rtstream
                       │
                       ▼
                  4 AI indexes + 18 alerts
```

---

See [`docs/REPO_MAP.md`](../docs/REPO_MAP.md) for how the rtstream flows through the rest of the package, and [`docs/GENAI_ROADMAP.md`](../docs/GENAI_ROADMAP.md) §"Other free-tier limitations we hit" for the broader free-tier limitation discussion.
