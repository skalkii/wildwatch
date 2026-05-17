#!/usr/bin/env bash
# Pull a YouTube live URL through streamlink + ffmpeg, republish as RTSP
# via the locally-running mediamtx.
#
# Usage:
#   ./bridge/start_bridge.sh <youtube_url> <stream_slug>
#
# Example:
#   ./bridge/start_bridge.sh "https://www.youtube.com/watch?v=vr4o_AsrU1k" mara
#   # → rtsp://localhost:8554/mara
#
# Prerequisites:
#   docker compose -f bridge/docker-compose.yml up -d
#     - starts mediamtx (port 8554) + bore (public TCP tunnel)
#     - tail `docker logs <bore-container>` to read the bore.pub:<port>
#       remote URL (changes per restart)
#
# Then add the public URL as a NEW rtsp source from the dashboard:
#   rtsp://bore.pub:<port>/<stream_slug>
set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat >&2 <<USAGE
Usage: $0 <youtube_url> <stream_slug>

Examples:
  $0 "https://www.youtube.com/watch?v=AeMUdOPFcXI" namibia
  $0 "https://www.youtube.com/watch?v=vr4o_AsrU1k" wildafrica

After it's running, add rtsp://bore.pub:<port>/<stream_slug> as a new
RTSP source from the dashboard. Read the port from the bore container
logs (see bridge/docker-compose.yml).
USAGE
  exit 1
fi

YOUTUBE_URL="$1"
STREAM_SLUG="$2"

for cmd in streamlink ffmpeg; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: '$cmd' not found. Install with: brew install streamlink ffmpeg" >&2
    exit 2
  fi
done

if ! nc -z localhost 8554 2>/dev/null; then
  echo "ERROR: mediamtx is not listening on localhost:8554." >&2
  echo "       Start it first:" >&2
  echo "         docker compose -f bridge/docker-compose.yml up -d" >&2
  exit 3
fi

echo "Bridging YouTube -> RTSP"
echo "  source: ${YOUTUBE_URL}"
echo "  output: rtsp://localhost:8554/${STREAM_SLUG}"
echo "Press Ctrl+C to stop."
echo

# RTSP requires AAC with global headers — YouTube HLS streams ship raw
# AAC frames that fail "with no global headers is currently not supported".
# Re-encode audio.
#
# Video re-encode to H.264 Main profile @ 720p: VideoDB's rtstream-to-clip
# segmenter was dropping video and emitting audio-only `.ts` segments
# when fed High@1080p (YouTube's "best" variant). Main@720p is universally
# decodable and survives the VideoDB transcoder. ~30-50% CPU per stream
# on a modern laptop but produces clips that actually play.
streamlink --stream-segment-timeout 30 "$YOUTUBE_URL" 720p,720p60,480p,best -O \
  | ffmpeg -hide_banner -loglevel warning -re -i pipe:0 \
           -c:v libx264 -preset veryfast -profile:v main -level 4.0 \
           -vf scale=1280:720 -pix_fmt yuv420p \
           -g 60 -keyint_min 60 \
           -b:v 1500k -maxrate 1800k -bufsize 3000k \
           -c:a aac -b:a 96k -ar 44100 \
           -f rtsp -rtsp_transport tcp \
           "rtsp://localhost:8554/${STREAM_SLUG}"
