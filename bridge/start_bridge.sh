#!/usr/bin/env bash
# Pull a YouTube live URL through streamlink + ffmpeg and republish it
# as a local RTSP stream via mediamtx.
#
# Usage:
#   ./bridge/start_bridge.sh <youtube_url> <stream_slug>
#
# Example:
#   ./bridge/start_bridge.sh "https://www.youtube.com/watch?v=vr4o_AsrU1k" mara
#   # → rtsp://localhost:8554/mara
#
# Prerequisites (one-time install):
#   brew install streamlink ffmpeg mediamtx
#
# In a separate terminal you must already have mediamtx running:
#   mediamtx bridge/mediamtx.yml
#
# Once this script is running, verify the stream with VLC:
#   vlc rtsp://localhost:8554/<stream_slug>
#
# Then paste rtsp://localhost:8554/<stream_slug> back into the dashboard's
# "Use bridge" input on the parked source card. The dashboard promotes the
# source to kind=rtsp and re-runs ingest against the local RTSP URL.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat >&2 <<USAGE
Usage: $0 <youtube_url> <stream_slug>

Examples:
  $0 "https://www.youtube.com/watch?v=AeMUdOPFcXI" namibia
  $0 "https://www.youtube.com/watch?v=vr4o_AsrU1k" wildafrica

The resulting RTSP URL will be: rtsp://localhost:8554/<stream_slug>
USAGE
  exit 1
fi

YOUTUBE_URL="$1"
STREAM_SLUG="$2"

# Required binaries.
for cmd in streamlink ffmpeg; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: '$cmd' not found. Install with: brew install streamlink ffmpeg" >&2
    exit 2
  fi
done

# mediamtx running check — soft probe; if RTSP port refuses, warn but proceed.
if ! nc -z localhost 8554 2>/dev/null; then
  cat >&2 <<WARN
WARNING: mediamtx does not appear to be listening on localhost:8554.
         Start it in another terminal first:
             mediamtx bridge/mediamtx.yml
         (or 'docker compose -f bridge/docker-compose.yml up -d mediamtx').

Proceeding anyway — ffmpeg will fail loudly if mediamtx is missing.
WARN
fi

echo "Bridging YouTube → RTSP"
echo "  source: ${YOUTUBE_URL}"
echo "  output: rtsp://localhost:8554/${STREAM_SLUG}"
echo "Press Ctrl+C to stop."
echo

# streamlink pulls the best available HLS variant + pipes raw MPEG-TS into
# ffmpeg, which repackages without re-encoding (-c copy) to RTSP/TCP.
streamlink "$YOUTUBE_URL" best -O \
  | ffmpeg -hide_banner -loglevel warning -re -i pipe:0 \
           -c copy -f rtsp -rtsp_transport tcp \
           "rtsp://localhost:8554/${STREAM_SLUG}"
