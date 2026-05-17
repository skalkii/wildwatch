#!/usr/bin/env bash
# Pull a YouTube live URL through streamlink + ffmpeg, republish it as
# RTSP via mediamtx, and tunnel it to a public host via bore.pub.
#
# Usage:
#   ./bridge/start_bridge.sh <youtube_url> <stream_slug>
#
# Example:
#   ./bridge/start_bridge.sh "https://www.youtube.com/watch?v=vr4o_AsrU1k" mara
#   # → publishes locally at rtsp://localhost:8554/mara
#   # → exposes publicly at rtsp://bore.pub:<port>/mara
#   # Paste the bore.pub URL back into the dashboard.
#
# Prerequisites (one-time install):
#   brew install streamlink ffmpeg mediamtx bore-cli
#
# Why bore? VideoDB cloud must reach the stream — localhost:8554 isn't
# routable from the internet. bore.pub free TCP tunnel forwards a public
# host:port to your local 8554. The remote port changes each restart, so
# always read it from this script's output (or `logs/run-current/bore.log`)
# rather than hard-coding.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat >&2 <<USAGE
Usage: $0 <youtube_url> <stream_slug>

Examples:
  $0 "https://www.youtube.com/watch?v=AeMUdOPFcXI" namibia
  $0 "https://www.youtube.com/watch?v=vr4o_AsrU1k" wildafrica

After the script prints "PUBLIC URL: rtsp://bore.pub:<port>/<slug>",
paste that URL into the dashboard's "Use bridge" input.
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

# mediamtx running?
if ! nc -z localhost 8554 2>/dev/null; then
  echo "ERROR: mediamtx is not listening on localhost:8554." >&2
  echo "       Start it first (one of):" >&2
  echo "         mediamtx bridge/mediamtx.yml &" >&2
  echo "         docker compose -f bridge/docker-compose.yml up -d mediamtx" >&2
  exit 3
fi

# bore-cli available?
HAS_BORE=0
if command -v bore >/dev/null 2>&1; then
  HAS_BORE=1
fi

# Start bore tunnel if not already running. Log file is the source of
# truth for the (changing) remote port.
LOG_DIR="logs/run-current"
mkdir -p "$LOG_DIR"
BORE_LOG="${LOG_DIR}/bore.log"
BORE_URL=""
if [[ $HAS_BORE -eq 1 ]]; then
  if ! pgrep -f 'bore local 8554' >/dev/null; then
    echo "Starting bore tunnel..." | tee -a "$BORE_LOG"
    bore local 8554 --to bore.pub >> "$BORE_LOG" 2>&1 &
    # bore prints "listening at bore.pub:<port>" once connected.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      sleep 1
      BORE_URL=$(grep -oE 'bore\.pub:[0-9]+' "$BORE_LOG" | tail -1 || true)
      if [[ -n "$BORE_URL" ]]; then
        break
      fi
    done
  else
    BORE_URL=$(grep -oE 'bore\.pub:[0-9]+' "$BORE_LOG" | tail -1 || true)
  fi
fi

if [[ -n "$BORE_URL" ]]; then
  cat <<INFO
================================================================
PUBLIC URL: rtsp://${BORE_URL}/${STREAM_SLUG}

Paste that into the dashboard's "Use bridge" input. The remote
port changes whenever bore restarts; if VideoDB later loses the
stream, re-run this script and update the source.

(Local fallback for VLC verification: rtsp://localhost:8554/${STREAM_SLUG})
================================================================
INFO
else
  cat >&2 <<WARN
WARNING: 'bore' not found OR bore failed to start. VideoDB cannot
read rtsp://localhost:8554 — local streams are blocked. Either:
  - brew install bore-cli, then re-run this script, OR
  - expose port 8554 via cloudflared / ngrok / port-forwarding
    and use that public host:port in the dashboard.

Proceeding without a public tunnel. You'll still see frames in
VLC at rtsp://localhost:8554/${STREAM_SLUG} but the dashboard
"Use bridge" submission will be rejected by VideoDB.
WARN
fi

echo
echo "Bridging YouTube → RTSP"
echo "  source: ${YOUTUBE_URL}"
echo "  local:  rtsp://localhost:8554/${STREAM_SLUG}"
echo "Press Ctrl+C to stop the streamlink+ffmpeg leg (bore keeps running)."
echo

streamlink "$YOUTUBE_URL" best -O \
  | ffmpeg -hide_banner -loglevel warning -re -i pipe:0 \
           -c copy -f rtsp -rtsp_transport tcp \
           "rtsp://localhost:8554/${STREAM_SLUG}"
