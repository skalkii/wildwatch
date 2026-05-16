#!/usr/bin/env bash
# Auto-restart streamlink+ffmpeg bridges if they die.
# Loops every 30s. macOS default bash 3.x — avoid associative arrays.
set -uo pipefail

LOG_DIR="${LOG_DIR:-logs/run-current}"
mkdir -p "$LOG_DIR"

PAIRS=(
  "mara|https://www.youtube.com/watch?v=ACc7IkdOF-Y"
  "hwange|https://www.youtube.com/watch?v=-rXriX4SiQk"
  "amboseli|https://www.youtube.com/watch?v=XyPU5-pNg5E"
)

restart_bridge() {
  local name="$1"
  local url="$2"
  local log="${LOG_DIR}/bridge-${name}.log"
  echo "[$(date +%H:%M:%S)] watchdog restarting bridge: $name" | tee -a "$log"
  (streamlink --stream-segment-timeout 30 "$url" best -O \
    | ffmpeg -re -i pipe:0 -c:v copy -c:a aac -b:a 96k -ar 44100 \
        -f rtsp -rtsp_transport tcp "rtsp://localhost:8554/${name}") \
    >> "$log" 2>&1 &
}

while true; do
  for pair in "${PAIRS[@]}"; do
    name="${pair%%|*}"
    url="${pair##*|}"
    if ! pgrep -f "ffmpeg.*rtsp://localhost:8554/${name}" > /dev/null; then
      restart_bridge "$name" "$url"
    fi
  done
  sleep 30
done
