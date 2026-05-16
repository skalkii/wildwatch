#!/usr/bin/env bash
# Auto-restart bore TCP tunnel if it dies.
# bore.pub free service drops sessions every 30-60min. When restarted the
# REMOTE PORT CHANGES, so any VideoDB rtstreams pointing at the old URL go
# stale. Operator must re-run start_live_test.py with the new BORE_PUBLIC
# whenever bore restarts. The log file is the source of truth for current
# remote_port — grep for "listening at bore.pub:" to find it.

set -uo pipefail

LOG_DIR="${LOG_DIR:-logs/run-current}"
mkdir -p "$LOG_DIR"
LOG="${LOG_DIR}/bore.log"

restart_bore() {
  echo "[$(date +%H:%M:%S)] watchdog (re)starting bore" | tee -a "$LOG"
  bore local 8554 --to bore.pub >> "$LOG" 2>&1 &
}

while true; do
  if ! pgrep -f 'bore local 8554' > /dev/null; then
    restart_bore
    sleep 5  # let it print its listening URL
    grep -oE 'bore\.pub:[0-9]+' "$LOG" | tail -1 | tee -a "${LOG_DIR}/bore_current_url.txt"
  fi
  sleep 30
done
