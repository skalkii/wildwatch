#!/usr/bin/env python3
"""
WebSocket event listener for VideoDB with auto-reconnect and graceful shutdown.

Usage:
  python scripts/ws_listener.py [OPTIONS] [output_dir]

Arguments:
  output_dir  Directory for output files (default: /tmp or VIDEODB_EVENTS_DIR env var)

Options:
  --cwd=PATH  Load .env from PATH instead of the current working directory.
              Use this when launching from a directory other than the project root.
  --clear     Clear the events file before starting (use when starting a new session)

Output files:
  <output_dir>/videodb_events.jsonl  - All WebSocket events (JSONL format)
  <output_dir>/videodb_ws_id         - WebSocket connection ID
  <output_dir>/videodb_ws_pid        - Process ID for easy termination

Output (first line, for parsing):
  WS_ID=<connection_id>

Examples:
  python scripts/ws_listener.py --cwd=/path/to/project &
  python scripts/ws_listener.py --clear --cwd=/path/to/project
  python scripts/ws_listener.py --clear /tmp/mydir   # Custom output dir
  kill $(cat /tmp/videodb_ws_pid)                    # Stop the listener
"""

import asyncio
import json
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

# Retry config
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 60  # seconds


def parse_args():
    clear = False
    output_dir = None
    cwd = None

    args = sys.argv[1:]
    for arg in args:
        if arg == "--clear":
            clear = True
        elif arg.startswith("--cwd="):
            cwd = arg.split("=", 1)[1]
        elif not arg.startswith("-"):
            output_dir = arg

    if output_dir is None:
        output_dir = os.environ.get("VIDEODB_EVENTS_DIR", "/tmp")

    return clear, Path(output_dir), cwd


# Module-level defaults so importing this file (e.g. from pytest) doesn't
# parse sys.argv — which would silently consume pytest's positional test
# paths as our OUTPUT_DIR. Real CLI invocation runs `_init_from_argv()`
# inside `main()` to populate these.
CLEAR_EVENTS: bool = False
OUTPUT_DIR: Path = Path(os.environ.get("VIDEODB_EVENTS_DIR", "/tmp"))
USER_CWD: str | None = None

EVENTS_FILE: Path = OUTPUT_DIR / "videodb_events.jsonl"
WS_ID_FILE: Path = OUTPUT_DIR / "videodb_ws_id"
PID_FILE: Path = OUTPUT_DIR / "videodb_ws_pid"


def _init_from_argv() -> None:
    """Parse argv, load .env, recompute paths. Only called from main()."""
    global CLEAR_EVENTS, OUTPUT_DIR, USER_CWD, EVENTS_FILE, WS_ID_FILE, PID_FILE
    CLEAR_EVENTS, OUTPUT_DIR, USER_CWD = parse_args()
    if USER_CWD:
        load_dotenv(Path(USER_CWD) / ".env")
    else:
        load_dotenv()
    EVENTS_FILE = OUTPUT_DIR / "videodb_events.jsonl"
    WS_ID_FILE = OUTPUT_DIR / "videodb_ws_id"
    PID_FILE = OUTPUT_DIR / "videodb_ws_pid"


import videodb  # noqa: E402  -- load_dotenv must run before SDK init (skill pattern)

# Track if this is the first connection (for clearing events)
_first_connection = True


def log(msg: str):
    """Log with timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _open_user_only_append(path: Path):
    """Open ``path`` for append with 0o600 perms (user-only).

    Default `open(path, "a")` inherits process umask (typically 0o644 →
    world-readable). The events JSONL contains alert metadata + stream
    URLs + the ws_connection_id; PID file leaks the listener pid; ws_id
    file leaks the active connection id (which a local attacker could
    use to spoof events). Force 0o600 at open time so multi-user hosts
    can't snoop.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(str(path), flags, 0o600)
    return os.fdopen(fd, "a", encoding="utf-8")


def append_event(event: dict):
    """Append event to JSONL file with timestamps."""
    # Single clock read — using two separate datetime.now(UTC) calls
    # produces an inconsistent (ts, unix_ts) pair if a clock adjustment
    # or leap second lands between the two reads.
    now = datetime.now(UTC)
    event["ts"] = now.isoformat()
    event["unix_ts"] = now.timestamp()
    with _open_user_only_append(EVENTS_FILE) as f:
        f.write(json.dumps(event) + "\n")


def _write_user_only(path: Path, contents: str) -> None:
    """Write ``contents`` to ``path`` with 0o600 perms (user-only)."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(str(path), flags, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(contents)


def write_pid():
    """Write PID file for easy process management."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _write_user_only(PID_FILE, str(os.getpid()))


def cleanup_pid():
    """Remove PID file on exit."""
    try:
        PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


async def listen_with_retry():
    """Main listen loop with auto-reconnect and exponential backoff."""
    global _first_connection

    retry_count = 0
    backoff = INITIAL_BACKOFF

    while retry_count < MAX_RETRIES:
        try:
            conn = videodb.connect()
            ws_wrapper = conn.connect_websocket()
            ws = await ws_wrapper.connect()
            ws_id = ws.connection_id

            # Ensure output directory exists
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

            # Clear events file only on first connection if --clear flag is set
            if _first_connection and CLEAR_EVENTS:
                EVENTS_FILE.unlink(missing_ok=True)
                log("Cleared events file")
            _first_connection = False

            # Write ws_id to file for easy retrieval. 0o600 because the
            # ws_id is the live channel auth token — a local attacker
            # with read access could connect and forge events.
            _write_user_only(WS_ID_FILE, ws_id)

            # Print ws_id (parseable format for LLM)
            if retry_count == 0:
                print(f"WS_ID={ws_id}", flush=True)
            log(f"Connected (ws_id={ws_id})")

            # Reset retry state on successful connection
            retry_count = 0
            backoff = INITIAL_BACKOFF

            # Listen for messages
            async for msg in ws.receive():
                append_event(msg)
                channel = msg.get("channel", msg.get("event", "unknown"))
                text = msg.get("data", {}).get("text", "")
                if text:
                    print(f"[{channel}] {text[:80]}", flush=True)

            # If we exit the loop normally, connection was closed
            log("Connection closed by server")

        except asyncio.CancelledError:
            log("Shutdown requested")
            raise
        except Exception as e:
            retry_count += 1
            log(f"Connection error: {e}")

            if retry_count >= MAX_RETRIES:
                log(f"Max retries ({MAX_RETRIES}) exceeded, exiting")
                break

            log(f"Reconnecting in {backoff}s (attempt {retry_count}/{MAX_RETRIES})...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


async def main_async():
    """Async main with signal handling."""
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal():
        log("Received shutdown signal")
        shutdown_event.set()

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Run listener with cancellation support
    listen_task = asyncio.create_task(listen_with_retry())
    shutdown_task = asyncio.create_task(shutdown_event.wait())

    _done, pending = await asyncio.wait(
        [listen_task, shutdown_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Cancel remaining tasks
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    log("Shutdown complete")


def main():
    _init_from_argv()
    write_pid()
    try:
        asyncio.run(main_async())
    finally:
        cleanup_pid()


if __name__ == "__main__":
    main()
