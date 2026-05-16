"""First end-to-end VideoDB smoke: connect + Small-tier sandbox + immediate stop.

Cost: <$0.02 (Small tier $1/h, expected wall time <60s).

Usage:
    python scripts/sdk_smoke.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import videodb
from dotenv import load_dotenv
from videodb import SandboxTier

from wildwatch.sandbox import managed_sandbox


def main() -> int:
    load_dotenv()
    print("[1/4] videodb.connect() ...")
    conn = videodb.connect()
    coll = conn.get_collection()
    print(f"      ok   collection.id = {coll.id}")

    print("[2/4] ensure + wait_for_ready (Small tier) ...")
    with managed_sandbox(conn, tier=SandboxTier.small) as sb:
        print(f"      ok   sandbox.id     = {sb.id}")
        print(f"           sandbox.status = {sb.status}")
        print(f"           sandbox.tier   = {sb.tier}")
        print(f"           is_active      = {sb.is_active}")
        print("[3/4] no-op (immediate teardown to minimize burn) ...")

    print("[4/4] stopped + waited; managed_sandbox exited cleanly.")
    state_path = Path(__file__).resolve().parent.parent / ".state.json"
    if state_path.exists():
        print(f"      state file: {state_path}")
        print(f"      contents:   {state_path.read_text().strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
