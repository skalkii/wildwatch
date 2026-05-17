"""T-33 cross-modal correlation engine — live loop wired to webhooks.

Loops every ``--interval`` seconds. For each rule in CORRELATION_RULES,
issues rt.search per (index, query), filters shots client-side to the
rule's window, fires a synthesised alert via the public webhook when all
queries match.

Cooldown prevents the same rule from spamming back-to-back.

Lookups: rtstream id + per-kind index ids loaded from .state.json
(populated by scripts/bootstrap.py).

Usage:
    python scripts/run_correlation.py --duration 300 --interval 30
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import httpx  # noqa: E402
import videodb  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from wildwatch.correlation import (  # noqa: E402
    CORRELATION_RULES,
    CorrelationState,
    evaluate_rule,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("wildwatch.correlation_loop")

STATE_FILE = REPO_ROOT / ".state.json"


def _load_state() -> dict:
    if not STATE_FILE.exists():
        sys.exit("ERROR: .state.json missing — run scripts/bootstrap.py first")
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        sys.exit("ERROR: .state.json corrupt")


def _build_search_fn(rt, index_ids_by_kind: dict[str, str]):
    """Return search_fn(kind, query) -> list[shot dict] for evaluate_rule.

    Calls rt.search scoped to the right scene_index_id. Each shot is a
    RTStreamShot object — convert to dict so the pure evaluator sees a
    uniform shape.
    """

    def search(kind: str, query: str) -> list[dict]:
        idx_id = index_ids_by_kind.get(kind)
        if not idx_id:
            return []
        result = rt.search(query=query, index_id=idx_id)
        out = []
        for sh in getattr(result, "shots", None) or []:
            out.append(
                {
                    "start": getattr(sh, "start", None),
                    "end": getattr(sh, "end", None),
                    "text": getattr(sh, "text", ""),
                    "score": getattr(sh, "search_score", None),
                    "scene_index_id": getattr(sh, "scene_index_id", None),
                }
            )
        return out

    return search


def _post_correlation(base_url: str, hit, rt=None, window_s: int = 30) -> bool:
    """POST synthesised event to our /webhook/{tier}. Returns True on 200.

    If ``rt`` is provided, generates a playable clip URL for the window
    ``[fired_at - window_s, fired_at]`` so the alert links to the actual
    moment — this is the canonical pattern from VideoDB skills'
    rtstream-reference.md (``rt.generate_stream(start, end)``).
    """
    stream_url: str | None = None
    if rt is not None:
        try:
            start = max(0, int(hit.fired_at - window_s))
            end = int(hit.fired_at)
            stream_url = rt.generate_stream(start=start, end=end)
        except Exception as e:
            logger.warning(
                "rt.generate_stream(%s, %s) failed for %s: %s: %s",
                int(hit.fired_at - window_s),
                int(hit.fired_at),
                hit.rule_name,
                type(e).__name__,
                e,
            )

    payload = {
        "event_id": f"corr-{hit.rule_name}-{int(hit.fired_at)}",
        "label": hit.synthesis_label,
        "confidence": min(1.0, 0.5 + 0.1 * sum(len(v) for v in hit.evidence.values())),
        "explanation": (
            f"Cross-modal correlation '{hit.rule_name}' matched: "
            + ", ".join(
                f"{kind}/{query[:30]}={len(shots)} shot(s)"
                for (kind, query), shots in hit.evidence.items()
            )
        ),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(hit.fired_at)),
        "stream_url": stream_url,
    }
    url = f"{base_url}/webhook/{hit.tier}"
    try:
        resp = httpx.post(url, json=payload, timeout=10.0)
        if resp.status_code == 200:
            print(f"   POSTED correlation hit -> {url}")
            return True
        logger.warning("POST %s returned %s: %s", url, resp.status_code, resp.text[:200])
        return False
    except Exception as e:
        logger.warning("POST %s failed: %s: %s", url, type(e).__name__, e)
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=30, help="seconds between sweeps")
    ap.add_argument(
        "--duration",
        type=int,
        default=300,
        help="total seconds to run (0 = until Ctrl+C)",
    )
    ap.add_argument("--cooldown", type=int, default=300, help="seconds between same-rule fires")
    ap.add_argument(
        "--stream",
        default="fallback_intruder",
        help="state.rtstreams key to use",
    )
    args = ap.parse_args()

    load_dotenv()
    state = _load_state()
    base_url = state.get("webhook_base_url")
    if not base_url:
        sys.exit("ERROR: webhook_base_url unset in state")

    rt_state = state.get("rtstreams", {}).get(args.stream)
    if not rt_state:
        sys.exit(f"ERROR: no rtstreams.{args.stream} in state — run bootstrap.py first")

    rt_id = rt_state["id"]
    index_ids = rt_state.get("indexes", {})
    if not index_ids:
        sys.exit(f"ERROR: no indexes for stream {args.stream}")

    print(f"stream:  {args.stream}  rt_id={rt_id}")
    print(f"indexes: {index_ids}")
    print(f"rules:   {len(CORRELATION_RULES)}")
    print(f"webhook: {base_url}")
    print(f"interval={args.interval}s  duration={args.duration}s  cooldown={args.cooldown}s")
    print()

    conn = videodb.connect()
    coll = conn.get_collection()
    rt = None
    for s in coll.list_rtstreams():
        if s.id == rt_id:
            rt = s
            break
    if rt is None:
        sys.exit(f"ERROR: rtstream {rt_id} not found in collection")

    search_fn = _build_search_fn(rt, index_ids)
    state_engine = CorrelationState()
    deadline = time.time() + args.duration if args.duration > 0 else float("inf")
    sweep = 0
    fires = 0
    post_failures = 0
    try:
        while time.time() < deadline:
            sweep += 1
            print(f"[sweep {sweep}] {time.strftime('%H:%M:%S', time.localtime(time.time()))}")
            for rule in CORRELATION_RULES:
                # Capture per-rule so search latency across rules doesn't
                # leave earlier rules' windows stale relative to the last one.
                now = time.time()
                if not state_engine.should_fire(rule["name"], now, cooldown=args.cooldown):
                    continue
                hit = evaluate_rule(rule, search_fn, now)
                if hit is not None:
                    print(f"  FIRE  {hit.rule_name} -> {hit.synthesis_label} (tier {hit.tier})")
                    for (kind, _query), shots in hit.evidence.items():
                        for sh in shots[:2]:
                            print(
                                f"     [{kind}] {sh.get('start')}-{sh.get('end')}: "
                                f"{sh.get('text', '')[:80]}"
                            )
                    if _post_correlation(base_url, hit, rt=rt):
                        fires += 1
                    else:
                        post_failures += 1
                    state_engine.mark_fired(rule["name"], at=now)
                else:
                    print(f"  -      {rule['name']} (no all-match)")
            remaining = max(0, deadline - time.time())
            sleep_for = min(args.interval, remaining)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\ninterrupted.")

    print(f"\ntotal sweeps: {sweep}  fires: {fires}  post_failures: {post_failures}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
