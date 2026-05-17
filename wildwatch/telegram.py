"""Telegram Bot API sender for tiered WildWatch alerts.

`send_alert` posts a Markdown message to the chat configured via
``TELEGRAM_BOT_TOKEN`` + ``TELEGRAM_CHAT_ID`` env vars. Clip URLs are
wrapped in the VideoDB console player URL so tapping the link in the
phone app opens a playable view, not a raw HLS manifest.

`build_message` is a pure formatter exposed for testing. It humanises
the bracket-tagged AI output (`[SCENE]`, `[ANIMAL]`, `[NOTES]`,
`[SOUND]`, `[SIGNAL]`, `[SUMMARY]`) produced by the four prompts into
plain-text rows so the message reads as a digest, not a config dump.
"""

from __future__ import annotations

import hashlib
import html
import logging
import os
import re
import threading
from collections import OrderedDict
from typing import Any

import httpx

logger = logging.getLogger(__name__)

TIER_EMOJI: dict[int, str] = {1: "🟢", 2: "🟡", 3: "🔴"}
TIER_LABEL: dict[int, str] = {1: "INFO", 2: "NOTABLE", 3: "URGENT"}
PLAYER_PREFIX = "https://console.videodb.io/player?url="
SEND_MESSAGE_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"


_TAG_RE = re.compile(r"\[([A-Z_]+)\]")

# Acronyms preserved as-is when label-case-ifying snake_case event labels.
_ACRONYMS = {"HLS", "RTSP", "RTMP", "AI", "VLC", "AAC", "URL", "API", "ID"}


def friendly_label(label: str) -> str:
    """Turn `potential_human_intrusion_visual` into `Potential Human Intrusion Visual`.

    Splits on underscores, title-cases each word, preserves a small list
    of acronyms (HLS, RTSP, AI, …). Used in the Telegram header so
    rangers don't see snake_case identifiers.
    """
    if not label:
        return ""
    out: list[str] = []
    for w in label.split("_"):
        if not w:
            continue
        if w.upper() in _ACRONYMS:
            out.append(w.upper())
        else:
            out.append(w[0].upper() + w[1:].lower())
    return " ".join(out)


def _kv(s: str) -> dict[str, str]:
    """Parse ``key=value; key=value`` lists from a bracket-tag body."""
    out: dict[str, str] = {}
    for part in s.replace("\n", ";").split(";"):
        if "=" not in part:
            continue
        k, _, v = part.partition("=")
        k = k.strip()
        if k:
            out[k] = v.strip()
    return out


def humanise_explanation(text: str) -> str:
    """Turn bracket-tagged AI output into a readable digest.

    The four prompts (species / behavior / environment / audio) all
    emit ``[TAG] key=value; key=value`` lines that are great for the
    event engine but read like config dumps in Telegram. Parse those
    tags and emit one human-readable line per element. Strips brackets
    so Telegram's Markdown parser never trips on `[…]` link-syntax
    look-alikes (a separate `_escape_old_markdown` pass below handles
    the residual `_ * ` ` chars).

    Falls back to the raw text when no bracket structure is detected
    (e.g. for manually-fired alerts or rtstream callbacks).
    """
    if not text:
        return ""
    if "[" not in text:
        return text

    tokens = [t.strip() for t in _TAG_RE.split(text) if t and t.strip()]
    # `re.split` with a capture group emits: [pre_text, TAG, body, TAG, body, ...]
    # If `pre_text` was empty/whitespace it's filtered out; otherwise it's the
    # lead-in sentence we keep as the first line.
    parsed: dict = {
        "scene": None,
        "animals": [],
        "notes": None,
        "sounds": [],
        "signals": [],
        "summary": None,
        "lead": "",
    }
    if tokens and not _TAG_RE.fullmatch(f"[{tokens[0]}]") and not tokens[0].startswith("["):
        # First token before any [TAG] — lead-in sentence (the query / source line).
        parsed["lead"] = tokens.pop(0)

    i = 0
    while i < len(tokens):
        m = re.fullmatch(r"([A-Z_]+)", tokens[i])
        if not m:
            i += 1
            continue
        tag = m.group(1)
        body = tokens[i + 1] if i + 1 < len(tokens) else ""
        body = body.lstrip(":,; ").strip()
        if tag == "SCENE":
            parsed["scene"] = _kv(body)
        elif tag == "ANIMAL":
            parsed["animals"].append(_kv(body))
        elif tag == "NOTES":
            parsed["notes"] = f"{parsed['notes']} {body}" if parsed["notes"] else body
        elif tag == "SOUND":
            parsed["sounds"].append(_kv(body))
        elif tag == "SIGNAL":
            parsed["signals"].append(body)
        elif tag == "SUMMARY":
            parsed["summary"] = body
        i += 2

    has_structure = (
        parsed["scene"]
        or parsed["animals"]
        or parsed["notes"]
        or parsed["sounds"]
        or parsed["signals"]
        or parsed["summary"]
    )
    if not has_structure:
        return text

    lines: list[str] = []
    if parsed["lead"]:
        lines.append(parsed["lead"].rstrip(":"))

    if parsed["scene"]:
        sc = parsed["scene"]
        bits = []
        if sc.get("light_mode"):
            bits.append(sc["light_mode"].replace("_", " "))
        if sc.get("total"):
            bits.append(f"{sc['total']} animal(s)")
        if sc.get("state"):
            bits.append(sc["state"].replace("_", " "))
        if bits:
            lines.append("Scene: " + ", ".join(bits))

    for a in parsed["animals"]:
        species = (a.get("species") or "unknown").replace("_", " ")
        count = a.get("count") or "1"
        age_sex = a.get("age_sex")
        position = a.get("position")
        row = f"  - {species}"
        if count and count != "1":
            row += f" x{count}"
        if age_sex and age_sex.lower() != "unknown":
            row += f" ({age_sex})"
        if position:
            row += f" - {position}"
        lines.append(row)

    for s in parsed["sounds"]:
        cat = s.get("category", "")
        sound_type = (s.get("type") or "unknown").replace("_", " ")
        intensity = s.get("intensity")
        row = f"  - {sound_type}"
        if cat:
            row += f" ({cat})"
        if intensity:
            row += f" - {intensity}"
        lines.append(row)

    for sig in parsed["signals"]:
        lines.append(f"Signal: {sig.replace('_', ' ')}")

    if parsed["summary"]:
        lines.append(f"Audio summary: {parsed['summary']}")

    if parsed["notes"]:
        lines.append(f"Notes: {parsed['notes']}")

    return "\n".join(lines)


def _html_escape(s: str) -> str:
    """Escape HTML special chars in user-content for parse_mode=HTML."""
    if not s:
        return ""
    return html.escape(s, quote=False)


def build_message(
    tier: int,
    label: str,
    explanation: str | None,
    stream_url: str | None,
) -> str:
    """Return the HTML body posted to Telegram (parse_mode=HTML).

    HTML mode is used (not old Markdown) because:
      - clean ``<a href="URL">label</a>`` lets us hide long m3u8 URLs
        behind a short "Play clip" — the URL itself is never shown,
        so a 110-character signed manifest doesn't dominate the
        message. Old Markdown's ``[label](url)`` breaks on URLs
        containing ``?`` or ``=``.
      - escape rules are simpler (only ``< > &`` need escaping).
      - bracket-tagged AI output (``[SCENE]``, ``[ANIMAL]``) is
        unambiguous text — no special meaning in HTML mode.
    """
    emoji = TIER_EMOJI.get(tier, "⚪")
    tier_name = TIER_LABEL.get(tier, "?")
    pretty_label = friendly_label(label)
    parts = [f"{emoji} <b>[{_html_escape(tier_name)}] {_html_escape(pretty_label)}</b>"]
    if explanation:
        humanised = humanise_explanation(explanation)
        parts.append(_html_escape(humanised))
    if stream_url:
        # Hide the long m3u8 URL behind a short tappable label.
        # Telegram renders only the label text; the URL stays
        # invisible in the message body, so the chat reads cleanly
        # even when the signed manifest URL is 110+ chars.
        already_player = stream_url.startswith(PLAYER_PREFIX) or stream_url.startswith(
            "https://player.videodb.io"
        )
        link_label = "Play in browser" if already_player else "Play clip"
        # `quote=True` so any literal `"` in the URL doesn't break the
        # attribute. Stream URLs rarely have them but defensive.
        safe_url = html.escape(stream_url, quote=True)
        parts.append(f'▶ <a href="{safe_url}">{link_label}</a>')
    return "\n".join(parts)


# Bounded LRU keyed by sha256(label+raw). Accessed from multiple SDK
# worker threads (genai_friendly_explanation is invoked via
# asyncio.to_thread by send_alert), so the check-then-evict sequence
# below needs a lock — dict ops are individually atomic under the GIL
# but ``if len(c) > MAX: c.pop(next(iter(c)))`` is multi-step. Using
# OrderedDict + move_to_end gives real LRU semantics without
# breaking the public surface.
_REWRITE_CACHE: OrderedDict[str, str] = OrderedDict()
_REWRITE_CACHE_MAX = 256
_REWRITE_CACHE_LOCK = threading.Lock()


def _rewrite_cache_key(label: str, raw: str) -> str:
    """Cache key — collapses near-identical fires so a stream that emits
    the same event-engine prose 50 times doesn't run 50 GenAI round-trips."""
    return f"{label}:{hashlib.sha256(raw.encode('utf-8', errors='replace')).hexdigest()[:16]}"


def genai_friendly_explanation(
    coll: Any,
    tier: int,
    label: str,
    raw_explanation: str,
) -> str | None:
    """Use VideoDB's generate_text API to rewrite a Path-B explanation as a
    natural-language sentence the operator can read at a glance.

    Returns None on any failure — caller falls back to the bracket-parser
    in ``humanise_explanation``. Synchronous; called from a thread by
    ``send_alert`` via ``asyncio.to_thread``.

    Budget: ``model_name='basic'`` is cheap (~$0.0016 per 1000 tokens).
    Hard timeout via httpx in the SDK; if it stalls the alert ships with
    the parser-rendered fallback instead.
    """
    # Cache hit short-circuit — VideoDB's event engine emits very similar
    # prose for repeat fires of the same event. Under multi-stream load
    # the GenAI round-trip (12-18s cold start) blocked the webhook handler
    # until the SDK pool saturated. Cache keeps everything snappy.
    ckey = _rewrite_cache_key(label, raw_explanation)
    with _REWRITE_CACHE_LOCK:
        cached = _REWRITE_CACHE.get(ckey)
        if cached is not None:
            # Touch for LRU recency.
            _REWRITE_CACHE.move_to_end(ckey)
            return cached

    try:
        prompt = (
            "Rewrite the alert below as ONE short plain-English sentence "
            "that a park ranger reading their phone in the field can grasp "
            "in two seconds. Speak about what the camera saw, NOT about the "
            "alert system. Drop ALL of these: 'alert context', 'specifies "
            "triggering', 'conditions are met', 'flags contain', bracket "
            "tags like [SCENE]/[ANIMAL]/[NOTES], scene state codes "
            "(small_group, single_animal, etc.), query strings like "
            "'Query: ...', post-upload analysis jargon, 'detected' twice. "
            "Use everyday words ('a vehicle is visible', 'two zebras are "
            "drinking', 'a lion is resting near the waterhole'). No emoji, "
            "no markdown, no quotes around technical terms. Max 180 chars.\n\n"
            f"Alert label: {label}\n"
            f"Severity (1=info, 2=notable, 3=urgent): {tier}\n"
            f"Raw text the system generated:\n{raw_explanation[:1500]}"
        )
        out = coll.generate_text(prompt=prompt, model_name="basic")
        # VideoDB's generate_text returns `{"output": "..."}` for
        # response_type="text" (the default). Older / future versions
        # may use `text` / `response`. Look at every known shape.
        if isinstance(out, dict):
            out = out.get("output") or out.get("text") or out.get("response") or ""
        out = (out or "").strip()
        if not out:
            return None
        # Defensive trim — genai may sometimes ignore the 200-char cap.
        if len(out) > 400:
            out = out[:400].rstrip() + "..."
        # Cache the successful rewrite for future identical fires.
        # Lock the eviction → insert pair so a concurrent caller can't
        # see an over-sized cache or drop the entry we just inserted.
        with _REWRITE_CACHE_LOCK:
            while len(_REWRITE_CACHE) >= _REWRITE_CACHE_MAX:
                _REWRITE_CACHE.popitem(last=False)  # drop oldest
            _REWRITE_CACHE[ckey] = out
        return out
    except Exception as e:
        # Promoted from DEBUG to WARNING: a stuck or failing
        # generate_text call means every alert is falling back to the
        # raw bracket-tag parser, which the operator can't see without
        # this log line at default level.
        logger.warning("genai_friendly_explanation failed: %r", e)
        return None


# Module-level coll handle — webhooks.py wires this in at import time so
# send_alert can call generate_text without re-creating the SDK client
# on every alert. None means "skip the genai rewrite; use parser only".
_COLL_GETTER: Any = None


def configure_coll_getter(getter: Any) -> None:
    """Webhooks calls this once at import to give send_alert a way to fetch
    the cached VideoDB collection. ``getter`` is a sync callable returning
    a collection-like object exposing ``generate_text``.
    """
    global _COLL_GETTER
    _COLL_GETTER = getter


async def send_alert(
    tier: int,
    label: str,
    explanation: str | None = None,
    stream_url: str | None = None,
    *,
    bot_token: str | None = None,
    chat_id: str | None = None,
    use_genai: bool = True,
) -> dict:
    """Send a Markdown alert via Telegram Bot API.

    Raises ``RuntimeError`` with an explicit message if the bot token or chat
    id are unset, rather than the opaque ``KeyError`` ``os.environ[...]``
    would otherwise raise.

    When ``use_genai`` is True (default) AND ``configure_coll_getter`` was
    called AND the explanation is bracket-tagged Path-B output, we ask
    VideoDB's generate_text API to rewrite it as one human sentence
    before assembling the message. Fail-soft: any error falls back to
    the local bracket-parser via ``humanise_explanation``.
    """
    token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    if not token:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is unset; alert cannot be sent. "
            "Configure it in .env or pass bot_token explicitly."
        )
    if not chat:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID is unset; alert cannot be sent. "
            "Configure it in .env or pass chat_id explicitly."
        )

    # GenAI rewrite of the explanation. Fires on ANY non-trivial
    # explanation regardless of whether it's bracket-tagged Path-B
    # output or VideoDB event-engine prose. Both read robotically out
    # of the box (the event engine echoes the alert's prompt verbatim:
    # "The alert context specifies triggering when 'flags contain
    # human_made_object_visible AND the time is night'..."). The
    # rewriter converts both into a one-line ranger-friendly sentence.
    #
    # Skipped only when explanation is short (<40 chars — already
    # human-readable) or empty.
    final_explanation = explanation
    rich_enough = bool(explanation) and len(explanation or "") >= 40
    if use_genai and rich_enough and _COLL_GETTER is not None:
        try:
            import asyncio as _asyncio

            coll = await _asyncio.wait_for(_asyncio.to_thread(_COLL_GETTER), timeout=5.0)
            rewritten = await _asyncio.wait_for(
                _asyncio.to_thread(
                    genai_friendly_explanation, coll, tier, label, explanation or ""
                ),
                timeout=8.0,
            )
            if rewritten:
                final_explanation = rewritten
        except Exception as e:
            # WARNING not DEBUG — a stuck rewrite path is invisible at
            # default log level otherwise.
            logger.warning("send_alert: genai rewrite skipped: %r", e)

    text = build_message(
        tier=tier, label=label, explanation=final_explanation, stream_url=stream_url
    )
    url = SEND_MESSAGE_URL_TEMPLATE.format(token=token)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                url,
                json={
                    "chat_id": chat,
                    "text": text,
                    "parse_mode": "HTML",
                    # Disable Telegram's link-preview card on the console
                    # player URL — it renders an ugly empty box on mobile
                    # because the page is a JS-loaded SPA with no OG tags.
                    "disable_web_page_preview": True,
                },
            )
    except httpx.TransportError as e:
        # httpx transport-layer exceptions (ConnectTimeout, ConnectError,
        # ReadError) carry the full request URL via e.request.url — that
        # URL contains the bot token. If we let this propagate with
        # `from e`, logger.exception(..., exc_info=True) prints the chained
        # traceback INCLUDING the URL, leaking the token to logs.
        # Re-raise WITHOUT `from e` to break the exception chain.
        #
        # Preserve diagnostic info via a separate log line — operators
        # need to know what failed (timeout vs DNS vs reset) but should
        # not get the token in any traceback. `e.args` is the safe
        # surface: it carries the error message without the URL chain.
        logger.warning(
            "telegram network error: %s (args=%r)",
            type(e).__name__,
            getattr(e, "args", ()),
        )
        raise RuntimeError(f"telegram network error: {type(e).__name__}") from None
    # Don't call resp.raise_for_status() either — HTTPStatusError ALSO
    # stringifies the URL with the bot token. Surface status + sanitized
    # body in a RuntimeError instead.
    if resp.status_code >= 400:
        body = resp.text[:300] if resp.text else ""
        raise RuntimeError(f"telegram send failed: HTTP {resp.status_code} body={body!r}")
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "telegram send failed"))
    return payload


# ──── Daily-digest delivery to Telegram ────
# Unicode-block bar charts so the message looks visually similar to
# the dashboard modal without needing image generation. Telegram's
# HTML mode + a <pre> block preserves alignment for the bars.

_BAR_GLYPH = "█"  # one filled cell per scaled count
_BAR_MAX = 14  # max bar width in chars — keeps mobile-portrait clean


def _bar(count: int, peak: int, width: int = _BAR_MAX) -> str:
    if peak <= 0 or count <= 0:
        return ""
    n = max(1, round(count / peak * width))
    return _BAR_GLYPH * n


def _spark_24(hourly: list[int]) -> str:
    """Render 24 hourly counts as a single-line spark using 8-level blocks."""
    levels = " ▁▂▃▄▅▆▇█"
    peak = max(hourly or [0]) or 1
    return "".join(levels[min(8, round(v / peak * 8))] for v in hourly[:24])


# ──── QuickChart.io chart-image URLs ────
# Free public service that renders Chart.js JSON configs as PNGs.
# We send the URLs to Telegram via sendMediaGroup; Telegram fetches
# them server-side and renders as a 2x2 photo album. No deps, no
# image generation locally, no auth. Quota is generous for hackathon
# use; if QuickChart is unreachable the caller falls back to the
# text-only digest.
_QUICKCHART = "https://quickchart.io/chart"

_PALETTE_WARM = [
    "#cc785c",
    "#d49671",
    "#dcb286",
    "#e3cd9c",
    "#a39a8f",
    "#7a9e9f",
    "#5b818d",
    "#46647a",
]
_PALETTE_CATS = {
    "visual": "#38bdf8",
    "audio": "#a78bfa",
    "behaviour": "#22c55e",
    "environment": "#f59e0b",
    "threat": "#ef4444",
}
_CHART_BG = "#131e1c"  # matches dashboard dark surface
_CHART_TEXT = "#e6edeb"
_CHART_GRID = "#1f2a27"
_CHART_ACCENT = "#34d399"  # dashboard's dark-mode accent green


def _quickchart_url(config: dict, w: int = 720, h: int = 420) -> str:
    """Return a QuickChart GET URL that renders ``config`` as a PNG.

    JSON is passed inline so we don't depend on QuickChart's
    short-URL endpoint. The URL stays under ~3KB for the configs we
    build below, well within HTTP GET limits.
    """
    import json as _json
    from urllib.parse import quote

    c = quote(_json.dumps(config, separators=(",", ":")))
    bg = quote(_CHART_BG)
    return f"{_QUICKCHART}?w={w}&h={h}&bkg={bg}&c={c}&v=4"


def _chart_axes() -> dict:
    return {
        "x": {
            "ticks": {"color": _CHART_TEXT, "font": {"size": 11}},
            "grid": {"color": _CHART_GRID, "display": True},
        },
        "y": {
            "ticks": {"color": _CHART_TEXT, "font": {"size": 11}},
            "grid": {"color": _CHART_GRID},
            "beginAtZero": True,
        },
    }


def _digest_chart_urls(analytics: dict) -> list[tuple[str, str]]:
    """Build (caption, QuickChart-URL) pairs for the digest album.

    Returns up to four images — one per chart kind. Skips a chart
    entirely if its data is empty so the album doesn't carry placeholder
    "no data" tiles.
    """
    a = analytics or {}
    hourly = a.get("hourly") or []
    species = a.get("species") or []
    labels = a.get("top_labels") or []
    cats = a.get("categories") or {}

    pairs: list[tuple[str, str]] = []

    # 1. Hourly activity bar.
    if any(hourly):
        cfg = {
            "type": "bar",
            "data": {
                "labels": [f"{i:02d}" for i in range(24)],
                "datasets": [
                    {
                        "label": "Events",
                        "data": hourly,
                        "backgroundColor": _CHART_ACCENT,
                        "borderRadius": 3,
                    }
                ],
            },
            "options": {
                "plugins": {
                    "title": {
                        "display": True,
                        "text": "Hourly activity · last 24h",
                        "color": _CHART_TEXT,
                        "font": {"size": 16, "weight": "bold"},
                    },
                    "legend": {"display": False},
                },
                "scales": _chart_axes(),
            },
        }
        pairs.append(("🕒 Hourly activity", _quickchart_url(cfg, w=720, h=380)))

    # 2. Top species donut.
    sp = species[:8]
    if sp:
        cfg = {
            "type": "doughnut",
            "data": {
                "labels": [str(k) for k, _ in sp],
                "datasets": [
                    {
                        "data": [int(v) for _, v in sp],
                        "backgroundColor": _PALETTE_WARM[: len(sp)],
                        "borderColor": _CHART_BG,
                        "borderWidth": 2,
                    }
                ],
            },
            "options": {
                "plugins": {
                    "title": {
                        "display": True,
                        "text": "Top species seen",
                        "color": _CHART_TEXT,
                        "font": {"size": 16, "weight": "bold"},
                    },
                    "legend": {
                        "position": "right",
                        "labels": {"color": _CHART_TEXT, "font": {"size": 12}},
                    },
                    "datalabels": {"color": _CHART_TEXT, "font": {"weight": "bold"}},
                },
                "cutout": "55%",
            },
        }
        pairs.append(("🦁 Top species seen", _quickchart_url(cfg, w=720, h=420)))

    # 3. Event mix donut (categories — visual/audio/behaviour/env/threat).
    cat_labels = [
        k for k in ("visual", "audio", "behaviour", "environment", "threat") if cats.get(k, 0) > 0
    ]
    if cat_labels:
        cfg = {
            "type": "doughnut",
            "data": {
                "labels": [k.title() for k in cat_labels],
                "datasets": [
                    {
                        "data": [int(cats[k]) for k in cat_labels],
                        "backgroundColor": [_PALETTE_CATS[k] for k in cat_labels],
                        "borderColor": _CHART_BG,
                        "borderWidth": 2,
                    }
                ],
            },
            "options": {
                "plugins": {
                    "title": {
                        "display": True,
                        "text": "Event mix by type",
                        "color": _CHART_TEXT,
                        "font": {"size": 16, "weight": "bold"},
                    },
                    "legend": {
                        "position": "right",
                        "labels": {"color": _CHART_TEXT, "font": {"size": 12}},
                    },
                },
                "cutout": "55%",
            },
        }
        pairs.append(("🎯 Event mix by type", _quickchart_url(cfg, w=720, h=420)))

    # 4. Top labels horizontal bar.
    lbls = labels[:8]
    if lbls:
        cfg = {
            "type": "bar",
            "data": {
                "labels": [friendly_label(k)[:24] for k, _ in lbls],
                "datasets": [
                    {
                        "label": "Count",
                        "data": [int(v) for _, v in lbls],
                        "backgroundColor": _CHART_ACCENT,
                        "borderRadius": 3,
                    }
                ],
            },
            "options": {
                "indexAxis": "y",
                "plugins": {
                    "title": {
                        "display": True,
                        "text": "What fired most",
                        "color": _CHART_TEXT,
                        "font": {"size": 16, "weight": "bold"},
                    },
                    "legend": {"display": False},
                },
                "scales": {
                    "x": {
                        "ticks": {"color": _CHART_TEXT, "font": {"size": 11}},
                        "grid": {"color": _CHART_GRID},
                        "beginAtZero": True,
                    },
                    "y": {
                        "ticks": {"color": _CHART_TEXT, "font": {"size": 11}},
                        "grid": {"color": _CHART_GRID, "display": False},
                    },
                },
            },
        }
        pairs.append(("📈 What fired most", _quickchart_url(cfg, w=720, h=420)))

    return pairs


def build_digest_message(
    summary: str,
    analytics: dict,
    player_url: str | None,
    n_clips: int,
    n_events: int,
) -> str:
    """Compose the HTML Telegram body for a daily digest delivery.

    Mirrors the modal's information hierarchy: KPI stats, spark of
    hourly activity, top species + top labels as bar charts, the
    narration paragraph, and finally a tappable Play link.
    """
    a = analytics or {}
    tc = a.get("tier_counts") or {1: 0, 2: 0, 3: 0}
    hourly = a.get("hourly") or [0] * 24
    species = a.get("species") or []
    labels = a.get("top_labels") or []

    parts: list[str] = []
    parts.append("📊 <b>WildWatch · Daily Summary</b>")
    parts.append(f"<i>Last 24h · {n_events} events · {n_clips} clips in reel</i>")
    parts.append("")
    parts.append(
        f"🔵 <b>Info {tc.get(1, 0)}</b>   "
        f"🟡 <b>Notable {tc.get(2, 0)}</b>   "
        f"🔴 <b>Urgent {tc.get(3, 0)}</b>"
    )
    parts.append("")
    parts.append("<b>When</b> · activity per hour (00→23)")
    parts.append(f"<pre>{_html_escape(_spark_24(hourly))}</pre>")
    if labels:
        peak = max(c for _, c in labels[:6]) or 1
        rows = "\n".join(
            f"{friendly_label(lbl)[:22]:<22} {_bar(c, peak):<{_BAR_MAX}} {c}"
            for lbl, c in labels[:6]
        )
        parts.append("<b>What fired most</b>")
        parts.append(f"<pre>{_html_escape(rows)}</pre>")
    if species:
        peak = max(c for _, c in species[:6]) or 1
        rows = "\n".join(
            f"{str(sp)[:18]:<18} {_bar(c, peak):<{_BAR_MAX}} {c}" for sp, c in species[:6]
        )
        parts.append("<b>Top species seen</b>")
        parts.append(f"<pre>{_html_escape(rows)}</pre>")
    if summary:
        parts.append("<b>Narration</b>")
        parts.append(_html_escape(summary))
    if player_url:
        parts.append("")
        safe = _html_escape(player_url)
        parts.append(f'▶ <a href="{safe}">Watch full reel on VideoDB</a>')
    return "\n".join(parts)


async def send_digest(
    summary: str,
    analytics: dict,
    player_url: str | None,
    n_clips: int,
    n_events: int,
    *,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> dict:
    """Send the daily digest to Telegram.

    Two-step delivery:
      1. ``sendMediaGroup`` with up to 4 chart PNGs rendered by
         QuickChart.io. Telegram fetches each URL server-side and
         renders an album. First photo carries a caption with the
         header + KPI line.
      2. ``sendMessage`` with the narration paragraph + reel link.

    Falls back gracefully if QuickChart is unreachable or returns
    no usable URLs — the text-only message still goes out.
    """
    token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is unset; digest cannot be sent.")
    if not chat:
        raise RuntimeError("TELEGRAM_CHAT_ID is unset; digest cannot be sent.")

    a = analytics or {}
    tc = a.get("tier_counts") or {1: 0, 2: 0, 3: 0}
    header_caption = (
        f"📊 <b>WildWatch · Daily Summary</b>\n"
        f"<i>Last 24h · {n_events} events · {n_clips} clips in reel</i>\n"
        f"🔵 <b>Info {tc.get(1, 0)}</b>   "
        f"🟡 <b>Notable {tc.get(2, 0)}</b>   "
        f"🔴 <b>Urgent {tc.get(3, 0)}</b>"
    )
    # Trim caption to Telegram's 1024-char photo-caption cap. We
    # only attach a caption to the FIRST photo of the album.
    if len(header_caption) > 1020:
        header_caption = header_caption[:1010] + "\n…"

    chart_pairs = _digest_chart_urls(a)

    async with httpx.AsyncClient(timeout=15.0) as client:
        media_payload = None
        if chart_pairs:
            # sendMediaGroup takes an array of InputMediaPhoto objects.
            # Telegram fetches each URL itself — it does not relay them
            # through us. Quota is per-bot, not per-URL.
            media = []
            for i, (_cap, photo_url) in enumerate(chart_pairs[:4]):
                item: dict = {"type": "photo", "media": photo_url}
                if i == 0:
                    item["caption"] = header_caption
                    item["parse_mode"] = "HTML"
                media.append(item)
            try:
                group_url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
                media_resp = await client.post(group_url, json={"chat_id": chat, "media": media})
                if media_resp.status_code < 400 and media_resp.json().get("ok"):
                    media_payload = media_resp.json()
                else:
                    logger.warning(
                        "send_digest: sendMediaGroup HTTP %s body=%r",
                        media_resp.status_code,
                        media_resp.text[:300],
                    )
            except httpx.TransportError as e:
                logger.warning(
                    "send_digest: sendMediaGroup network err: %s (args=%r)",
                    type(e).__name__,
                    getattr(e, "args", ()),
                )

        # Always send the text message — carries the narration +
        # reel link. If the media group failed it ALSO carries the
        # header so the operator still sees stats.
        body_parts: list[str] = []
        if media_payload is None:
            # Fallback: include header + ASCII charts + KPIs.
            body_parts.append(build_digest_message(summary, a, player_url, n_clips, n_events))
        else:
            # Album already showed the header; text message focuses
            # on narration + link.
            if summary:
                body_parts.append("📝 <b>Narration</b>")
                body_parts.append(_html_escape(summary))
            if player_url:
                body_parts.append("")
                safe = _html_escape(player_url)
                body_parts.append(f'▶ <a href="{safe}">Watch full reel on VideoDB</a>')
        body = "\n".join(body_parts) or "(empty digest)"
        if len(body) > 4090:
            body = body[:4080] + "\n…"
        msg_url = SEND_MESSAGE_URL_TEMPLATE.format(token=token)
        try:
            resp = await client.post(
                msg_url,
                json={
                    "chat_id": chat,
                    "text": body,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
        except httpx.TransportError as e:
            logger.warning(
                "telegram digest text network error: %s (args=%r)",
                type(e).__name__,
                getattr(e, "args", ()),
            )
            raise RuntimeError(f"telegram network error: {type(e).__name__}") from None
        if resp.status_code >= 400:
            body_resp = resp.text[:300] if resp.text else ""
            raise RuntimeError(
                f"telegram digest send failed: HTTP {resp.status_code} body={body_resp!r}"
            )
        payload = resp.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description", "telegram digest send failed"))
        # Return text-message payload (the canonical receipt) with a
        # hint about whether the album was delivered.
        payload["_album_sent"] = media_payload is not None
        payload["_album_count"] = len(chart_pairs) if media_payload else 0
        return payload
