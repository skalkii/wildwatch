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

import html
import logging
import os
import re
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


_REWRITE_CACHE: dict[str, str] = {}
_REWRITE_CACHE_MAX = 256


def _rewrite_cache_key(label: str, raw: str) -> str:
    """Cache key — collapses near-identical fires so a stream that emits
    the same event-engine prose 50 times doesn't run 50 GenAI round-trips."""
    import hashlib

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
    cached = _REWRITE_CACHE.get(ckey)
    if cached is not None:
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
        if len(_REWRITE_CACHE) > _REWRITE_CACHE_MAX:
            # Cheap LRU-ish eviction: drop one arbitrary entry.
            _REWRITE_CACHE.pop(next(iter(_REWRITE_CACHE)))
        _REWRITE_CACHE[ckey] = out
        return out
    except Exception as e:
        logger.debug("genai_friendly_explanation failed: %r", e)
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
            logger.debug("send_alert: genai rewrite skipped: %r", e)

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
    """Send a daily digest message to Telegram.

    Same auth + transport semantics as send_alert. Splits across
    multiple sends only if the body exceeds Telegram's 4096-char
    cap — usually fits in one.
    """
    token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is unset; digest cannot be sent.")
    if not chat:
        raise RuntimeError("TELEGRAM_CHAT_ID is unset; digest cannot be sent.")

    body = build_digest_message(summary, analytics, player_url, n_clips, n_events)
    # Telegram caps a single sendMessage at 4096 chars. The digest
    # body is comfortably under that even with 6 species + 6 labels.
    if len(body) > 4090:
        body = body[:4080] + "\n…"
    url = SEND_MESSAGE_URL_TEMPLATE.format(token=token)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                url,
                json={
                    "chat_id": chat,
                    "text": body,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
    except httpx.TransportError as e:
        logger.warning(
            "telegram digest network error: %s (args=%r)",
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
    return payload
