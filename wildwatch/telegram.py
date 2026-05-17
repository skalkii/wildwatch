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


def _escape_old_markdown(s: str) -> str:
    """Escape Telegram old-Markdown special chars in user-content strings.

    The four chars `_ * ` [` have special meaning in old Markdown mode:
      - `_italic_` / `*bold*` / `` `code` `` — wraps formatting
      - `[text](url)` — link syntax; an unclosed `[` triggers
        "can't parse entities: Can't find end of the entity ..."

    Our Path-B explanation strings carry bracket-tagged AI output
    (``[SCENE] ...``, ``[ANIMAL] ...``) directly from the species
    prompt. Without escaping, Telegram 400-rejects the message and
    the webhook handler then returns 500 — the phone never buzzes
    even though the SDK call "succeeded".
    """
    if not s:
        return ""
    out: list[str] = []
    for ch in s:
        if ch in ("_", "*", "`", "["):
            out.append("\\")
        out.append(ch)
    return "".join(out)


def build_message(
    tier: int,
    label: str,
    explanation: str | None,
    stream_url: str | None,
) -> str:
    """Return the Markdown body posted to Telegram.

    Note: Markdown link syntax `[label](url)` breaks Telegram's parser when
    the URL itself contains `?` + `=` (our console-player wrapper). Send raw
    URLs inline instead; Telegram auto-detects + makes them tappable.

    The ``explanation`` field is escaped via ``_escape_old_markdown``
    because Path-B alerts pass bracket-tagged AI output verbatim, which
    Telegram's parser interprets as unclosed link syntax and rejects.
    """
    emoji = TIER_EMOJI.get(tier, "⚪")
    tier_name = TIER_LABEL.get(tier, "?")
    parts = [f"{emoji} *[{tier_name}]* `{label}`"]
    if explanation:
        # 1. Humanise bracket-tagged AI output to plain lines.
        # 2. Escape Markdown specials so unbalanced `[` / `*` / `_` / `` ` ``
        #    chars left in the lead-in sentence don't break Telegram's parser.
        humanised = humanise_explanation(explanation)
        parts.append(_escape_old_markdown(humanised))
    if stream_url:
        already_player = stream_url.startswith(PLAYER_PREFIX) or stream_url.startswith(
            "https://player.videodb.io"
        )
        if already_player:
            # SDK-generated player URL — single link.
            parts.append(f"▶ {stream_url}")
        else:
            # Single link only — raw HLS m3u8. Works in iOS Safari natively
            # and opens in VLC on any platform via tap-and-play. User
            # feedback: the console.videodb.io wrapper added an ugly empty
            # preview box on mobile + still required the same VLC fallback
            # for Android, so it gave no value over the raw link.
            parts.append(f"▶ {stream_url}")
    return "\n".join(parts)


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
                    "parse_mode": "Markdown",
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
