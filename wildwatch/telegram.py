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
from urllib.parse import quote

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
            # Send TWO links: console.videodb.io/player (Chrome/desktop JS HLS
            # player) AND the raw m3u8 (iOS Safari plays HLS natively, VLC /
            # any external player opens it from the tap-and-play action).
            # Operators using mobile Android Telegram occasionally report the
            # console player not loading; the raw m3u8 gives them a fallback.
            encoded = quote(stream_url, safe="")
            parts.append(f"▶ Play: {PLAYER_PREFIX}{encoded}")
            parts.append(f"🎞 Raw HLS (iOS/VLC): {stream_url}")
    return "\n".join(parts)


async def send_alert(
    tier: int,
    label: str,
    explanation: str | None = None,
    stream_url: str | None = None,
    *,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> dict:
    """Send a Markdown alert via Telegram Bot API.

    Raises ``RuntimeError`` with an explicit message if the bot token or chat
    id are unset, rather than the opaque ``KeyError`` ``os.environ[...]``
    would otherwise raise.
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
    text = build_message(tier=tier, label=label, explanation=explanation, stream_url=stream_url)
    url = SEND_MESSAGE_URL_TEMPLATE.format(token=token)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                url,
                json={
                    "chat_id": chat,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": False,
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
