"""Telegram Bot API sender for tiered WildWatch alerts.

`send_alert` posts a Markdown message to the chat configured via
``TELEGRAM_BOT_TOKEN`` + ``TELEGRAM_CHAT_ID`` env vars. Clip URLs are
wrapped in the VideoDB console player URL so tapping the link in the
phone app opens a playable view, not a raw HLS manifest.

`build_message` is a pure formatter exposed for testing.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

TIER_EMOJI: dict[int, str] = {1: "🟢", 2: "🟡", 3: "🔴"}
TIER_LABEL: dict[int, str] = {1: "INFO", 2: "NOTABLE", 3: "URGENT"}
PLAYER_PREFIX = "https://console.videodb.io/player?url="
SEND_MESSAGE_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"


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
        parts.append(_escape_old_markdown(explanation))
    if stream_url:
        # console.videodb.io/player has a JS HLS player that works in any
        # browser (Chrome on Android/desktop has no native HLS). URL-encode
        # the inner URL so the nested ?url= query doesn't break the parser.
        # If the URL is already a videodb player URL, send raw — no double-wrap.
        # Use startswith (not substring) so a URL that merely contains
        # 'console.videodb.io' as a parameter value still gets re-wrapped.
        already_player = stream_url.startswith(PLAYER_PREFIX) or stream_url.startswith(
            "https://player.videodb.io"
        )
        if already_player:
            parts.append(f"▶ {stream_url}")
        else:
            encoded = quote(stream_url, safe="")
            parts.append(f"▶ {PLAYER_PREFIX}{encoded}")
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
