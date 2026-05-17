"""Telegram Bot API sender for tiered WildWatch alerts.

`send_alert` posts a Markdown message to the chat configured via
``TELEGRAM_BOT_TOKEN`` + ``TELEGRAM_CHAT_ID`` env vars. Clip URLs are
wrapped in the VideoDB console player URL so tapping the link in the
phone app opens a playable view, not a raw HLS manifest.

`build_message` is a pure formatter exposed for testing.
"""

from __future__ import annotations

import os
from urllib.parse import quote

import httpx

TIER_EMOJI: dict[int, str] = {1: "🟢", 2: "🟡", 3: "🔴"}
TIER_LABEL: dict[int, str] = {1: "INFO", 2: "NOTABLE", 3: "URGENT"}
PLAYER_PREFIX = "https://console.videodb.io/player?url="
SEND_MESSAGE_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"


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
    """
    emoji = TIER_EMOJI.get(tier, "⚪")
    tier_name = TIER_LABEL.get(tier, "?")
    parts = [f"{emoji} *[{tier_name}]* `{label}`"]
    if explanation:
        parts.append(explanation)
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
    # Don't call resp.raise_for_status() — its HTTPStatusError stringifies
    # the full request URL, which contains the bot token. When the
    # exception propagates up to logger.exception(..., exc_info=True) in
    # webhooks.py, that traceback writes the token into the log file.
    # Instead, surface status_code + sanitized body in a RuntimeError.
    if resp.status_code >= 400:
        body = resp.text[:300] if resp.text else ""
        raise RuntimeError(f"telegram send failed: HTTP {resp.status_code} body={body!r}")
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "telegram send failed"))
    return payload
