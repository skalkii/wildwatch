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
        if "player.videodb.io" in stream_url or "console.videodb.io" in stream_url:
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
    """Send a Markdown alert via Telegram Bot API."""
    token = bot_token or os.environ["TELEGRAM_BOT_TOKEN"]
    chat = chat_id or os.environ["TELEGRAM_CHAT_ID"]
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
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "telegram send failed"))
    return payload
