"""Tests for wildwatch.telegram.send_alert (httpx mocked via respx)."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from wildwatch.telegram import TIER_EMOJI, build_message, send_alert

TOKEN = "1234:abcdefg"
CHAT_ID = "8636175241"
PLAYER_TEMPLATE = "https://console.videodb.io/player?url="


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", CHAT_ID)


# ──── build_message: pure formatter ───────────────────────────────────────


def test_build_message_includes_tier_emoji() -> None:
    msg = build_message(tier=3, label="POACHING_ALERT", explanation="shot heard", stream_url=None)
    assert TIER_EMOJI[3] in msg
    # Snake-case event labels are now rendered as Title Case English.
    assert "Poaching Alert" in msg
    assert "shot heard" in msg


def test_build_message_sends_raw_hls_url_only() -> None:
    msg = build_message(
        tier=1,
        label="juvenile_present",
        explanation="calf at water edge",
        stream_url="https://rt.stream.videodb.io/m/foo.m3u8",
    )
    # User feedback: the console.videodb.io wrapper adds an empty preview
    # box on mobile Telegram + provides no value over the raw HLS link.
    # Only ONE link goes through now: the raw m3u8 (iOS Safari plays
    # natively; Android opens it in VLC tap-and-play).
    assert "console.videodb.io/player?url=" not in msg
    assert "https://rt.stream.videodb.io/m/foo.m3u8" in msg
    assert "▶" in msg


def test_build_message_skips_wrap_for_already_player_urls() -> None:
    msg = build_message(
        tier=2,
        label="alarm_call",
        explanation=None,
        stream_url="https://player.videodb.io/watch?v=Kk28WvXPjjE",
    )
    # No double-wrap: raw player URL passed through.
    assert "console.videodb.io/player?url=" not in msg
    assert "https://player.videodb.io/watch?v=Kk28WvXPjjE" in msg


def test_build_message_omits_play_link_when_stream_url_missing() -> None:
    msg = build_message(tier=2, label="alarm_call", explanation=None, stream_url=None)
    assert "play clip" not in msg.lower()


def test_build_message_unknown_tier_falls_back_to_white_circle() -> None:
    msg = build_message(tier=99, label="x", explanation=None, stream_url=None)
    assert "⚪" in msg


# ──── send_alert: posts to Telegram Bot API ───────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_send_alert_posts_to_send_message() -> None:
    route = respx.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    await send_alert(
        tier=2,
        label="alarm_call_detected",
        explanation="baboon alarm",
        stream_url=None,
    )
    assert route.called
    body = json.loads(route.calls.last.request.content)
    assert body["chat_id"] == CHAT_ID
    assert "baboon alarm" in body["text"]
    assert body.get("parse_mode") == "Markdown"


@pytest.mark.asyncio
@respx.mock
async def test_send_alert_raises_on_non_ok_response() -> None:
    respx.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": False, "description": "chat not found"})
    )
    with pytest.raises(RuntimeError, match="chat not found"):
        await send_alert(tier=1, label="x", explanation=None, stream_url=None)


@pytest.mark.asyncio
@respx.mock
async def test_send_alert_raises_on_http_error() -> None:
    respx.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage").mock(
        return_value=httpx.Response(500, json={"ok": False})
    )
    # send_alert now raises a sanitized RuntimeError instead of httpx's
    # HTTPStatusError so the bot token in the request URL doesn't leak
    # into log tracebacks.
    with pytest.raises(RuntimeError, match=r"telegram send failed: HTTP 500"):
        await send_alert(tier=1, label="x", explanation=None, stream_url=None)


@pytest.mark.asyncio
@respx.mock
async def test_send_alert_uses_env_creds_when_called_without_args() -> None:
    route = respx.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    await send_alert(tier=3, label="POACHING_ALERT", explanation=None, stream_url=None)
    assert route.called
