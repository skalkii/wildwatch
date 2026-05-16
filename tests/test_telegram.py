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
    assert "POACHING_ALERT" in msg
    assert "shot heard" in msg


def test_build_message_includes_raw_stream_url() -> None:
    # Raw URL: no Markdown link, no console-player wrapping. Telegram auto-
    # detects + mobile browsers route .m3u8 to native player.
    msg = build_message(
        tier=1,
        label="juvenile_present",
        explanation="calf at water edge",
        stream_url="https://rt.stream.videodb.io/m/foo.m3u8",
    )
    assert "https://rt.stream.videodb.io/m/foo.m3u8" in msg
    assert "▶" in msg


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
    with pytest.raises(httpx.HTTPStatusError):
        await send_alert(tier=1, label="x", explanation=None, stream_url=None)


@pytest.mark.asyncio
@respx.mock
async def test_send_alert_uses_env_creds_when_called_without_args() -> None:
    route = respx.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True, "result": {}})
    )
    await send_alert(tier=3, label="POACHING_ALERT", explanation=None, stream_url=None)
    assert route.called
