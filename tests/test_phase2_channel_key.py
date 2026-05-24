"""Phase 2 — send_message channel key path tests.

Verifies that send_message routes to /api/transform with X-Channel-Key
when HERMES_ECLAW_PREFER_TRANSFORM_VIA_CHANNEL_KEY=true, and falls back
to /api/channel/message when the flag is false (default).
"""
from __future__ import annotations

import os
import sys

import pytest
from aiohttp import web, ClientSession
from aiohttp.test_utils import TestServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


_BRIDGE_ENV_KEYS = (
    "HERMES_ECLAW_API_KEY",
    "HERMES_ECLAW_DEVICE_ID",
    "HERMES_ECLAW_ENTITY_ID",
    "HERMES_ECLAW_BOT_SECRET",
    "HERMES_ECLAW_CALLBACK_TOKEN",
    "HERMES_ECLAW_PREFER_TRANSFORM_VIA_CHANNEL_KEY",
)


@pytest.fixture(autouse=True)
def _restore_bridge_env():
    old_env = {k: os.environ.get(k) for k in _BRIDGE_ENV_KEYS}
    yield
    for k, v in old_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _ensure_bridge_env(api_key: str = "eck_testkey"):
    os.environ["HERMES_ECLAW_API_KEY"] = api_key
    os.environ["HERMES_ECLAW_DEVICE_ID"] = "dev-uuid"
    os.environ["HERMES_ECLAW_ENTITY_ID"] = "3"
    os.environ["HERMES_ECLAW_BOT_SECRET"] = "botsecret"
    os.environ["HERMES_ECLAW_CALLBACK_TOKEN"] = "tok"


def _reload_bridge(prefer_channel_key: bool = False, api_key: str = "eck_testkey"):
    """Force reimport so bridge re-reads env vars."""
    _ensure_bridge_env(api_key=api_key)
    os.environ["HERMES_ECLAW_PREFER_TRANSFORM_VIA_CHANNEL_KEY"] = "true" if prefer_channel_key else "false"
    for k in list(sys.modules):
        if k == "plugin" or k.startswith("plugin."):
            del sys.modules[k]
    from plugin import eclaw_bridge  # noqa: WPS433
    return eclaw_bridge


# ── channel key path ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_message_uses_transform_when_flag_true():
    bridge = _reload_bridge(prefer_channel_key=True)
    assert bridge.PREFER_TRANSFORM_VIA_CHANNEL_KEY is True

    captured: dict = {}

    async def handler(req):
        captured["headers"] = dict(req.headers)
        captured["body"] = await req.json()
        return web.json_response({"success": True})

    app = web.Application()
    app.router.add_post("/api/transform", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            await bridge.send_message(s, "hello via channel key")

        assert "x-channel-key" in {k.lower() for k in captured["headers"]}
        assert captured["body"]["actAs"] == "channel"
        assert captured["body"]["deviceId"] == "dev-uuid"
        assert captured["body"]["entityId"] == 3
        assert captured["body"]["message"] == "hello via channel key"
        assert "botSecret" not in captured["body"]
        assert "channel_api_key" not in captured["body"]
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_send_message_channel_key_forwards_sender_hint():
    bridge = _reload_bridge(prefer_channel_key=True)

    captured: dict = {}

    async def handler(req):
        captured["body"] = await req.json()
        return web.json_response({"success": True})

    app = web.Application()
    app.router.add_post("/api/transform", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            await bridge.send_message(
                s, "reply", sender_hint={"kind": "entity", "entityId": 5}
            )
        assert captured["body"]["senderHint"] == {"kind": "entity", "entityId": 5}
    finally:
        await server.close()


# ── legacy path (default) ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_message_uses_channel_message_when_flag_false():
    bridge = _reload_bridge(prefer_channel_key=False)
    assert bridge.PREFER_TRANSFORM_VIA_CHANNEL_KEY is False

    captured: dict = {}

    async def handler(req):
        captured["body"] = await req.json()
        return web.json_response({"success": True})

    app = web.Application()
    app.router.add_post("/api/channel/message", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            await bridge.send_message(s, "hello legacy")

        assert captured["body"]["channel_api_key"] == "eck_testkey"
        assert captured["body"]["botSecret"] == "botsecret"
        assert "actAs" not in captured["body"]
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_send_message_falls_back_when_api_key_empty():
    """Even if flag=true, empty API_KEY forces legacy path."""
    bridge = _reload_bridge(prefer_channel_key=True, api_key="")
    # PREFER_TRANSFORM_VIA_CHANNEL_KEY may be true but API_KEY is ""
    # The condition `PREFER_TRANSFORM_VIA_CHANNEL_KEY and API_KEY` is False.

    captured: dict = {}

    async def handler(req):
        captured["path"] = req.path
        captured["body"] = await req.json()
        return web.json_response({"success": True})

    app = web.Application()
    app.router.add_post("/api/channel/message", handler)
    app.router.add_post("/api/transform", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            await bridge.send_message(s, "fallback")
        assert captured["path"] == "/api/channel/message"
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_send_message_retries_eclaw_429_with_retry_after(monkeypatch):
    """EClaw delivery back-pressure should retry instead of dropping a reply."""
    bridge = _reload_bridge(prefer_channel_key=True)
    sleeps = []

    async def fake_sleep(delay_s):
        sleeps.append(delay_s)

    monkeypatch.setattr(bridge, "_sleep", fake_sleep)
    calls = {"n": 0}

    async def handler(req):
        calls["n"] += 1
        await req.json()
        if calls["n"] == 1:
            return web.json_response(
                {"success": False, "error": "rate limit"},
                status=429,
                headers={"Retry-After": "2.5"},
            )
        return web.json_response({"success": True})

    app = web.Application()
    app.router.add_post("/api/transform", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            data = await bridge.send_message(s, "retry me")

        assert data == {"success": True}
        assert calls["n"] == 2
        assert sleeps == [2.5]
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_eclaw_rate_limiter_waits_after_window_capacity():
    bridge = _reload_bridge(prefer_channel_key=True)
    now = [0.0]
    sleeps = []

    async def fake_sleep(delay_s):
        sleeps.append(delay_s)
        now[0] += delay_s

    limiter = bridge._SlidingWindowRateLimiter(
        2,
        window_s=60.0,
        clock=lambda: now[0],
        sleep=fake_sleep,
    )

    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert sleeps == [60.0]
