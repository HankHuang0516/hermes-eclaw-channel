"""Bridge shim: daemon-up → use daemon, daemon-down → fall back to subprocess.

We exercise plugin/eclaw_bridge.py::ask_hermes against:
  1. A mock daemon serving JSON.
  2. A daemon URL that connection-refuses (port closed).
  3. A daemon returning {ok:false, error:{kind:"timeout"}} — no fallback,
     surface user-facing error verbatim.
  4. A daemon returning {ok:false, error:{kind:"spawn_failed"}} — DOES fall
     back to subprocess.
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _ensure_bridge_env():
    os.environ.setdefault("HERMES_ECLAW_API_KEY", "x")
    os.environ.setdefault("HERMES_ECLAW_DEVICE_ID", "x")
    os.environ.setdefault("HERMES_ECLAW_ENTITY_ID", "1")
    os.environ.setdefault("HERMES_ECLAW_BOT_SECRET", "x")
    os.environ.setdefault("HERMES_ECLAW_CALLBACK_TOKEN", "x")


def _reload_bridge():
    """Force reimport so bridge re-reads HERMES_DAEMON_URL etc."""
    for k in list(sys.modules):
        if k == "plugin" or k.startswith("plugin."):
            del sys.modules[k]
    from plugin import eclaw_bridge  # noqa: WPS433
    return eclaw_bridge


async def _mock_daemon(handler):
    app = web.Application()
    app.router.add_post("/chat", handler)
    server = TestServer(app)
    await server.start_server()
    return server


async def test_daemon_up_returns_reply(monkeypatch):
    _ensure_bridge_env()

    async def chat(req):
        body = await req.json()
        assert "prompt" in body and "request_id" in body
        return web.json_response({"ok": True, "reply": "from daemon", "silent": False, "duration_ms": 5})

    server = await _mock_daemon(chat)
    try:
        url = f"http://{server.host}:{server.port}"
        monkeypatch.setenv("HERMES_DAEMON_URL", url)
        bridge = _reload_bridge()

        # If daemon fallback fires we'd hit subprocess — replace it with a sentinel
        async def boom(prompt):
            raise AssertionError("subprocess should not be called when daemon returned ok")
        monkeypatch.setattr(bridge, "_ask_hermes_subprocess", boom)

        reply = await bridge.ask_hermes("hi")
        assert reply == "from daemon"
    finally:
        await server.close()


async def test_daemon_down_falls_back(monkeypatch):
    _ensure_bridge_env()
    # 9 is the discard port; connection refused fast.
    monkeypatch.setenv("HERMES_DAEMON_URL", "http://127.0.0.1:9")
    bridge = _reload_bridge()

    called = {"n": 0}

    async def fake_subprocess(prompt):
        called["n"] += 1
        return "subprocess reply"

    monkeypatch.setattr(bridge, "_ask_hermes_subprocess", fake_subprocess)

    reply = await bridge.ask_hermes("hi")
    assert reply == "subprocess reply"
    assert called["n"] == 1


async def test_daemon_clean_timeout_no_fallback(monkeypatch):
    _ensure_bridge_env()

    async def chat(req):
        return web.json_response({"ok": False, "error": {"kind": "timeout", "detail": "after 900s"}}, status=504)

    server = await _mock_daemon(chat)
    try:
        url = f"http://{server.host}:{server.port}"
        monkeypatch.setenv("HERMES_DAEMON_URL", url)
        bridge = _reload_bridge()

        async def boom(prompt):
            raise AssertionError("subprocess should NOT be called on clean timeout")
        monkeypatch.setattr(bridge, "_ask_hermes_subprocess", boom)

        reply = await bridge.ask_hermes("hi")
        assert reply == "[Hermes 回應超時]"
    finally:
        await server.close()


async def test_daemon_503_falls_back(monkeypatch):
    _ensure_bridge_env()

    async def chat(req):
        return web.json_response({"ok": False, "error": {"kind": "spawn_failed", "detail": "boom"}}, status=503)

    server = await _mock_daemon(chat)
    try:
        url = f"http://{server.host}:{server.port}"
        monkeypatch.setenv("HERMES_DAEMON_URL", url)
        bridge = _reload_bridge()

        called = {"n": 0}

        async def fake_subprocess(prompt):
            called["n"] += 1
            return "fallback worked"

        monkeypatch.setattr(bridge, "_ask_hermes_subprocess", fake_subprocess)

        reply = await bridge.ask_hermes("hi")
        assert reply == "fallback worked"
        assert called["n"] == 1
    finally:
        await server.close()


async def test_daemon_silent_returns_silent_token(monkeypatch):
    _ensure_bridge_env()

    async def chat(req):
        return web.json_response({"ok": True, "reply": "", "silent": True, "duration_ms": 1})

    server = await _mock_daemon(chat)
    try:
        url = f"http://{server.host}:{server.port}"
        monkeypatch.setenv("HERMES_DAEMON_URL", url)
        bridge = _reload_bridge()
        reply = await bridge.ask_hermes("hi")
        assert reply == bridge.SILENT_TOKEN
    finally:
        await server.close()


async def test_no_daemon_url_uses_subprocess(monkeypatch):
    _ensure_bridge_env()
    monkeypatch.delenv("HERMES_DAEMON_URL", raising=False)
    bridge = _reload_bridge()

    called = {"n": 0}

    async def fake_subprocess(prompt):
        called["n"] += 1
        return "legacy"

    monkeypatch.setattr(bridge, "_ask_hermes_subprocess", fake_subprocess)
    reply = await bridge.ask_hermes("hi")
    assert reply == "legacy"
    assert called["n"] == 1
