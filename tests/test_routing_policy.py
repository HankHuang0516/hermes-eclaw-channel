"""Phase 4 of EClaw#2285 — hermes bridge consumes /api/channel/routing-policy
and forwards `senderHint` on /api/channel/message instead of using the
deprecated /api/entity/speak-to endpoint.
"""
from __future__ import annotations

import os
import sys

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from aiohttp import ClientSession

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
    """Force reimport so the routing-policy cache is fresh per test."""
    for k in list(sys.modules):
        if k == "plugin" or k.startswith("plugin."):
            del sys.modules[k]
    from plugin import eclaw_bridge  # noqa: WPS433
    return eclaw_bridge


# ── derive_sender_hint mapping ────────────────────────────────────────────


def test_derive_sender_hint_entity_with_public_code():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    hint = bridge.derive_sender_hint({"fromEntityId": 5, "fromPublicCode": "abc123"})
    assert hint == {"kind": "entity", "entityId": 5, "publicCode": "abc123"}


def test_derive_sender_hint_user_kinds_have_no_routing():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    for k in ("client", "user", "system", "kanban"):
        assert bridge.derive_sender_hint({"from": k}) == {"kind": "user"}


def test_derive_sender_hint_broadcast():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    assert bridge.derive_sender_hint(
        {"isBroadcast": True, "fromEntityId": 7}
    ) == {"kind": "broadcast"}


def test_derive_sender_hint_unknown_fallback():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    assert bridge.derive_sender_hint({}) == {"kind": "unknown"}
    assert bridge.derive_sender_hint({"from": "mystery"}) == {"kind": "unknown"}


# ── apply_routing_policy ──────────────────────────────────────────────────


def test_apply_routing_policy_empty_returns_prompt_unchanged():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    assert bridge.apply_routing_policy("hello", "") == "hello"
    assert bridge.apply_routing_policy("hello", "   \n  ") == "hello"


def test_apply_routing_policy_wraps_block_before_prompt():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    out = bridge.apply_routing_policy("hello", "ROUTING block")
    assert "[EClaw central routing policy]" in out
    assert "ROUTING block" in out
    assert "[End EClaw central routing policy]" in out
    assert out.endswith("\nhello")


# ── fetch_routing_policy + cache ──────────────────────────────────────────


async def _mock_server(handler, path="/api/channel/routing-policy"):
    app = web.Application()
    app.router.add_get(path, handler)
    server = TestServer(app)
    await server.start_server()
    return server


@pytest.mark.asyncio
async def test_fetch_routing_policy_returns_policy_text():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    bridge._routing_policy_cache = None  # reset

    async def handler(_req):
        return web.json_response({"success": True, "policy": "ROUTING POLICY", "version": "1.0"})

    server = await _mock_server(handler)
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            policy = await bridge.fetch_routing_policy(s)
        assert policy == "ROUTING POLICY"
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_fetch_routing_policy_returns_empty_on_404():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    bridge._routing_policy_cache = None

    async def handler(_req):
        return web.json_response({}, status=404)

    server = await _mock_server(handler)
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            policy = await bridge.fetch_routing_policy(s)
        assert policy == ""
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_fetch_routing_policy_caches_result():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    bridge._routing_policy_cache = None

    call_count = 0

    async def handler(_req):
        nonlocal call_count
        call_count += 1
        return web.json_response({"success": True, "policy": f"P{call_count}"})

    server = await _mock_server(handler)
    try:
        bridge.API_BASE = str(server.make_url("")).rstrip("/")
        async with ClientSession() as s:
            first = await bridge.fetch_routing_policy(s)
            second = await bridge.fetch_routing_policy(s)
        assert first == "P1"
        assert second == "P1"  # cached
        assert call_count == 1
    finally:
        await server.close()


# ── send_message forwards senderHint ──────────────────────────────────────


@pytest.mark.asyncio
async def test_send_message_forwards_sender_hint_in_body():
    _ensure_bridge_env()
    bridge = _reload_bridge()

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
            await bridge.send_message(
                s,
                "reply",
                sender_hint={"kind": "entity", "entityId": 5, "publicCode": "abc123"},
            )
        assert captured["body"]["senderHint"] == {
            "kind": "entity",
            "entityId": 5,
            "publicCode": "abc123",
        }
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_send_message_omits_sender_hint_when_none():
    _ensure_bridge_env()
    bridge = _reload_bridge()

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
            await bridge.send_message(s, "reply")  # no sender_hint
        assert "senderHint" not in captured["body"]
    finally:
        await server.close()
