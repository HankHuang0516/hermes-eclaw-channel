"""card_35cb55fc — wallpaper activity-state redesign (#5 Hermes bridge side).

The bridge forwards THIS entity's REAL runtime activity to EClaw's
POST /api/entity/heartbeat `runtimeState` field so the live wallpaper reflects
what the agent is doing. Hermes exposes a clean busy/idle signal (a model turn
is in flight, tracked around ask_hermes); it has no bridge-observable
stuck/crashed state, so it only ever emits "busy"/"idle" and the backend
degrades to lastSendAt freshness for the rest.

These tests pin:
  1. the pure inflight -> runtimeState mapping,
  2. that ask_hermes flips the in-flight counter busy->idle around a turn,
  3. that make_app wires the heartbeat lifecycle hooks when enabled.

Tests are async (pytest asyncio_mode=auto) so a running loop is active when the
module is re-imported / web.Application() is constructed.
"""
from __future__ import annotations

import os
import sys

import pytest

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
    for k in list(sys.modules):
        if k == "plugin" or k.startswith("plugin."):
            del sys.modules[k]
    from plugin import eclaw_bridge  # noqa: WPS433
    return eclaw_bridge


async def test_runtime_state_from_inflight_mapping():
    _ensure_bridge_env()
    bridge = _reload_bridge()
    assert bridge.runtime_state_from_inflight(0) == "idle"
    assert bridge.runtime_state_from_inflight(1) == "busy"
    assert bridge.runtime_state_from_inflight(3) == "busy"
    # only ever the two values Hermes can observe
    for n in range(0, 5):
        assert bridge.runtime_state_from_inflight(n) in ("busy", "idle")


async def test_ask_hermes_flips_inflight_busy_then_idle(monkeypatch):
    _ensure_bridge_env()
    # No daemon → subprocess path; monkeypatch it to observe the counter.
    monkeypatch.delenv("HERMES_DAEMON_URL", raising=False)
    bridge = _reload_bridge()

    seen_during = {}

    async def fake_subprocess(prompt: str) -> str:
        # While the turn runs, the bridge must report busy.
        seen_during["inflight"] = bridge._runtime_inflight
        seen_during["state"] = bridge.runtime_state_from_inflight(bridge._runtime_inflight)
        return "ok"

    monkeypatch.setattr(bridge, "_ask_hermes_subprocess", fake_subprocess)

    assert bridge._runtime_inflight == 0  # idle before
    reply = await bridge.ask_hermes("hi")
    assert reply == "ok"
    assert seen_during["inflight"] == 1
    assert seen_during["state"] == "busy"
    assert bridge._runtime_inflight == 0  # back to idle after (finally ran)


async def test_ask_hermes_decrements_inflight_on_error(monkeypatch):
    _ensure_bridge_env()
    monkeypatch.delenv("HERMES_DAEMON_URL", raising=False)
    bridge = _reload_bridge()

    async def boom(prompt: str) -> str:
        raise RuntimeError("turn blew up")

    monkeypatch.setattr(bridge, "_ask_hermes_subprocess", boom)

    with pytest.raises(RuntimeError):
        await bridge.ask_hermes("hi")
    # counter must not leak on error, else the wallpaper would stick on "busy"
    assert bridge._runtime_inflight == 0


async def test_make_app_registers_heartbeat_when_enabled(monkeypatch):
    _ensure_bridge_env()
    monkeypatch.setenv("HERMES_RUNTIME_HEARTBEAT_ENABLED", "true")
    bridge = _reload_bridge()
    app = bridge.make_app()
    assert bridge._start_runtime_heartbeat in app.on_startup
    assert bridge._stop_runtime_heartbeat in app.on_cleanup


async def test_make_app_skips_heartbeat_when_disabled(monkeypatch):
    _ensure_bridge_env()
    monkeypatch.setenv("HERMES_RUNTIME_HEARTBEAT_ENABLED", "false")
    bridge = _reload_bridge()
    app = bridge.make_app()
    assert bridge._start_runtime_heartbeat not in app.on_startup
