"""HTTP contract tests for daemon/hermes_daemon.py.

Stubs daemon.hermes_worker.run_chat with deterministic fakes so we exercise
the wire surface (auth, error mapping, JSON vs SSE, busy gate) without needing
a live Hermes binary.
"""
from __future__ import annotations

import os
import sys

import pytest
from aiohttp.test_utils import TestServer, TestClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture
def daemon_module(monkeypatch):
    """Fresh import of daemon.hermes_daemon with sane env."""
    monkeypatch.setenv("HERMES_DAEMON_TOKEN", "test-token")
    monkeypatch.setenv("HERMES_DAEMON_QUEUE_MAX", "8")
    # Force a re-import so the module reads our env.
    for k in list(sys.modules):
        if k.startswith("daemon."):
            del sys.modules[k]
    if "daemon" in sys.modules:
        del sys.modules["daemon"]
    from daemon import hermes_daemon  # noqa: WPS433
    return hermes_daemon


@pytest.fixture
async def client(daemon_module):
    server = TestServer(daemon_module.make_app())
    async with TestClient(server) as c:
        yield c


async def test_health_ok(client, daemon_module):
    r = await client.get("/health")
    assert r.status == 200
    body = await r.json()
    assert body["status"] == "ok"
    assert body["service"] == "hermes-daemon"
    assert "uptime_s" in body and body["in_flight"] == 0


async def test_version(client, daemon_module):
    r = await client.get("/version")
    assert r.status == 200
    body = await r.json()
    assert body["service"] == "hermes-daemon"
    assert body["version"] == daemon_module.VERSION


async def test_chat_unauthorized(client):
    r = await client.post("/chat", json={"prompt": "hi", "request_id": "1"})
    assert r.status == 401


async def test_chat_bad_request(client):
    r = await client.post(
        "/chat",
        json={"request_id": "1"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert r.status == 400
    body = await r.json()
    assert body["error"]["kind"] == "bad_request"


async def test_chat_json_done(client, daemon_module, monkeypatch):
    async def fake_run(prompt, timeout=None):
        return daemon_module.hermes_worker.HermesResult(silent=False, reply="hello", duration_ms=42)

    monkeypatch.setattr(daemon_module.hermes_worker, "run_chat", fake_run)

    r = await client.post(
        "/chat",
        json={"prompt": "hi", "request_id": "abc"},
        headers={"Authorization": "Bearer test-token", "Accept": "application/json"},
    )
    assert r.status == 200
    body = await r.json()
    assert body["ok"] is True
    assert body["reply"] == "hello"
    assert body["silent"] is False
    assert body["duration_ms"] == 42


async def test_chat_json_silent(client, daemon_module, monkeypatch):
    async def fake_run(prompt, timeout=None):
        return daemon_module.hermes_worker.HermesResult(silent=True, reply="", duration_ms=10)

    monkeypatch.setattr(daemon_module.hermes_worker, "run_chat", fake_run)

    r = await client.post(
        "/chat",
        json={"prompt": "hi", "request_id": "abc"},
        headers={"Authorization": "Bearer test-token", "Accept": "application/json"},
    )
    assert r.status == 200
    body = await r.json()
    assert body["silent"] is True
    assert body["reply"] == ""


@pytest.mark.parametrize("kind,status", [
    ("timeout", 504),
    ("spawn_failed", 503),
    ("hermes_exit", 502),
])
async def test_chat_json_errors(client, daemon_module, monkeypatch, kind, status):
    async def fake_run(prompt, timeout=None):
        raise daemon_module.hermes_worker.HermesError(kind, f"{kind}-detail")

    monkeypatch.setattr(daemon_module.hermes_worker, "run_chat", fake_run)

    r = await client.post(
        "/chat",
        json={"prompt": "hi", "request_id": "abc"},
        headers={"Authorization": "Bearer test-token", "Accept": "application/json"},
    )
    assert r.status == status
    body = await r.json()
    assert body["ok"] is False
    assert body["error"]["kind"] == kind


async def test_chat_sse_done(client, daemon_module, monkeypatch):
    async def fake_run(prompt, timeout=None):
        return daemon_module.hermes_worker.HermesResult(silent=False, reply="streamed", duration_ms=7)

    monkeypatch.setattr(daemon_module.hermes_worker, "run_chat", fake_run)

    r = await client.post(
        "/chat",
        json={"prompt": "hi", "request_id": "abc"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert r.status == 200
    assert r.headers["Content-Type"].startswith("text/event-stream")
    body = (await r.read()).decode()
    assert "event: started" in body
    assert "event: done" in body
    assert '"reply": "streamed"' in body


async def test_chat_sse_silent(client, daemon_module, monkeypatch):
    async def fake_run(prompt, timeout=None):
        return daemon_module.hermes_worker.HermesResult(silent=True, reply="", duration_ms=3)

    monkeypatch.setattr(daemon_module.hermes_worker, "run_chat", fake_run)

    r = await client.post(
        "/chat",
        json={"prompt": "hi", "request_id": "abc"},
        headers={"Authorization": "Bearer test-token"},
    )
    body = (await r.read()).decode()
    assert "event: silent" in body
    assert "event: done" not in body
