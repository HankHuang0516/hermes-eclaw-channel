"""Phase H1 stability primitives — idle-timeout helper + stale-webhook drop.

These exercise the small functions in isolation so we can prove the deadline
math + drop logic without spawning a real hermes subprocess or aiohttp server.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Bridge import requires these env vars; set before import.
os.environ.setdefault("HERMES_ECLAW_API_KEY", "x")
os.environ.setdefault("HERMES_ECLAW_DEVICE_ID", "x")
os.environ.setdefault("HERMES_ECLAW_ENTITY_ID", "1")
os.environ.setdefault("HERMES_ECLAW_BOT_SECRET", "x")
os.environ.setdefault("HERMES_ECLAW_CALLBACK_TOKEN", "x")

from daemon import hermes_worker as w  # noqa: E402


class _FakeProc:
    """Minimal asyncio.subprocess.Process stand-in for deadline tests."""

    def __init__(self):
        self.returncode = None
        self._exit = asyncio.Event()

    def set_exited(self, rc=0):
        self.returncode = rc
        self._exit.set()

    async def wait(self):
        await self._exit.wait()
        return self.returncode


async def test_wait_returns_exit_when_proc_finishes():
    proc = _FakeProc()
    last_activity = [asyncio.get_running_loop().time()]
    started = asyncio.get_running_loop().time()

    async def finish_soon():
        await asyncio.sleep(0.1)
        proc.set_exited(0)

    asyncio.create_task(finish_soon())
    outcome = await w._wait_for_idle_or_exit(
        proc, last_activity, idle_deadline_s=5, wall_deadline_s=5, started_at=started,
    )
    assert outcome == "exit"


async def test_wait_returns_idle_when_no_chunks():
    """Idle deadline fires when last_activity never gets bumped."""
    proc = _FakeProc()
    loop = asyncio.get_running_loop()
    started = loop.time()
    last_activity = [started]

    outcome = await w._wait_for_idle_or_exit(
        proc, last_activity,
        idle_deadline_s=1,  # 1s — small so the test stays fast
        wall_deadline_s=10,
        started_at=started,
    )
    assert outcome == "idle"


async def test_wait_returns_wall_when_idle_keeps_resetting():
    """Activity keeps last_activity fresh, but wall-clock still fires."""
    proc = _FakeProc()
    loop = asyncio.get_running_loop()
    started = loop.time()
    last_activity = [started]

    async def keep_active():
        for _ in range(10):
            await asyncio.sleep(0.2)
            last_activity[0] = asyncio.get_running_loop().time()

    asyncio.create_task(keep_active())
    outcome = await w._wait_for_idle_or_exit(
        proc, last_activity,
        idle_deadline_s=5,
        wall_deadline_s=1,  # 1s — fires before idle could
        started_at=started,
    )
    assert outcome == "wall"


async def test_drain_stream_bumps_activity():
    """Each chunk arrival must bump last_activity[0]."""
    class _FakeStream:
        def __init__(self, chunks):
            self._chunks = list(chunks)

        async def read(self, n):
            if not self._chunks:
                return b""
            return self._chunks.pop(0)

    sink = []
    last_activity = [0.0]
    stream = _FakeStream([b"a", b"bc", b"def"])
    await w._drain_stream(stream, sink, last_activity)

    assert b"".join(sink) == b"abcdef"
    # last_activity got bumped at least once (i.e., is no longer 0.0).
    assert last_activity[0] > 0.0


def test_stale_webhook_threshold_default():
    """Bridge default threshold is 300s per Phase H1 spec."""
    from plugin import eclaw_bridge as b
    assert b.STALE_WEBHOOK_THRESHOLD_S == 300


def test_stale_webhook_age_calc():
    """Sanity: a Date.now()-style ms timestamp from 10 minutes ago is > 300s old."""
    from plugin import eclaw_bridge as b
    ten_min_ago_ms = (time.time() - 600) * 1000
    age_s = time.time() - (ten_min_ago_ms / 1000)
    assert age_s > b.STALE_WEBHOOK_THRESHOLD_S


def test_consecutive_timeouts_default_threshold():
    """Default trips at 2 consecutive timeouts so a one-off doesn't disable resume."""
    assert w.MAX_CONSECUTIVE_TIMEOUTS == 2


async def test_consecutive_timeouts_auto_disables_resume(monkeypatch):
    """N consecutive idle timeouts must auto-engage HERMES_NO_RESUME, regardless
    of which call number we're on. Reproduces the pre-fix bug where only the
    first call could trip the fuse — the new behaviour must hold even after
    the daemon has been running for a while.

    We mock the subprocess machinery so the test exercises the counter +
    fuse logic without spawning a real ``hermes`` binary.
    """

    monkeypatch.setattr(w, "_call_count", 0)
    monkeypatch.setattr(w, "_consecutive_timeouts", 0)
    monkeypatch.setattr(w, "_resume_auto_disabled", False)

    class _Proc:
        returncode = None

        def __init__(self):
            self.stdout = SimpleNamespace()
            self.stderr = SimpleNamespace()

        def kill(self):
            self.returncode = -9

        async def wait(self):
            return self.returncode if self.returncode is not None else -9

    async def _fake_create(*_a, **_k):
        return _Proc()

    async def _fake_drain(*_a, **_k):
        return None

    async def _always_idle(*_a, **_k):
        return "idle"

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create)
    monkeypatch.setattr(w, "_drain_stream", _fake_drain)
    monkeypatch.setattr(w, "_wait_for_idle_or_exit", _always_idle)

    # Pre-load _call_count so we're past the "first call" — the old bug was
    # that calls 2..N could never trip the fuse.
    w._call_count = 5
    monkeypatch.setattr(w, "MAX_CONSECUTIVE_TIMEOUTS", 2)

    for _ in range(2):
        with pytest.raises(w.HermesError):
            await w.run_chat("hello")

    assert w._resume_auto_disabled is True


async def test_consecutive_timeouts_resets_on_success(monkeypatch):
    """A clean run between timeouts must reset the streak so a one-off
    timeout never compounds into the auto-disable threshold."""
    monkeypatch.setattr(w, "_call_count", 0)
    monkeypatch.setattr(w, "_consecutive_timeouts", 0)
    monkeypatch.setattr(w, "_resume_auto_disabled", False)
    monkeypatch.setattr(w, "MAX_CONSECUTIVE_TIMEOUTS", 2)

    class _Proc:
        def __init__(self, ok):
            self.returncode = 0 if ok else None
            self.stdout = SimpleNamespace()
            self.stderr = SimpleNamespace()

        def kill(self):
            self.returncode = -9

        async def wait(self):
            return self.returncode if self.returncode is not None else -9

    state = {"mode": "idle"}

    async def _fake_create(*_a, **_k):
        return _Proc(ok=state["mode"] == "exit")

    async def _fake_drain(*_a, **_k):
        return None

    async def _outcome(*_a, **_k):
        return state["mode"]

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create)
    monkeypatch.setattr(w, "_drain_stream", _fake_drain)
    monkeypatch.setattr(w, "_wait_for_idle_or_exit", _outcome)
    # Make the success path decode cleanly.
    monkeypatch.setattr(
        w, "extract_hermes_reply", lambda _raw: "ok-reply"
    )

    # 1 timeout → counter=1, fuse not yet engaged.
    state["mode"] = "idle"
    with pytest.raises(w.HermesError):
        await w.run_chat("first")
    assert w._consecutive_timeouts == 1
    assert w._resume_auto_disabled is False

    # Clean reply → counter resets.
    state["mode"] = "exit"
    result = await w.run_chat("second")
    assert result.reply == "ok-reply"
    assert w._consecutive_timeouts == 0

    # 1 more timeout — would have tripped the old (cumulative) counter,
    # but with the reset it's just streak=1 again.
    state["mode"] = "idle"
    with pytest.raises(w.HermesError):
        await w.run_chat("third")
    assert w._consecutive_timeouts == 1
    assert w._resume_auto_disabled is False
