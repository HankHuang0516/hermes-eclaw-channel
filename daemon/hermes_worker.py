"""Subprocess wrapper around the `hermes` CLI.

Owns the asyncio lock and the timeout policy. Imported by ``hermes_daemon``
and reused by ``plugin/eclaw_bridge.py`` for the subprocess fallback path.

Stays a thin module so daemon-mode and fallback-mode share envelope-stripping
behaviour bit-for-bit.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import textwrap
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("hermes-worker")

SILENT_TOKEN = "[SILENT]"

# Wall-clock cap — last-resort kill switch. The idle-activity timeout is the
# primary deadline; this only fires if a hermes call streams forever without
# ever producing the session-summary footer.
DEFAULT_TIMEOUT = int(os.environ.get("HERMES_DAEMON_CHAT_TIMEOUT_SECS",
                                     os.environ.get("HERMES_TIMEOUT_SECS", "900")))

# Idle-activity timeout — kill the subprocess when no stdout/stderr chunk has
# arrived for this many seconds. NousResearch hermes-agent issue #4815: a
# wall-clock timeout kills work that's still streaming; what we actually want
# is "agent stopped emitting tokens." Set to 60s so a hung agent dies fast
# but a slow-but-progressing reasoning chain isn't punished.
IDLE_TIMEOUT = int(os.environ.get("HERMES_IDLE_TIMEOUT_SECS", "60"))

# Skip `--continue` (don't resume the most recent session) — used as a manual
# kill switch when an unrecoverable session is wedging every restart, and
# auto-engaged after MAX_CONSECUTIVE_TIMEOUTS consecutive timeouts (issue
# #7536: stuck-session-on-restart loop). Once auto-engaged, stays on for the
# daemon's lifetime.
HERMES_NO_RESUME = os.environ.get("HERMES_NO_RESUME", "").lower() in ("1", "true", "yes")

# Auto-disable resume after this many consecutive timeouts. Was previously
# "first call only," but a daemon that rolls past its first call and *then*
# enters a wedge (e.g. a session jsonl gets corrupted mid-lifetime by a kill
# -9, or a long-running session crosses a hermes CLI version boundary on
# rebuild) would keep re-poisoning itself forever. 2 consecutive timeouts is
# enough signal that resume is the problem, not the LLM.
MAX_CONSECUTIVE_TIMEOUTS = int(os.environ.get("HERMES_MAX_CONSECUTIVE_TIMEOUTS", "2"))

HERMES_BIN = os.environ.get("HERMES_BIN", "/home/node/hermes-agent/.venv/bin/hermes")
HERMES_CWD = os.environ.get("HERMES_CWD", "/home/node/hermes-agent")
HERMES_PATH_PREPEND = os.environ.get("HERMES_PATH_PREPEND", "/home/node/.local/bin")

# Module state for the auto-disable-resume logic. Per-process is fine —
# one daemon process per container, and on restart we re-evaluate.
_call_count = 0
_consecutive_timeouts = 0
_resume_auto_disabled = False

_QUOTA_LINE_RE = re.compile(r"^\[Quota:.*$", re.MULTILINE)
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07")
_HERMES_HEAD_RE = re.compile(r"^\s*─+\s*⚕\s*Hermes\s*─+", re.MULTILINE)
_HERMES_TAIL_RE = re.compile(r"^(?:Resume this session with:|Session:\s+|Duration:\s+|Messages:\s+)", re.MULTILINE)
_PURE_RULE_RE = re.compile(r"^[\s─━│╭╮╰╯═]+$")

_RICH_OPEN_BRACKET_RE = re.compile(r"\[")


def strip_eclaw_context(text: str) -> str:
    """Drop EClaw's auto-injected context blocks before passing to Hermes.

    Mirrors ``plugin/eclaw_bridge.py::_strip_eclaw_context``. Keep these in
    sync — divergence will desync prompts between daemon and fallback paths.
    """
    for marker in ("\n[Local Variables available:", "\n[AVAILABLE TOOLS"):
        idx = text.find(marker)
        if idx >= 0:
            text = text[:idx]
    text = _QUOTA_LINE_RE.sub("", text)
    return text.strip()


def escape_rich_brackets(text: str) -> str:
    """Escape ``[`` so hermes CLI's rich.console banner doesn't crash.

    hermes-agent ``cli.py`` runs ``console.print(f"[bold blue]Query:[/] {q}")``
    on every chat invocation. Rich treats ``[anything]`` as markup tags; an
    EClaw kanban notification like ``[D/P2][i18n] task title`` parses as a
    malformed tag and raises ``MarkupError`` → ``rc=1`` before the LLM is
    ever called. We saw this crash 14× on 2026-04-28 alone, every kanban
    nudge to Hermes silently failing.

    Doubling ``[`` to ``\\[`` is rich's documented escape: the display banner
    renders ``[`` literally, and the LLM receives the prompt with literal
    ``\\[`` characters. LLMs treat ``\\[D/P2\\]`` as semantically equivalent
    to ``[D/P2]`` (just an escape-sequence form of the same content), so the
    label remains intelligible — and the alternative was a 100% delivery
    failure for any prompt with brackets in it.
    """
    return _RICH_OPEN_BRACKET_RE.sub(r"\\[", text)


def extract_hermes_reply(stdout: str) -> str:
    """Pull the agent's response out of the verbose CLI envelope.

    Mirrors ``plugin/eclaw_bridge.py::_extract_hermes_reply``.
    """
    head = list(_HERMES_HEAD_RE.finditer(stdout))
    if head:
        body_start = head[-1].end()
        tail = _HERMES_TAIL_RE.search(stdout, body_start)
        body = stdout[body_start:tail.start()] if tail else stdout[body_start:]
    else:
        body = stdout

    cleaned = []
    for ln in body.splitlines():
        if ln.startswith("session_id:"):
            continue
        if _PURE_RULE_RE.match(ln):
            continue
        cleaned.append(ln.rstrip())
    text = textwrap.dedent("\n".join(cleaned)).strip()
    return re.sub(r"\n{3,}", "\n\n", text)


@dataclass
class HermesResult:
    silent: bool
    reply: str
    duration_ms: int


class HermesError(Exception):
    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


_lock = asyncio.Lock()


async def _drain_stream(stream: asyncio.StreamReader, sink: list, last_activity: list) -> None:
    """Read chunks from a subprocess pipe, append to sink, bump last_activity.

    Each chunk arrival counts as activity — that's what makes the idle-timeout
    "agent is still emitting tokens" rather than wall-clock.
    """
    loop = asyncio.get_running_loop()
    while True:
        chunk = await stream.read(4096)
        if not chunk:
            return
        sink.append(chunk)
        last_activity[0] = loop.time()


async def _wait_for_idle_or_exit(
    proc: asyncio.subprocess.Process,
    last_activity: list,
    idle_deadline_s: int,
    wall_deadline_s: int,
    started_at: float,
) -> str:
    """Wait until proc exits, idle-timeout fires, or wall-clock fires.

    Returns 'exit' | 'idle' | 'wall'. Caller is responsible for killing on
    timeout returns. Driving this with a tick loop instead of a single
    wait_for so we can re-check idle vs wall on every wake.
    """
    loop = asyncio.get_running_loop()
    while True:
        if proc.returncode is not None:
            return "exit"
        now = loop.time()
        if now - started_at >= wall_deadline_s:
            return "wall"
        if now - last_activity[0] >= idle_deadline_s:
            return "idle"
        # Sleep until next deadline boundary, capped at 1s to keep tick fine.
        next_wake = min(
            idle_deadline_s - (now - last_activity[0]),
            wall_deadline_s - (now - started_at),
            1.0,
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=max(0.05, next_wake))
            return "exit"
        except asyncio.TimeoutError:
            continue


async def run_chat(prompt: str, timeout: Optional[int] = None) -> HermesResult:
    """Spawn one `hermes chat -q ... [--continue]` invocation.

    Lock-serialised — concurrent hermes CLI calls corrupt session jsonl files.
    Caller passes already-stripped prompt; we run strip_eclaw_context defensively
    so daemon callers can pass raw user text and still get safe output.

    Phase H1 changes:
      - Idle-activity timeout (no stdout chunks for IDLE_TIMEOUT seconds → kill).
        This is the primary deadline now; wall-clock is a backstop only.
      - --continue is dropped when HERMES_NO_RESUME=1 OR after the first call
        post-boot times out (auto-detection of stuck-session-on-restart).
    """
    global _call_count, _consecutive_timeouts, _resume_auto_disabled
    clean = strip_eclaw_context(prompt)
    safe = escape_rich_brackets(clean)
    wall_deadline = timeout if timeout is not None else DEFAULT_TIMEOUT
    use_continue = not (HERMES_NO_RESUME or _resume_auto_disabled)
    _call_count += 1

    args = [HERMES_BIN, "chat", "-q", safe]
    if use_continue:
        args.append("--continue")

    log.info(
        "[hermes] spawning chat, prompt_len=%d, idle=%ds, wall=%ds, resume=%s, call#=%d",
        len(clean), IDLE_TIMEOUT, wall_deadline, use_continue, _call_count,
    )

    env = {**os.environ, "PATH": HERMES_PATH_PREPEND + ":" + os.environ.get("PATH", "")}

    loop = asyncio.get_running_loop()
    started = loop.time()

    async with _lock:
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                cwd=HERMES_CWD,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            raise HermesError("spawn_failed", str(e)) from e

        stdout_chunks: list = []
        stderr_chunks: list = []
        last_activity = [loop.time()]
        drain_out = asyncio.create_task(_drain_stream(proc.stdout, stdout_chunks, last_activity))
        drain_err = asyncio.create_task(_drain_stream(proc.stderr, stderr_chunks, last_activity))

        outcome = await _wait_for_idle_or_exit(
            proc, last_activity, IDLE_TIMEOUT, wall_deadline, started,
        )

        if outcome != "exit":
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            # Ensure drain tasks finish before reading results.
            for t in (drain_out, drain_err):
                try:
                    await asyncio.wait_for(t, timeout=2)
                except Exception:
                    t.cancel()
            # Track consecutive timeouts and auto-disable --continue once we
            # cross MAX_CONSECUTIVE_TIMEOUTS in a row (NousResearch issue
            # #7536, stuck-session-on-restart loop). Bumping this counter
            # even when resume is already disabled is harmless — the gate
            # below short-circuits on _resume_auto_disabled.
            _consecutive_timeouts += 1
            if (
                use_continue
                and not _resume_auto_disabled
                and _consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS
            ):
                _resume_auto_disabled = True
                log.warning(
                    "[hermes] %d consecutive %s timeouts with --continue → "
                    "auto-disabling session resume for the rest of this "
                    "daemon's lifetime",
                    _consecutive_timeouts, outcome,
                )
            kind = "timeout" if outcome == "idle" else "timeout"
            detail = (f"idle for {IDLE_TIMEOUT}s" if outcome == "idle"
                      else f"wall-clock {wall_deadline}s")
            raise HermesError(kind, detail)

        # Process exited normally — wait for drains to finish.
        for t in (drain_out, drain_err):
            try:
                await asyncio.wait_for(t, timeout=5)
            except Exception:
                t.cancel()

    duration_ms = int((loop.time() - started) * 1000)
    stdout_bytes = b"".join(stdout_chunks)
    stderr_bytes = b"".join(stderr_chunks)

    if proc.returncode != 0:
        # 4000-char cap: enough for full Python traceback (the rich.MarkupError
        # crash that caused this very file to exist had its key frame at byte
        # ~700, beyond the previous 500-char limit — silent diagnosis loss).
        raise HermesError("hermes_exit", f"rc={proc.returncode}: {stderr_bytes.decode(errors='replace')[:4000]}")

    # Clean exit ⇒ resume isn't poisoned right now; reset the streak so a
    # one-off timeout doesn't drift us into the auto-disable threshold.
    _consecutive_timeouts = 0

    raw = _ANSI_RE.sub("", stdout_bytes.decode(errors="replace"))
    reply = extract_hermes_reply(raw)
    silent = SILENT_TOKEN in reply or not reply
    log.info("[hermes] reply_len=%d silent=%s duration_ms=%d", len(reply), silent, duration_ms)
    return HermesResult(silent=silent, reply="" if silent else reply, duration_ms=duration_ms)
