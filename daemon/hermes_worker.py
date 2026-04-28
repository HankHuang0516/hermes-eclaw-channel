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

DEFAULT_TIMEOUT = int(os.environ.get("HERMES_DAEMON_CHAT_TIMEOUT_SECS",
                                     os.environ.get("HERMES_TIMEOUT_SECS", "900")))
HERMES_BIN = os.environ.get("HERMES_BIN", "/home/node/hermes-agent/.venv/bin/hermes")
HERMES_CWD = os.environ.get("HERMES_CWD", "/home/node/hermes-agent")
HERMES_PATH_PREPEND = os.environ.get("HERMES_PATH_PREPEND", "/home/node/.local/bin")

_QUOTA_LINE_RE = re.compile(r"^\[Quota:.*$", re.MULTILINE)
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07")
_HERMES_HEAD_RE = re.compile(r"^\s*─+\s*⚕\s*Hermes\s*─+", re.MULTILINE)
_HERMES_TAIL_RE = re.compile(r"^(?:Resume this session with:|Session:\s+|Duration:\s+|Messages:\s+)", re.MULTILINE)
_PURE_RULE_RE = re.compile(r"^[\s─━│╭╮╰╯═]+$")


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


async def run_chat(prompt: str, timeout: Optional[int] = None) -> HermesResult:
    """Spawn one `hermes chat -q ... --continue` invocation.

    Lock-serialised — concurrent hermes CLI calls corrupt session jsonl files.
    Caller passes already-stripped prompt; we run strip_eclaw_context defensively
    so daemon callers can pass raw user text and still get safe output.
    """
    clean = strip_eclaw_context(prompt)
    deadline = timeout if timeout is not None else DEFAULT_TIMEOUT
    log.info("[hermes] spawning chat, prompt_len=%d, timeout=%d", len(clean), deadline)

    args = [HERMES_BIN, "chat", "-q", clean, "--continue"]
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

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=deadline)
        except asyncio.TimeoutError as e:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            raise HermesError("timeout", f"after {deadline}s") from e

    duration_ms = int((loop.time() - started) * 1000)

    if proc.returncode != 0:
        raise HermesError("hermes_exit", f"rc={proc.returncode}: {stderr.decode()[:500]}")

    raw = _ANSI_RE.sub("", stdout.decode())
    reply = extract_hermes_reply(raw)
    silent = SILENT_TOKEN in reply or not reply
    log.info("[hermes] reply_len=%d silent=%s duration_ms=%d", len(reply), silent, duration_ms)
    return HermesResult(silent=silent, reply="" if silent else reply, duration_ms=duration_ms)
