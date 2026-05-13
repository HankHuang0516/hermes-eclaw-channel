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
import shutil
import subprocess
import tempfile
import textwrap
import uuid
from pathlib import Path
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


# PR-only flow for file-edit tasks. The hermes daemon container does not mount
# the EClaw repository, so prompts that ask Hermes to edit files can otherwise
# loop forever grepping paths that do not exist. When a task looks like a
# file-edit task, clone the repo into an ephemeral workdir, let Hermes edit
# there, then commit/push/open a PR. Text-only tasks keep the original flow.
PR_ONLY_ENABLED = os.environ.get("HERMES_PR_ONLY_ENABLED", "1").lower() in ("1", "true", "yes")
PR_REPO_URL = os.environ.get("HERMES_PR_REPO_URL", "https://github.com/HankHuang0516/EClaw.git")
PR_REPO_FULL_NAME = os.environ.get("HERMES_PR_REPO_FULL_NAME", "HankHuang0516/EClaw")
PR_BASE_BRANCH = os.environ.get("HERMES_PR_BASE_BRANCH", "main")
PR_BRANCH_PREFIX = os.environ.get("HERMES_PR_BRANCH_PREFIX", "hermes")
PR_REVIEWER = os.environ.get("HERMES_PR_REVIEWER", "HankHuang0516")
PR_WORKDIR_PARENT = os.environ.get("HERMES_PR_WORKDIR_PARENT", tempfile.gettempdir())

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

# Policy / FWD wrapper blocks EClaw stamps onto every channel inbound. These
# pollute prompts (every chat is dressed up to look like an i18n PR ask) and —
# critically — drove #142: the classifier matched `i18n` / `pull request` /
# `.js` inside the *wrapper* and routed routine text replies through the
# clone-and-PR flow, opening a fresh CRLF runaway PR on every reply.
_POLICY_BLOCK_RES = (
    re.compile(r"\[EClaw central routing policy\].*?\[End EClaw central routing policy\]", re.DOTALL),
    re.compile(r"\[EClaw managed prompt policy[^\]]*\].*?\[End EClaw managed prompt policy\]", re.DOTALL),
    re.compile(r"\[MENTIONS\s*[—-]\s*IMPORTANT ROUTING HINT\].*?(?=\n\[|\Z)", re.DOTALL),
)
_FWD_HEADER_RE = re.compile(r"^\[EClaw from [^\]]+\][^\n]*\n?", re.MULTILINE)
_BOT_TO_BOT_HEADER_RE = re.compile(r"^\[Bot-to-Bot message from [^\]]+\][^\n]*\n?", re.MULTILINE)

# Strong file-edit signals: explicit path mention OR explicit PR intent.
# Bare keywords like `i18n`, `\bpr\b`, `\bedit\b` were chasing every chat
# reply through the clone flow — see #142 root cause.
_FILE_EDIT_RE = re.compile(
    r"("
    # explicit relative paths
    r"backend/public/shared/i18n\.js"
    r"|backend/[^\s]+\.(?:js|ts|py|kt|html|css|json|md|sh)"
    r"|frontend/[^\s]+\.(?:js|ts|py|kt|html|css|json|md|sh)"
    r"|scripts?/[^\s]+\.(?:js|ts|py|kt|html|css|json|md|sh)"
    r"|app/src/[^\s]+\.(?:js|ts|py|kt|html|css|json|md|sh)"
    # explicit edit/PR intent in EN
    r"|\bopen (?:a )?pull request\b|\bopen (?:a )?PR\b"
    r"|\bsend (?:a )?pull request\b|\bsend (?:a )?PR\b"
    r"|\bfile-edit\b"
    # explicit edit/PR intent in zh
    r"|開出 PR|提 PR|開 PR|送 PR|做出 PR"
    r"|翻譯.*?i18n|翻译.*?i18n"
    r")",
    re.IGNORECASE,
)
_TEXT_ONLY_RE = re.compile(r"\b(reply|answer|summari[sz]e|explain|translate only|只回覆|純文字|status update|heartbeat|progress)\b", re.IGNORECASE)


def strip_eclaw_context(text: str) -> str:
    """Drop EClaw's auto-injected context blocks before passing to Hermes.

    Mirrors ``plugin/eclaw_bridge.py::_strip_eclaw_context``. Keep these in
    sync — divergence will desync prompts between daemon and fallback paths.

    Strips, in order:
    - ``[EClaw central routing policy]`` / ``[EClaw managed prompt policy …]``
      wrapper blocks (the source of #142 — every channel reply was getting
      routed through clone-and-PR because the classifier matched keywords
      inside these blocks rather than in the user's actual prompt).
    - ``[MENTIONS — IMPORTANT ROUTING HINT]`` mention table.
    - ``[EClaw from entity:N:NAME]`` and ``[Bot-to-Bot message from Entity N (NAME)]``
      single-line FWD headers.
    - Legacy ``[Local Variables available: …]`` / ``[AVAILABLE TOOLS …]`` tails.
    - ``[Quota: …]`` line (Hermes was way too willing to "[SILENT]").
    """
    for block_re in _POLICY_BLOCK_RES:
        text = block_re.sub("", text)
    text = _FWD_HEADER_RE.sub("", text)
    text = _BOT_TO_BOT_HEADER_RE.sub("", text)
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



def is_file_edit_task(prompt: str) -> bool:
    """Heuristic classifier for tasks that need repo file access.

    Conservative bias: only route to PR-only when the prompt clearly mentions
    file/code/i18n edit intent. Text-only replies must not pay clone/PR cost.
    """
    text = strip_eclaw_context(prompt or "")
    if not text:
        return False
    if _TEXT_ONLY_RE.search(text) and not _FILE_EDIT_RE.search(text):
        return False
    return bool(_FILE_EDIT_RE.search(text))


def _task_slug(prompt: str) -> str:
    text = strip_eclaw_context(prompt or "")
    # Prefer the first title-ish line; cap before slugifying so branch names stay sane.
    line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "file-edit")[:80]
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", line).strip("-._").lower()
    return (slug or "file-edit")[:48]


def _github_pat() -> str:
    # Do not log or print this value. The bridge/vault/bootstrap layer should
    # expose device-owned HERMES_GH_PAT to the daemon process at runtime.
    return os.environ.get("HERMES_GH_PAT", "").strip()


def _redact_secret(text: str, token: str | None = None) -> str:
    out = text or ""
    for secret in (token, os.environ.get("HERMES_GH_PAT"), os.environ.get("GH_TOKEN"), os.environ.get("GITHUB_TOKEN")):
        if secret:
            out = out.replace(secret, "***")
    return out




def _tail_text(chunks: list, *, limit: int = 2000) -> str:
    """Return a redacted text tail for timeout diagnostics.

    Timeout bugs are often only diagnosable from the last tool/CLI lines. Keep
    this bounded and secret-redacted so daemon logs and JSON/SSE errors can
    include useful evidence without leaking tokens.
    """
    if not chunks:
        return ""
    tail = b"".join(chunks)[-limit:]
    return _redact_secret(tail.decode(errors="replace"))


def _run(cmd: list[str], *, cwd: str | Path | None = None, env: dict | None = None, timeout: int = 120) -> subprocess.CompletedProcess:
    token = (env or {}).get("HERMES_GH_PAT")
    try:
        return subprocess.run(
            cmd,
            cwd=str(cwd) if cwd is not None else None,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        detail = f"{cmd[0]} exited {e.returncode}: {_redact_secret((e.stderr or e.stdout or '')[:1200], token)}"
        raise HermesError("pr_flow_failed", detail) from e
    except subprocess.TimeoutExpired as e:
        detail = f"{cmd[0]} timed out after {timeout}s: {_redact_secret(str(e)[:1200], token)}"
        raise HermesError("pr_flow_failed", detail) from e


def _make_git_askpass(tmpdir: Path) -> Path:
    script = tmpdir / "git-askpass.sh"
    script.write_text(
        "#!/usr/bin/env sh\n"
        "case \"$1\" in\n"
        "  *Username*) printf '%s' 'x-access-token' ;;\n"
        "  *Password*) printf '%s' \"$HERMES_GH_PAT\" ;;\n"
        "  *) printf '%s' \"$HERMES_GH_PAT\" ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    script.chmod(0o700)
    return script


def _pr_env(base_env: dict, tmpdir: Path, token: str) -> dict:
    askpass = _make_git_askpass(tmpdir)
    return {
        **base_env,
        "HERMES_GH_PAT": token,
        "GH_TOKEN": token,
        "GITHUB_TOKEN": token,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": str(askpass),
    }


async def _run_pr_only_chat(prompt: str, timeout: int) -> HermesResult:
    """Execute a file-edit prompt inside a cloned EClaw repo and open a PR."""
    token = _github_pat()
    if not token:
        raise HermesError("missing_github_pat", "HERMES_GH_PAT required for file-edit PR-only flow")
    if not shutil.which("git"):
        raise HermesError("missing_tool", "git CLI required for file-edit PR-only flow")
    if not shutil.which("gh"):
        raise HermesError("missing_tool", "gh CLI required for file-edit PR-only flow")

    slug = _task_slug(prompt)
    branch = f"{PR_BRANCH_PREFIX}/{slug}-{uuid.uuid4().hex[:8]}"
    parent = Path(PR_WORKDIR_PARENT)
    parent.mkdir(parents=True, exist_ok=True)
    base_env = {**os.environ, "PATH": HERMES_PATH_PREPEND + ":" + os.environ.get("PATH", "")}

    with tempfile.TemporaryDirectory(prefix="hermes-pr-", dir=str(parent)) as td:
        tmpdir = Path(td)
        env = _pr_env(base_env, tmpdir, token)
        repo_dir = tmpdir / "EClaw"
        log.info("[hermes-pr] cloning repo for file-edit task branch=%s slug=%s", branch, slug)
        _run(["git", "clone", "--depth", "1", "--branch", PR_BASE_BRANCH, PR_REPO_URL, str(repo_dir)], env=env, timeout=180)

        # CRLF defense — #142.
        # EClaw's .gitattributes mandates ``*.js text eol=lf`` but a number of
        # legacy blobs are still committed with CRLF. A vanilla clone then
        # checkout-normalises them, leaving 38+ files dirty in the working
        # tree before Hermes touches anything — every "did Hermes change
        # files?" check then returns yes and we ship a +15k/-15k CRLF
        # runaway PR. Disable the per-attribute EOL filter for this clone
        # (local-only override via .git/info/attributes — does not pollute
        # the working tree's tracked .gitattributes) and re-checkout from
        # the index so the working tree matches the committed bytes exactly.
        (repo_dir / ".git" / "info").mkdir(parents=True, exist_ok=True)
        (repo_dir / ".git" / "info" / "attributes").write_text("* -text\n", encoding="utf-8")
        _run(["git", "checkout", "HEAD", "--", "."], cwd=repo_dir, env=env)

        _run(["git", "checkout", "-b", branch], cwd=repo_dir, env=env)
        _run(["git", "config", "user.name", "Hermes Bot"], cwd=repo_dir, env=env)
        _run(["git", "config", "user.email", "hermes-bot@users.noreply.github.com"], cwd=repo_dir, env=env)

        pr_prompt = "\n".join([
            "[Hermes PR-only file-edit workspace]",
            f"Repository has been cloned to: {repo_dir}",
            f"Current branch: {branch}",
            "Make the requested file edits in this working tree only.",
            "Do not run git commit, git push, or gh pr create; the daemon will do that after you finish.",
            "Keep the diff minimal and do not edit unrelated files.",
            "[End Hermes PR-only file-edit workspace]",
            "",
            prompt,
        ])
        result = await _run_chat_subprocess(pr_prompt, timeout=timeout, cwd=str(repo_dir), use_continue=False)
        status = _run(["git", "status", "--porcelain"], cwd=repo_dir, env=env).stdout.strip()
        ahead = int(_run(
            ["git", "rev-list", "--count", f"origin/{PR_BASE_BRANCH}..HEAD"],
            cwd=repo_dir, env=env,
        ).stdout.strip() or "0")
        # Hermes occasionally runs `git commit` itself despite the workspace
        # preamble telling it not to. Detect both cases: dirty worktree
        # (uncommitted) OR commits-ahead-of-base (Hermes self-committed).
        # Without the rev-list check, a Hermes-self-committed branch looks
        # clean → daemon returns no-PR → TemporaryDirectory cleanup destroys
        # the commit.
        if not status and ahead == 0:
            reply = result.reply or "Hermes completed, but produced no file diff; no PR was opened."
            return HermesResult(silent=result.silent, reply=reply, duration_ms=result.duration_ms)

        # Second guard: dirty-but-EOL-only working tree (belt-and-braces over
        # the CRLF defense above). If every dirty file is whitespace/EOL noise,
        # abort before commit so we don't ship another runaway PR.
        if status and ahead == 0:
            real_diff = _run(
                ["git", "diff", "--shortstat", "--ignore-cr-at-eol", "--ignore-all-space"],
                cwd=repo_dir, env=env,
            ).stdout.strip()
            if not real_diff:
                log.warning("[hermes-pr] dirty worktree is EOL/whitespace-only; skipping PR")
                reply = result.reply or "Hermes produced no semantic file changes (EOL/whitespace only); no PR was opened."
                return HermesResult(silent=result.silent, reply=reply, duration_ms=result.duration_ms)

        commit_title = f"Hermes file edit: {slug}"
        if status:
            _run(["git", "add", "-A"], cwd=repo_dir, env=env)
            _run(["git", "commit", "-m", commit_title], cwd=repo_dir, env=env)
        else:
            existing = _run(
                ["git", "log", "-1", "--pretty=%s", "HEAD"],
                cwd=repo_dir, env=env,
            ).stdout.strip()
            if existing:
                commit_title = existing
        _run(["git", "push", "origin", branch], cwd=repo_dir, env=env, timeout=180)
        body = "\n".join([
            "Automated Hermes PR-only file-edit flow.",
            "",
            "- Source: hermes-daemon PR-only clone workspace",
            "- Branch created by daemon; review/merge handled by LOBSTER/Hank policy",
            "- Token values are never printed in logs",
            "",
            "Hermes reply:",
            result.reply or "(silent/no textual reply)",
        ])
        cmd = [
            "gh", "pr", "create",
            "--repo", PR_REPO_FULL_NAME,
            "--base", PR_BASE_BRANCH,
            "--head", branch,
            "--title", commit_title,
            "--body", body,
        ]
        if PR_REVIEWER:
            cmd += ["--assignee", PR_REVIEWER]
        pr_url = _run(cmd, cwd=repo_dir, env=env, timeout=180).stdout.strip()
        reply = (result.reply + "\n\n" if result.reply else "") + f"Opened PR: {pr_url}"
        return HermesResult(silent=False, reply=reply, duration_ms=result.duration_ms)


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
    """Route text tasks to Hermes directly and file-edit tasks to PR-only flow."""
    clean = strip_eclaw_context(prompt)
    wall_deadline = timeout if timeout is not None else DEFAULT_TIMEOUT
    if PR_ONLY_ENABLED and is_file_edit_task(clean):
        log.info("[hermes-pr] file-edit task detected; using PR-only clone flow")
        return await _run_pr_only_chat(clean, timeout=wall_deadline)
    return await _run_chat_subprocess(clean, timeout=wall_deadline)


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


async def _run_chat_subprocess(prompt: str, timeout: Optional[int] = None, *, cwd: Optional[str] = None, use_continue: Optional[bool] = None) -> HermesResult:
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
    if use_continue is None:
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
                cwd=cwd or HERMES_CWD,
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
            stdout_tail = _tail_text(stdout_chunks)
            stderr_tail = _tail_text(stderr_chunks)
            if stdout_tail:
                detail += f"; stdout_tail={stdout_tail!r}"
            if stderr_tail:
                detail += f"; stderr_tail={stderr_tail!r}"
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
