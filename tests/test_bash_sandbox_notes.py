"""Regression for the daemon's Bash sandbox notes (#5 / Hermes API calls).

Context: #5 could not POST to the EClaw API (move kanban cards, update crons,
vote in negotiations) and reported it as a "botSecret egress gate". The live
investigation found there is NO secret/botSecret egress gate — an authenticated
`curl` carrying #5's botSecret to eclawbot.com returns HTTP 200. The real
blocker is the hermes-agent approval gate (tools/approval.py::DANGEROUS_PATTERNS):
`python3 -c`/`-e` and heredocs ALWAYS match "script execution via -e/-c flag" /
"...via heredoc", and the daemon spawns `hermes chat -q` with non-interactive
stdin + no approval callback, so those auto-deny and time out.

These notes are injected at the top of every chat spawn (hermes_worker
``_run_chat_subprocess``) so the agent reaches for `curl` (not `python3 -c`) for
HTTP/API work from the first turn. The old note recommended
``python3 -c "import json…"`` as a workaround — but that itself trips the -c
gate, so it must never be reintroduced.
"""
from __future__ import annotations

import os
import sys

# Stub bridge-required env so importing the daemon module doesn't blow up.
os.environ.setdefault("HERMES_ECLAW_API_KEY", "x")
os.environ.setdefault("HERMES_ECLAW_DEVICE_ID", "x")
os.environ.setdefault("HERMES_ECLAW_ENTITY_ID", "5")
os.environ.setdefault("HERMES_ECLAW_BOT_SECRET", "x")
os.environ.setdefault("HERMES_ECLAW_CALLBACK_TOKEN", "x")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from daemon import hermes_worker as w  # noqa: E402

NOTES = w._BASH_SANDBOX_NOTES


def test_notes_steer_api_calls_to_curl() -> None:
    """The supported EClaw API path is a plain curl to eclawbot.com."""
    assert "curl" in NOTES
    assert "eclawbot.com/api/mission/cards" in NOTES
    assert "botSecret=$B" in NOTES


def test_notes_warn_against_python_c_and_heredoc_for_http() -> None:
    assert "NEVER" in NOTES
    assert "python3 -c" in NOTES
    assert "heredoc" in NOTES


def test_notes_do_not_recommend_python_c_as_workaround() -> None:
    """The old (broken) advice — `python3 -c "import json; data=json.load…"` —
    matches the approval gate's -c pattern and times out. Never reintroduce it."""
    assert "import json; data=json.load" not in NOTES


def test_notes_creds_extraction_is_shell_only() -> None:
    """Creds are read with grep/cut (no jq/python), so the example never trips
    the gate and runs in the minimal daemon container (no jq installed)."""
    assert ".eclaw-creds.json" in NOTES
    assert "grep -o" in NOTES
    assert "cut -d" in NOTES


def test_notes_survive_prompt_cleaning_pipeline() -> None:
    """Notes are prepended then run through strip_eclaw_context before spawn
    (hermes_worker ``_run_chat_subprocess``). The policy-block remover must not
    eat them, and the curl guidance + user body must both survive."""
    cleaned = w.strip_eclaw_context(NOTES + "user prompt body")
    assert "curl" in cleaned
    assert "eclawbot.com/api/mission/cards" in cleaned
    assert "user prompt body" in cleaned
