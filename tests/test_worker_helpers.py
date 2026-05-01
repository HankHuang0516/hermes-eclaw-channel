"""Lock daemon worker's strip/extract output to the bridge's existing helpers.

These two implementations MUST stay byte-equal — divergence breaks prompts on
one path and keeps the other working, which is the worst kind of subtle bug.
The bridge originals are the source of truth (they already shipped to prod).
"""
from __future__ import annotations

import os
import sys

import pytest

# Stub bridge-required env so importing eclaw_bridge doesn't blow up.
os.environ.setdefault("HERMES_ECLAW_API_KEY", "x")
os.environ.setdefault("HERMES_ECLAW_DEVICE_ID", "x")
os.environ.setdefault("HERMES_ECLAW_ENTITY_ID", "1")
os.environ.setdefault("HERMES_ECLAW_BOT_SECRET", "x")
os.environ.setdefault("HERMES_ECLAW_CALLBACK_TOKEN", "x")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from daemon import hermes_worker as w  # noqa: E402
from plugin import eclaw_bridge as b  # noqa: E402


FIXTURES = [
    "",
    "plain text reply",
    "─ ⚕ Hermes ─────\nhello world\nResume this session with: foo",
    "[SILENT]",
    "pre\n[Local Variables available: a=1]\npost",
    "pre\n[AVAILABLE TOOLS x]\npost",
    'foo\n[Quota: 5/10 remaining — output "[SILENT]" if nothing]\nbar',
    "\x1b[31mred\x1b[0m text",
    "─ ⚕ Hermes ─────\nfirst\n─ ⚕ Hermes ─────\nsecond\nSession: abc",
]


BRACKET_FIXTURES = [
    "",
    "no brackets here",
    "[D/P2][i18n] kanban search-scope keys (ja/ko/de)",
    "⏰ 任務催促：[Feature archive: vector-search] 已停滯 8.4h",
    "🚫 卡片「[E2E-found] Info page hero 區塊在 11 locales 退回英文」",
    "nested ] without open is fine",
    "[bold]fake markup[/bold] should escape too",
]


@pytest.mark.parametrize("text", FIXTURES)
def test_strip_parity(text: str) -> None:
    assert w.strip_eclaw_context(text) == b._strip_eclaw_context(text)


@pytest.mark.parametrize("text", FIXTURES)
def test_extract_parity(text: str) -> None:
    assert w.extract_hermes_reply(text) == b._extract_hermes_reply(text)


@pytest.mark.parametrize("text", BRACKET_FIXTURES)
def test_escape_brackets_parity(text: str) -> None:
    """Daemon and bridge must escape identically — divergence = silent split brain."""
    assert w.escape_rich_brackets(text) == b._escape_rich_brackets(text)


def test_escape_brackets_doubles_open() -> None:
    """`[` → `\\[` (rich's documented escape for literal display)."""
    assert w.escape_rich_brackets("[D/P2]") == r"\[D/P2]"
    assert w.escape_rich_brackets("[a][b][c]") == r"\[a]\[b]\[c]"


def test_escape_brackets_no_change_when_no_open() -> None:
    """Pure text and ``]``-only input pass through untouched."""
    assert w.escape_rich_brackets("hello world") == "hello world"
    assert w.escape_rich_brackets("trailing ] only") == "trailing ] only"


def test_silent_token_constant() -> None:
    assert w.SILENT_TOKEN == "[SILENT]"


def test_apply_prompt_policy_wraps_hermes_prompt() -> None:
    wrapped = b.apply_prompt_policy("hello", "## Device Instructions\nReport progress.")
    assert wrapped.startswith("[EClaw managed prompt policy - hermes]\n")
    assert "## Device Instructions\nReport progress." in wrapped
    assert wrapped.endswith("\nhello")


def test_apply_prompt_policy_empty_noop() -> None:
    assert b.apply_prompt_policy("hello", "") == "hello"
