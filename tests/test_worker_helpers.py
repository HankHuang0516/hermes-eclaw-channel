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


@pytest.mark.parametrize("text", FIXTURES)
def test_strip_parity(text: str) -> None:
    assert w.strip_eclaw_context(text) == b._strip_eclaw_context(text)


@pytest.mark.parametrize("text", FIXTURES)
def test_extract_parity(text: str) -> None:
    assert w.extract_hermes_reply(text) == b._extract_hermes_reply(text)


def test_silent_token_constant() -> None:
    assert w.SILENT_TOKEN == "[SILENT]"
