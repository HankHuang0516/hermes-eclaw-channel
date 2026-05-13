"""PR-only file-edit flow guardrails for hermes daemon."""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from daemon import hermes_worker as w  # noqa: E402


def test_file_edit_classifier_detects_i18n_edit_task():
    prompt = "Please update backend/public/shared/i18n.js and open a PR for one key"
    assert w.is_file_edit_task(prompt) is True


def test_file_edit_classifier_ignores_plain_text_reply():
    assert w.is_file_edit_task('reply "hello" only') is False
    assert w.is_file_edit_task('summarize the last benchmark result') is False


# --- #142 classifier-tightening regressions ------------------------------
#
# Before #142, _FILE_EDIT_RE matched bare ``i18n`` / ``\bpr\b`` / conversational
# verbs (`edit`, `fix`, `update`, `commit`). Combined with policy-block
# pollution, that fired on every chat reply and shipped a CRLF runaway PR
# per inbound. These tests pin down the contract: classifier must demand an
# explicit file path or strong PR-intent phrase.

POLICY_HEADER = (
    "[EClaw central routing policy]\n"
    "Routes containing pull request, i18n, .js, backend/public/shared/i18n.js\n"
    "[End EClaw central routing policy]\n"
    "[EClaw managed prompt policy - claude_code]\n"
    "policy body mentioning i18n\n"
    "[End EClaw managed prompt policy]\n\n"
)


@pytest.mark.parametrize("body", [
    # Plain conversational replies — must NOT trigger file-edit flow.
    "OK 收到，繼續看下去",
    "Hermes #5 progress report: chunks 1-8 done, 165 keys remaining.",
    "PR ready for review, please merge when CI is green",
    "Can you fix the i18n drift later this week?",
    "I'll update you after the meeting",
    "edit your last reply and resend",  # 'edit' alone should not fire
    "the commit history shows three regressions",  # 'commit' alone should not fire
])
def test_classifier_ignores_conversational_pr_chatter(body):
    prompt = POLICY_HEADER + body
    assert w.is_file_edit_task(prompt) is False, f"false positive on: {body!r}"


@pytest.mark.parametrize("body", [
    "Please edit backend/public/shared/i18n.js to add the new ms keys.",
    "Open a PR adding scripts/fix-drift.js",
    "請幫我提 PR 修 backend/public/shared/i18n.js 的 ms 字段",
    "open a pull request for app/src/main/AndroidManifest.xml typo",
    "file-edit: append to scripts/migrate-vault.sh",
])
def test_classifier_detects_explicit_file_edit_intent(body):
    prompt = POLICY_HEADER + body
    assert w.is_file_edit_task(prompt) is True, f"false negative on: {body!r}"


def test_classifier_does_not_match_policy_block_alone():
    """The policy header in isolation must not fire the file-edit flow.

    Root cause for #142: the policy block itself mentions `pull request`,
    `i18n`, `.js`, and `backend/public/shared/i18n.js`. The classifier was
    matching those keywords inside the wrapper and routing every channel
    reply through clone-and-PR.
    """
    assert w.is_file_edit_task(POLICY_HEADER) is False
    assert w.is_file_edit_task(POLICY_HEADER + "OK") is False


def test_strip_removes_central_routing_policy_block():
    raw = POLICY_HEADER + "actual user text"
    assert w.strip_eclaw_context(raw) == "actual user text"


def test_strip_removes_fwd_and_bot_to_bot_headers():
    raw = (
        "[EClaw from entity:1:LOBSTER]\n"
        "[Bot-to-Bot message from Entity 1 (LOBSTER)]\n"
        "real message body"
    )
    assert w.strip_eclaw_context(raw) == "real message body"


def test_task_slug_uses_stripped_first_line():
    """Slug must come from the stripped prompt, not the policy header.

    Before #142, every clone got slug ``eclaw-central-routing-policy`` because
    that was the first line of the un-stripped prompt.
    """
    raw = POLICY_HEADER + "i18n(ms): fill 2026-05-13 drift keys"
    slug = w._task_slug(raw)
    assert "eclaw-central-routing-policy" not in slug
    assert "i18n" in slug or "ms" in slug or "drift" in slug


def test_task_slug_is_branch_safe():
    slug = w._task_slug('[infra/P0] Hermes PR-only flow — clone→branch→push for file-edit tasks')
    assert slug
    assert '/' not in slug
    assert ' ' not in slug
    assert len(slug) <= 48


def test_git_askpass_script_does_not_embed_token(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_GH_PAT', 'secret-token-value')
    script = w._make_git_askpass(tmp_path)
    content = script.read_text()
    assert 'secret-token-value' not in content
    assert '$HERMES_GH_PAT' in content


@pytest.mark.asyncio
async def test_file_edit_requires_github_pat(monkeypatch):
    monkeypatch.setattr(w, 'PR_ONLY_ENABLED', True)
    monkeypatch.delenv('HERMES_GH_PAT', raising=False)
    with pytest.raises(w.HermesError) as e:
        await w.run_chat('Please edit backend/public/shared/i18n.js')
    assert e.value.kind == 'missing_github_pat'
