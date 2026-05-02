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
