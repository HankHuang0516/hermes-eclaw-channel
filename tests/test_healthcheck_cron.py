"""Unit tests for the Hermes H2 health-check runner."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "hermes-healthcheck-cron.py"


def load_module():
    spec = importlib.util.spec_from_file_location("hermes_healthcheck_cron", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_state_roundtrip(tmp_path):
    mod = load_module()
    state_file = tmp_path / "state.json"
    assert mod.load_state(state_file) == {}

    mod.save_state(state_file, {"failure_count": 2, "last_error": "boom"})
    assert mod.load_state(state_file)["failure_count"] == 2


def test_append_event_jsonl(tmp_path):
    mod = load_module()
    events_file = tmp_path / "events.jsonl"
    mod.append_event(events_file, {"ok": True, "duration_ms": 7})
    mod.append_event(events_file, {"ok": False, "error": "x"})

    rows = [
        json.loads(line)
        for line in events_file.read_text(encoding="utf-8").splitlines()
    ]
    assert rows == [{"duration_ms": 7, "ok": True}, {"error": "x", "ok": False}]


def test_compute_sla_summary_uses_rolling_window(tmp_path):
    mod = load_module()
    events_file = tmp_path / "events.jsonl"
    now = 1_800_000_000

    old_ts = datetime.fromtimestamp(
        now - 7200, tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")
    ok_ts = datetime.fromtimestamp(
        now - 120, tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")
    fail_ts = datetime.fromtimestamp(
        now - 60, tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")
    mod.append_event(events_file, {"ts": old_ts, "ok": False})
    mod.append_event(events_file, {"ts": ok_ts, "ok": True})
    mod.append_event(events_file, {"ts": fail_ts, "ok": False})
    events_file.write_text(
        events_file.read_text(encoding="utf-8") + "{bad json\n",
        encoding="utf-8",
    )

    summary = mod.compute_sla_summary(
        events_file,
        now=now,
        window_secs=3600,
        target_pct=99.0,
    )

    assert summary["total_checks"] == 2
    assert summary["ok_checks"] == 1
    assert summary["failed_checks"] == 1
    assert summary["uptime_pct"] == 50.0
    assert summary["within_sla"] is False
    assert summary["last_check_at"] == fail_ts


def test_failure_message_redacts_secrets(monkeypatch):
    mod = load_module()
    monkeypatch.setenv("HERMES_GH_PAT", "secret-token")
    msg = mod.failure_message(3, "git failed with secret-token")
    assert "secret-token" not in msg
    assert "Consecutive failures: 3" in msg


def test_safe_repo_label_strips_embedded_credentials(monkeypatch):
    mod = load_module()
    monkeypatch.setenv("HERMES_GH_PAT", "secret-token")

    assert (
        mod._safe_repo_label("https://x-access-token:secret-token@github.com/HankHuang0516/EClaw.git")
        == "https://github.com/HankHuang0516/EClaw.git"
    )
    assert mod._safe_repo_label("git@github.com:HankHuang0516/EClaw.git") == "github.com:HankHuang0516/EClaw.git"


def test_alert_commander_payload(monkeypatch):
    mod = load_module()
    monkeypatch.setenv("HERMES_ECLAW_API_BASE", "https://example.invalid")
    monkeypatch.setenv("HERMES_ECLAW_DEVICE_ID", "dev")
    monkeypatch.setenv("HERMES_ECLAW_ENTITY_ID", "5")
    monkeypatch.setenv("HERMES_ECLAW_BOT_SECRET", "secret")
    monkeypatch.setenv("HERMES_HEALTH_COMMANDER_TARGET", "2")
    captured = {}

    def fake_post_json(url, body, timeout_s=15):
        captured["url"] = url
        captured["body"] = body
        captured["timeout_s"] = timeout_s
        return {"success": True}

    monkeypatch.setattr(mod, "post_json", fake_post_json)

    assert mod.alert_commander("health failed") is True
    assert captured["url"] == "https://example.invalid/api/transform"
    assert captured["body"]["entityId"] == 5
    assert captured["body"]["speakTo"] == ["2"]
    assert captured["body"]["message"] == "health failed"


def test_alert_commander_skips_bad_entity_id(monkeypatch):
    mod = load_module()
    monkeypatch.setenv("HERMES_ECLAW_DEVICE_ID", "dev")
    monkeypatch.setenv("HERMES_ECLAW_ENTITY_ID", "not-an-int")
    monkeypatch.setenv("HERMES_ECLAW_BOT_SECRET", "secret")

    def fake_post_json(url, body, timeout_s=15):
        raise AssertionError("alert should be skipped before POST")

    monkeypatch.setattr(mod, "post_json", fake_post_json)

    assert mod.alert_commander("health failed") is False


def test_main_records_failure_and_alerts_at_threshold(tmp_path, monkeypatch):
    mod = load_module()
    state_file = tmp_path / "state.json"
    events_file = tmp_path / "events.jsonl"
    monkeypatch.setenv("HERMES_HEALTH_STATE_FILE", str(state_file))
    monkeypatch.setenv("HERMES_HEALTH_EVENTS_FILE", str(events_file))
    monkeypatch.setenv("HERMES_HEALTH_ALERT_AFTER_FAILURES", "2")

    def fail_daemon():
        raise RuntimeError("daemon down")

    alerts = []
    monkeypatch.setattr(mod, "check_daemon_health", fail_daemon)
    monkeypatch.setattr(mod, "alert_commander", lambda msg: alerts.append(msg) or True)

    assert mod.main() == 1
    assert mod.load_state(state_file)["failure_count"] == 1
    assert alerts == []

    assert mod.main() == 1
    state = mod.load_state(state_file)
    assert state["failure_count"] == 2
    assert state["last_alert_failure_count"] == 2
    assert len(alerts) == 1


def test_main_success_resets_failure_state(tmp_path, monkeypatch):
    mod = load_module()
    state_file = tmp_path / "state.json"
    events_file = tmp_path / "events.jsonl"
    mod.save_state(state_file, {"failure_count": 3, "last_alert_failure_count": 3})
    monkeypatch.setenv("HERMES_HEALTH_STATE_FILE", str(state_file))
    monkeypatch.setenv("HERMES_HEALTH_EVENTS_FILE", str(events_file))
    monkeypatch.setattr(mod, "check_daemon_health", lambda: {"status": "ok"})
    monkeypatch.setattr(mod, "check_git_push", lambda: {"mode": "dry-run"})

    assert mod.main() == 0
    state = mod.load_state(state_file)
    assert state["failure_count"] == 0
    assert state["last_error"] is None
    assert state["sla"]["total_checks"] == 1
    assert state["sla"]["uptime_pct"] == 100.0
