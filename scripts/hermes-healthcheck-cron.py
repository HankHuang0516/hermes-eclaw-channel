#!/usr/bin/env python3
"""Hermes H2 operational health-check runner.

Designed to be executed every 6h by docker-compose or cron. It verifies the
Hermes daemon health endpoint, verifies git push capability with a dry-run by
default, records probe events for SLA calculation, and alerts EClaw after a
configurable consecutive-failure threshold.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE_FILE = "~/.hermes/eclaw-healthcheck-state.json"
DEFAULT_EVENTS_FILE = "~/.hermes/eclaw-healthcheck-events.jsonl"
DEFAULT_SLA_WINDOW_SECS = 7 * 24 * 60 * 60
DEFAULT_SLA_TARGET_PCT = 99.0


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _path_from_env(name: str, default: str) -> Path:
    return Path(os.environ.get(name, default)).expanduser()


def _redact(text: str) -> str:
    out = text or ""
    for key in ("HERMES_GH_PAT", "GH_TOKEN", "GITHUB_TOKEN", "HERMES_ECLAW_BOT_SECRET"):
        secret = os.environ.get(key)
        if secret:
            out = out.replace(secret, "***")
    return out


def _safe_repo_label(repo_url: str) -> str:
    """Return a repo label safe for event logs, even if credentials are embedded."""
    try:
        parsed = urllib.parse.urlsplit(repo_url)
    except ValueError:
        return _redact(repo_url).split("@")[-1]

    if parsed.scheme and parsed.netloc:
        hostname = parsed.hostname or ""
        if parsed.port:
            hostname = f"{hostname}:{parsed.port}"
        return urllib.parse.urlunsplit((parsed.scheme, hostname, parsed.path, "", ""))
    return _redact(repo_url).split("@")[-1]


def load_state(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {"failure_count": 0, "state_corrupt": True}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def append_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, sort_keys=True) + "\n")


def _event_epoch(event: dict[str, Any]) -> float | None:
    ts = event.get("ts")
    if isinstance(ts, (int, float)):
        return float(ts)
    if not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def compute_sla_summary(
    path: Path,
    *,
    now: float | None = None,
    window_secs: int | None = None,
    target_pct: float | None = None,
) -> dict[str, Any]:
    """Compute the rolling uptime SLA from the append-only health event log."""
    now = time.time() if now is None else now
    window_secs = window_secs if window_secs is not None else int(
        os.environ.get("HERMES_HEALTH_SLA_WINDOW_SECS", str(DEFAULT_SLA_WINDOW_SECS))
    )
    target_pct = target_pct if target_pct is not None else float(
        os.environ.get("HERMES_HEALTH_SLA_TARGET_PCT", str(DEFAULT_SLA_TARGET_PCT))
    )
    window_start = now - window_secs
    total = 0
    ok = 0
    first_ts = None
    last_ts = None

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = []

    for line in lines:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        event_ts = _event_epoch(event)
        if event_ts is None or event_ts < window_start or event_ts > now + 300:
            continue
        total += 1
        if event.get("ok") is True:
            ok += 1
        first_ts = event_ts if first_ts is None else min(first_ts, event_ts)
        last_ts = event_ts if last_ts is None else max(last_ts, event_ts)

    failed = total - ok
    uptime_pct = round((ok / total) * 100, 2) if total else None
    first_check_at = (
        datetime.fromtimestamp(first_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if first_ts else None
    )
    last_check_at = (
        datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if last_ts else None
    )
    return {
        "window_secs": window_secs,
        "target_pct": target_pct,
        "total_checks": total,
        "ok_checks": ok,
        "failed_checks": failed,
        "uptime_pct": uptime_pct,
        "within_sla": (uptime_pct >= target_pct) if uptime_pct is not None else None,
        "first_check_at": first_check_at,
        "last_check_at": last_check_at,
    }


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None, timeout: int = 120) -> str:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd is not None else None,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=True,
        )
        return proc.stdout + proc.stderr
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or e.stdout or "")[:2000]
        raise RuntimeError(f"{cmd[0]} exited {e.returncode}: {_redact(detail)}") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"{cmd[0]} timed out after {timeout}s: {_redact(str(e)[:2000])}") from e


def check_daemon_health() -> dict[str, Any]:
    url = os.environ.get("HERMES_HEALTH_DAEMON_URL", "http://127.0.0.1:8645/health")
    headers = {}
    token = os.environ.get("HERMES_DAEMON_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=float(os.environ.get("HERMES_HEALTH_DAEMON_TIMEOUT_S", "10"))) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        raise RuntimeError(f"daemon health failed: {_redact(str(e))}") from e
    if data.get("status") != "ok":
        raise RuntimeError(f"daemon health not ok: {data}")
    return data


def _git_token() -> str:
    return (
        os.environ.get("HERMES_GH_PAT")
        or os.environ.get("GH_TOKEN")
        or os.environ.get("GITHUB_TOKEN")
        or ""
    ).strip()


def _askpass_script(tmpdir: Path) -> Path:
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


def check_git_push() -> dict[str, Any]:
    if not shutil.which("git"):
        raise RuntimeError("git CLI missing")
    token = _git_token()
    if not token:
        raise RuntimeError("HERMES_GH_PAT/GH_TOKEN/GITHUB_TOKEN missing")

    repo_url = os.environ.get("HERMES_HEALTH_REPO_URL") or os.environ.get(
        "HERMES_PR_REPO_URL", "https://github.com/HankHuang0516/EClaw.git"
    )
    branch = os.environ.get("HERMES_HEALTH_BRANCH", "hermes-healthcheck")
    mode = os.environ.get("HERMES_HEALTH_PUSH_MODE", "dry-run").lower()
    write_mode = mode in {"write", "push", "1", "true", "yes"}

    with tempfile.TemporaryDirectory(prefix="hermes-health-") as td:
        tmpdir = Path(td)
        repo = tmpdir / "repo"
        repo.mkdir()
        askpass = _askpass_script(tmpdir)
        env = {
            **os.environ,
            "HERMES_GH_PAT": token,
            "GIT_ASKPASS": str(askpass),
            "GIT_TERMINAL_PROMPT": "0",
        }

        run(["git", "init"], cwd=repo, env=env)
        run(["git", "remote", "add", "origin", repo_url], cwd=repo, env=env)
        run(["git", "config", "user.name", "Hermes Healthcheck"], cwd=repo, env=env)
        run(["git", "config", "user.email", "hermes-healthcheck@users.noreply.github.com"], cwd=repo, env=env)
        fetch = subprocess.run(
            ["git", "fetch", "--depth", "1", "origin", f"refs/heads/{branch}:refs/remotes/origin/{branch}"],
            cwd=str(repo),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        if fetch.returncode == 0:
            run(["git", "checkout", "-B", branch, f"refs/remotes/origin/{branch}"], cwd=repo, env=env)
        else:
            run(["git", "checkout", "-b", branch], cwd=repo, env=env)

        probe = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "nonce": uuid.uuid4().hex,
            "mode": "write" if write_mode else "dry-run",
        }
        (repo / ".hermes-healthcheck.json").write_text(json.dumps(probe, sort_keys=True) + "\n", encoding="utf-8")
        run(["git", "add", ".hermes-healthcheck.json"], cwd=repo, env=env)
        run(["git", "commit", "-m", "Hermes healthcheck probe"], cwd=repo, env=env)
        push_cmd = ["git", "push"]
        if not write_mode:
            push_cmd.append("--dry-run")
        push_cmd += ["origin", f"HEAD:refs/heads/{branch}"]
        run(push_cmd, cwd=repo, env=env, timeout=180)
        return {"repo": _safe_repo_label(repo_url), "branch": branch, "mode": probe["mode"]}


def post_json(url: str, body: dict[str, Any], timeout_s: float = 15) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        text = resp.read().decode("utf-8")
    return json.loads(text) if text else {}


def alert_commander(message: str) -> bool:
    if not _env_bool("HERMES_HEALTH_ALERT_ENABLED", True):
        return False
    api_base = os.environ.get("HERMES_ECLAW_API_BASE", "https://eclawbot.com").rstrip("/")
    device_id = os.environ.get("HERMES_ECLAW_DEVICE_ID")
    entity_id = os.environ.get("HERMES_ECLAW_ENTITY_ID")
    bot_secret = os.environ.get("HERMES_ECLAW_BOT_SECRET")
    if not (device_id and entity_id and bot_secret):
        print("[healthcheck] alert skipped: EClaw bot credentials missing", file=sys.stderr)
        return False

    target = os.environ.get("HERMES_HEALTH_COMMANDER_TARGET", "2")
    try:
        entity_id_int = int(entity_id)
    except ValueError:
        print("[healthcheck] alert skipped: HERMES_ECLAW_ENTITY_ID must be an integer", file=sys.stderr)
        return False

    body: dict[str, Any] = {
        "deviceId": device_id,
        "entityId": entity_id_int,
        "botSecret": bot_secret,
        "message": message,
        "state": "IDLE",
    }
    if target:
        body["speakTo"] = [target]

    try:
        data = post_json(f"{api_base}/api/transform", body)
    except Exception as e:  # noqa: BLE001
        print(f"[healthcheck] alert post failed: {_redact(str(e))}", file=sys.stderr)
        return False
    if data.get("success") is False:
        print(f"[healthcheck] alert rejected: {_redact(json.dumps(data)[:1000])}", file=sys.stderr)
        return False
    return True


def failure_message(failure_count: int, error: str) -> str:
    return "\n".join([
        "[Hermes H2 health-check alert]",
        f"Consecutive failures: {failure_count}",
        f"Daemon: {os.environ.get('HERMES_HEALTH_DAEMON_URL', 'http://127.0.0.1:8645/health')}",
        f"Git probe branch: {os.environ.get('HERMES_HEALTH_BRANCH', 'hermes-healthcheck')}",
        f"Error: {_redact(error)[:1000]}",
    ])


def main() -> int:
    started = time.time()
    state_path = _path_from_env("HERMES_HEALTH_STATE_FILE", DEFAULT_STATE_FILE)
    events_path = _path_from_env("HERMES_HEALTH_EVENTS_FILE", DEFAULT_EVENTS_FILE)
    threshold = int(os.environ.get("HERMES_HEALTH_ALERT_AFTER_FAILURES", "3"))
    state = load_state(state_path)
    event: dict[str, Any] = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ok": False,
        "checks": {},
    }

    try:
        event["checks"]["daemon"] = check_daemon_health()
        event["checks"]["git_push"] = check_git_push()
        event["ok"] = True
        state.update({
            "failure_count": 0,
            "last_ok_at": event["ts"],
            "last_error": None,
            "last_alert_failure_count": 0,
        })
        print("[healthcheck] ok")
        return_code = 0
    except Exception as e:  # noqa: BLE001
        error = _redact(str(e))
        failure_count = int(state.get("failure_count") or 0) + 1
        event["error"] = error
        state.update({
            "failure_count": failure_count,
            "last_failure_at": event["ts"],
            "last_error": error,
        })
        print(f"[healthcheck] failed ({failure_count}): {error}", file=sys.stderr)
        last_alert = int(state.get("last_alert_failure_count") or 0)
        if failure_count >= threshold and last_alert < failure_count:
            if alert_commander(failure_message(failure_count, error)):
                state["last_alert_failure_count"] = failure_count
                state["last_alert_at"] = event["ts"]
        return_code = 1
    finally:
        event["duration_ms"] = int((time.time() - started) * 1000)
        append_event(events_path, event)
        state["sla"] = compute_sla_summary(events_path)
        save_state(state_path, state)

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
