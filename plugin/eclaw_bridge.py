#!/usr/bin/env python3
"""
Hermes × EClaw channel bridge — POC

接 EClaw channel webhook，呼叫 Hermes Agent 拿回覆，POST 回 EClaw。

為什麼不做 Hermes plugin：
  Hermes webhook gateway 的 `deliver` 目標沒有「POST 回 source HTTP」選項，
  要自己寫 custom delivery 或從 post_llm_call hook 硬塞。
  對 POC 直接寫 bridge 最快、可讀性最高。將來若要 production-grade
  (multi-session 記憶、tool calls、streaming)，再改成 Hermes 原生 plugin。

流程：
  [EClaw] --POST--> [this bridge :8644] --hermes chat -p--> [response text]
                                       --POST /api/channel/message--> [EClaw]

Auth：EClaw 會帶 Authorization: Bearer <callback_token>。bridge 驗 token 通過才處理。

環境變數（必填）：
  HERMES_ECLAW_API_KEY      eck_...
  HERMES_ECLAW_DEVICE_ID    UUID
  HERMES_ECLAW_ENTITY_ID    整數 (bridge 接手的 entity slot)
  HERMES_ECLAW_BOT_SECRET   bind 時拿到的
  HERMES_ECLAW_API_BASE     default https://eclawbot.com
  HERMES_ECLAW_CALLBACK_TOKEN  register 時產的
  HERMES_PORT               default 8644
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import textwrap
import time
import uuid
from typing import Any

from aiohttp import ClientConnectorError, ClientSession, ClientTimeout, web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("eclaw-bridge")


# --- Config ---------------------------------------------------------------

API_KEY = os.environ["HERMES_ECLAW_API_KEY"]
DEVICE_ID = os.environ["HERMES_ECLAW_DEVICE_ID"]
ENTITY_ID = int(os.environ["HERMES_ECLAW_ENTITY_ID"])
BOT_SECRET = os.environ["HERMES_ECLAW_BOT_SECRET"]
API_BASE = os.environ.get("HERMES_ECLAW_API_BASE", "https://eclawbot.com")
CALLBACK_TOKEN = os.environ["HERMES_ECLAW_CALLBACK_TOKEN"]
PORT = int(os.environ.get("HERMES_PORT", "8644"))

# Optional: route Hermes calls through the long-lived HTTP daemon (option B).
# When unset, the bridge keeps spawning hermes subprocesses as before.
DAEMON_URL = os.environ.get("HERMES_DAEMON_URL", "").rstrip("/")
DAEMON_TOKEN = os.environ.get("HERMES_DAEMON_TOKEN", "")
DAEMON_CONNECT_TIMEOUT = float(os.environ.get("HERMES_DAEMON_CONNECT_TIMEOUT", "5"))

# Drop webhook deliveries timestamped older than this many seconds. EClaw
# retries failed webhooks; on bridge restart, the in-flight backlog can land
# all at once, all stale. Replaying them just re-triggers whatever wedged us
# (NousResearch issue #7536 stuck-session-on-restart loop). Default 5min;
# bump if your latency budget exceeds that.
STALE_WEBHOOK_THRESHOLD_S = int(os.environ.get("HERMES_STALE_WEBHOOK_THRESHOLD_S", "300"))
_BOOT_TS = time.time()

# Phase 2: when True and API_KEY is set, send_message uses /api/transform
# with X-Channel-Key header + actAs:"channel" instead of /api/channel/message.
# Default False — existing deployments are unaffected.
PREFER_TRANSFORM_VIA_CHANNEL_KEY = (
    os.environ.get("HERMES_ECLAW_PREFER_TRANSFORM_VIA_CHANNEL_KEY", "").lower() == "true"
)

# Kept as a safety net — if Hermes still happens to output this exact token
# (e.g. from system prompt or memory), skip the reply.
SILENT_TOKEN = "[SILENT]"


def apply_prompt_policy(prompt: str, compiled_prompt: str) -> str:
    """Prepend centrally managed EClaw prompt policy when available."""
    policy = (compiled_prompt or "").strip()
    if not policy:
        return prompt
    return "\n".join([
        "[EClaw managed prompt policy - hermes]",
        policy,
        "[End EClaw managed prompt policy]",
        "",
        prompt,
    ])


def apply_routing_policy(prompt: str, routing_policy: str) -> str:
    """Prepend the EClaw central smart-routing policy when available.

    Phase 4 of EClaw#2285. Server-managed; bridges fetch on first inbound
    and cache the result. Fails open: empty policy returns prompt unchanged.
    """
    policy = (routing_policy or "").strip()
    if not policy:
        return prompt
    return "\n".join([
        "[EClaw central routing policy]",
        policy,
        "[End EClaw central routing policy]",
        "",
        prompt,
    ])


def derive_sender_hint(msg: dict) -> dict | None:
    """Map an inbound webhook payload to a senderHint for /api/channel/message.

    Mirrors the codex-eclaw-bridge implementation. The hint has the lowest
    precedence on the server: explicit speakTo / broadcast and in-text
    @-mentions still win. See EClaw#2285.

    Mapping:
      - isBroadcast → kind=broadcast
      - from in {client,user,system,kanban} → kind=user (no routing)
      - fromEntityId / fromPublicCode present → kind=entity
      - otherwise → kind=unknown (server treats as no routing)
    """
    if msg.get("isBroadcast"):
        return {"kind": "broadcast"}
    from_key = (msg.get("from") or "").lower()
    if from_key in {"client", "user", "system", "kanban"}:
        return {"kind": "user"}
    from_eid = msg.get("fromEntityId")
    from_pc = msg.get("fromPublicCode")
    if isinstance(from_eid, int) or isinstance(from_pc, str):
        hint: dict = {"kind": "entity"}
        if isinstance(from_eid, int):
            hint["entityId"] = from_eid
        if isinstance(from_pc, str) and from_pc:
            hint["publicCode"] = from_pc
        return hint
    return {"kind": "unknown"}


# --- EClaw API ------------------------------------------------------------

async def send_message(
    session: ClientSession,
    text: str,
    state: str = "IDLE",
    sender_hint: dict | None = None,
) -> dict:
    """Send reply via whichever auth path is configured.

    Phase 2: when HERMES_ECLAW_PREFER_TRANSFORM_VIA_CHANNEL_KEY=true and
    API_KEY is set, posts to /api/transform with X-Channel-Key header +
    actAs:"channel" — gaining @-mention auto-routing and A2A queue
    side-effects without a per-entity botSecret in the bridge env.

    Phase 4 of EClaw#2285 (senderHint) applies to both paths.
    """
    if PREFER_TRANSFORM_VIA_CHANNEL_KEY and API_KEY:
        # Phase 2: channel key path
        body: dict = {
            "deviceId": DEVICE_ID,
            "entityId": ENTITY_ID,
            "actAs": "channel",
            "message": text,
            "state": state,
        }
        if sender_hint:
            body["senderHint"] = sender_hint
        headers = {"X-Channel-Key": API_KEY}
        async with session.post(f"{API_BASE}/api/transform", json=body, headers=headers) as r:
            data = await r.json()
            if not data.get("success"):
                log.error("send_message via transform failed: %s", data)
            return data
    else:
        # Default path: /api/channel/message (Phase 4 fallback)
        body = {
            "channel_api_key": API_KEY,
            "deviceId": DEVICE_ID,
            "entityId": ENTITY_ID,
            "botSecret": BOT_SECRET,
            "message": text,
            "state": state,
        }
        if sender_hint:
            body["senderHint"] = sender_hint
        async with session.post(f"{API_BASE}/api/channel/message", json=body) as r:
            data = await r.json()
            if not data.get("success"):
                log.error("send_message failed: %s", data)
            return data


# Cached once per process — the routing policy is static across messages and
# sessions. Bridges restart cheaply; we don't refresh in-band.
_routing_policy_cache: str | None = None


async def fetch_routing_policy(session: ClientSession) -> str:
    """GET /api/channel/routing-policy — central smart-routing system prompt.

    EClaw#2285 Phase 4. Cached for the lifetime of the bridge process. Fails
    open with "" so older servers (pre-EClaw#2287) or transient network
    errors don't block message delivery.
    """
    global _routing_policy_cache
    if _routing_policy_cache is not None:
        return _routing_policy_cache
    params = {"channel": "hermes", "lang": "en"}
    try:
        async with session.get(f"{API_BASE}/api/channel/routing-policy", params=params) as r:
            if r.status >= 400:
                _routing_policy_cache = ""
                return ""
            data = await r.json()
    except Exception as e:  # noqa: BLE001
        log.warning("routing policy fetch skipped: %s", e)
        _routing_policy_cache = ""
        return ""

    if data.get("success") is False:
        _routing_policy_cache = ""
        return ""
    policy = (data.get("policy") or "").strip()
    _routing_policy_cache = policy
    return policy


async def fetch_prompt_policy(session: ClientSession) -> str:
    """GET /api/channel/prompt-policy for this bound Hermes entity.

    Failure is non-fatal: channel delivery must continue even if policy preview
    or deployment is temporarily unavailable.
    """
    params = {
        "deviceId": DEVICE_ID,
        "entityId": str(ENTITY_ID),
        "botSecret": BOT_SECRET,
        "channel": "hermes",
    }
    try:
        async with session.get(f"{API_BASE}/api/channel/prompt-policy", params=params) as r:
            if r.status >= 400:
                log.warning("prompt policy fetch failed: http %d", r.status)
                return ""
            data = await r.json()
    except Exception as e:
        log.warning("prompt policy fetch skipped: %s", e)
        return ""

    if data.get("success") is False:
        return ""
    return ((data.get("policy") or {}).get("compiledPrompt") or "").strip()


# --- Hermes invocation ---------------------------------------------------

# NOTE on regex: the quota line can embed nested brackets (`"[SILENT]"`),
# so we can't rely on the outer `]` to close — just consume to end-of-line.
_QUOTA_LINE_RE = re.compile(r"^\[Quota:.*$", re.MULTILINE)
_RICH_OPEN_BRACKET_RE = re.compile(r"\[")


def _strip_eclaw_context(text: str) -> str:
    """Strip EClaw's auto-injected context blocks that pollute the prompt.

    EClaw's server prepends/appends several things that are either redundant
    or actively harmful for Hermes:

      - ``[Local Variables available: ...]`` / ``[AVAILABLE TOOLS ...]`` —
        meant for OpenClaw bots; noise for us.
      - ``[Quota: N/M bot-to-bot remaining — output "[SILENT]" if
        nothing worth replying]`` — the instruction Hermes is way too
        willing to comply with, causing false silent skips.
    """
    for marker in ("\n[Local Variables available:", "\n[AVAILABLE TOOLS"):
        idx = text.find(marker)
        if idx >= 0:
            text = text[:idx]
    text = _QUOTA_LINE_RE.sub("", text)
    return text.strip()


def _escape_rich_brackets(text: str) -> str:
    """Mirror of ``daemon/hermes_worker.py::escape_rich_brackets``.

    See that function for the full rationale. Both code paths (daemon HTTP +
    subprocess fallback) MUST escape identically — divergence means kanban
    nudges crash on one path and succeed on the other, masking the bug.
    """
    return _RICH_OPEN_BRACKET_RE.sub(r"\\[", text)


HERMES_TIMEOUT = int(os.environ.get("HERMES_TIMEOUT_SECS", "900"))

# Strip ANSI escape sequences (color, cursor, etc.) so verbose hermes
# output is readable when piped back to EClaw chat.
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07")

# Markers used by the verbose hermes CLI to bracket the agent's reply.
# Top: ` ─  ⚕ Hermes  ─...─ `
# Tail: `Resume this session with:` or `Session:` summary block
_HERMES_HEAD_RE = re.compile(r"^\s*─+\s*⚕\s*Hermes\s*─+", re.MULTILINE)
_HERMES_TAIL_RE = re.compile(r"^(?:Resume this session with:|Session:\s+|Duration:\s+|Messages:\s+)", re.MULTILINE)
_PURE_RULE_RE = re.compile(r"^[\s─━│╭╮╰╯═]+$")


def _extract_hermes_reply(stdout: str) -> str:
    """Pull the agent's response out of the verbose CLI envelope.

    Without -Q, hermes prints a banner box, query echo, the response inside
    an `⚕ Hermes` box, and a session-summary footer. We want only the
    response body.
    """
    head = list(_HERMES_HEAD_RE.finditer(stdout))
    if head:
        # Use the LAST head — multi-turn output has multiple boxes.
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

# Serialise Hermes calls — each spawn writes to ~/.hermes/sessions and
# concurrent hermes CLI processes can corrupt session state.
_hermes_lock = asyncio.Lock()


class _DaemonUnavailable(Exception):
    """Daemon couldn't be reached — caller should fall back to subprocess."""


async def _ask_hermes_via_daemon(prompt: str) -> str:
    """POST /chat to the daemon (JSON mode for now — bridge buffers anyway).

    Connection-level failures raise _DaemonUnavailable so the caller falls back
    to subprocess. Clean daemon errors (timeout / hermes_exit) become user-facing
    text; we don't retry in subprocess for those.
    """
    url = f"{DAEMON_URL}/chat"
    headers = {"Accept": "application/json"}
    if DAEMON_TOKEN:
        headers["Authorization"] = f"Bearer {DAEMON_TOKEN}"
    payload = {
        "prompt": _strip_eclaw_context(prompt),
        "request_id": str(uuid.uuid4()),
    }

    timeout = ClientTimeout(connect=DAEMON_CONNECT_TIMEOUT, total=None)
    try:
        async with ClientSession(timeout=timeout) as s:
            async with s.post(url, json=payload, headers=headers) as r:
                if r.status in (502, 503):
                    detail = await r.text()
                    log.warning("[hermes] daemon transport error %d: %s", r.status, detail[:200])
                    raise _DaemonUnavailable(f"http {r.status}")
                data = await r.json()
    except ClientConnectorError as e:
        log.warning("[hermes] daemon connect failed: %s", e)
        raise _DaemonUnavailable(str(e)) from e
    except asyncio.TimeoutError as e:
        log.warning("[hermes] daemon connect timeout")
        raise _DaemonUnavailable("connect timeout") from e

    if not data.get("ok"):
        err = data.get("error", {})
        kind = err.get("kind", "unknown")
        if kind == "timeout":
            return "[Hermes 回應超時]"
        if kind == "hermes_exit":
            return "[Hermes 回覆失敗 — 請查 log]"
        # spawn_failed / busy: degrade to subprocess
        log.warning("[hermes] daemon error %s, falling back: %s", kind, err.get("detail"))
        raise _DaemonUnavailable(kind)

    if data.get("silent"):
        return SILENT_TOKEN  # process_message() already handles this
    return data.get("reply", "")


async def ask_hermes(prompt: str) -> str:
    """Dispatch to daemon if configured, else legacy subprocess.

    Daemon disabled by default; set HERMES_DAEMON_URL to opt in. Connection-level
    failures fall back to subprocess so a daemon outage degrades gracefully.
    """
    if DAEMON_URL:
        try:
            return await _ask_hermes_via_daemon(prompt)
        except _DaemonUnavailable:
            log.warning("[hermes] daemon unreachable, falling back to subprocess")
    return await _ask_hermes_subprocess(prompt)


async def _ask_hermes_subprocess(prompt: str) -> str:
    """
    呼叫 Hermes CLI，回 stdout（quiet mode：只剩最終回覆）。

    加 timeout 保護：容器裡 PID 1 不 reap child，subprocess 若異常退出
    可能變 zombie 導致 communicate() 永遠不返回。timeout 兜底。

    Legacy path. Used directly when HERMES_DAEMON_URL is unset, and as a
    fallback when the daemon is unreachable.
    """
    clean = _strip_eclaw_context(prompt)
    safe = _escape_rich_brackets(clean)
    log.info("[hermes] spawning chat, prompt_len=%d", len(clean))

    # --continue reuses the most recent session → agent retains conversation
    # memory across calls even though each spawn is a fresh process.
    # -Q (quiet) deliberately omitted so tool-call previews and streaming
    # deltas land in stdout, giving the bridge progress signal it can
    # mid-task forward (also feeds ANSI-stripped output to EClaw chat).
    args = [
        "/home/node/hermes-agent/.venv/bin/hermes",  # skip `uv run` overhead
        "chat", "-q", safe, "--continue",
    ]

    async with _hermes_lock:  # serialize to protect session files
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                cwd="/home/node/hermes-agent",
                env={**os.environ, "PATH": "/home/node/.local/bin:" + os.environ.get("PATH", "")},
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            log.error("[hermes] failed to spawn: %s", e)
            return "[Hermes 啟動失敗]"

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=HERMES_TIMEOUT)
        except asyncio.TimeoutError:
            log.error("[hermes] timed out after %ds", HERMES_TIMEOUT)
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            return "[Hermes 回應超時]"

    if proc.returncode != 0:
        # 4000-char cap: prior 500-char limit truncated mid-traceback and
        # hid the rich.MarkupError crash that motivated this very escape.
        log.error("[hermes] exit %d: %s", proc.returncode, stderr.decode()[:4000])
        return "[Hermes 回覆失敗 — 請查 log]"

    raw = _ANSI_RE.sub("", stdout.decode())
    reply = _extract_hermes_reply(raw)
    log.info("[hermes] reply_len=%d", len(reply))
    return reply



# --- Webhook handler -----------------------------------------------------

async def handle_webhook(request: web.Request) -> web.Response:
    # Bearer auth — EClaw echoes back the callback_token we gave at register()
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != CALLBACK_TOKEN:
        log.warning("unauthorized webhook attempt from %s", request.remote)
        return web.json_response({"error": "unauthorized"}, status=401)

    try:
        body = await request.json()
    except Exception as e:
        return web.json_response({"error": f"bad body: {e}"}, status=400)

    # Drop stale webhook deliveries — EClaw retries failed webhooks, and on
    # bridge restart the backlog can all land at once and re-trigger the
    # exact problem we just restarted to escape (NousResearch issue #7536).
    # We compare msg.timestamp (Date.now() from EClaw) against the bridge's
    # boot wall time + a wall-time threshold; messages from before the
    # bridge booted (or older than threshold) get a 200 OK + log + drop.
    msg_ts_ms = body.get("timestamp")
    if isinstance(msg_ts_ms, (int, float)):
        msg_age_s = time.time() - (msg_ts_ms / 1000)
        if msg_age_s > STALE_WEBHOOK_THRESHOLD_S:
            log.warning(
                "[stale-drop] webhook age %.1fs > %ds threshold; ignoring "
                "msg from entity %s (event=%s)",
                msg_age_s, STALE_WEBHOOK_THRESHOLD_S,
                body.get("fromEntityId") or "user", body.get("event"),
            )
            return web.json_response({"ok": True, "dropped": "stale"})

    # ACK immediately so EClaw doesn't time out; process in background.
    asyncio.create_task(process_message(body))
    return web.json_response({"ok": True})


async def process_message(msg: dict) -> None:
    """Kick Hermes, deliver reply via appropriate EClaw endpoint."""
    event = msg.get("event", "message")
    text = msg.get("text", "") or ""
    entity_id = msg.get("entityId")
    from_entity_id = msg.get("fromEntityId")
    from_character = msg.get("fromCharacter")
    eclaw_ctx = msg.get("eclaw_context") or {}

    if entity_id != ENTITY_ID:
        log.info("ignore message for entity %s (we are %d)", entity_id, ENTITY_ID)
        return

    # Build prompt — enrich bot-to-bot/broadcast with sender context so Hermes
    # knows who's talking. We intentionally do NOT inject EClaw's
    # "output [SILENT] if nothing worth replying" quota instruction: Hermes
    # is too willing to comply and nearly every broadcast came back silent,
    # which looked like the bridge was stuck. Reply on every inbound; if
    # noise becomes an issue, filter at the sender side.
    prompt = text
    if event in ("entity_message", "broadcast") and from_entity_id is not None:
        # Do NOT prepend missionHints: the server already embeds them inside
        # `text` (via materializeChannelText). Prepending again put
        # `[AVAILABLE TOOLS ...]` ahead of the body, and _strip_eclaw_context's
        # `\n[AVAILABLE TOOLS` truncation then wiped both hints and the body —
        # the bridge forwarded only the bridge's own header (prompt_len≈36),
        # and Hermes replied "Bot-to-Bot 訊息但似乎沒有附帶任何內容".
        sender = f"Entity {from_entity_id}" + (f" ({from_character})" if from_character else "")
        prefix = "Broadcast from" if event == "broadcast" else "Bot-to-Bot from"
        prompt = f"[{prefix} {sender}]\n{text}" if text else f"[{prefix} {sender}]"

    async with ClientSession() as s:
        compiled_policy = await fetch_prompt_policy(s)
        routing_policy = await fetch_routing_policy(s)

    prompt = apply_prompt_policy(prompt, compiled_policy)
    prompt = apply_routing_policy(prompt, routing_policy)

    log.info("event=%s from=%s text=%r", event, from_entity_id or "user", text[:80])
    reply = await ask_hermes(prompt)

    if not reply or SILENT_TOKEN in reply:
        log.info("reply empty or silent — skip delivery")
        return

    # EClaw#2285 Phase 4: derive senderHint from the inbound payload and pass
    # it to /api/channel/message. The server resolves speakTo centrally, so
    # we no longer need the dual send_message + speak_to path that previously
    # leaned on the deprecated /api/entity/speak-to endpoint.
    sender_hint = derive_sender_hint(msg)
    async with ClientSession() as s:
        await send_message(s, reply, sender_hint=sender_hint)


# --- Boot ----------------------------------------------------------------

async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "eclaw-bridge", "entityId": ENTITY_ID})


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/webhooks/eclaw", handle_webhook)
    return app


if __name__ == "__main__":
    log.info("starting on :%d (entity=%d, device=%s)", PORT, ENTITY_ID, DEVICE_ID[:8])
    web.run_app(make_app(), host="0.0.0.0", port=PORT, access_log=None)
