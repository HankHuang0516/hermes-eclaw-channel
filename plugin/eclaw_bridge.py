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
import subprocess
from typing import Any

from aiohttp import ClientSession, web

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

SILENT_TOKEN = "[SILENT]"  # EClaw convention: bot outputs this to skip reply


# --- EClaw API ------------------------------------------------------------

async def send_message(session: ClientSession, text: str, state: str = "IDLE") -> dict:
    """POST /api/channel/message — reply to the user on the user's wallpaper."""
    body = {
        "channel_api_key": API_KEY,
        "deviceId": DEVICE_ID,
        "entityId": ENTITY_ID,
        "botSecret": BOT_SECRET,
        "message": text,
        "state": state,
    }
    async with session.post(f"{API_BASE}/api/channel/message", json=body) as r:
        data = await r.json()
        if not data.get("success"):
            log.error("send_message failed: %s", data)
        return data


async def speak_to(session: ClientSession, to_entity_id: int, text: str, expects_reply: bool = False) -> None:
    """POST /api/entity/speak-to — bot-to-bot reply."""
    body = {
        "deviceId": DEVICE_ID,
        "fromEntityId": ENTITY_ID,
        "botSecret": BOT_SECRET,
        "toEntityId": to_entity_id,
        "text": text,
        "expects_reply": expects_reply,
    }
    async with session.post(f"{API_BASE}/api/entity/speak-to", json=body) as r:
        if r.status >= 400:
            log.error("speak_to %d failed: %s", to_entity_id, await r.text())


# --- Hermes invocation ---------------------------------------------------

def _strip_eclaw_context(text: str) -> str:
    """Remove EClaw's auto-appended [Local Variables available: ...] block.

    EClaw injects available env vars / skills into every message. Our bridge
    already inherits the env, so this is noise for Hermes.
    """
    for marker in ("\n[Local Variables available:", "\n[AVAILABLE TOOLS"):
        idx = text.find(marker)
        if idx >= 0:
            text = text[:idx]
    return text.strip()


HERMES_TIMEOUT = int(os.environ.get("HERMES_TIMEOUT_SECS", "90"))

# Serialise Hermes calls — each spawn writes to ~/.hermes/sessions and
# concurrent hermes CLI processes can corrupt session state.
_hermes_lock = asyncio.Lock()


async def ask_hermes(prompt: str) -> str:
    """
    呼叫 Hermes CLI，回 stdout（quiet mode：只剩最終回覆）。

    加 timeout 保護：容器裡 PID 1 不 reap child，subprocess 若異常退出
    可能變 zombie 導致 communicate() 永遠不返回。timeout 90s 兜底。

    TODO(cost): 每次 spawn 新 process，冷啟動慢。production 應改呼 Hermes HTTP API
    或常駐 Python worker。
    """
    clean = _strip_eclaw_context(prompt)
    log.info("[hermes] spawning chat, prompt_len=%d", len(clean))

    # --continue reuses the most recent session → agent retains conversation
    # memory across calls even though each spawn is a fresh process.
    args = [
        "/home/node/hermes-agent/.venv/bin/hermes",  # skip `uv run` overhead
        "chat", "-Q", "-q", clean, "--continue",
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
        log.error("[hermes] exit %d: %s", proc.returncode, stderr.decode()[:500])
        return "[Hermes 回覆失敗 — 請查 log]"

    lines = [ln for ln in stdout.decode().splitlines() if not ln.startswith("session_id:")]
    reply = "\n".join(lines).strip()
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

    # Build prompt — enrich bot-to-bot with source context, like openclaw does
    prompt = text
    if event in ("entity_message", "broadcast") and from_entity_id is not None:
        sender = f"Entity {from_entity_id}" + (f" ({from_character})" if from_character else "")
        prefix = "Broadcast from" if event == "broadcast" else "Bot-to-Bot from"
        hints = eclaw_ctx.get("missionHints", "")
        quota = ""
        if eclaw_ctx.get("b2bRemaining") is not None:
            quota = f"[Quota: {eclaw_ctx['b2bRemaining']}/{eclaw_ctx.get('b2bMax', 8)} — output \"{SILENT_TOKEN}\" if nothing worth replying]"
        prompt = "\n".join(x for x in [f"[{prefix} {sender}]", quota, hints, text] if x)

    log.info("event=%s from=%s text=%r", event, from_entity_id or "user", text[:80])
    reply = await ask_hermes(prompt)

    if not reply or SILENT_TOKEN in reply:
        log.info("reply empty or silent — skip delivery")
        return

    async with ClientSession() as s:
        if event == "message":
            await send_message(s, reply)
        elif event in ("entity_message", "broadcast") and from_entity_id is not None:
            # Per openclaw pattern: both update own wallpaper AND speak back
            await send_message(s, reply)
            await speak_to(s, from_entity_id, reply)


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
