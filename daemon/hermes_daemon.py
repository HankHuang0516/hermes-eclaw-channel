"""aiohttp HTTP daemon wrapping the Hermes CLI worker.

Wire contract: ``docs/API-bridge-http-daemon.md``.

Endpoints:
    POST /chat       SSE by default; JSON if Accept: application/json.
    GET  /health     uptime + queue depth.
    GET  /version    semantic version + git sha.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Optional

from aiohttp import web

from . import hermes_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hermes-daemon")

VERSION = "0.1.0"
GIT_SHA = os.environ.get("HERMES_DAEMON_GIT_SHA", "unknown")[:12]
DAEMON_TOKEN = os.environ.get("HERMES_DAEMON_TOKEN", "")
QUEUE_MAX = int(os.environ.get("HERMES_DAEMON_QUEUE_MAX", "8"))
BIND = os.environ.get("HERMES_DAEMON_BIND", "127.0.0.1")
PORT = int(os.environ.get("HERMES_DAEMON_PORT", "8645"))


_state = {
    "started_at": time.time(),
    "in_flight": 0,
    "queue_depth": 0,
}


def _check_auth(req: web.Request) -> Optional[web.Response]:
    if not DAEMON_TOKEN:
        return None  # auth disabled (dev mode)
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != DAEMON_TOKEN:
        return web.json_response({"error": "unauthorized"}, status=401)
    return None


def _err(kind: str, detail: str, status: int) -> web.Response:
    return web.json_response({"ok": False, "error": {"kind": kind, "detail": detail}}, status=status)


async def health(_: web.Request) -> web.Response:
    uptime_s = int(time.time() - _state["started_at"])
    # Surface Phase H1 worker state — Docker HEALTHCHECK + autoheal sidecar
    # read this; if `resume_auto_disabled=true` while in_flight=0 and
    # queue_depth=0, the daemon is alive but on degraded mode (started fresh
    # because session resume tripped on first call after boot).
    return web.json_response({
        "status": "ok",
        "service": "hermes-daemon",
        "uptime_s": uptime_s,
        "in_flight": _state["in_flight"],
        "queue_depth": _state["queue_depth"],
        "queue_max": QUEUE_MAX,
        "calls_total": hermes_worker._call_count,
        "resume_auto_disabled": hermes_worker._resume_auto_disabled,
        "no_resume_env": hermes_worker.HERMES_NO_RESUME,
        "idle_timeout_s": hermes_worker.IDLE_TIMEOUT,
        "wall_timeout_s": hermes_worker.DEFAULT_TIMEOUT,
    })


async def version(_: web.Request) -> web.Response:
    return web.json_response({
        "service": "hermes-daemon",
        "version": VERSION,
        "git_sha": GIT_SHA,
        "started_at": _state["started_at"],
    })


async def _run_and_collect(prompt: str, request_id: str) -> hermes_worker.HermesResult:
    _state["in_flight"] += 1
    try:
        return await hermes_worker.run_chat(prompt)
    finally:
        _state["in_flight"] -= 1


async def _chat_json(req: web.Request, body: dict) -> web.Response:
    request_id = body["request_id"]
    try:
        result = await _run_and_collect(body["prompt"], request_id)
    except hermes_worker.HermesError as e:
        log.warning("[hermes] %s rid=%s: %s", e.kind, request_id[:8], e.detail[:500])
        status = {"timeout": 504, "spawn_failed": 503, "hermes_exit": 502}.get(e.kind, 500)
        return _err(e.kind, e.detail, status)
    return web.json_response({
        "ok": True,
        "reply": result.reply,
        "silent": result.silent,
        "duration_ms": result.duration_ms,
    })


async def _chat_sse(req: web.Request, body: dict) -> web.StreamResponse:
    request_id = body["request_id"]
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    await resp.prepare(req)

    async def emit(event: str, payload: dict) -> None:
        await resp.write(f"event: {event}\n".encode())
        await resp.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode())

    await emit("started", {"request_id": request_id, "ts": time.time()})

    try:
        result = await _run_and_collect(body["prompt"], request_id)
    except hermes_worker.HermesError as e:
        log.warning("[hermes] %s rid=%s: %s", e.kind, request_id[:8], e.detail[:500])
        await emit("error", {"request_id": request_id, "kind": e.kind, "detail": e.detail})
        await resp.write_eof()
        return resp

    if result.silent:
        await emit("silent", {"request_id": request_id})
    else:
        await emit("done", {
            "request_id": request_id,
            "reply": result.reply,
            "duration_ms": result.duration_ms,
        })
    await resp.write_eof()
    return resp


async def chat(req: web.Request) -> web.StreamResponse:
    auth_err = _check_auth(req)
    if auth_err:
        return auth_err

    try:
        body = await req.json()
    except Exception as e:
        return _err("bad_request", f"json parse: {e}", 400)

    if "prompt" not in body or not isinstance(body["prompt"], str):
        return _err("bad_request", "prompt required (string)", 400)
    if "request_id" not in body or not isinstance(body["request_id"], str):
        body["request_id"] = str(uuid.uuid4())

    if _state["queue_depth"] >= QUEUE_MAX:
        # Load-shedding: 503 (Service Unavailable) signals the upstream to back
        # off + circuit-break. 429 implies "retry now" which makes sustained
        # overload worse — vLLM RFC #18826 + tianpan.co backpressure pattern.
        return _err("busy", f"queue depth {_state['queue_depth']} >= {QUEUE_MAX}", 503)

    _state["queue_depth"] += 1
    try:
        wants_json = req.headers.get("Accept", "").startswith("application/json")
        if wants_json:
            return await _chat_json(req, body)
        return await _chat_sse(req, body)
    finally:
        _state["queue_depth"] -= 1


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/version", version)
    app.router.add_post("/chat", chat)
    return app


def main() -> None:
    log.info("starting hermes-daemon on %s:%d (auth=%s, queue_max=%d)",
             BIND, PORT, "on" if DAEMON_TOKEN else "off", QUEUE_MAX)
    web.run_app(make_app(), host=BIND, port=PORT, access_log=None)


if __name__ == "__main__":
    main()
