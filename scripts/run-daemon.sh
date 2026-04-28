#!/usr/bin/env bash
# Local launcher for the Hermes HTTP daemon (option B).
# Wire contract: docs/API-bridge-http-daemon.md
set -euo pipefail

cd "$(dirname "$0")/.."

: "${HERMES_DAEMON_BIND:=127.0.0.1}"
: "${HERMES_DAEMON_PORT:=8645}"
: "${HERMES_DAEMON_QUEUE_MAX:=8}"

export HERMES_DAEMON_BIND HERMES_DAEMON_PORT HERMES_DAEMON_QUEUE_MAX

# HERMES_DAEMON_TOKEN, HERMES_DAEMON_CHAT_TIMEOUT_SECS, HERMES_BIN, HERMES_CWD
# can also be set in the environment; defaults live in daemon/hermes_worker.py.

exec python3 -m daemon.hermes_daemon
