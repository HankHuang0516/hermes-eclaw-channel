FROM debian:bookworm-slim

# hermes-daemon's PR-only flow needs git for clone/commit/push, GitHub CLI
# for `gh pr create`, and curl for the autoheal /health probe. The
# Python runtime aiohttp + business deps come from the hermes venv mounted
# at /home/node/hermes-agent/.venv, but the venv's `python3` is a symlink
# to /usr/bin/python3 — so we DO need a system python3 here. bookworm
# ships python3.11 which matches the venv's pyvenv.cfg.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git gnupg python3 \
    && mkdir -p -m 755 /etc/apt/keyrings \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      | gpg --dearmor -o /etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Match openclaw-b's node user (uid 1000) so the bind-mounted host paths
# (./project-b/hermes-agent, ./project-b/.hermes, ./project-b/uv-local) —
# all owned by uid 1000 — are readable/writable from this container.
RUN groupadd -g 1000 node && useradd -u 1000 -g 1000 -m -d /home/node -s /bin/bash node

USER node
WORKDIR /home/node/hermes-agent

# Compose mounts ./hermes-eclaw-channel:/app:ro and overrides command, so
# COPY/CMD here would be dead weight. The daemon entrypoint is
# /home/node/hermes-agent/.venv/bin/python3 -m daemon.hermes_daemon.
