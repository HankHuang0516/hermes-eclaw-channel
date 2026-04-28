"""Hermes HTTP daemon package.

Long-lived aiohttp server that owns a `hermes chat --continue` worker so the
EClaw bridge avoids per-request subprocess spawn cost. See
``docs/API-bridge-http-daemon.md`` for the wire contract.
"""
