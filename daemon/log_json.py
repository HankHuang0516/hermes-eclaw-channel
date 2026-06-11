"""Structured JSON logging for the Hermes daemon (H2 t6 Part A).

Every record is one JSON object per line so a Railway log drain (or any
collector) can ingest without a parsing grammar:

    {"timestamp": "...", "level": "INFO", "logger": "hermes-daemon",
     "message": "...", "event": "...", "duration_ms": 123, "error": "..."}

`event` / `duration_ms` / `error` appear only when supplied via
``log.info(..., extra={"event": "clone", "duration_ms": 42})`` or when the
record carries exception info. Plain-text format is gone by design — the
card acceptance is "JSON only, no plain text".
"""

import json
import logging
from datetime import datetime, timezone

# logging.LogRecord attributes that are plumbing, not payload. Anything NOT
# in this set that lands on the record (i.e. passed via ``extra=``) is
# forwarded into the JSON object.
_RESERVED = frozenset(
    (
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "msg", "message", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "taskName", "thread", "threadName",
    )
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key in payload:
                continue
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                value = repr(value)
            payload[key] = value
        if record.exc_info and record.exc_info[1] is not None:
            payload.setdefault("error", repr(record.exc_info[1]))
        return json.dumps(payload, ensure_ascii=False)


def configure_json_logging(level: int = logging.INFO) -> None:
    """Install the JSON formatter on the root logger (idempotent)."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
