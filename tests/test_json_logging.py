"""Log-shape tests for structured JSON logging (H2 t6 Part A acceptance)."""

import io
import json
import logging

from daemon.log_json import JsonFormatter, configure_json_logging


def _capture(emit):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("hermes-test")
    logger.handlers[:] = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    emit(logger)
    return stream.getvalue().strip().splitlines()


def test_every_line_is_valid_json_with_required_fields():
    lines = _capture(lambda log: log.info("clone started branch=%s", "x"))
    assert len(lines) == 1
    rec = json.loads(lines[0])
    for field in ("timestamp", "level", "logger", "message"):
        assert field in rec, field
    assert rec["level"] == "INFO"
    assert rec["logger"] == "hermes-test"
    assert rec["message"] == "clone started branch=x"
    # ISO-8601 UTC timestamp
    assert rec["timestamp"].endswith("+00:00")


def test_extra_fields_surface_in_payload():
    lines = _capture(
        lambda log: log.info(
            "task done", extra={"event": "pr_open", "duration_ms": 1234}
        )
    )
    rec = json.loads(lines[0])
    assert rec["event"] == "pr_open"
    assert rec["duration_ms"] == 1234


def test_exception_populates_error_field():
    def emit(log):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            log.exception("clone failed")

    rec = json.loads(_capture(emit)[0])
    assert rec["level"] == "ERROR"
    assert "boom" in rec["error"]


def test_non_serializable_extra_is_repred_not_crashing():
    lines = _capture(lambda log: log.info("x", extra={"obj": object()}))
    rec = json.loads(lines[0])
    assert rec["obj"].startswith("<object object")


def test_configure_json_logging_is_idempotent():
    configure_json_logging()
    configure_json_logging()
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)


def test_no_plain_text_format_remains_in_daemon_source():
    src = open("daemon/hermes_daemon.py", encoding="utf-8").read()
    assert "basicConfig" not in src
    assert "%(asctime)s [%(levelname)s]" not in src
