"""Tests for the structlog configuration."""

from __future__ import annotations

import json
import logging

import pytest

from smart_agri.utils import configure_logging, get_logger


class TestConfigureLogging:
    def test_json_renderer_emits_parseable_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Task containers log to stdout and Airflow captures the stream, so the
        default rendering has to survive machine parsing."""
        configure_logging("INFO", force_json=True)
        get_logger("test").info("pipeline_finished", rows=42, zone="silver")

        payload = json.loads(capsys.readouterr().out.strip())
        assert payload["event"] == "pipeline_finished"
        assert payload["rows"] == 42
        assert payload["zone"] == "silver"
        assert payload["level"] == "info"
        assert "timestamp" in payload

    def test_console_renderer_is_not_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging("INFO", force_json=False)
        get_logger("test").info("hello")

        out = capsys.readouterr().out
        assert "hello" in out
        with pytest.raises(json.JSONDecodeError):
            json.loads(out.strip())

    def test_level_is_applied(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging("WARNING", force_json=True)
        logger = get_logger("test")
        logger.info("suppressed")
        logger.warning("emitted")

        out = capsys.readouterr().out
        assert "suppressed" not in out
        assert "emitted" in out

    def test_is_idempotent(self) -> None:
        configure_logging("INFO", force_json=True)
        configure_logging("DEBUG", force_json=True)
        assert logging.getLogger().level == logging.DEBUG

    @pytest.mark.parametrize("level", ["not-a-level", ""])
    def test_unknown_level_falls_back_to_info(self, level: str) -> None:
        configure_logging(level, force_json=True)
        assert logging.getLogger().level == logging.INFO


class TestGetLogger:
    def test_binds_initial_context(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging("INFO", force_json=True)
        get_logger("test", run_id="abc123").info("started")

        assert json.loads(capsys.readouterr().out.strip())["run_id"] == "abc123"

    def test_returns_an_eagerly_bound_logger(self) -> None:
        """Never a BoundLoggerLazyProxy: callers rely on `.bind()` and the level
        filter being in place from the first call."""
        configure_logging("INFO", force_json=True)
        logger = get_logger("test")
        assert type(logger).__name__ != "BoundLoggerLazyProxy"
        assert hasattr(logger, "bind")
        assert hasattr(logger, "info")
