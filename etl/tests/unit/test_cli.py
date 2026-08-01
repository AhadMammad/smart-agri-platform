"""Tests for the CLI surface that Airflow's DockerOperator drives."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from smart_agri import __version__
from smart_agri.cli import app
from smart_agri.health import CheckResult

runner = CliRunner()


class TestVersion:
    def test_prints_package_version(self) -> None:
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert result.stdout.strip() == __version__


class TestDoctor:
    def test_exits_zero_when_every_service_is_healthy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "smart_agri.cli.run_health_checks",
            lambda **_: [
                CheckResult("postgres", healthy=True, detail="ok"),
                CheckResult("hdfs", healthy=True, detail="ok"),
            ],
        )
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "All 2 services healthy." in result.stdout

    def test_exits_non_zero_when_any_service_is_unhealthy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The non-zero exit is what makes an Airflow task fail loudly."""
        monkeypatch.setattr(
            "smart_agri.cli.run_health_checks",
            lambda **_: [
                CheckResult("postgres", healthy=True, detail="ok"),
                CheckResult("hdfs", healthy=False, detail="ConnectionError"),
            ],
        )
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 1
        assert "[FAIL] hdfs" in result.stdout

    def test_timeout_option_is_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, float] = {}

        def fake_run(**kwargs: float) -> list[CheckResult]:
            captured.update(kwargs)
            return [CheckResult("postgres", healthy=True, detail="ok")]

        monkeypatch.setattr("smart_agri.cli.run_health_checks", fake_run)
        assert runner.invoke(app, ["doctor", "--timeout", "2.5"]).exit_code == 0
        assert captured["timeout_s"] == 2.5


class TestCallback:
    def test_log_level_override_is_applied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: list[str] = []
        monkeypatch.setattr(
            "smart_agri.cli.configure_logging", lambda level: captured.append(level)
        )
        assert runner.invoke(app, ["--log-level", "DEBUG", "version"]).exit_code == 0
        assert captured == ["DEBUG"]

    def test_falls_back_to_configured_log_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SMART_AGRI_LOG_LEVEL", "WARNING")
        captured: list[str] = []
        monkeypatch.setattr(
            "smart_agri.cli.configure_logging", lambda level: captured.append(level)
        )
        assert runner.invoke(app, ["version"]).exit_code == 0
        assert captured == ["WARNING"]

    def test_no_arguments_lists_the_available_commands(self) -> None:
        """The image's default CMD is `--help`; this guards against the click/typer
        metavar incompatibility that made help rendering raise instead of print."""
        result = runner.invoke(app, [])
        assert result.exit_code == 2  # click's standard "no args, here's the help"
        assert "doctor" in result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)

    def test_help_renders_cleanly(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "doctor" in result.output
        assert "version" in result.output
