"""Tests for the service connectivity checks."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import pytest
import responses

from smart_agri.health import (
    CheckResult,
    ClickHouseCheck,
    HdfsCheck,
    HealthCheck,
    HiveMetastoreCheck,
    run_health_checks,
)

if TYPE_CHECKING:
    from smart_agri.config import Settings


class TestCheckResult:
    def test_renders_pass(self) -> None:
        assert str(CheckResult("hdfs", healthy=True, detail="ok")).startswith("[PASS] hdfs")

    def test_renders_fail(self) -> None:
        assert str(CheckResult("hdfs", healthy=False, detail="boom")).startswith("[FAIL] hdfs")


class TestHealthCheckBase:
    def test_probe_exception_becomes_a_failed_result(self, settings: Settings) -> None:
        class Exploding(HealthCheck):
            service = "exploding"

            def _probe(self) -> str:
                raise TimeoutError("took too long")

        result = Exploding(settings).run()
        assert result.healthy is False
        assert result.detail == "TimeoutError: took too long"

    def test_successful_probe_detail_is_preserved(self, settings: Settings) -> None:
        class Fine(HealthCheck):
            service = "fine"

            def _probe(self) -> str:
                return "all good"

        assert Fine(settings).run() == CheckResult("fine", healthy=True, detail="all good")


class TestClickHouseCheck:
    @responses.activate
    def test_reports_server_version(self, settings: Settings) -> None:
        responses.add(responses.GET, "http://clickhouse:8123/", body="24.8.4.13", status=200)
        result = ClickHouseCheck(settings).run()
        assert result.healthy is True
        assert "v24.8.4.13" in result.detail

    @responses.activate
    def test_http_error_fails_the_check(self, settings: Settings) -> None:
        responses.add(responses.GET, "http://clickhouse:8123/", status=503)
        assert ClickHouseCheck(settings).run().healthy is False


class TestHdfsCheck:
    @responses.activate
    def test_lists_lake_zones(self, settings: Settings) -> None:
        responses.add(
            responses.GET,
            "http://namenode:9870/webhdfs/v1/lake",
            json={
                "FileStatuses": {
                    "FileStatus": [
                        {"pathSuffix": "gold"},
                        {"pathSuffix": "bronze"},
                        {"pathSuffix": "silver"},
                        {"pathSuffix": "quarantine"},
                    ]
                }
            },
            status=200,
        )
        result = HdfsCheck(settings).run()
        assert result.healthy is True
        # Zones are sorted so the output is stable across NameNode listings.
        assert "bronze, gold, quarantine, silver" in result.detail

    @responses.activate
    def test_empty_lake_root_is_a_failure(self, settings: Settings) -> None:
        responses.add(
            responses.GET,
            "http://namenode:9870/webhdfs/v1/lake",
            json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        result = HdfsCheck(settings).run()
        assert result.healthy is False
        assert "make hdfs-init" in result.detail


class TestHiveMetastoreCheck:
    def test_open_port_passes(self, settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
        class DummySocket:
            def __enter__(self) -> DummySocket:
                return self

            def __exit__(self, *_: object) -> None:
                return None

        def accept(*_args: object, **_kwargs: object) -> DummySocket:
            return DummySocket()

        monkeypatch.setattr(socket, "create_connection", accept)
        assert HiveMetastoreCheck(settings).run().healthy is True

    def test_refused_connection_fails(
        self, settings: Settings, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def refuse(*_: object, **__: object) -> None:
            raise ConnectionRefusedError("connection refused")

        monkeypatch.setattr(socket, "create_connection", refuse)
        result = HiveMetastoreCheck(settings).run()
        assert result.healthy is False
        assert "ConnectionRefusedError" in result.detail


class TestRunHealthChecks:
    def test_runs_the_selected_checks_in_order(self, settings: Settings) -> None:
        class First(HealthCheck):
            service = "first"

            def _probe(self) -> str:
                return "1"

        class Second(HealthCheck):
            service = "second"

            def _probe(self) -> str:
                raise RuntimeError("down")

        results = run_health_checks(settings, checks=[First, Second])
        assert [r.service for r in results] == ["first", "second"]
        assert [r.healthy for r in results] == [True, False]

    def test_one_failure_does_not_prevent_later_checks(self, settings: Settings) -> None:
        """A single unreachable service must not mask the state of the others."""

        class Broken(HealthCheck):
            service = "broken"

            def _probe(self) -> str:
                raise OSError("no route to host")

        class Working(HealthCheck):
            service = "working"

            def _probe(self) -> str:
                return "fine"

        results = run_health_checks(settings, checks=[Broken, Working])
        assert len(results) == 2
        assert results[1].healthy is True
