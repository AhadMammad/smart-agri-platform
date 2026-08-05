"""Tests for the Phase 2 CLI commands.

These are the exact invocations Airflow's DockerOperator makes, so a broken
argument here breaks every DAG task.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from typer.testing import CliRunner

from smart_agri.cli import app
from smart_agri.io.control import RunStats

if TYPE_CHECKING:
    from collections.abc import Sequence

runner = CliRunner()


class _RecordingPipeline:
    """Stands in for a real pipeline so no backing service is touched."""

    def __init__(self, name: str, log: list[tuple[str, date]]) -> None:
        self.name = name
        self._log = log

    def run(self, logical_date: date) -> RunStats:
        self._log.append((self.name, logical_date))
        return RunStats(rows_read=10, rows_written=9, rows_quarantined=1)


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, date]]:
    """Capture every pipeline the CLI would execute."""
    log: list[tuple[str, date]] = []

    def fake_get_pipeline(name: str, context: Any = None) -> _RecordingPipeline:
        del context
        return _RecordingPipeline(name, log)

    monkeypatch.setattr("smart_agri.pipelines.get_pipeline", fake_get_pipeline)
    return log


class TestListPipelines:
    def test_lists_every_stage(self) -> None:
        result = runner.invoke(app, ["list-pipelines"])
        assert result.exit_code == 0
        for expected in (
            "bronze.farm",
            "silver.dim_farm",
            "gold.field_soil_daily",
            "load.dim_farm",
        ):
            assert expected in result.output

    def test_stages_are_numbered_in_execution_order(self) -> None:
        output = runner.invoke(app, ["list-pipelines"]).output
        assert output.index("stage 1") < output.index("stage 4")
        assert output.index("bronze.farm") < output.index("load.dim_farm")


class TestRun:
    def test_runs_the_named_pipeline_on_the_given_date(self, runs: list[tuple[str, date]]) -> None:
        result = runner.invoke(app, ["run", "silver.dim_farm", "--date", "2026-08-01"])
        assert result.exit_code == 0
        assert runs == [("silver.dim_farm", date(2026, 8, 1))]

    def test_reports_row_counts(self, runs: list[tuple[str, date]]) -> None:
        result = runner.invoke(app, ["run", "bronze.farm", "--date", "2026-08-01"])
        assert "read=10 written=9 quarantined=1" in result.output

    def test_unknown_pipeline_exits_with_a_usage_error(self) -> None:
        """Exit code 2, not a stack trace: a DAG typo should say what was expected."""
        result = runner.invoke(app, ["run", "silver.nonexistent"])
        assert result.exit_code == 2
        assert "unknown pipeline" in result.output

    def test_malformed_date_is_rejected(self) -> None:
        result = runner.invoke(app, ["run", "bronze.farm", "--date", "01-08-2026"])
        assert result.exit_code == 2
        assert "expected YYYY-MM-DD" in result.output

    def test_date_defaults_to_today(self, runs: list[tuple[str, date]]) -> None:
        assert runner.invoke(app, ["run", "bronze.farm"]).exit_code == 0
        assert runs[0][1] is not None


class TestRunAll:
    def test_executes_every_pipeline_in_stage_order(self, runs: list[tuple[str, date]]) -> None:
        from smart_agri.pipelines import SOIL_SENSOR_STAGES

        result = runner.invoke(app, ["run-all", "--date", "2026-08-01"])
        assert result.exit_code == 0

        expected = [name for stage in SOIL_SENSOR_STAGES for name in stage]
        assert [name for name, _ in runs] == expected

    def test_all_stages_share_one_logical_date(self, runs: list[tuple[str, date]]) -> None:
        runner.invoke(app, ["run-all", "--date", "2026-08-01"])
        assert {logical for _, logical in runs} == {date(2026, 8, 1)}


class TestSeed:
    def test_unknown_profile_exits_with_a_usage_error(self) -> None:
        result = runner.invoke(app, ["seed", "--profile", "enormous"])
        assert result.exit_code == 2
        assert "unknown generator profile" in result.output

    def test_reports_the_planned_volume_before_writing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `large` seed takes minutes; the operator should see the size first."""
        seeded: list[bool] = []

        class FakeSeeder:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del args, kwargs

            def seed(self, *, truncate: bool = True) -> Any:
                from smart_agri.generator.seeder import SeedResult

                seeded.append(truncate)
                return SeedResult({"farm": 5, "field": 20, "sensor_reading": 28_272})

        monkeypatch.setattr("smart_agri.generator.seeder.DatasetSeeder", FakeSeeder)
        result = runner.invoke(app, ["seed", "--profile", "small"])

        assert result.exit_code == 0
        assert "profile=small" in result.output
        assert "sensor readings)" in result.output
        assert "farm" in result.output
        assert "28,272" in result.output
        assert seeded == [True]

    def test_keep_appends_instead_of_truncating(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seeded: list[bool] = []

        class FakeSeeder:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del args, kwargs

            def seed(self, *, truncate: bool = True) -> Any:
                from smart_agri.generator.seeder import SeedResult

                seeded.append(truncate)
                return SeedResult({"farm": 1})

        monkeypatch.setattr("smart_agri.generator.seeder.DatasetSeeder", FakeSeeder)
        runner.invoke(app, ["seed", "--keep"])
        assert seeded == [False]


class TestInitClickHouse:
    def test_missing_directory_exits_with_a_usage_error(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["init-clickhouse", "--ddl-dir", str(tmp_path / "absent")])
        assert result.exit_code == 2
        assert "ddl directory not found" in result.output

    def test_directory_without_sql_files_exits_with_a_usage_error(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["init-clickhouse", "--ddl-dir", str(tmp_path)])
        assert result.exit_code == 2
        assert "no .sql files" in result.output

    def test_applies_every_file_in_name_order(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "b").mkdir()
        (tmp_path / "020_second.sql").write_text("CREATE TABLE b (x Int64);")
        (tmp_path / "b" / "010_first.sql").write_text("CREATE TABLE a (x Int64);")

        applied: list[str] = []

        class FakeSink:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del args, kwargs

            def __enter__(self) -> FakeSink:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def execute_script(self, script: str) -> int:
                applied.append(script.strip())
                return 1

        monkeypatch.setattr("smart_agri.io.ClickHouseSink", FakeSink)
        result = runner.invoke(app, ["init-clickhouse", "--ddl-dir", str(tmp_path)])

        assert result.exit_code == 0
        assert len(applied) == 2
        assert "applied 2 file(s)" in result.output


class TestRealDdlFiles:
    """The DDL the CLI will actually apply must at least parse into statements."""

    @pytest.fixture(scope="class")
    def ddl_files(self) -> Sequence[Path]:
        root = Path(__file__).resolve().parents[3] / "clickhouse" / "ddl"
        if not root.is_dir():
            pytest.skip("clickhouse/ddl not present")
        return sorted(root.rglob("*.sql"))

    def test_ddl_files_exist(self, ddl_files: Sequence[Path]) -> None:
        assert ddl_files

    def test_every_statement_is_idempotent(self, ddl_files: Sequence[Path]) -> None:
        """`init-clickhouse` runs on every deploy, so nothing may fail on a
        second application."""
        for path in ddl_files:
            for statement in path.read_text().split(";"):
                head = statement.strip().upper()
                if head.startswith("CREATE"):
                    assert "IF NOT EXISTS" in head, f"{path.name}: {head[:60]}"
