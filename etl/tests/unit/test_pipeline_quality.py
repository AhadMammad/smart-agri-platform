"""Tests for the data quality gate inside `BasePipeline.run`.

Three promises are pinned here:

- an error-severity breach stops the run *before* the data it describes is
  written, and nothing downstream sees it;
- every run, failed or not, leaves its check results behind — a failed run
  that cannot say which check stopped it is undiagnosable;
- recording those results can never replace the real error with its own.
"""

from __future__ import annotations

from datetime import date
from typing import ClassVar

import pandera.polars as pa
import polars as pl
import pytest
from pandera.engines import polars_engine
from tests.fakes import FakeControlStore, FakeHdfsStore
from typer.testing import CliRunner

from smart_agri.config import DataQualitySettings, LakeZone, Settings
from smart_agri.io.control import RunStatus
from smart_agri.pipelines.base import BasePipeline, PipelineContext, RunContext
from smart_agri.quality import CheckResult, CheckType, DataQualityError, Outcome, Severity

LOGICAL_DATE = date(2026, 8, 1)

SCHEMA = pa.DataFrameSchema(
    {"x": pa.Column(polars_engine.Int64, pa.Check.in_range(0, 10))},
    strict=True,
    name="tiny",
)


class Guarded(BasePipeline):
    """Extracts `rows`, validates against `SCHEMA`, and records what it loads."""

    name = "test.guarded"
    dataset = "guarded"
    schema = SCHEMA
    max_rejection_rate = 0.5
    rows: ClassVar[list[int]] = [1, 2, 999]
    loaded: ClassVar[list[pl.DataFrame]] = []

    def extract(self, run: RunContext) -> pl.DataFrame:
        return pl.DataFrame({"x": self.rows})

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        self.loaded.append(frame)
        return frame.height


def _error(name: str) -> CheckResult:
    return CheckResult(name, CheckType.VOLUME, Severity.ERROR, Outcome.FAILED)


@pytest.fixture(autouse=True)
def _reset_loaded() -> None:
    Guarded.loaded = []


@pytest.fixture
def control() -> FakeControlStore:
    return FakeControlStore()


@pytest.fixture
def hdfs() -> FakeHdfsStore:
    return FakeHdfsStore()


def _context(
    control: FakeControlStore, hdfs: FakeHdfsStore, *, enforce: bool = True
) -> PipelineContext:
    return PipelineContext(
        settings=Settings(dq=DataQualitySettings(enforce=enforce)),
        hdfs=hdfs,  # type: ignore[arg-type]
        control=control,  # type: ignore[arg-type]
    )


@pytest.fixture
def context(control: FakeControlStore, hdfs: FakeHdfsStore) -> PipelineContext:
    return _context(control, hdfs)


def _quarantine(hdfs: FakeHdfsStore) -> list[str]:
    root = hdfs.zone_path(LakeZone.QUARANTINE, "guarded")
    return [path for path in hdfs.files if path.startswith(root)]


class TestSchemaResults:
    def test_a_clean_run_records_a_passing_rejection_rate(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        """Every validated run leaves at least one result, so a quiet day is
        distinguishable from a run that never checked anything."""

        class Clean(Guarded):
            rows: ClassVar[list[int]] = [1, 2, 3]

        Clean(context).run(LOGICAL_DATE)

        (check,) = control.checks
        assert check["check_name"] == "rejection_rate"
        assert check["outcome"] == "passed"
        assert check["run_id"] == control.last_run["run_id"]

    def test_quarantined_rows_are_recorded_per_check(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        stats = Guarded(context).run(LOGICAL_DATE)

        (schema_check,) = control.checks_named("schema.x.in_range(0, 10)")
        assert schema_check["outcome"] == "warned"
        assert (schema_check["rows_failed"], schema_check["rows_evaluated"]) == (1, 3)
        assert control.checks_named("rejection_rate")[0]["outcome"] == "passed"
        assert stats.rows_quarantined == 1
        assert control.last_run["status"] is RunStatus.SUCCESS

    def test_a_pipeline_without_a_schema_records_nothing(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        class Unchecked(Guarded):
            schema = None

        Unchecked(context).run(LOGICAL_DATE)
        assert control.checks == []


class TestRejectionGate:
    def test_a_breach_fails_the_run_before_anything_is_written(
        self, context: PipelineContext, control: FakeControlStore, hdfs: FakeHdfsStore
    ) -> None:
        class Broken(Guarded):
            rows: ClassVar[list[int]] = [1, 997, 998, 999]

        with pytest.raises(DataQualityError):
            Broken(context).run(LOGICAL_DATE)

        assert Guarded.loaded == []
        assert _quarantine(hdfs) == [], "a rejected frame is not quarantined piecemeal"
        assert control.last_run["status"] is RunStatus.FAILED
        assert control.last_run["stats"].rows_read == 4
        assert control.checks_named("rejection_rate")[0]["outcome"] == "failed"

    def test_disabled_enforcement_quarantines_and_continues(
        self, control: FakeControlStore, hdfs: FakeHdfsStore
    ) -> None:
        """The backfill escape hatch: the breach is kept, as a warning."""

        class Broken(Guarded):
            rows: ClassVar[list[int]] = [1, 997, 998, 999]

        stats = Broken(_context(control, hdfs, enforce=False)).run(LOGICAL_DATE)

        assert stats.rows_written == 1
        assert _quarantine(hdfs) != []
        gate = control.checks_named("rejection_rate")[0]
        assert gate["outcome"] == "warned"
        assert control.last_run["status"] is RunStatus.SUCCESS


class TestLoadGates:
    def test_a_pre_load_error_means_nothing_is_written(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        class VolumeDrop(Guarded):
            def _pre_load_checks(self, frame: pl.DataFrame, run: RunContext) -> None:
                self.checks.record(_error("volume.rows_written"))

        with pytest.raises(DataQualityError, match="volume.rows_written"):
            VolumeDrop(context).run(LOGICAL_DATE)

        assert Guarded.loaded == []
        assert control.last_run["status"] is RunStatus.FAILED
        assert [c["outcome"] for c in control.checks_named("volume.rows_written")] == ["failed"]

    def test_a_post_load_error_fails_the_run_after_writing(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        """The data is written but the run fails, so downstream never starts."""

        class ShortLoad(Guarded):
            def _post_load_checks(self, written: int, run: RunContext) -> None:
                self.checks.record(_error("reconciliation.row_count"))

        with pytest.raises(DataQualityError):
            ShortLoad(context).run(LOGICAL_DATE)

        assert len(Guarded.loaded) == 1
        assert control.last_run["status"] is RunStatus.FAILED
        assert control.last_run["stats"].rows_written == 2

    def test_a_warn_breach_never_stops_the_run(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        class Noisy(Guarded):
            def _post_load_checks(self, written: int, run: RunContext) -> None:
                self.checks.record(
                    CheckResult("volume.spike", CheckType.VOLUME, Severity.WARN, Outcome.WARNED)
                )

        Noisy(context).run(LOGICAL_DATE)
        assert control.last_run["status"] is RunStatus.SUCCESS


class TestResultsOnFailure:
    def test_a_crash_still_records_the_checks_that_ran(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        class CrashOnLoad(Guarded):
            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                msg = "hdfs unavailable"
                raise RuntimeError(msg)

        with pytest.raises(RuntimeError, match="hdfs unavailable"):
            CrashOnLoad(context).run(LOGICAL_DATE)

        assert {c["check_name"] for c in control.checks} == {
            "schema.x.in_range(0, 10)",
            "rejection_rate",
        }
        assert control.last_run["error"] == "hdfs unavailable"

    def test_failing_to_record_results_does_not_mask_the_real_error(
        self, hdfs: FakeHdfsStore
    ) -> None:
        """If etl-meta-db is what broke, the operator needs the pipeline's error."""

        class UnwritableResults(FakeControlStore):
            def record_checks(self, *_: object) -> None:
                msg = "etl-meta-db down"
                raise ConnectionError(msg)

        control = UnwritableResults()

        class CrashOnLoad(Guarded):
            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                msg = "hdfs unavailable"
                raise RuntimeError(msg)

        with pytest.raises(RuntimeError, match="hdfs unavailable"):
            CrashOnLoad(_context(control, hdfs)).run(LOGICAL_DATE)

        assert control.last_run["status"] is RunStatus.FAILED

    def test_a_failure_to_record_results_on_success_fails_the_run(
        self, hdfs: FakeHdfsStore
    ) -> None:
        """Losing every result silently would leave a green run with no history."""

        class UnwritableResults(FakeControlStore):
            def record_checks(self, *_: object) -> None:
                msg = "etl-meta-db down"
                raise ConnectionError(msg)

        control = UnwritableResults()

        with pytest.raises(ConnectionError):
            Guarded(_context(control, hdfs)).run(LOGICAL_DATE)

        assert control.last_run["status"] is RunStatus.FAILED

    def test_each_run_starts_with_a_fresh_collector(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        pipeline = Guarded(context)
        pipeline.run(LOGICAL_DATE)
        pipeline.run(LOGICAL_DATE)

        first, second = (run["run_id"] for run in control.runs)
        assert len([c for c in control.checks if c["run_id"] == first]) == 2
        assert len([c for c in control.checks if c["run_id"] == second]) == 2


class TestCliExitCode:
    def test_a_data_quality_stop_exits_with_code_3(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Distinct from a crash (1), so an operator can tell them apart."""
        from smart_agri.cli import DQ_FAILURE_EXIT_CODE, app

        class Stopped:
            def run(self, _: date) -> None:
                raise DataQualityError("silver.fact_irrigation", [_error("join.dim_field")])

        monkeypatch.setattr("smart_agri.pipelines.get_pipeline", lambda _name: Stopped())
        result = CliRunner().invoke(app, ["run", "silver.fact_irrigation", "--date", "2026-08-01"])

        assert result.exit_code == DQ_FAILURE_EXIT_CODE == 3
        assert "stopped by data quality checks" in result.stderr
        assert "[failed] join.dim_field" in result.stderr
