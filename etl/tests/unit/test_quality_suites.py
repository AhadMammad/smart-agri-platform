"""Tests for the data quality suites that run outside the pipelines.

Freshness exists for the failure no pipeline can report: not running at all.
Reconciliation exists for the one no single stage can see: the warehouse
drifting from the source. Both are pinned against the in-memory fakes, with the
real spec-derived pairings checked separately so a new table cannot slip past.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest
from tests.fakes import FakeClickHouseSink, FakeControlStore, FakePostgresSource
from typer.testing import CliRunner

from smart_agri.io.control import RunStats, RunStatus
from smart_agri.pipelines import DOMAIN_STAGES
from smart_agri.pipelines.base import PipelineContext
from smart_agri.pipelines.load import FACT_LOAD_SPECS
from smart_agri.quality import DataQualityError, Outcome
from smart_agri.quality import suites as suites_module
from smart_agri.quality.suites import (
    DEFAULT_MAX_AGE,
    SourcePair,
    check_freshness,
    domain_quality_suites,
    dq_suites,
    freshness_specs,
    reconcile_domain,
    run_suite,
    source_pairs,
    summarise,
)

LOGICAL_DATE = date(2026, 8, 1)
NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
WATERMARK = datetime(2026, 8, 1, tzinfo=UTC)


@pytest.fixture
def control() -> FakeControlStore:
    return FakeControlStore()


@pytest.fixture
def postgres() -> FakePostgresSource:
    return FakePostgresSource()


@pytest.fixture
def clickhouse() -> FakeClickHouseSink:
    return FakeClickHouseSink()


@pytest.fixture
def context(
    control: FakeControlStore, postgres: FakePostgresSource, clickhouse: FakeClickHouseSink
) -> PipelineContext:
    return PipelineContext(
        control=control,  # type: ignore[arg-type]
        postgres=postgres,  # type: ignore[arg-type]
        clickhouse=clickhouse,  # type: ignore[arg-type]
    )


def _succeed(control: FakeControlStore, pipeline: str, at: datetime) -> None:
    control.now = at
    run_id = control.start_run(pipeline, LOGICAL_DATE)
    control.finish_run(run_id, RunStatus.SUCCESS, RunStats())


# --- freshness ---------------------------------------------------------------
class TestFreshnessSpecs:
    def test_every_final_stage_pipeline_has_an_sla(self) -> None:
        """The final stage only succeeds once the whole domain has."""
        covered = {spec.pipeline for spec in freshness_specs()}
        expected = {name for stages in DOMAIN_STAGES.values() for name in stages[-1]}
        assert covered == expected

    def test_daily_domains_allow_a_day_and_two_hours(self) -> None:
        assert timedelta(hours=26) == DEFAULT_MAX_AGE


class TestCheckFreshness:
    @pytest.fixture
    def only_one_sla(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            suites_module,
            "freshness_specs",
            lambda: (suites_module.FreshnessSpec("load.fact_harvest", "analytics"),),
        )

    @pytest.mark.usefixtures("only_one_sla")
    def test_a_recent_success_is_fresh(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        _succeed(control, "load.fact_harvest", NOW - timedelta(hours=3))

        (result,) = check_freshness(context, now=NOW)

        assert result.outcome is Outcome.PASSED
        assert result.observed_value == 3.0
        assert result.threshold == 26.0

    @pytest.mark.usefixtures("only_one_sla")
    def test_a_stale_pipeline_warns(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        """A paused DAG: every run that did happen succeeded, and still nothing is new."""
        _succeed(control, "load.fact_harvest", NOW - timedelta(hours=30))
        assert check_freshness(context, now=NOW)[0].outcome is Outcome.WARNED

    @pytest.mark.usefixtures("only_one_sla")
    def test_only_successes_count(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        """A DAG failing every hour is running, and is still stale."""
        _succeed(control, "load.fact_harvest", NOW - timedelta(hours=40))
        control.now = NOW - timedelta(minutes=5)
        failed = control.start_run("load.fact_harvest", LOGICAL_DATE)
        control.finish_run(failed, RunStatus.FAILED, error="boom")

        assert check_freshness(context, now=NOW)[0].outcome is Outcome.WARNED

    @pytest.mark.usefixtures("only_one_sla")
    def test_a_pipeline_that_never_succeeded_is_flagged(self, context: PipelineContext) -> None:
        (result,) = check_freshness(context, now=NOW)

        assert result.outcome is Outcome.WARNED
        assert result.observed_value is None
        assert result.details["reason"] == "never succeeded"


# --- reconciliation ----------------------------------------------------------
class TestSourcePairs:
    def test_every_fact_load_is_reconciled_somewhere(self) -> None:
        """A new fact table is reconciled the moment it is loaded."""
        paired = {pair.warehouse_table for d in DOMAIN_STAGES for pair in source_pairs(d)}
        assert {spec.table for spec in FACT_LOAD_SPECS} <= paired

    def test_incremental_facts_are_bounded_by_their_watermark(self) -> None:
        pairs = {p.warehouse_table: p for p in source_pairs("soil_sensor")}

        reading = pairs["fact_sensor_reading"]
        assert reading.source_table == "agri.sensor_reading"
        assert reading.watermark_pipeline == "bronze.sensor_reading"
        assert reading.watermark_column == "updated_at"
        assert pairs["dim_farm"].watermark_pipeline is None

    def test_loads_with_no_postgres_origin_are_skipped(self) -> None:
        """Weather comes from an API, the marts from Gold: nothing to compare."""
        assert source_pairs("weather") == ()
        tables = {p.warehouse_table for p in source_pairs("analytics")}
        assert not {t for t in tables if t.startswith("agg_")}


HARVEST = SourcePair(
    load_pipeline="load.fact_harvest",
    warehouse_table="fact_harvest",
    source_table="agri.harvest",
    watermark_pipeline="bronze.harvest",
    watermark_column="updated_at",
)
FARM = SourcePair(
    load_pipeline="load.dim_farm", warehouse_table="dim_farm", source_table="agri.farm"
)


def _rows(n: int, *, after_watermark: int = 0) -> pl.DataFrame:
    stamps = [WATERMARK - timedelta(hours=1)] * n + [
        WATERMARK + timedelta(hours=1)
    ] * after_watermark
    return pl.DataFrame({"id": list(range(len(stamps))), "updated_at": stamps})


class TestReconcileDomain:
    @pytest.fixture(autouse=True)
    def _pairs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(suites_module, "source_pairs", lambda _domain: (HARVEST, FARM))

    @pytest.fixture
    def stocked(
        self,
        control: FakeControlStore,
        postgres: FakePostgresSource,
        clickhouse: FakeClickHouseSink,
    ) -> None:
        control.watermarks["bronze.harvest"] = WATERMARK
        postgres.tables["agri.harvest"] = _rows(100)
        postgres.tables["agri.farm"] = _rows(5)
        clickhouse.replacing_keys["fact_harvest"] = ["id"]
        clickhouse.tables["fact_harvest"] = _rows(100)
        clickhouse.tables["dim_farm"] = _rows(5)

    def _outcomes(self, context: PipelineContext) -> dict[str, Outcome]:
        return {r.check_name: r.outcome for r in reconcile_domain("analytics", context)}

    @pytest.mark.usefixtures("stocked")
    def test_a_matching_warehouse_passes_both_checks(self, context: PipelineContext) -> None:
        assert set(self._outcomes(context).values()) == {Outcome.PASSED}
        assert len(self._outcomes(context)) == 4

    @pytest.mark.usefixtures("stocked")
    def test_rows_written_after_the_watermark_are_not_missing(
        self, context: PipelineContext, postgres: FakePostgresSource
    ) -> None:
        """They simply have not been extracted yet."""
        postgres.tables["agri.harvest"] = _rows(100, after_watermark=40)
        assert self._outcomes(context)["reconcile.fact_harvest.completeness"] is Outcome.PASSED

    @pytest.mark.usefixtures("stocked")
    def test_more_rows_than_the_source_is_a_duplication_error(
        self, context: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        clickhouse.tables["dim_farm"] = _rows(6)
        assert self._outcomes(context)["reconcile.dim_farm.no_duplicates"] is Outcome.FAILED

    @pytest.mark.usefixtures("stocked")
    def test_the_merged_count_is_what_is_compared(
        self, context: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        """Duplicates a ReplacingMergeTree will collapse are not real rows."""
        clickhouse.tables["fact_harvest"] = pl.concat([_rows(100), _rows(100)])
        assert self._outcomes(context)["reconcile.fact_harvest.no_duplicates"] is Outcome.PASSED

    @pytest.mark.usefixtures("stocked")
    def test_a_shortfall_beyond_the_tolerance_warns(
        self, context: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        clickhouse.tables["fact_harvest"] = _rows(97)
        results = {r.check_name: r for r in reconcile_domain("analytics", context)}

        completeness = results["reconcile.fact_harvest.completeness"]
        assert completeness.outcome is Outcome.WARNED
        assert completeness.observed_value == pytest.approx(0.03)

    @pytest.mark.usefixtures("stocked")
    def test_a_shortfall_within_the_tolerance_passes(
        self, context: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        """Silver drops orphans and filtered rows by design."""
        clickhouse.tables["fact_harvest"] = _rows(98)
        assert self._outcomes(context)["reconcile.fact_harvest.completeness"] is Outcome.PASSED

    @pytest.mark.usefixtures("stocked")
    def test_a_source_never_extracted_is_skipped(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        del control.watermarks["bronze.harvest"]
        assert self._outcomes(context)["reconcile.fact_harvest.completeness"] is Outcome.SKIPPED


# --- running a suite ---------------------------------------------------------
class TestSuiteRegistry:
    def test_freshness_and_each_reconciled_domain_are_suites(self) -> None:
        assert set(dq_suites()) == {
            "freshness",
            "export",
            "reconcile.soil_sensor",
            "reconcile.analytics",
        }

    def test_domain_suites_name_registered_suites(self) -> None:
        for names in domain_quality_suites().values():
            assert set(names) <= set(dq_suites())


class TestRunSuite:
    @pytest.fixture(autouse=True)
    def _one_stale_sla(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            suites_module,
            "freshness_specs",
            lambda: (suites_module.FreshnessSpec("load.fact_harvest", "analytics"),),
        )

    def test_a_suite_is_its_own_run_with_its_results_attached(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        results = run_suite("freshness", LOGICAL_DATE, context)

        assert control.last_run["pipeline"] == "dq.freshness"
        assert control.last_run["status"] is RunStatus.SUCCESS
        assert [c["run_id"] for c in control.checks] == [control.last_run["run_id"]]
        assert results[0].outcome is Outcome.WARNED

    def test_an_error_check_fails_the_suite_after_recording_it(
        self, context: PipelineContext, control: FakeControlStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from smart_agri.quality import Severity

        monkeypatch.setattr(
            suites_module,
            "freshness_specs",
            lambda: (
                suites_module.FreshnessSpec(
                    "load.fact_harvest", "analytics", severity=Severity.ERROR
                ),
            ),
        )

        with pytest.raises(DataQualityError):
            run_suite("freshness", LOGICAL_DATE, context)

        assert control.last_run["status"] is RunStatus.FAILED
        assert control.checks[0]["outcome"] == "failed"

    def test_an_unknown_suite_names_the_valid_ones(self, context: PipelineContext) -> None:
        with pytest.raises(KeyError, match="valid suites: export, freshness"):
            run_suite("nope", LOGICAL_DATE, context)

    def test_the_summary_counts_each_outcome(self, context: PipelineContext) -> None:
        assert summarise(run_suite("freshness", LOGICAL_DATE, context)) == "warned=1"


class TestDqCli:
    def test_list_names_every_suite(self) -> None:
        from smart_agri.cli import app

        result = CliRunner().invoke(app, ["dq", "list"])
        assert result.exit_code == 0
        assert result.stdout.split() == sorted(dq_suites())

    def test_an_unknown_suite_exits_2(self) -> None:
        from smart_agri.cli import app

        result = CliRunner().invoke(app, ["dq", "run", "nope", "--date", "2026-08-01"])
        assert result.exit_code == 2
        assert "unknown data quality suite" in result.stderr
