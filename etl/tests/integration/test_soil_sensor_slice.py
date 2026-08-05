"""End-to-end integration test for the soil-sensor slice.

Requires the core stack and a migrated, seeded database:

    make up-core && make migrate && make init-clickhouse && make seed
    make test-integration

Runs on the platform network (see the `test` stage in docker/etl/Dockerfile),
because a WebHDFS write is redirected to `datanode:9864` — a hostname that
resolves nowhere else.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import pytest

from smart_agri.config import LakeZone, Settings
from smart_agri.io import ClickHouseSink, ControlStore, HdfsStore, PostgresSource
from smart_agri.pipelines import SOIL_SENSOR_STAGES, PipelineContext, get_pipeline

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

LOGICAL_DATE = date(2026, 8, 1)


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def context(settings: Settings) -> PipelineContext:
    return PipelineContext(settings=settings)


@pytest.fixture(scope="module")
def seeded(settings: Settings) -> None:
    """Fail early and clearly if the source database has no data."""
    postgres = PostgresSource(settings.postgres)
    count = postgres.scalar("SELECT count(*) FROM agri.sensor_reading")
    if not count:
        pytest.fail("agri.sensor_reading is empty — run `make migrate && make seed` first")


@pytest.fixture(scope="module")
def slice_run(context: PipelineContext, seeded: None, settings: Settings) -> Iterator[None]:
    """Run the whole slice once, then clean up the lake paths it created."""
    del seeded

    # A prior run's watermark would make Bronze extract nothing and leave the
    # rest of the test asserting against stale data.
    ControlStore(settings.postgres).reset_watermark("bronze.sensor_reading")

    for stage in SOIL_SENSOR_STAGES:
        for name in stage:
            get_pipeline(name, context).run(LOGICAL_DATE)

    yield


class TestSourceExtraction:
    def test_bronze_snapshots_land_in_the_lake(self, slice_run: None, settings: Settings) -> None:
        del slice_run
        store = HdfsStore(settings.hdfs)
        for dataset in ("farm", "field", "sensor"):
            path = store.zone_path(
                LakeZone.BRONZE, dataset, f"snapshot_date={LOGICAL_DATE.isoformat()}"
            )
            assert store.exists(path), f"missing bronze snapshot for {dataset}"
            assert store.read_parquet_dir(path).height > 0

    def test_bronze_readings_land_and_advance_the_watermark(
        self, slice_run: None, settings: Settings
    ) -> None:
        del slice_run
        store = HdfsStore(settings.hdfs)
        path = store.zone_path(
            LakeZone.BRONZE, "sensor_reading", f"ingest_date={LOGICAL_DATE.isoformat()}"
        )
        assert store.read_parquet_dir(path).height > 0

        watermark = ControlStore(settings.postgres).get_watermark("bronze.sensor_reading")
        assert watermark is not None
        assert watermark > datetime(2000, 1, 1, tzinfo=UTC)


class TestLakeZones:
    def test_silver_dimensions_are_written(self, slice_run: None, settings: Settings) -> None:
        del slice_run
        store = HdfsStore(settings.hdfs)
        for dataset in ("dim_farm", "dim_field", "dim_sensor"):
            path = store.zone_path(
                LakeZone.SILVER, dataset, f"snapshot_date={LOGICAL_DATE.isoformat()}"
            )
            assert store.read_parquet_dir(path).height > 0, dataset

    def test_silver_fact_excludes_decommissioned_sensors(
        self, slice_run: None, settings: Settings
    ) -> None:
        del slice_run
        store = HdfsStore(settings.hdfs)
        facts = store.read_parquet_dir(store.zone_path(LakeZone.SILVER, "fact_sensor_reading"))
        sensors = store.read_parquet_dir(
            store.zone_path(
                LakeZone.SILVER, "dim_sensor", f"snapshot_date={LOGICAL_DATE.isoformat()}"
            )
        )
        dead = set(sensors.filter(sensors["status"] == "decommissioned")["sensor_id"].to_list())
        assert not set(facts["sensor_id"].to_list()) & dead

    def test_gold_aggregate_is_written(self, slice_run: None, settings: Settings) -> None:
        del slice_run
        store = HdfsStore(settings.hdfs)
        gold = store.read_parquet_dir(
            store.zone_path(
                LakeZone.GOLD, "field_soil_daily", f"run_date={LOGICAL_DATE.isoformat()}"
            )
        )
        assert gold.height > 0
        assert set(gold["moisture_stress"].to_list()) <= {"dry", "optimal", "wet", "unknown"}


class TestWarehouse:
    @pytest.fixture
    def sink(self, settings: Settings) -> Iterator[ClickHouseSink]:
        with ClickHouseSink(settings.clickhouse) as instance:
            yield instance

    @pytest.mark.parametrize(
        "table",
        ["dim_farm", "dim_field", "dim_sensor", "fact_sensor_reading", "agg_field_soil_daily"],
    )
    def test_every_table_is_populated(
        self, slice_run: None, sink: ClickHouseSink, table: str
    ) -> None:
        del slice_run
        assert sink.table_exists(table), f"{table} missing — run `make init-clickhouse`"
        assert sink.count(table) > 0, f"{table} is empty"

    def test_the_dashboard_query_returns_rows(self, slice_run: None, sink: ClickHouseSink) -> None:
        """The exact shape the Superset moisture-trend chart asks for."""
        del slice_run
        result = sink.query(
            """
            SELECT reading_date, region, avg(avg_soil_moisture_pct) AS moisture
            FROM agg_field_soil_daily
            GROUP BY reading_date, region
            ORDER BY reading_date, region
            """
        )
        assert result.height > 0
        assert result["moisture"].drop_nulls().min() >= 0

    def test_facts_reference_real_dimensions(self, slice_run: None, sink: ClickHouseSink) -> None:
        """A broken key would show as silently missing rows on every chart."""
        del slice_run
        orphans = sink.scalar(
            """
            SELECT count()
            FROM fact_sensor_reading AS f
            LEFT JOIN dim_field AS d ON f.field_id = d.field_id
            WHERE d.field_id = 0
            """
        )
        assert orphans == 0

    def test_materialized_view_populates_the_weekly_rollup(
        self, slice_run: None, sink: ClickHouseSink
    ) -> None:
        del slice_run
        assert sink.scalar("SELECT count() FROM v_field_soil_weekly") > 0

    def test_latest_condition_view_has_one_row_per_field(
        self, slice_run: None, sink: ClickHouseSink
    ) -> None:
        total = sink.scalar("SELECT count() FROM v_field_latest_condition")
        distinct = sink.scalar("SELECT uniqExact(field_id) FROM v_field_latest_condition")
        assert total == distinct


class TestIdempotency:
    def test_rerunning_the_slice_does_not_change_the_warehouse(
        self, slice_run: None, context: PipelineContext, settings: Settings
    ) -> None:
        """The property that makes a retried Airflow task safe.

        Notably this also guards the bug where an empty incremental batch
        overwrote a populated Bronze partition and emptied everything
        downstream.
        """
        del slice_run

        with ClickHouseSink(settings.clickhouse) as sink:
            before = {
                table: sink.count(table)
                for table in ("dim_farm", "fact_sensor_reading", "agg_field_soil_daily")
            }

        for stage in SOIL_SENSOR_STAGES:
            for name in stage:
                get_pipeline(name, context).run(LOGICAL_DATE)

        with ClickHouseSink(settings.clickhouse) as sink:
            after = {table: sink.count(table) for table in before}

        assert after == before


class TestRunLog:
    def test_every_pipeline_recorded_a_successful_run(
        self, slice_run: None, settings: Settings
    ) -> None:
        del slice_run
        postgres = PostgresSource(settings.postgres)
        rows = postgres.read_query(
            """
            SELECT DISTINCT ON (pipeline) pipeline, status
            FROM etl_control.run_log
            WHERE logical_date = DATE '2026-08-01'
            ORDER BY pipeline, started_at DESC
            """
        )
        recorded = dict(zip(rows["pipeline"].to_list(), rows["status"].to_list(), strict=True))
        expected = {name for stage in SOIL_SENSOR_STAGES for name in stage}

        assert expected <= set(recorded)
        assert all(recorded[name] == "success" for name in expected)
