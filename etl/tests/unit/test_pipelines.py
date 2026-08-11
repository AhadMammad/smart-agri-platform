"""Tests for the pipeline framework and the soil-sensor stages.

Everything runs against the in-memory fakes in `tests.fakes`, so real Polars
frames flow through the real transformation code without a stack behind it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import polars as pl
import pytest
from tests.fakes import (
    FakeClickHouseSink,
    FakeControlStore,
    FakeHdfsStore,
    FakePostgresSource,
)

from smart_agri.config import LakeZone
from smart_agri.io.control import RunStatus
from smart_agri.pipelines import get_pipeline
from smart_agri.pipelines.base import BasePipeline, PipelineContext, RunContext
from smart_agri.pipelines.bronze import (
    DATA_FILE,
    INGEST_PARTITION_KEY,
    SNAPSHOT_PARTITION_KEY,
)
from smart_agri.pipelines.gold import DRY_THRESHOLD_PCT, RUN_PARTITION_KEY, WET_THRESHOLD_PCT

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGICAL_DATE = date(2026, 8, 1)
PARTITION = LOGICAL_DATE.isoformat()


# --- source fixtures ---------------------------------------------------------
def _farm_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "farm_id": [1, 2],
            "farm_code": ["EG-001", "NG-001"],
            "name": ["Nile Delta Farm 1", " Kano Farm 1 "],
            "country_code": ["EG", "ng"],
            "region": ["Nile Delta", "Kano Plains"],
            "latitude": [30.9, 11.7],
            "longitude": [31.1, 8.6],
            "area_ha": [120.0, 250.0],
        }
    )


def _field_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "field_id": [10, 11, 12],
            "field_code": ["EG-001-F01", "EG-001-F02", "NG-001-F01"],
            "farm_id": [1, 1, 2],
            "name": ["North Block 1", "South Block 2", "River Block 1"],
            "area_ha": [50.0, 55.0, 100.0],
            "soil_type": ["CLAY", "clay_loam", "sandy"],
            "latitude": [30.902, 30.898, 11.703],
            "longitude": [31.103, 31.098, 8.604],
        }
    )


def _sensor_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sensor_id": [100, 101, 102],
            "sensor_code": ["EG-001-F01-S01", "EG-001-F02-S01", "NG-001-F01-S01"],
            "field_id": [10, 11, 12],
            "sensor_type": ["soil_probe"] * 3,
            "model": ["TerraProbe TP-200"] * 3,
            "depth_cm": [20, 30, 45],
            "installed_on": [date(2025, 5, 1)] * 3,
            # One decommissioned unit: Silver must exclude its readings.
            "status": ["active", "faulty", "decommissioned"],
        }
    )


def _reading_rows() -> pl.DataFrame:
    def ts(hour: int) -> datetime:
        return datetime(2026, 8, 1, hour, tzinfo=UTC)

    return pl.DataFrame(
        {
            "reading_id": [1000, 1001, 1002, 1003],
            "sensor_id": [100, 100, 101, 102],
            "reading_ts": [ts(0), ts(12), ts(6), ts(6)],
            "soil_moisture_pct": [30.0, 34.0, 12.0, 20.0],
            "soil_temperature_c": [25.0, 28.0, 31.0, 33.0],
            "soil_ph": [7.1, 7.2, 6.4, 5.9],
            "soil_ec_ds_m": [0.8, 0.9, 0.4, 0.5],
            "battery_pct": [90.0, 88.0, 70.0, 60.0],
            "updated_at": [ts(1), ts(13), ts(7), ts(7)],
        }
    )


@pytest.fixture
def hdfs() -> FakeHdfsStore:
    return FakeHdfsStore()


@pytest.fixture
def control() -> FakeControlStore:
    return FakeControlStore()


@pytest.fixture
def clickhouse() -> FakeClickHouseSink:
    return FakeClickHouseSink()


@pytest.fixture
def postgres() -> FakePostgresSource:
    return FakePostgresSource(
        {
            "agri.farm": _farm_rows(),
            "agri.field": _field_rows(),
            "agri.sensor": _sensor_rows(),
            "agri.sensor_reading": _reading_rows(),
        }
    )


@pytest.fixture
def context(
    postgres: FakePostgresSource,
    hdfs: FakeHdfsStore,
    control: FakeControlStore,
    clickhouse: FakeClickHouseSink,
) -> PipelineContext:
    return PipelineContext(
        postgres=postgres,  # type: ignore[arg-type]
        hdfs=hdfs,  # type: ignore[arg-type]
        control=control,  # type: ignore[arg-type]
        clickhouse=clickhouse,  # type: ignore[arg-type]
    )


@pytest.fixture
def bronze_loaded(context: PipelineContext) -> Iterator[PipelineContext]:
    """Context with the Bronze stage already run."""
    for name in ("bronze.farm", "bronze.field", "bronze.sensor", "bronze.sensor_reading"):
        get_pipeline(name, context).run(LOGICAL_DATE)
    yield context


@pytest.fixture
def silver_loaded(bronze_loaded: PipelineContext) -> Iterator[PipelineContext]:
    """Context with Bronze and Silver both run."""
    for name in (
        "silver.dim_farm",
        "silver.dim_field",
        "silver.dim_sensor",
        "silver.fact_sensor_reading",
    ):
        get_pipeline(name, bronze_loaded).run(LOGICAL_DATE)
    yield bronze_loaded


# --- framework ---------------------------------------------------------------
class TestBasePipelineTemplate:
    def test_steps_run_in_order(self, context: PipelineContext) -> None:
        calls: list[str] = []

        class Recorder(BasePipeline):
            name = "test.recorder"
            dataset = "recorder"

            def extract(self, run: RunContext) -> pl.DataFrame:
                calls.append("extract")
                return pl.DataFrame({"x": [1]})

            def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:
                calls.append("transform")
                return frame

            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                calls.append("load")
                return frame.height

        Recorder(context).run(LOGICAL_DATE)
        assert calls == ["extract", "transform", "load"]

    def test_success_is_recorded_with_row_counts(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        class Simple(BasePipeline):
            name = "test.simple"
            dataset = "simple"

            def extract(self, run: RunContext) -> pl.DataFrame:
                return pl.DataFrame({"x": [1, 2, 3]})

            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                return frame.height

        stats = Simple(context).run(LOGICAL_DATE)

        assert (stats.rows_read, stats.rows_written) == (3, 3)
        assert control.last_run["status"] is RunStatus.SUCCESS

    def test_failure_is_recorded_and_re_raised(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        """The Airflow task must fail, and the run log must say why."""

        class Broken(BasePipeline):
            name = "test.broken"
            dataset = "broken"

            def extract(self, run: RunContext) -> pl.DataFrame:
                msg = "source unavailable"
                raise RuntimeError(msg)

            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                return 0

        with pytest.raises(RuntimeError, match="source unavailable"):
            Broken(context).run(LOGICAL_DATE)

        assert control.last_run["status"] is RunStatus.FAILED
        assert control.last_run["error"] == "source unavailable"

    def test_invalid_rows_are_quarantined_not_dropped_silently(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        import pandera.polars as pa
        from pandera.engines import polars_engine

        schema = pa.DataFrameSchema(
            {"x": pa.Column(polars_engine.Int64, pa.Check.in_range(0, 10))},
            strict=True,
            name="tiny",
        )

        class Guarded(BasePipeline):
            name = "test.guarded"
            dataset = "guarded"
            max_rejection_rate = 0.5

            def extract(self, run: RunContext) -> pl.DataFrame:
                return pl.DataFrame({"x": [1, 2, 999]})

            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                return frame.height

        Guarded.schema = schema
        stats = Guarded(context).run(LOGICAL_DATE)

        assert stats.rows_read == 3
        assert stats.rows_written == 2
        assert stats.rows_quarantined == 1

        quarantine = hdfs.zone_path(
            LakeZone.QUARANTINE, "guarded", f"logical_date={PARTITION}", "rejected.parquet"
        )
        assert hdfs.files[quarantine]["x"].to_list() == [999]

    def test_excessive_rejection_rate_fails_the_run(self, context: PipelineContext) -> None:
        """Half the file failing means the source or contract changed; carrying
        on would publish a plausible but wrong number."""
        import pandera.polars as pa
        from pandera.engines import polars_engine

        class Strict(BasePipeline):
            name = "test.strict"
            dataset = "strict"
            max_rejection_rate = 0.10
            schema = pa.DataFrameSchema(
                {"x": pa.Column(polars_engine.Int64, pa.Check.in_range(0, 10))},
                strict=True,
                name="tiny",
            )

            def extract(self, run: RunContext) -> pl.DataFrame:
                return pl.DataFrame({"x": [1, 999, 998, 997]})

            def load(self, frame: pl.DataFrame, run: RunContext) -> int:
                return frame.height

        with pytest.raises(ValueError, match="above the 10% threshold"):
            Strict(context).run(LOGICAL_DATE)

    def test_context_builds_connectors_lazily(self) -> None:
        """Constructing a pipeline must open no sockets."""
        assert PipelineContext()._postgres is None


class TestRunContext:
    def test_partition_is_the_iso_date(self) -> None:
        assert RunContext(date(2026, 8, 1), "p").partition == "2026-08-01"


# --- bronze ------------------------------------------------------------------
class TestBronzeSnapshot:
    def test_writes_a_snapshot_partition(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        stats = get_pipeline("bronze.farm", context).run(LOGICAL_DATE)
        path = hdfs.zone_path(
            LakeZone.BRONZE, "farm", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
        )
        assert stats.rows_written == 2
        assert hdfs.files[path].height == 2

    def test_rerun_replaces_rather_than_appends(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Idempotency: the partition is cleared before it is rewritten."""
        get_pipeline("bronze.farm", context).run(LOGICAL_DATE)
        get_pipeline("bronze.farm", context).run(LOGICAL_DATE)

        path = hdfs.zone_path(
            LakeZone.BRONZE, "farm", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
        )
        assert hdfs.files[path].height == 2
        assert hdfs.removed.count(path.rsplit("/", 1)[0]) == 2

    def test_bronze_does_not_clean_its_input(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Raw stays raw — the untrimmed name and lowercase country survive."""
        get_pipeline("bronze.farm", context).run(LOGICAL_DATE)
        path = hdfs.zone_path(
            LakeZone.BRONZE, "farm", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
        )
        frame = hdfs.files[path]
        assert " Kano Farm 1 " in frame["name"].to_list()
        assert "ng" in frame["country_code"].to_list()


class TestBronzeIncremental:
    def test_first_run_without_a_watermark_extracts_everything(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        stats = get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        assert stats.rows_read == 4
        assert control.watermarks["bronze.sensor_reading"] == datetime(2026, 8, 1, 13, tzinfo=UTC)

    def test_second_run_extracts_only_new_rows(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        stats = get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        assert stats.rows_read == 0

    def test_watermark_never_moves_backwards(self, control: FakeControlStore) -> None:
        late = datetime(2026, 8, 1, 13, tzinfo=UTC)
        early = datetime(2026, 7, 1, 0, tzinfo=UTC)
        control.set_watermark("p", "t", "updated_at", late)
        control.set_watermark("p", "t", "updated_at", early)
        assert control.watermarks["p"] == late

    def test_watermark_is_not_advanced_when_nothing_is_extracted(
        self, context: PipelineContext, control: FakeControlStore
    ) -> None:
        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        first = control.watermarks["bronze.sensor_reading"]
        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        assert control.watermarks["bronze.sensor_reading"] == first

    def test_empty_batch_does_not_overwrite_a_populated_partition(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Regression: an incremental partition must never be replaced.

        Once the watermark has caught up the next extract returns nothing. If
        that empty frame were written over the partition, the readings already
        landed there would vanish — and Silver, Gold and ClickHouse would all
        quietly empty on the following run.
        """
        directory = hdfs.zone_path(
            LakeZone.BRONZE, "sensor_reading", f"{INGEST_PARTITION_KEY}={PARTITION}"
        )

        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        after_first = hdfs.read_parquet_dir(directory).height

        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)
        assert hdfs.read_parquet_dir(directory).height == after_first == 4
        assert directory not in hdfs.removed, "incremental partitions must not be cleared"

    def test_a_later_batch_is_appended_beside_the_earlier_one(
        self, context: PipelineContext, hdfs: FakeHdfsStore, postgres: FakePostgresSource
    ) -> None:
        """Append, not replace: the watermark guarantees no overlap."""
        directory = hdfs.zone_path(
            LakeZone.BRONZE, "sensor_reading", f"{INGEST_PARTITION_KEY}={PARTITION}"
        )
        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)

        late = _reading_rows().with_columns(
            pl.col("reading_id") + 100,
            pl.lit(datetime(2026, 8, 1, 20, tzinfo=UTC)).alias("updated_at"),
        )
        postgres.tables["agri.sensor_reading"] = pl.concat([_reading_rows(), late])

        get_pipeline("bronze.sensor_reading", context).run(LOGICAL_DATE)

        assert len(hdfs.list_files(directory, ".parquet")) == 2
        assert hdfs.read_parquet_dir(directory).height == 8


# --- silver ------------------------------------------------------------------
class TestSilverDimensions:
    def test_farm_names_are_trimmed_and_country_codes_uppercased(
        self, bronze_loaded: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        get_pipeline("silver.dim_farm", bronze_loaded).run(LOGICAL_DATE)
        frame = hdfs.files[
            hdfs.zone_path(
                LakeZone.SILVER, "dim_farm", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ]
        assert "Kano Farm 1" in frame["farm_name"].to_list()
        assert set(frame["country_code"].to_list()) == {"EG", "NG"}

    def test_soil_types_are_lowercased(
        self, bronze_loaded: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        get_pipeline("silver.dim_field", bronze_loaded).run(LOGICAL_DATE)
        frame = hdfs.files[
            hdfs.zone_path(
                LakeZone.SILVER, "dim_field", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ]
        assert set(frame["soil_type"].to_list()) == {"clay", "clay_loam", "sandy"}

    def test_sensor_dimension_is_denormalised_to_the_farm(
        self, bronze_loaded: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        get_pipeline("silver.dim_sensor", bronze_loaded).run(LOGICAL_DATE)
        frame = hdfs.files[
            hdfs.zone_path(
                LakeZone.SILVER, "dim_sensor", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ]
        assert {"farm_id", "field_code"} <= set(frame.columns)


class TestSilverFact:
    @pytest.fixture
    def fact(self, bronze_loaded: PipelineContext, hdfs: FakeHdfsStore) -> pl.DataFrame:
        get_pipeline("silver.fact_sensor_reading", bronze_loaded).run(LOGICAL_DATE)
        return hdfs.files[
            hdfs.zone_path(
                LakeZone.SILVER,
                "fact_sensor_reading",
                f"{INGEST_PARTITION_KEY}={PARTITION}",
                DATA_FILE,
            )
        ]

    def test_decommissioned_sensor_readings_are_excluded(self, fact: pl.DataFrame) -> None:
        """Stale device noise would otherwise drag every field average off."""
        assert 102 not in fact["sensor_id"].to_list()

    def test_faulty_sensor_readings_are_kept(self, fact: pl.DataFrame) -> None:
        """Their readings are real; Gold decides what to do with them."""
        assert 101 in fact["sensor_id"].to_list()

    def test_reading_date_is_materialised(self, fact: pl.DataFrame) -> None:
        assert fact["reading_date"].dtype == pl.Date
        assert set(fact["reading_date"].to_list()) == {date(2026, 8, 1)}

    def test_farm_and_field_keys_are_joined_in(self, fact: pl.DataFrame) -> None:
        assert {"field_id", "farm_id"} <= set(fact.columns)
        assert fact.filter(pl.col("sensor_id") == 100)["farm_id"].to_list() == [1, 1]


# --- gold --------------------------------------------------------------------
class TestGoldFieldSoilDaily:
    @pytest.fixture
    def gold(self, silver_loaded: PipelineContext, hdfs: FakeHdfsStore) -> pl.DataFrame:
        get_pipeline("gold.field_soil_daily", silver_loaded).run(LOGICAL_DATE)
        return hdfs.files[
            hdfs.zone_path(
                LakeZone.GOLD,
                "field_soil_daily",
                f"{RUN_PARTITION_KEY}={PARTITION}",
                DATA_FILE,
            )
        ]

    def test_one_row_per_field_per_day(self, gold: pl.DataFrame) -> None:
        # Fields 10 and 11 have live sensors; field 12's only probe is dead.
        assert gold.height == 2
        assert set(gold["field_id"].to_list()) == {10, 11}

    def test_averages_are_correct(self, gold: pl.DataFrame) -> None:
        row = gold.filter(pl.col("field_id") == 10).to_dicts()[0]
        assert row["reading_count"] == 2
        assert row["active_sensors"] == 1
        assert row["avg_soil_moisture_pct"] == pytest.approx(32.0)
        assert row["min_soil_moisture_pct"] == pytest.approx(30.0)
        assert row["max_soil_moisture_pct"] == pytest.approx(34.0)

    def test_dimension_attributes_are_carried_on_the_row(self, gold: pl.DataFrame) -> None:
        """So a chart needs no joins at query time."""
        assert {"farm_name", "region", "soil_type", "field_area_ha"} <= set(gold.columns)

    @pytest.mark.parametrize(
        ("moisture", "expected"),
        [
            (DRY_THRESHOLD_PCT - 1, "dry"),
            (DRY_THRESHOLD_PCT + 1, "optimal"),
            (WET_THRESHOLD_PCT + 1, "wet"),
        ],
    )
    def test_moisture_stress_classification(
        self, silver_loaded: PipelineContext, hdfs: FakeHdfsStore, moisture: float, expected: str
    ) -> None:
        path = hdfs.zone_path(
            LakeZone.SILVER,
            "fact_sensor_reading",
            f"{INGEST_PARTITION_KEY}={PARTITION}",
            DATA_FILE,
        )
        hdfs.files[path] = hdfs.files[path].with_columns(
            pl.lit(moisture, dtype=pl.Float64).alias("soil_moisture_pct")
        )

        get_pipeline("gold.field_soil_daily", silver_loaded).run(LOGICAL_DATE)
        gold = hdfs.files[
            hdfs.zone_path(
                LakeZone.GOLD, "field_soil_daily", f"{RUN_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ]
        assert set(gold["moisture_stress"].to_list()) == {expected}

    def test_all_null_moisture_is_unknown_not_dry(
        self, silver_loaded: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """The distinction matters on a dashboard that drives irrigation."""
        path = hdfs.zone_path(
            LakeZone.SILVER,
            "fact_sensor_reading",
            f"{INGEST_PARTITION_KEY}={PARTITION}",
            DATA_FILE,
        )
        hdfs.files[path] = hdfs.files[path].with_columns(
            pl.lit(None, dtype=pl.Float64).alias("soil_moisture_pct")
        )

        get_pipeline("gold.field_soil_daily", silver_loaded).run(LOGICAL_DATE)
        gold = hdfs.files[
            hdfs.zone_path(
                LakeZone.GOLD, "field_soil_daily", f"{RUN_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ]
        assert set(gold["moisture_stress"].to_list()) == {"unknown"}


# --- load --------------------------------------------------------------------
class TestLoad:
    @pytest.fixture
    def gold_loaded(self, silver_loaded: PipelineContext) -> PipelineContext:
        get_pipeline("gold.field_soil_daily", silver_loaded).run(LOGICAL_DATE)
        return silver_loaded

    def test_dimensions_reach_clickhouse(
        self, gold_loaded: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        get_pipeline("load.dim_farm", gold_loaded).run(LOGICAL_DATE)
        assert clickhouse.tables["dim_farm"].height == 2

    def test_fact_reaches_clickhouse(
        self, gold_loaded: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        get_pipeline("load.fact_sensor_reading", gold_loaded).run(LOGICAL_DATE)
        assert clickhouse.tables["fact_sensor_reading"].height == 3

    def test_aggregate_reaches_clickhouse(
        self, gold_loaded: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        get_pipeline("load.field_soil_daily", gold_loaded).run(LOGICAL_DATE)
        assert clickhouse.tables["agg_field_soil_daily"].height == 2

    def test_reload_replaces_rather_than_duplicating(
        self, gold_loaded: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        get_pipeline("load.fact_sensor_reading", gold_loaded).run(LOGICAL_DATE)
        get_pipeline("load.fact_sensor_reading", gold_loaded).run(LOGICAL_DATE)
        assert clickhouse.tables["fact_sensor_reading"].height == 3

    def test_duplicate_readings_across_partitions_are_deduplicated(
        self, gold_loaded: PipelineContext, hdfs: FakeHdfsStore, clickhouse: FakeClickHouseSink
    ) -> None:
        """A re-ingest of an overlapping window must not double-count."""
        source = hdfs.zone_path(
            LakeZone.SILVER,
            "fact_sensor_reading",
            f"{INGEST_PARTITION_KEY}={PARTITION}",
            DATA_FILE,
        )
        duplicate = hdfs.zone_path(
            LakeZone.SILVER, "fact_sensor_reading", "ingest_date=2026-08-02", DATA_FILE
        )
        hdfs.files[duplicate] = hdfs.files[source]

        get_pipeline("load.fact_sensor_reading", gold_loaded).run(LOGICAL_DATE)
        assert clickhouse.tables["fact_sensor_reading"].height == 3


# --- end to end --------------------------------------------------------------
class TestFullSlice:
    def test_every_stage_runs_in_order(self, context: PipelineContext) -> None:
        from smart_agri.pipelines import SOIL_SENSOR_STAGES

        for stage in SOIL_SENSOR_STAGES:
            for name in stage:
                get_pipeline(name, context).run(LOGICAL_DATE)

        sink = context.clickhouse
        assert sink.tables["agg_field_soil_daily"].height == 2  # type: ignore[attr-defined]
        assert sink.tables["dim_sensor"].height == 3  # type: ignore[attr-defined]

    def test_the_whole_slice_is_idempotent(self, context: PipelineContext) -> None:
        """Re-running everything must produce identical warehouse contents."""
        from smart_agri.pipelines import SOIL_SENSOR_STAGES

        def run_all() -> pl.DataFrame:
            for stage in SOIL_SENSOR_STAGES:
                for name in stage:
                    get_pipeline(name, context).run(LOGICAL_DATE)
            return context.clickhouse.tables["agg_field_soil_daily"]  # type: ignore[attr-defined]

        assert run_all().equals(run_all())
