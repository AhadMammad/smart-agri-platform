"""Tests for the weather pipelines.

The properties that matter are the merge precedence — a measurement must beat a
prediction for the same day — and the derived agronomy in Gold, which is what
the irrigation dashboard actually plots.
"""

from __future__ import annotations

import itertools
from datetime import date
from typing import TYPE_CHECKING

import polars as pl
import pytest
from tests.fakes import FakeClickHouseSink, FakeControlStore, FakeHdfsStore, FakePostgresSource

from smart_agri.config import LakeZone
from smart_agri.io import WEATHER_SCHEMA
from smart_agri.pipelines import get_pipeline
from smart_agri.pipelines.base import PipelineContext
from smart_agri.pipelines.bronze import DATA_FILE, SNAPSHOT_PARTITION_KEY
from smart_agri.pipelines.weather import (
    DRY_BALANCE_MM,
    GDD_BASE_C,
    GDD_CAP_C,
    INGEST_PARTITION_KEY,
    RUN_PARTITION_KEY,
    WET_BALANCE_MM,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

LOGICAL_DATE = date(2026, 7, 10)
PARTITION = LOGICAL_DATE.isoformat()

DAYS = [date(2026, 7, day) for day in range(1, 8)]


def _weather_rows(
    source: str,
    days: Sequence[date],
    *,
    farm_code: str = "EG-001",
    precipitation: Sequence[float] | None = None,
    temp_mean: Sequence[float] | None = None,
    et0: Sequence[float] | None = None,
) -> pl.DataFrame:
    n = len(days)
    rain = list(precipitation) if precipitation is not None else [0.0] * n
    temps = list(temp_mean) if temp_mean is not None else [25.0] * n
    evap = list(et0) if et0 is not None else [4.0] * n

    return pl.DataFrame(
        {
            "farm_code": [farm_code] * n,
            "latitude": [30.9] * n,
            "longitude": [31.1] * n,
            "weather_date": list(days),
            "source": [source] * n,
            "temperature_2m_max": [t + 6 for t in temps],
            "temperature_2m_min": [t - 6 for t in temps],
            "temperature_2m_mean": temps,
            "precipitation_sum": rain,
            "rain_sum": rain,
            "precipitation_hours": [2.0] * n,
            "et0_fao_evapotranspiration": evap,
            "shortwave_radiation_sum": [22.0] * n,
            "wind_speed_10m_max": [15.0] * n,
            "weather_code": [0] * n,
        },
        schema=WEATHER_SCHEMA,
    )


def _farm_snapshot() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "farm_id": [1],
            "farm_code": ["EG-001"],
            "name": ["Nile Delta Farm 1"],
            "country_code": ["EG"],
            "region": ["Nile Delta"],
            "latitude": [30.9],
            "longitude": [31.1],
            "area_ha": [120.0],
        }
    )


def _field_snapshot() -> pl.DataFrame:
    """Bronze field rows, so `silver.dim_field` can be run rather than faked."""
    return pl.DataFrame(
        {
            "field_id": [10, 11],
            "field_code": ["EG-001-F01", "EG-001-F02"],
            "farm_id": [1, 1],
            "name": ["North Block 1", "South Block 2"],
            "area_ha": [50.0, 55.0],
            "soil_type": ["clay", "clay_loam"],
            "latitude": [30.902, 30.898],
            "longitude": [31.103, 31.098],
        }
    )


def _silver_dim_farm() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "farm_id": [1],
            "farm_code": ["EG-001"],
            "farm_name": ["Nile Delta Farm 1"],
            "country_code": ["EG"],
            "region": ["Nile Delta"],
            "latitude": [30.9],
            "longitude": [31.1],
            "area_ha": [120.0],
        }
    )


def _silver_dim_field() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "field_id": [10, 11],
            "field_code": ["EG-001-F01", "EG-001-F02"],
            "farm_id": [1, 1],
            "farm_code": ["EG-001", "EG-001"],
            "field_name": ["North Block 1", "South Block 2"],
            "area_ha": [50.0, 55.0],
            "soil_type": ["clay", "clay_loam"],
            "latitude": [30.902, 30.898],
            "longitude": [31.103, 31.098],
        }
    )


@pytest.fixture
def hdfs() -> FakeHdfsStore:
    return FakeHdfsStore()


@pytest.fixture
def clickhouse() -> FakeClickHouseSink:
    return FakeClickHouseSink()


@pytest.fixture
def context(hdfs: FakeHdfsStore, clickhouse: FakeClickHouseSink) -> PipelineContext:
    return PipelineContext(
        postgres=FakePostgresSource(),  # type: ignore[arg-type]
        hdfs=hdfs,  # type: ignore[arg-type]
        control=FakeControlStore(),  # type: ignore[arg-type]
        clickhouse=clickhouse,  # type: ignore[arg-type]
    )


def _seed_lake(
    hdfs: FakeHdfsStore,
    *,
    archive: pl.DataFrame | None = None,
    forecast: pl.DataFrame | None = None,
) -> None:
    """Place the Bronze and Silver inputs the weather stages read."""
    hdfs.files[
        hdfs.zone_path(LakeZone.BRONZE, "farm", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE)
    ] = _farm_snapshot()
    hdfs.files[
        hdfs.zone_path(LakeZone.BRONZE, "field", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE)
    ] = _field_snapshot()
    hdfs.files[
        hdfs.zone_path(
            LakeZone.SILVER, "dim_farm", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
        )
    ] = _silver_dim_farm()
    hdfs.files[
        hdfs.zone_path(
            LakeZone.SILVER, "dim_field", f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
        )
    ] = _silver_dim_field()

    if archive is not None:
        hdfs.files[
            hdfs.zone_path(
                LakeZone.BRONZE,
                "weather_archive",
                f"{INGEST_PARTITION_KEY}={PARTITION}",
                DATA_FILE,
            )
        ] = archive
    if forecast is not None:
        hdfs.files[
            hdfs.zone_path(
                LakeZone.BRONZE,
                "weather_forecast",
                f"{INGEST_PARTITION_KEY}={PARTITION}",
                DATA_FILE,
            )
        ] = forecast


def _silver_frame(hdfs: FakeHdfsStore) -> pl.DataFrame:
    return hdfs.files[
        hdfs.zone_path(
            LakeZone.SILVER,
            "fact_weather_daily",
            f"{SNAPSHOT_PARTITION_KEY}={PARTITION}",
            DATA_FILE,
        )
    ]


def _gold_frame(hdfs: FakeHdfsStore, dataset: str) -> pl.DataFrame:
    return hdfs.files[
        hdfs.zone_path(LakeZone.GOLD, dataset, f"{RUN_PARTITION_KEY}={PARTITION}", DATA_FILE)
    ]


class TestSilverMerge:
    def test_archive_and_forecast_are_combined(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        _seed_lake(
            hdfs,
            archive=_weather_rows("archive", DAYS[:4]),
            forecast=_weather_rows("forecast", DAYS[4:]),
        )
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)

        silver = _silver_frame(hdfs)
        assert silver.height == len(DAYS)
        assert set(silver["source"].to_list()) == {"archive", "forecast"}

    def test_a_measurement_beats_a_prediction_for_the_same_day(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """The endpoints overlap by design. Where both cover a date the archive
        wins: it is what happened, the forecast is what was expected to happen,
        and a rainfall-vs-moisture chart must not compare against a prediction.
        """
        overlap = DAYS[:3]
        _seed_lake(
            hdfs,
            archive=_weather_rows("archive", overlap, precipitation=[10.0, 10.0, 10.0]),
            forecast=_weather_rows("forecast", overlap, precipitation=[99.0, 99.0, 99.0]),
        )
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)

        silver = _silver_frame(hdfs)
        assert silver.height == 3, "one row per farm per day"
        assert set(silver["source"].to_list()) == {"archive"}
        assert silver["precipitation_mm"].to_list() == [10.0, 10.0, 10.0]

    def test_is_actual_marks_measurements(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        _seed_lake(
            hdfs,
            archive=_weather_rows("archive", DAYS[:3]),
            forecast=_weather_rows("forecast", DAYS[3:]),
        )
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)

        silver = _silver_frame(hdfs)
        actual = silver.filter(pl.col("is_actual"))
        predicted = silver.filter(~pl.col("is_actual"))
        assert set(actual["source"].to_list()) == {"archive"}
        assert set(predicted["source"].to_list()) == {"forecast"}

    def test_forecast_only_still_produces_rows(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Before the first backfill there is no archive at all; the daily run
        must still deliver something rather than fail."""
        _seed_lake(hdfs, forecast=_weather_rows("forecast", DAYS))
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)
        assert _silver_frame(hdfs).height == len(DAYS)

    def test_weather_for_an_unknown_farm_is_dropped(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """An inner join on the farm snapshot: weather for a farm that no longer
        exists must not reach the warehouse with a null key."""
        _seed_lake(
            hdfs,
            archive=pl.concat(
                [
                    _weather_rows("archive", DAYS[:2]),
                    _weather_rows("archive", DAYS[:2], farm_code="ZZ-999"),
                ]
            ),
        )
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)

        silver = _silver_frame(hdfs)
        assert set(silver["farm_code"].to_list()) == {"EG-001"}
        assert silver["farm_id"].null_count() == 0


class TestGoldDimDate:
    def test_covers_every_day_of_the_series(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Derived from the data rather than a fixed range, so it can never fall
        short of the facts joining to it."""
        _seed_lake(hdfs, archive=_weather_rows("archive", DAYS))
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)
        get_pipeline("gold.dim_date", context).run(LOGICAL_DATE)

        dim = _gold_frame(hdfs, "dim_date")
        assert dim["date_key"].min() == DAYS[0]
        assert dim["date_key"].max() == DAYS[-1]
        assert dim.height == len(DAYS)

    def test_fills_gaps_in_the_weather_series(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """A missing weather day must still get a date row, or a chart grouped
        by date would silently skip it."""
        sparse = [DAYS[0], DAYS[-1]]
        _seed_lake(hdfs, archive=_weather_rows("archive", sparse))
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)
        get_pipeline("gold.dim_date", context).run(LOGICAL_DATE)

        assert _gold_frame(hdfs, "dim_date").height == 7

    def test_weekend_flag_is_correct(self, context: PipelineContext, hdfs: FakeHdfsStore) -> None:
        _seed_lake(hdfs, archive=_weather_rows("archive", DAYS))
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)
        get_pipeline("gold.dim_date", context).run(LOGICAL_DATE)

        dim = _gold_frame(hdfs, "dim_date")
        # 2026-07-04 is a Saturday and 2026-07-05 a Sunday.
        weekend = dim.filter(pl.col("is_weekend"))["date_key"].to_list()
        assert weekend == [date(2026, 7, 4), date(2026, 7, 5)]


class TestGoldFieldWeather:
    @pytest.fixture
    def gold(self, context: PipelineContext, hdfs: FakeHdfsStore) -> pl.DataFrame:
        _seed_lake(
            hdfs,
            archive=_weather_rows(
                "archive",
                DAYS,
                precipitation=[0.0, 20.0, 0.0, 0.0, 5.0, 0.0, 0.0],
                temp_mean=[25.0, 18.0, 8.0, 35.0, 25.0, 25.0, 25.0],
                et0=[5.0, 1.0, 3.0, 6.0, 4.0, 4.0, 4.0],
            ),
        )
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)
        get_pipeline("gold.field_weather_daily", context).run(LOGICAL_DATE)
        return _gold_frame(hdfs, "field_weather_daily")

    def test_every_field_inherits_its_farm_series(self, gold: pl.DataFrame) -> None:
        assert set(gold["field_id"].to_list()) == {10, 11}
        assert gold.height == len(DAYS) * 2

    def test_degree_days_use_the_base_temperature(self, gold: pl.DataFrame) -> None:
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        # 25 °C mean, base 10 -> 15 degree-days.
        assert one_field["gdd_base10"].to_list()[0] == pytest.approx(25.0 - GDD_BASE_C)

    def test_cold_days_accumulate_no_degree_days(self, gold: pl.DataFrame) -> None:
        """Below the base temperature growth stops; a negative contribution
        would let a cold snap undo earlier accumulation."""
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        assert one_field["gdd_base10"].to_list()[2] == 0.0  # 8 °C mean

    def test_heat_is_capped(self, gold: pl.DataFrame) -> None:
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        # 35 °C mean, capped at 30 -> 20 degree-days, not 25.
        assert one_field["gdd_base10"].to_list()[3] == pytest.approx(GDD_CAP_C - GDD_BASE_C)

    def test_cumulative_degree_days_only_rise(self, gold: pl.DataFrame) -> None:
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        values = one_field["gdd_cumulative"].to_list()
        assert all(b >= a for a, b in itertools.pairwise(values))

    def test_rolling_rainfall_accumulates(self, gold: pl.DataFrame) -> None:
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        # Total rain across the window is 25 mm, all inside seven days.
        assert one_field["rainfall_7d_mm"].to_list()[-1] == pytest.approx(25.0)

    def test_rolling_windows_do_not_leak_between_fields(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Windows are computed `over` field_id. Without that, one field's rain
        would bleed into the next field's rolling total."""
        _seed_lake(hdfs, archive=_weather_rows("archive", DAYS, precipitation=[10.0] * 7))
        get_pipeline("silver.fact_weather_daily", context).run(LOGICAL_DATE)
        get_pipeline("gold.field_weather_daily", context).run(LOGICAL_DATE)

        gold = _gold_frame(hdfs, "field_weather_daily")
        for field_id in (10, 11):
            series = gold.filter(pl.col("field_id") == field_id).sort("weather_date")
            assert series["rainfall_7d_mm"].to_list()[0] == pytest.approx(10.0)
            assert series["rainfall_7d_mm"].to_list()[-1] == pytest.approx(70.0)

    def test_water_balance_is_rain_minus_evaporation(self, gold: pl.DataFrame) -> None:
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        # Day two: 20 mm rain against 1 mm ET0.
        assert one_field["water_balance_mm"].to_list()[1] == pytest.approx(19.0)

    def test_aridity_flag_follows_the_balance(self, gold: pl.DataFrame) -> None:
        one_field = gold.filter(pl.col("field_id") == 10).sort("weather_date")
        rows = one_field.to_dicts()

        assert rows[1]["aridity_flag"] == "wet"  # +19 mm
        assert rows[0]["aridity_flag"] == "dry"  # -5 mm
        assert all(
            (row["water_balance_mm"] > WET_BALANCE_MM) == (row["aridity_flag"] == "wet")
            for row in rows
        )
        assert all(
            (row["water_balance_mm"] < DRY_BALANCE_MM) == (row["aridity_flag"] == "dry")
            for row in rows
        )

    def test_dimension_attributes_are_carried(self, gold: pl.DataFrame) -> None:
        """So a chart needs no joins at query time."""
        assert {"region", "country_code", "farm_code", "field_area_ha"} <= set(gold.columns)


class TestLoads:
    @pytest.fixture
    def loaded(self, context: PipelineContext, hdfs: FakeHdfsStore) -> FakeClickHouseSink:
        _seed_lake(hdfs, archive=_weather_rows("archive", DAYS))
        for name in (
            "silver.fact_weather_daily",
            "gold.dim_date",
            "gold.field_weather_daily",
            "load.dim_date",
            "load.fact_weather_daily",
            "load.field_weather_daily",
        ):
            get_pipeline(name, context).run(LOGICAL_DATE)
        return context.clickhouse  # type: ignore[return-value]

    @pytest.mark.parametrize("table", ["dim_date", "fact_weather_daily", "agg_field_weather_daily"])
    def test_every_table_is_populated(self, loaded: FakeClickHouseSink, table: str) -> None:
        assert loaded.count(table) > 0

    def test_reload_replaces_rather_than_duplicating(
        self, context: PipelineContext, hdfs: FakeHdfsStore, clickhouse: FakeClickHouseSink
    ) -> None:
        _seed_lake(hdfs, archive=_weather_rows("archive", DAYS))
        for _ in range(2):
            for name in (
                "silver.fact_weather_daily",
                "gold.field_weather_daily",
                "load.field_weather_daily",
            ):
                get_pipeline(name, context).run(LOGICAL_DATE)

        assert clickhouse.count("agg_field_weather_daily") == len(DAYS) * 2


class TestWeatherSliceIsIdempotent:
    def test_running_every_stage_twice_changes_nothing(
        self, context: PipelineContext, hdfs: FakeHdfsStore, clickhouse: FakeClickHouseSink
    ) -> None:
        from smart_agri.pipelines import WEATHER_STAGES

        _seed_lake(hdfs, archive=_weather_rows("archive", DAYS))
        # The source extracts at the head of the weather stages need a live
        # Postgres, so only the lake-side stages are exercised here.
        lake_stages = [
            name for stage in WEATHER_STAGES for name in stage if not name.startswith("bronze.")
        ]

        def run_all() -> pl.DataFrame:
            for name in lake_stages:
                get_pipeline(name, context).run(LOGICAL_DATE)
            return clickhouse.tables["agg_field_weather_daily"]

        assert run_all().equals(run_all())
