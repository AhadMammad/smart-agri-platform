"""Weather: Open-Meteo through the lake and into ClickHouse.

Two Bronze pipelines rather than one, because no single endpoint covers the
whole timeline. The archive holds measurements but lags real time by several
days; the forecast endpoint reaches a couple of weeks either side of today but
its recent past is still model output. Silver merges them and lets the
measurement win wherever both exist — the distinction matters, because a
dashboard comparing rainfall to soil moisture must not treat a prediction as
something that happened.

Gold turns the raw variables into the agronomy the dashboards actually ask for:
growing-degree days, rolling rainfall, and the water balance between what fell
and what evaporated.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.config import LakeZone
from smart_agri.contracts import (
    BRONZE_WEATHER,
    GOLD_DIM_DATE,
    GOLD_FIELD_WEATHER_DAILY,
    SILVER_FACT_WEATHER_DAILY,
)
from smart_agri.io import OpenMeteoSource, WeatherSource
from smart_agri.pipelines.base import BasePipeline
from smart_agri.pipelines.bronze import DATA_FILE, SNAPSHOT_PARTITION_KEY
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from smart_agri.io.openmeteo import WeatherWindow
    from smart_agri.pipelines.base import RunContext

logger = get_logger(__name__)

INGEST_PARTITION_KEY = "ingest_date"
RUN_PARTITION_KEY = "run_date"

#: Base temperature for the generic degree-day column. Ten degrees suits the
#: warm-season crops that dominate the dataset; a per-crop figure belongs on the
#: planting, not on a weather row that no crop owns.
GDD_BASE_C = 10.0
GDD_CAP_C = 30.0

#: Water-balance thresholds, in millimetres per day, for the aridity flag.
WET_BALANCE_MM = 2.0
DRY_BALANCE_MM = -3.0


class _WeatherPipeline(BasePipeline):
    """Shared plumbing for the weather stages."""

    def _read_bronze_snapshot(self, dataset: str, run: RunContext) -> pl.DataFrame:
        path = self.context.hdfs.zone_path(
            LakeZone.BRONZE, dataset, f"{SNAPSHOT_PARTITION_KEY}={run.partition}", DATA_FILE
        )
        return self.context.hdfs.read_parquet(path)

    def _farm_locations(self, run: RunContext) -> list[tuple[str, float, float]]:
        """Farm coordinates from the current Bronze snapshot.

        Weather is fetched per farm rather than per field: fields inherit their
        farm's series, which keeps API calls proportional to farms.
        """
        farms = self._read_bronze_snapshot("farm", run)
        return [
            (row["farm_code"], float(row["latitude"]), float(row["longitude"]))
            for row in farms.iter_rows(named=True)
        ]

    def _source(self) -> OpenMeteoSource:
        return OpenMeteoSource(self.context.settings.open_meteo)

    def _write(self, frame: pl.DataFrame, zone: LakeZone, key: str, run: RunContext) -> int:
        directory = self._partition_path(zone, key, run)
        self.context.hdfs.remove(directory)
        return self.context.hdfs.write_parquet(frame, f"{directory}/{DATA_FILE}")


class BronzeWeatherArchivePipeline(_WeatherPipeline):
    """Historical measurements from the archive endpoint.

    One request per farm covering the whole backfill window, which is why the
    backfill DAG is cheap despite spanning a year: five farms, five calls.
    """

    name = "bronze.weather_archive"
    dataset = "weather_archive"
    schema = BRONZE_WEATHER

    def extract(self, run: RunContext) -> pl.DataFrame:
        source = self._source()
        window = source.archive_window(
            self.context.settings.open_meteo.backfill_start, run.logical_date
        )
        return source.fetch_many(self._farm_locations(run), window)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return self._write(frame, LakeZone.BRONZE, INGEST_PARTITION_KEY, run)


class BronzeWeatherForecastPipeline(_WeatherPipeline):
    """Recent past and near future from the forecast endpoint.

    Covers the days the archive has not caught up with yet, so the series has no
    hole between the archive's end and today.
    """

    name = "bronze.weather_forecast"
    dataset = "weather_forecast"
    schema = BRONZE_WEATHER

    def extract(self, run: RunContext) -> pl.DataFrame:
        source = self._source()
        window: WeatherWindow = source.forecast_window(run.logical_date)
        return source.fetch_many(self._farm_locations(run), window)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return self._write(frame, LakeZone.BRONZE, INGEST_PARTITION_KEY, run)


class SilverFactWeatherDailyPipeline(_WeatherPipeline):
    """One row per farm per day, measurements preferred over predictions."""

    name = "silver.fact_weather_daily"
    dataset = "fact_weather_daily"
    schema = SILVER_FACT_WEATHER_DAILY

    def extract(self, run: RunContext) -> pl.DataFrame:
        archive = self._read_bronze_weather("weather_archive")
        forecast = self._read_bronze_weather("weather_forecast")

        combined = [frame for frame in (archive, forecast) if not frame.is_empty()]
        if not combined:
            logger.warning("no_bronze_weather", pipeline=self.name)
            return pl.DataFrame()

        weather = pl.concat(combined, how="vertical_relaxed")
        farms = self._read_bronze_snapshot("farm", run).select("farm_id", "farm_code")
        return weather.join(farms, on="farm_code", how="inner")

    def _read_bronze_weather(self, dataset: str) -> pl.DataFrame:
        """Read every ingest partition of a Bronze weather dataset.

        All partitions, not just this run's: a backfill lands one partition and
        the daily runs land others, and Silver has to see the whole series.
        """
        return self.context.hdfs.read_parquet_dir(
            self.context.hdfs.zone_path(LakeZone.BRONZE, dataset), missing_ok=True
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return frame

        typed = frame.select(
            pl.col("farm_id").cast(pl.Int64),
            pl.col("farm_code"),
            pl.col("weather_date").cast(pl.Date),
            pl.col("source"),
            (pl.col("source") == WeatherSource.ARCHIVE.value).alias("is_actual"),
            pl.col("latitude").cast(pl.Float64),
            pl.col("longitude").cast(pl.Float64),
            pl.col("temperature_2m_max").cast(pl.Float64).alias("temp_max_c"),
            pl.col("temperature_2m_min").cast(pl.Float64).alias("temp_min_c"),
            pl.col("temperature_2m_mean").cast(pl.Float64).alias("temp_mean_c"),
            pl.col("precipitation_sum").cast(pl.Float64).alias("precipitation_mm"),
            pl.col("rain_sum").cast(pl.Float64).alias("rain_mm"),
            pl.col("precipitation_hours").cast(pl.Float64),
            pl.col("et0_fao_evapotranspiration").cast(pl.Float64).alias("et0_mm"),
            pl.col("shortwave_radiation_sum").cast(pl.Float64).alias("solar_radiation_mj_m2"),
            pl.col("wind_speed_10m_max").cast(pl.Float64).alias("wind_speed_max_kmh"),
            pl.col("weather_code").cast(pl.Int32),
        )

        # Where the archive and the forecast both cover a date, the archive
        # wins: it is what happened, the other is what was expected to happen.
        # Sorting actuals last and keeping "last" is what encodes that.
        return (
            typed.sort("farm_id", "weather_date", "is_actual")
            .unique(subset=["farm_id", "weather_date"], keep="last")
            .sort("farm_id", "weather_date")
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return self._write(frame, LakeZone.SILVER, SNAPSHOT_PARTITION_KEY, run)


class GoldDimDatePipeline(_WeatherPipeline):
    """The date dimension, spanning whatever the weather series covers.

    Derived from the data rather than generated for a fixed range, so it never
    falls short of the facts that join to it.
    """

    name = "gold.dim_date"
    dataset = "dim_date"
    schema = GOLD_DIM_DATE

    def extract(self, run: RunContext) -> pl.DataFrame:
        path = self.context.hdfs.zone_path(
            LakeZone.SILVER,
            "fact_weather_daily",
            f"{SNAPSHOT_PARTITION_KEY}={run.partition}",
            DATA_FILE,
        )
        return self.context.hdfs.read_parquet(path)

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return frame

        first = frame["weather_date"].min()
        last = frame["weather_date"].max()

        dates = pl.date_range(first, last, interval="1d", eager=True).alias("date_key")  # type: ignore[arg-type]
        return (
            pl.DataFrame({"date_key": dates})
            .with_columns(
                pl.col("date_key").dt.year().cast(pl.Int32).alias("year"),
                pl.col("date_key").dt.quarter().cast(pl.Int8).alias("quarter"),
                pl.col("date_key").dt.month().cast(pl.Int8).alias("month"),
                pl.col("date_key").dt.strftime("%B").alias("month_name"),
                pl.col("date_key").dt.day().cast(pl.Int8).alias("day_of_month"),
                pl.col("date_key").dt.weekday().cast(pl.Int8).alias("day_of_week"),
                pl.col("date_key").dt.strftime("%A").alias("day_name"),
                pl.col("date_key").dt.week().cast(pl.Int8).alias("iso_week"),
            )
            # Polars weekday is 1 = Monday, so 6 and 7 are the weekend.
            .with_columns((pl.col("day_of_week") >= 6).alias("is_weekend"))  # noqa: PLR2004
            .select(GOLD_DIM_DATE.columns.keys())
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return self._write(frame, LakeZone.GOLD, RUN_PARTITION_KEY, run)


class GoldFieldWeatherDailyPipeline(_WeatherPipeline):
    """Weather pushed down to field level, with the derived agronomy.

    Fields inherit their farm's series — Phase 1 fixed coordinates at farm level
    — but the metrics are computed per field so they can be sliced alongside the
    soil and yield aggregates without another join.
    """

    name = "gold.field_weather_daily"
    dataset = "field_weather_daily"
    schema = GOLD_FIELD_WEATHER_DAILY

    def extract(self, run: RunContext) -> pl.DataFrame:
        weather = self.context.hdfs.read_parquet(
            self.context.hdfs.zone_path(
                LakeZone.SILVER,
                "fact_weather_daily",
                f"{SNAPSHOT_PARTITION_KEY}={run.partition}",
                DATA_FILE,
            )
        )
        if weather.is_empty():
            return weather

        fields = self._read_silver_dim("dim_field", run).select(
            "field_id", "field_code", "farm_id", "area_ha"
        )
        farms = self._read_silver_dim("dim_farm", run).select("farm_id", "region", "country_code")

        return weather.join(fields, on="farm_id", how="inner").join(
            farms, on="farm_id", how="inner"
        )

    def _read_silver_dim(self, dataset: str, run: RunContext) -> pl.DataFrame:
        path = self.context.hdfs.zone_path(
            LakeZone.SILVER, dataset, f"{SNAPSHOT_PARTITION_KEY}={run.partition}", DATA_FILE
        )
        return self.context.hdfs.read_parquet(path)

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return frame

        # Rolling windows are per field and strictly chronological; sorting is
        # not cosmetic here, it is what makes the window correct.
        ordered = frame.sort("field_id", "weather_date")

        with_daily = ordered.with_columns(
            # Growing-degree days, capped: growth does not keep accelerating
            # with heat, and an uncapped sum inflates hot-region totals.
            pl.max_horizontal(
                pl.min_horizontal(pl.col("temp_mean_c"), pl.lit(GDD_CAP_C)) - GDD_BASE_C,
                pl.lit(0.0),
            ).alias("gdd_base10"),
            (pl.col("precipitation_mm") - pl.col("et0_mm")).alias("water_balance_mm"),
            pl.col("area_ha").alias("field_area_ha"),
        )

        windowed = with_daily.with_columns(
            pl.col("gdd_base10").cum_sum().over("field_id").alias("gdd_cumulative"),
            pl.col("precipitation_mm")
            .rolling_sum(window_size=7, min_periods=1)
            .over("field_id")
            .alias("rainfall_7d_mm"),
            pl.col("precipitation_mm")
            .rolling_sum(window_size=30, min_periods=1)
            .over("field_id")
            .alias("rainfall_30d_mm"),
            pl.col("et0_mm")
            .rolling_sum(window_size=7, min_periods=1)
            .over("field_id")
            .alias("et0_7d_mm"),
            pl.col("water_balance_mm")
            .rolling_sum(window_size=7, min_periods=1)
            .over("field_id")
            .alias("water_balance_7d_mm"),
        )

        return (
            windowed.with_columns(
                # A day with no reading is "unknown", not "dry" — the difference
                # matters on a dashboard that drives irrigation decisions.
                pl.when(pl.col("water_balance_mm").is_null())
                .then(pl.lit("unknown"))
                .when(pl.col("water_balance_mm") > WET_BALANCE_MM)
                .then(pl.lit("wet"))
                .when(pl.col("water_balance_mm") < DRY_BALANCE_MM)
                .then(pl.lit("dry"))
                .otherwise(pl.lit("balanced"))
                .alias("aridity_flag")
            )
            .with_columns(
                pl.col(
                    "gdd_base10",
                    "gdd_cumulative",
                    "rainfall_7d_mm",
                    "rainfall_30d_mm",
                    "et0_7d_mm",
                    "water_balance_mm",
                    "water_balance_7d_mm",
                ).round(3)
            )
            .select(GOLD_FIELD_WEATHER_DAILY.columns.keys())
            .sort("weather_date", "field_id")
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return self._write(frame, LakeZone.GOLD, RUN_PARTITION_KEY, run)


class LoadDimDatePipeline(_WeatherPipeline):
    """Replace the ClickHouse date dimension."""

    name = "load.dim_date"
    dataset = "dim_date"
    table = "dim_date"

    def extract(self, run: RunContext) -> pl.DataFrame:
        return self.context.hdfs.read_parquet(
            self.context.hdfs.zone_path(
                LakeZone.GOLD, self.dataset, f"{RUN_PARTITION_KEY}={run.partition}", DATA_FILE
            )
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.table, frame)


class LoadFactWeatherDailyPipeline(_WeatherPipeline):
    """Replace the ClickHouse weather fact."""

    name = "load.fact_weather_daily"
    dataset = "fact_weather_daily"
    table = "fact_weather_daily"

    def extract(self, run: RunContext) -> pl.DataFrame:
        return self.context.hdfs.read_parquet(
            self.context.hdfs.zone_path(
                LakeZone.SILVER,
                self.dataset,
                f"{SNAPSHOT_PARTITION_KEY}={run.partition}",
                DATA_FILE,
            )
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.table, frame)


class LoadFieldWeatherDailyPipeline(_WeatherPipeline):
    """Replace the ClickHouse field-level weather aggregate."""

    name = "load.field_weather_daily"
    dataset = "field_weather_daily"
    table = "agg_field_weather_daily"

    def extract(self, run: RunContext) -> pl.DataFrame:
        return self.context.hdfs.read_parquet(
            self.context.hdfs.zone_path(
                LakeZone.GOLD, self.dataset, f"{RUN_PARTITION_KEY}={run.partition}", DATA_FILE
            )
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.table, frame)


def days_between(window: WeatherWindow) -> int:
    """Inclusive day count of a window, for logging and tests."""
    return 0 if window.is_empty else (window.end - window.start + timedelta(days=1)).days
