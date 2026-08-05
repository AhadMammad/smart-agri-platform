"""Silver: typed, validated, conformed.

This is where raw extracts become the modelled star schema: columns are renamed
to their analytical names, natural keys are joined in so downstream stages never
need the source again, and every value is range-checked. Rows that fail are
quarantined by the base class rather than dropped.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from smart_agri.config import LakeZone
from smart_agri.contracts import (
    SILVER_DIM_FARM,
    SILVER_DIM_FIELD,
    SILVER_DIM_SENSOR,
    SILVER_FACT_SENSOR_READING,
)
from smart_agri.pipelines.base import BasePipeline
from smart_agri.pipelines.bronze import (
    DATA_FILE,
    INGEST_PARTITION_KEY,
    SNAPSHOT_PARTITION_KEY,
)

if TYPE_CHECKING:
    from smart_agri.pipelines.base import RunContext


class _SilverPipeline(BasePipeline):
    """Shared plumbing for reading Bronze and writing Silver."""

    def _read_bronze_snapshot(self, dataset: str, run: RunContext) -> pl.DataFrame:
        """Read a Bronze dimension snapshot for this run's date."""
        path = self.context.hdfs.zone_path(
            LakeZone.BRONZE, dataset, f"{SNAPSHOT_PARTITION_KEY}={run.partition}", DATA_FILE
        )
        return self.context.hdfs.read_parquet(path)

    def _write_silver(self, frame: pl.DataFrame, key: str, run: RunContext) -> int:
        directory = self._partition_path(LakeZone.SILVER, key, run)
        self.context.hdfs.remove(directory)
        return self.context.hdfs.write_parquet(frame, f"{directory}/{DATA_FILE}")

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        return self._write_silver(frame, SNAPSHOT_PARTITION_KEY, run)


class SilverDimFarmPipeline(_SilverPipeline):
    """Farm dimension."""

    name = "silver.dim_farm"
    dataset = "dim_farm"
    schema = SILVER_DIM_FARM

    def extract(self, run: RunContext) -> pl.DataFrame:
        return self._read_bronze_snapshot("farm", run)

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return frame.select(
            pl.col("farm_id").cast(pl.Int64),
            pl.col("farm_code").str.strip_chars(),
            pl.col("name").str.strip_chars().alias("farm_name"),
            # Country codes arrive inconsistently cased from some sources; the
            # dimension is the right place to settle on one form.
            pl.col("country_code").str.strip_chars().str.to_uppercase(),
            pl.col("region").str.strip_chars(),
            pl.col("latitude").cast(pl.Float64),
            pl.col("longitude").cast(pl.Float64),
            pl.col("area_ha").cast(pl.Float64),
        )


class SilverDimFieldPipeline(_SilverPipeline):
    """Field dimension, carrying the farm's business key for convenience."""

    name = "silver.dim_field"
    dataset = "dim_field"
    schema = SILVER_DIM_FIELD

    def extract(self, run: RunContext) -> pl.DataFrame:
        fields = self._read_bronze_snapshot("field", run)
        farms = self._read_bronze_snapshot("farm", run).select("farm_id", "farm_code")
        # Inner join: a field whose farm is absent is a referential break, and
        # the row must not reach Silver pretending to be complete.
        return fields.join(farms, on="farm_id", how="inner")

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return frame.select(
            pl.col("field_id").cast(pl.Int64),
            pl.col("field_code").str.strip_chars(),
            pl.col("farm_id").cast(pl.Int64),
            pl.col("farm_code").str.strip_chars(),
            pl.col("name").str.strip_chars().alias("field_name"),
            pl.col("area_ha").cast(pl.Float64),
            pl.col("soil_type").str.strip_chars().str.to_lowercase(),
        )


class SilverDimSensorPipeline(_SilverPipeline):
    """Sensor dimension, denormalised up to the farm."""

    name = "silver.dim_sensor"
    dataset = "dim_sensor"
    schema = SILVER_DIM_SENSOR

    def extract(self, run: RunContext) -> pl.DataFrame:
        sensors = self._read_bronze_snapshot("sensor", run)
        fields = self._read_bronze_snapshot("field", run).select(
            "field_id", "field_code", "farm_id"
        )
        return sensors.join(fields, on="field_id", how="inner")

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return frame.select(
            pl.col("sensor_id").cast(pl.Int64),
            pl.col("sensor_code").str.strip_chars(),
            pl.col("field_id").cast(pl.Int64),
            pl.col("field_code").str.strip_chars(),
            pl.col("farm_id").cast(pl.Int64),
            pl.col("sensor_type").str.strip_chars().str.to_lowercase(),
            pl.col("model").str.strip_chars(),
            pl.col("depth_cm").cast(pl.Int32),
            pl.col("installed_on").cast(pl.Date),
            pl.col("status").str.strip_chars().str.to_lowercase(),
        )


class SilverFactSensorReadingPipeline(_SilverPipeline):
    """Sensor reading fact, keyed up to field and farm.

    Readings from decommissioned sensors are excluded: they are stale device
    noise rather than measurements, and averaging them would drag every field
    metric off. Faulty sensors are kept — their readings are real, and the Gold
    layer decides what to do with them.
    """

    name = "silver.fact_sensor_reading"
    dataset = "fact_sensor_reading"
    schema = SILVER_FACT_SENSOR_READING

    def extract(self, run: RunContext) -> pl.DataFrame:
        # Read the partition as a directory, not a single file: the incremental
        # Bronze pipeline appends one file per batch, so a date that was ingested
        # more than once holds several.
        path = self.context.hdfs.zone_path(
            LakeZone.BRONZE, "sensor_reading", f"{INGEST_PARTITION_KEY}={run.partition}"
        )
        readings = self.context.hdfs.read_parquet_dir(path, missing_ok=True)
        if readings.is_empty():
            return readings

        sensors = self._read_bronze_snapshot("sensor", run).select(
            "sensor_id", "field_id", "status"
        )
        fields = self._read_bronze_snapshot("field", run).select("field_id", "farm_id")

        return (
            readings.join(sensors, on="sensor_id", how="inner")
            .join(fields, on="field_id", how="inner")
            .filter(pl.col("status") != "decommissioned")
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return frame.select(
            pl.col("reading_id").cast(pl.Int64),
            pl.col("sensor_id").cast(pl.Int64),
            pl.col("field_id").cast(pl.Int64),
            pl.col("farm_id").cast(pl.Int64),
            pl.col("reading_ts"),
            # Materialised so Gold can group without recomputing, and so the
            # ClickHouse fact table can partition on it.
            pl.col("reading_ts").dt.date().alias("reading_date"),
            pl.col("soil_moisture_pct").cast(pl.Float64),
            pl.col("soil_temperature_c").cast(pl.Float64),
            pl.col("soil_ph").cast(pl.Float64),
            pl.col("soil_ec_ds_m").cast(pl.Float64),
            pl.col("battery_pct").cast(pl.Float64),
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        # Partitioned by ingest date to mirror Bronze, so a re-ingest of one
        # window replaces exactly the Silver partition it produced.
        return self._write_silver(frame, INGEST_PARTITION_KEY, run)
