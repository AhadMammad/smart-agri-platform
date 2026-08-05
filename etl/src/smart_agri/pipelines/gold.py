"""Gold: the business-ready daily field metrics the dashboard reads.

One row per field per day, with the dimension attributes already joined so a
chart needs no joins at query time, plus the moisture-stress classification that
turns a raw percentage into something an agronomist can filter on.

Phase 2 recomputes the whole aggregate on every run. At demo scale that costs
nothing and removes a class of partial-update bugs; Phase 6 makes it
incremental once there is enough history for it to matter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from smart_agri.config import LakeZone
from smart_agri.contracts import GOLD_FIELD_SOIL_DAILY
from smart_agri.pipelines.base import BasePipeline
from smart_agri.pipelines.bronze import DATA_FILE, SNAPSHOT_PARTITION_KEY
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from smart_agri.pipelines.base import RunContext

logger = get_logger(__name__)

RUN_PARTITION_KEY = "run_date"

# Volumetric moisture bands used to classify field condition. Deliberately
# coarse and soil-independent for Phase 2; Phase 6 refines them per soil type,
# where field capacity and wilting point actually differ.
DRY_THRESHOLD_PCT = 15.0
WET_THRESHOLD_PCT = 38.0


class GoldFieldSoilDailyPipeline(BasePipeline):
    """Daily soil metrics per field."""

    name = "gold.field_soil_daily"
    dataset = "field_soil_daily"
    schema = GOLD_FIELD_SOIL_DAILY

    def extract(self, run: RunContext) -> pl.DataFrame:
        """Read every Silver fact partition, plus the current dimensions.

        Reading all partitions is what makes this a full recompute; the
        dimensions come from the current snapshot so a renamed field shows its
        new name across all history.
        """
        facts = self.context.hdfs.read_parquet_dir(
            self.context.hdfs.zone_path(LakeZone.SILVER, "fact_sensor_reading"),
            missing_ok=True,
        )
        if facts.is_empty():
            logger.warning("no_silver_facts", pipeline=self.name)
            return facts

        farms = self._read_silver_dim("dim_farm", run)
        fields = self._read_silver_dim("dim_field", run)

        return facts.join(
            fields.select("field_id", "field_code", "field_name", "soil_type", "area_ha").rename(
                {"area_ha": "field_area_ha"}
            ),
            on="field_id",
            how="inner",
        ).join(
            farms.select("farm_id", "farm_code", "farm_name", "country_code", "region"),
            on="farm_id",
            how="inner",
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return frame

        aggregated = frame.group_by(
            "reading_date",
            "farm_id",
            "farm_code",
            "farm_name",
            "country_code",
            "region",
            "field_id",
            "field_code",
            "field_name",
            "soil_type",
            "field_area_ha",
        ).agg(
            # Cast explicitly: `n_unique` and `len` yield UInt32, while the Gold
            # contract and the ClickHouse column are both Int64.
            pl.col("sensor_id").n_unique().cast(pl.Int64).alias("active_sensors"),
            pl.len().cast(pl.Int64).alias("reading_count"),
            pl.col("soil_moisture_pct").mean().alias("avg_soil_moisture_pct"),
            pl.col("soil_moisture_pct").min().alias("min_soil_moisture_pct"),
            pl.col("soil_moisture_pct").max().alias("max_soil_moisture_pct"),
            pl.col("soil_temperature_c").mean().alias("avg_soil_temperature_c"),
            pl.col("soil_ph").mean().alias("avg_soil_ph"),
            pl.col("soil_ec_ds_m").mean().alias("avg_soil_ec_ds_m"),
            pl.col("battery_pct").min().alias("min_battery_pct"),
        )

        return (
            aggregated.with_columns(
                # A day where every probe dropped its moisture channel is
                # "unknown", not "dry" — the distinction matters on a dashboard
                # that drives irrigation decisions.
                pl.when(pl.col("avg_soil_moisture_pct").is_null())
                .then(pl.lit("unknown"))
                .when(pl.col("avg_soil_moisture_pct") < DRY_THRESHOLD_PCT)
                .then(pl.lit("dry"))
                .when(pl.col("avg_soil_moisture_pct") > WET_THRESHOLD_PCT)
                .then(pl.lit("wet"))
                .otherwise(pl.lit("optimal"))
                .alias("moisture_stress")
            )
            .with_columns(
                pl.col(
                    "avg_soil_moisture_pct",
                    "min_soil_moisture_pct",
                    "max_soil_moisture_pct",
                    "avg_soil_temperature_c",
                    "avg_soil_ph",
                    "avg_soil_ec_ds_m",
                    "min_battery_pct",
                ).round(3)
            )
            .select(GOLD_FIELD_SOIL_DAILY.columns.keys())
            .sort("reading_date", "field_id")
        )

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        directory = self._partition_path(LakeZone.GOLD, RUN_PARTITION_KEY, run)
        self.context.hdfs.remove(directory)
        return self.context.hdfs.write_parquet(frame, f"{directory}/{DATA_FILE}")

    def _read_silver_dim(self, dataset: str, run: RunContext) -> pl.DataFrame:
        path = self.context.hdfs.zone_path(
            LakeZone.SILVER, dataset, f"{SNAPSHOT_PARTITION_KEY}={run.partition}", DATA_FILE
        )
        return self.context.hdfs.read_parquet(path)
