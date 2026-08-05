"""Load: lake to ClickHouse.

Gold and Silver Parquet is read into Polars and inserted as Arrow. Phase 2 uses
full-refresh loads — the datasets are small, and replacing a table is trivially
idempotent, which is the property that matters while the pipeline is still being
proven end to end. `ClickHouseSink.delete_partition` already exists for Phase 6,
where volumes make partition-level replacement worthwhile.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from smart_agri.config import LakeZone
from smart_agri.pipelines.base import BasePipeline
from smart_agri.pipelines.bronze import DATA_FILE, SNAPSHOT_PARTITION_KEY
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    import polars as pl

    from smart_agri.pipelines.base import PipelineContext, RunContext

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class DimLoadSpec:
    """Declares a dimension load. Adding one is data, not code."""

    dataset: str
    table: str


class LoadDimensionPipeline(BasePipeline):
    """Replace a ClickHouse dimension from its current Silver snapshot."""

    def __init__(self, spec: DimLoadSpec, context: PipelineContext | None = None) -> None:
        self.spec = spec
        self.name = f"load.{spec.dataset}"
        self.dataset = spec.dataset
        super().__init__(context)

    def extract(self, run: RunContext) -> pl.DataFrame:
        path = self.context.hdfs.zone_path(
            LakeZone.SILVER,
            self.spec.dataset,
            f"{SNAPSHOT_PARTITION_KEY}={run.partition}",
            DATA_FILE,
        )
        return self.context.hdfs.read_parquet(path)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.spec.table, frame)


class LoadFactSensorReadingPipeline(BasePipeline):
    """Replace the reading fact table from every Silver partition."""

    name = "load.fact_sensor_reading"
    dataset = "fact_sensor_reading"
    table = "fact_sensor_reading"

    def extract(self, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return self.context.hdfs.read_parquet_dir(
            self.context.hdfs.zone_path(LakeZone.SILVER, self.dataset),
            missing_ok=True,
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return frame
        # A re-ingest can produce the same reading in two partitions; the source
        # primary key is what makes the load deduplicable.
        return frame.unique(subset=["reading_id"], keep="last").sort("reading_ts")

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.table, frame)


class LoadGoldFieldSoilDailyPipeline(BasePipeline):
    """Replace the daily field aggregate that the dashboard queries."""

    name = "load.field_soil_daily"
    dataset = "field_soil_daily"
    table = "agg_field_soil_daily"

    def extract(self, run: RunContext) -> pl.DataFrame:
        from smart_agri.pipelines.gold import RUN_PARTITION_KEY

        path = self.context.hdfs.zone_path(
            LakeZone.GOLD, self.dataset, f"{RUN_PARTITION_KEY}={run.partition}", DATA_FILE
        )
        return self.context.hdfs.read_parquet(path)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.table, frame)


DIM_LOAD_SPECS: tuple[DimLoadSpec, ...] = (
    DimLoadSpec(dataset="dim_farm", table="dim_farm"),
    DimLoadSpec(dataset="dim_field", table="dim_field"),
    DimLoadSpec(dataset="dim_sensor", table="dim_sensor"),
)
