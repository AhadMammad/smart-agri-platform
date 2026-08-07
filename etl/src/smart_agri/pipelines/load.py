"""Load: lake to ClickHouse.

Three shapes cover every table in the warehouse, so a load is a declaration
rather than a class:

* **Dimensions** read the current Silver snapshot — one file, the whole table.
* **Facts** read every Silver partition and deduplicate on the source key,
  because incremental Silver lands one file per batch and a re-ingest of an
  overlapping window can present the same row twice.
* **Gold** reads the run partition of an aggregate.

Loads are full-refresh: `replace_table` swaps the contents in one step, which is
trivially idempotent and removes an entire class of partial-update bugs. At the
volumes here that costs nothing. `ClickHouseSink.delete_partition` exists for
the point where it stops being free.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from smart_agri.config import LakeZone
from smart_agri.pipelines.base import BasePipeline
from smart_agri.pipelines.bronze import DATA_FILE, SNAPSHOT_PARTITION_KEY
from smart_agri.pipelines.gold import RUN_PARTITION_KEY
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


@dataclass(frozen=True, slots=True)
class FactLoadSpec:
    """Declares a fact load.

    `dedupe_on` is the source primary key, and it is not optional: without it a
    re-extract that overlaps an earlier batch double-counts every row it
    repeats, which shows up as a silently inflated total rather than an error.
    """

    dataset: str
    table: str
    dedupe_on: str
    sort_by: str


@dataclass(frozen=True, slots=True)
class GoldLoadSpec:
    """Declares a Gold aggregate load."""

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


class LoadFactPipeline(BasePipeline):
    """Replace a ClickHouse fact table from every Silver partition."""

    def __init__(self, spec: FactLoadSpec, context: PipelineContext | None = None) -> None:
        self.spec = spec
        self.name = f"load.{spec.dataset}"
        self.dataset = spec.dataset
        super().__init__(context)

    def extract(self, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return self.context.hdfs.read_parquet_dir(
            self.context.hdfs.zone_path(LakeZone.SILVER, self.dataset),
            missing_ok=True,
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return frame
        return frame.unique(subset=[self.spec.dedupe_on], keep="last").sort(self.spec.sort_by)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.spec.table, frame)


class LoadGoldPipeline(BasePipeline):
    """Replace a ClickHouse aggregate from this run's Gold partition."""

    def __init__(self, spec: GoldLoadSpec, context: PipelineContext | None = None) -> None:
        self.spec = spec
        self.name = f"load.{spec.dataset}"
        self.dataset = spec.dataset
        super().__init__(context)

    def extract(self, run: RunContext) -> pl.DataFrame:
        path = self.context.hdfs.zone_path(
            LakeZone.GOLD, self.dataset, f"{RUN_PARTITION_KEY}={run.partition}", DATA_FILE
        )
        return self.context.hdfs.read_parquet(path)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:  # noqa: ARG002
        return self.context.clickhouse.replace_table(self.spec.table, frame)


#: Every dimension in the star schema. The ClickHouse table name matches the
#: Silver dataset throughout, so the warehouse and the lake stay legible as one
#: model rather than two vocabularies.
DIM_LOAD_SPECS: tuple[DimLoadSpec, ...] = (
    # Phase 2
    DimLoadSpec(dataset="dim_farm", table="dim_farm"),
    DimLoadSpec(dataset="dim_field", table="dim_field"),
    DimLoadSpec(dataset="dim_sensor", table="dim_sensor"),
    # Phase 6 — reference
    DimLoadSpec(dataset="dim_region", table="dim_region"),
    DimLoadSpec(dataset="dim_soil_type", table="dim_soil_type"),
    DimLoadSpec(dataset="dim_crop", table="dim_crop"),
    DimLoadSpec(dataset="dim_crop_variety", table="dim_crop_variety"),
    # Phase 6 — operations and machinery
    DimLoadSpec(dataset="dim_planting", table="dim_planting"),
    DimLoadSpec(dataset="dim_machine", table="dim_machine"),
)

#: Every fact. `sort_by` is the natural time order, which makes the loaded table
#: match the lake byte for byte on a re-run and keeps ClickHouse's parts sorted
#: the way the primary key expects.
FACT_LOAD_SPECS: tuple[FactLoadSpec, ...] = (
    # Phase 2
    FactLoadSpec("fact_sensor_reading", "fact_sensor_reading", "reading_id", "reading_ts"),
    # Phase 6 — operations
    FactLoadSpec("fact_irrigation", "fact_irrigation", "irrigation_id", "event_ts"),
    FactLoadSpec(
        "fact_input_application", "fact_input_application", "application_id", "applied_on"
    ),
    FactLoadSpec("fact_harvest", "fact_harvest", "harvest_id", "harvested_on"),
    FactLoadSpec("fact_field_cost", "fact_field_cost", "cost_id", "cost_date"),
    # Phase 6 — machinery
    FactLoadSpec("fact_machine_operation", "fact_machine_operation", "operation_id", "started_at"),
    FactLoadSpec("fact_machine_telemetry", "fact_machine_telemetry", "telemetry_id", "reading_ts"),
    FactLoadSpec("fact_machine_fault", "fact_machine_fault", "fault_id", "occurred_at"),
    # Phase 6 — imagery
    FactLoadSpec("fact_field_index", "fact_field_index", "observation_id", "observed_on"),
)

#: Gold aggregates. The `agg_` prefix marks a table as pre-aggregated, so a
#: chart author can tell at a glance which tables are safe to scan whole.
GOLD_LOAD_SPECS: tuple[GoldLoadSpec, ...] = (
    GoldLoadSpec(dataset="field_soil_daily", table="agg_field_soil_daily"),
    GoldLoadSpec(dataset="field_crop_health_daily", table="agg_field_crop_health_daily"),
    GoldLoadSpec(dataset="field_irrigation_daily", table="agg_field_irrigation_daily"),
    GoldLoadSpec(dataset="machine_daily", table="agg_machine_daily"),
    GoldLoadSpec(dataset="planting_economics", table="agg_planting_economics"),
)
