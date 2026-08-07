"""Bronze: raw extracts from Postgres, landed as Parquet.

Two strategies, matching the plan:

* **Snapshot** for dimensions and reference data. They are small, so a full copy
  per run is simpler than change tracking and gives every run a complete,
  self-contained picture.
* **Incremental** for facts. Rows are pulled where the watermark column has
  advanced, and the watermark moves *only after* the write succeeds — so a
  crash mid-run re-extracts rather than skips.

Both are driven by a spec rather than a subclass per table: adding a source is
declaring a `SnapshotSpec` or an `IncrementalSpec`, not writing a pipeline.

Bronze does not clean anything. Data is stored as extracted, because a Bronze
layer that "fixes" its input destroys the evidence needed to debug the source.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from smart_agri.config import LakeZone
from smart_agri.contracts import (
    BRONZE_CROP,
    BRONZE_CROP_VARIETY,
    BRONZE_FARM,
    BRONZE_FIELD,
    BRONZE_FIELD_COST,
    BRONZE_FIELD_INDEX_OBSERVATION,
    BRONZE_HARVEST,
    BRONZE_INPUT_APPLICATION,
    BRONZE_IRRIGATION_EVENT,
    BRONZE_MACHINE,
    BRONZE_MACHINE_FAULT,
    BRONZE_MACHINE_OPERATION,
    BRONZE_MACHINE_TELEMETRY,
    BRONZE_PLANTING,
    BRONZE_REGION,
    BRONZE_SENSOR,
    BRONZE_SENSOR_READING,
    BRONZE_SOIL_TYPE,
)
from smart_agri.pipelines.base import BasePipeline
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    import pandera.polars as pa
    import polars as pl

    from smart_agri.pipelines.base import PipelineContext, RunContext

logger = get_logger(__name__)

SNAPSHOT_PARTITION_KEY = "snapshot_date"
INGEST_PARTITION_KEY = "ingest_date"
DATA_FILE = "data.parquet"


@dataclass(frozen=True, slots=True)
class SnapshotSpec:
    """A source table copied in full each run."""

    dataset: str
    source_table: str
    schema: pa.DataFrameSchema


@dataclass(frozen=True, slots=True)
class IncrementalSpec:
    """A source table extracted by watermark."""

    dataset: str
    source_table: str
    schema: pa.DataFrameSchema
    watermark_column: str = "updated_at"


class BronzeSnapshotPipeline(BasePipeline):
    """Full copy of a source table into Bronze.

    Idempotent: the partition directory is cleared before it is written, so a
    re-run for the same date replaces rather than appends.
    """

    def __init__(self, spec: SnapshotSpec, context: PipelineContext | None = None) -> None:
        self.spec = spec
        self.name = f"bronze.{spec.dataset}"
        self.dataset = spec.dataset
        self.schema = spec.schema
        super().__init__(context)

    def extract(self, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        return self.context.postgres.read_query(f"SELECT * FROM {self.spec.source_table}")

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        directory = self._partition_path(LakeZone.BRONZE, SNAPSHOT_PARTITION_KEY, run)
        self.context.hdfs.remove(directory)
        return self.context.hdfs.write_parquet(frame, f"{directory}/{DATA_FILE}")


class BronzeIncrementalPipeline(BasePipeline):
    """Watermark-driven extract of a fact table into Bronze."""

    def __init__(self, spec: IncrementalSpec, context: PipelineContext | None = None) -> None:
        self.spec = spec
        self.name = f"bronze.{spec.dataset}"
        self.dataset = spec.dataset
        self.schema = spec.schema
        super().__init__(context)

    def extract(self, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        """Rows newer than the watermark, oldest first.

        A missing watermark means the pipeline has never completed, which is a
        first full load rather than an error.
        """
        column = self.spec.watermark_column
        watermark = self.context.control.get_watermark(self.name)

        if watermark is None:
            logger.info("no_watermark_full_extract", pipeline=self.name)
            sql = f"SELECT * FROM {self.spec.source_table} ORDER BY {column}"
            params = None
        else:
            logger.info("incremental_extract", pipeline=self.name, since=watermark.isoformat())
            sql = (
                f"SELECT * FROM {self.spec.source_table} " f"WHERE {column} > $1 ORDER BY {column}"
            )
            params = [watermark]

        return self.context.postgres.read_query(sql, params)

    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        """Append this batch to the partition, then advance the watermark.

        Two decisions here are load-bearing:

        **Append, never replace.** The snapshot pipelines clear their partition
        before writing because they re-extract everything. This one does not:
        the watermark means a later run extracts only *newer* rows, so replacing
        the partition would delete every row the earlier runs had landed.

        **An empty batch writes nothing.** Re-running after the watermark has
        caught up must be a no-op, not an instruction to overwrite a populated
        partition with zero rows — which would silently empty the warehouse
        three stages downstream.

        The watermark advances only after the write succeeds; the other order
        would drop rows whenever a write failed.
        """
        if frame.is_empty():
            logger.info("no_new_rows", pipeline=self.name, partition=run.partition)
            return 0

        # Narrow the type explicitly: Polars' `.max()` is typed as Any, and
        # the datetime formatting below is only meaningful for a datetime.
        high_water = cast("datetime", frame[self.spec.watermark_column].max())
        directory = self._partition_path(LakeZone.BRONZE, INGEST_PARTITION_KEY, run)

        # Named after the batch's high-water mark: a re-extract of the same
        # batch overwrites the same file, while a genuinely newer batch lands
        # beside it. Deterministic, so re-runs never accumulate duplicates.
        filename = f"part-{high_water:%Y%m%dT%H%M%S%f}.parquet"
        written = self.context.hdfs.write_parquet(frame, f"{directory}/{filename}")

        self.context.control.set_watermark(
            pipeline=self.name,
            source_table=self.spec.source_table,
            watermark_column=self.spec.watermark_column,
            value=high_water,
        )
        return written


# --- source declarations -----------------------------------------------------
#
# Small, slowly-changing tables are snapshotted; anything that grows with time
# is extracted by watermark. Adding a source means adding a line here.

SNAPSHOT_SPECS: tuple[SnapshotSpec, ...] = (
    # Phase 2 — the soil-sensor slice
    SnapshotSpec("farm", "agri.farm", BRONZE_FARM),
    SnapshotSpec("field", "agri.field", BRONZE_FIELD),
    SnapshotSpec("sensor", "agri.sensor", BRONZE_SENSOR),
    # Phase 5 — reference data and the remaining dimensions
    SnapshotSpec("region", "agri.region", BRONZE_REGION),
    SnapshotSpec("soil_type", "agri.soil_type", BRONZE_SOIL_TYPE),
    SnapshotSpec("crop", "agri.crop", BRONZE_CROP),
    SnapshotSpec("crop_variety", "agri.crop_variety", BRONZE_CROP_VARIETY),
    SnapshotSpec("machine", "agri.machine", BRONZE_MACHINE),
    # A planting is small and mutable — its status changes from growing to
    # harvested — so a snapshot keeps the current truth without change tracking.
    SnapshotSpec("planting", "agri.planting", BRONZE_PLANTING),
)

INCREMENTAL_SPECS: tuple[IncrementalSpec, ...] = (
    # Phase 2
    IncrementalSpec("sensor_reading", "agri.sensor_reading", BRONZE_SENSOR_READING),
    # Phase 5 — operations
    IncrementalSpec("irrigation_event", "agri.irrigation_event", BRONZE_IRRIGATION_EVENT),
    IncrementalSpec("input_application", "agri.input_application", BRONZE_INPUT_APPLICATION),
    IncrementalSpec("harvest", "agri.harvest", BRONZE_HARVEST),
    IncrementalSpec("field_cost", "agri.field_cost", BRONZE_FIELD_COST),
    # Phase 5 — machinery
    IncrementalSpec("machine_operation", "agri.machine_operation", BRONZE_MACHINE_OPERATION),
    IncrementalSpec("machine_telemetry", "agri.machine_telemetry", BRONZE_MACHINE_TELEMETRY),
    IncrementalSpec("machine_fault", "agri.machine_fault", BRONZE_MACHINE_FAULT),
    # Phase 5 — imagery
    IncrementalSpec(
        "field_index_observation",
        "agri.field_index_observation",
        BRONZE_FIELD_INDEX_OBSERVATION,
    ),
)
