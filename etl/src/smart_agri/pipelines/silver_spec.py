"""Declarative Silver: conform a Bronze dataset without writing a pipeline.

Phase 2 wrote one Silver class per dataset. That does not scale to twenty
tables, and the plan asks for the opposite: adding a table should be
configuration, not code.

Everything Silver actually does to a Bronze extract is one of a handful of
operations — bring in a key from another dataset, rename a column to its
analytical name, normalise case and whitespace, coerce a type, drop the
audit columns, deduplicate. A `SilverSpec` names which of those apply, and one
generic pipeline executes them in a fixed order.

The order is deliberate: joins first (so a renamed column cannot break a join
key), then renames, then cleaning, then casts, then column selection, then
deduplication.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.config import LakeZone
from smart_agri.pipelines.base import BasePipeline
from smart_agri.pipelines.bronze import (
    DATA_FILE,
    INGEST_PARTITION_KEY,
    SNAPSHOT_PARTITION_KEY,
)
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import pandera.polars as pa
    from polars.datatypes import DataTypeClass

    #: `pl.Float64` is a DataTypeClass while `pl.Float64()` is a DataType.
    #: Polars accepts either, so the spec does too.
    CastTarget = DataTypeClass | pl.DataType

    from smart_agri.pipelines.base import PipelineContext, RunContext

logger = get_logger(__name__)

#: Source bookkeeping columns. They belong in Bronze, where they record when the
#: row was captured, but carrying them into Silver invites a chart to group by
#: an ingestion timestamp and call it a business date.
AUDIT_COLUMNS: tuple[str, ...] = ("created_at", "updated_at")


@dataclass(frozen=True, slots=True)
class JoinSpec:
    """A key or attribute pulled in from another Bronze snapshot.

    Always an inner join. A fact whose parent is missing is a referential break,
    and letting it through with a null key would put an orphan on a dashboard.
    """

    dataset: str
    on: tuple[str, ...]
    columns: tuple[str, ...]
    """Columns to bring across, besides the join keys."""


@dataclass(frozen=True, slots=True, eq=False)
class SilverSpec:
    """How one Bronze dataset becomes one Silver dataset."""

    dataset: str
    bronze: str
    schema: pa.DataFrameSchema

    incremental: bool = False
    """Read the ingest partition rather than the snapshot partition."""

    joins: tuple[JoinSpec, ...] = ()
    filters: tuple[pl.Expr, ...] = ()
    """Rows to exclude, evaluated after the joins so they can use joined columns.

    Filtering is a Silver concern, not a Bronze one: Bronze keeps whatever the
    source sent, and Silver decides what belongs in the conformed model.
    """

    rename: Mapping[str, str] = dataclass_field(default_factory=dict)
    trim: tuple[str, ...] = ()
    """Text columns to strip of surrounding whitespace."""

    lower: tuple[str, ...] = ()
    upper: tuple[str, ...] = ()
    cast: Mapping[str, CastTarget] = dataclass_field(default_factory=dict)
    derive: Mapping[str, pl.Expr] = dataclass_field(default_factory=dict)
    """Columns computed from others — a date off a timestamp, a duration, a flag.

    Materialised here rather than left to every consumer: a derivation repeated
    in a dozen chart definitions is a dozen chances to write it differently.
    """

    dedupe_on: tuple[str, ...] = ()
    """Natural key. A re-ingest can present the same row twice."""

    exclude: tuple[str, ...] = AUDIT_COLUMNS

    @property
    def partition_key(self) -> str:
        return INGEST_PARTITION_KEY if self.incremental else SNAPSHOT_PARTITION_KEY


class SilverPipeline(BasePipeline):
    """Applies a `SilverSpec`. One class for every conformed dataset."""

    def __init__(self, spec: SilverSpec, context: PipelineContext | None = None) -> None:
        self.spec = spec
        self.name = f"silver.{spec.dataset}"
        self.dataset = spec.dataset
        self.schema = spec.schema
        super().__init__(context)

    # -- extract -------------------------------------------------------------
    def extract(self, run: RunContext) -> pl.DataFrame:
        frame = self._read_bronze(self.spec.bronze, run, incremental=self.spec.incremental)
        if frame.is_empty():
            logger.warning("no_bronze_rows", pipeline=self.name, dataset=self.spec.bronze)
            return frame

        for join in self.spec.joins:
            lookup = self._read_bronze(join.dataset, run, incremental=False)
            wanted = [*join.on, *join.columns]
            missing = [column for column in wanted if column not in lookup.columns]
            if missing:
                msg = f"{join.dataset} is missing join columns {missing}"
                raise KeyError(msg)

            # Drop any column the join would duplicate: the fact's own copy is
            # the one Silver keeps, and a `_right` suffix would break the
            # column selection below.
            clash = [c for c in join.columns if c in frame.columns]
            source = frame.drop(clash) if clash else frame
            frame = source.join(lookup.select(wanted), on=list(join.on), how="inner")

        if self.spec.filters:
            before = frame.height
            for condition in self.spec.filters:
                frame = frame.filter(condition)
            if frame.height != before:
                logger.info(
                    "silver_filtered",
                    pipeline=self.name,
                    excluded=before - frame.height,
                )

        return frame

    def _read_bronze(self, dataset: str, run: RunContext, *, incremental: bool) -> pl.DataFrame:
        key = INGEST_PARTITION_KEY if incremental else SNAPSHOT_PARTITION_KEY
        base = self.context.hdfs.zone_path(LakeZone.BRONZE, dataset, f"{key}={run.partition}")

        if incremental:
            # An incremental partition holds one file per batch.
            return self.context.hdfs.read_parquet_dir(base, missing_ok=True)
        return self.context.hdfs.read_parquet(f"{base}/{DATA_FILE}")

    # -- transform -----------------------------------------------------------
    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        """Apply the spec in a fixed order.

        Renames precede cleaning so the clean lists name analytical columns;
        casts precede derivations so a derivation sees a coerced type; the
        column selection is last but one, so everything the schema wants exists
        by then and everything it does not is dropped.
        """
        if frame.is_empty():
            return frame

        frame = self._rename(frame)
        frame = self._clean(frame)
        frame = self._cast_and_derive(frame)
        frame = self._select(frame)
        return self._dedupe(frame)

    def _rename(self, frame: pl.DataFrame) -> pl.DataFrame:
        present = {old: new for old, new in self.spec.rename.items() if old in frame.columns}
        return frame.rename(present) if present else frame

    def _clean(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Normalise text: strip whitespace, then case where the spec asks."""
        spec = self.spec
        cleaners: list[pl.Expr] = []

        for column in spec.trim:
            if column in frame.columns:
                cleaners.append(pl.col(column).str.strip_chars())
        for column, transform in (("lower", "to_lowercase"), ("upper", "to_uppercase")):
            for name in getattr(spec, column):
                if name in frame.columns:
                    stripped = pl.col(name).str.strip_chars()
                    cleaners.append(getattr(stripped.str, transform)())

        return frame.with_columns(cleaners) if cleaners else frame

    def _cast_and_derive(self, frame: pl.DataFrame) -> pl.DataFrame:
        spec = self.spec

        casts = [
            pl.col(column).cast(dtype)
            for column, dtype in spec.cast.items()
            if column in frame.columns
        ]
        if casts:
            frame = frame.with_columns(casts)

        # After the casts, so a derivation sees the coerced type rather than
        # whatever the driver happened to hand back.
        if spec.derive:
            frame = frame.with_columns([expr.alias(name) for name, expr in spec.derive.items()])
        return frame

    def _select(self, frame: pl.DataFrame) -> pl.DataFrame:
        wanted = list(self.spec.schema.columns.keys())
        absent = [column for column in wanted if column not in frame.columns]
        if absent:
            msg = (
                f"{self.name}: Bronze is missing {absent}. Check the spec's renames, "
                f"joins and derivations against the source table."
            )
            raise KeyError(msg)
        return frame.select(wanted)

    def _dedupe(self, frame: pl.DataFrame) -> pl.DataFrame:
        spec = self.spec
        if not spec.dedupe_on:
            return frame

        before = frame.height
        frame = frame.unique(subset=list(spec.dedupe_on), keep="last")
        if frame.height != before:
            logger.info(
                "silver_deduplicated",
                pipeline=self.name,
                dropped=before - frame.height,
                key=list(spec.dedupe_on),
            )
        # Sorted on the primary key so a re-run writes byte-identical Parquet.
        return frame.sort(next(iter(spec.schema.columns)))

    # -- load ----------------------------------------------------------------
    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        directory = self._partition_path(LakeZone.SILVER, self.spec.partition_key, run)
        self.context.hdfs.remove(directory)
        return self.context.hdfs.write_parquet(frame, f"{directory}/{DATA_FILE}")


def silver_dataset_names(specs: Sequence[SilverSpec]) -> tuple[str, ...]:
    """Dataset names in declaration order, for the DAGs and the registry."""
    return tuple(spec.dataset for spec in specs)
