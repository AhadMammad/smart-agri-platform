"""The pipeline template every stage is built from.

`BasePipeline` fixes the shape — extract, transform, validate, load — and owns
everything that must happen identically for every pipeline: run-log bookkeeping,
quarantining rejected rows, and structured logging.

Note the ordering. The plan named the steps extract → validate → transform →
load; they run here as extract → **transform** → **validate** → load, because a
schema is only meaningful against a stage's *output*. Bronze transforms nothing,
so its validation still guards the raw extract exactly as intended.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.config import LakeZone, Settings, get_settings
from smart_agri.contracts import ValidationResult, validate
from smart_agri.io import ClickHouseSink, ControlStore, HdfsStore, PostgresSource, RunStats
from smart_agri.io.control import RunStatus
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from datetime import date

    import pandera.polars as pa

logger = get_logger(__name__)


class PipelineContext:
    """Everything a pipeline needs from the outside world.

    Each connector is built lazily and can be replaced at construction, which is
    what lets a pipeline be unit-tested against fakes without a live stack.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        postgres: PostgresSource | None = None,
        hdfs: HdfsStore | None = None,
        control: ControlStore | None = None,
        clickhouse: ClickHouseSink | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._postgres = postgres
        self._hdfs = hdfs
        self._control = control
        self._clickhouse = clickhouse

    @property
    def postgres(self) -> PostgresSource:
        if self._postgres is None:
            self._postgres = PostgresSource(self.settings.postgres)
        return self._postgres

    @property
    def hdfs(self) -> HdfsStore:
        if self._hdfs is None:
            self._hdfs = HdfsStore(self.settings.hdfs)
        return self._hdfs

    @property
    def control(self) -> ControlStore:
        if self._control is None:
            self._control = ControlStore(self.settings.postgres)
        return self._control

    @property
    def clickhouse(self) -> ClickHouseSink:
        if self._clickhouse is None:
            self._clickhouse = ClickHouseSink(self.settings.clickhouse)
        return self._clickhouse


@dataclass(frozen=True, slots=True)
class RunContext:
    """Per-execution state handed to each step."""

    logical_date: date
    pipeline: str

    @property
    def partition(self) -> str:
        """The logical date as it appears in a lake partition directory."""
        return self.logical_date.isoformat()


class BasePipeline(ABC):
    """One stage of one dataset.

    Subclasses declare `name` and `dataset`, implement `extract` and `load`, and
    optionally override `transform`. Validation is automatic whenever `schema`
    is set.
    """

    name: str
    dataset: str
    schema: pa.DataFrameSchema | None = None

    #: Rejection rate above which the run fails instead of quarantining. A
    #: handful of bad probe readings is normal; half the file failing means the
    #: source or the contract has changed and the run must not proceed.
    max_rejection_rate: float = 0.10

    def __init__(self, context: PipelineContext | None = None) -> None:
        self.context = context or PipelineContext()

    # -- steps ---------------------------------------------------------------
    @abstractmethod
    def extract(self, run: RunContext) -> pl.DataFrame:
        """Read this stage's input."""

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        """Reshape the extracted data. Identity unless overridden."""
        return frame

    @abstractmethod
    def load(self, frame: pl.DataFrame, run: RunContext) -> int:
        """Write the validated result. Returns the number of rows written."""

    # -- orchestration -------------------------------------------------------
    def run(self, logical_date: date) -> RunStats:
        """Execute the pipeline, recording the outcome in the run log.

        A failure is logged and re-raised: the Airflow task must fail, and the
        run-log row must say why.
        """
        run = RunContext(logical_date=logical_date, pipeline=self.name)
        log = logger.bind(pipeline=self.name, logical_date=run.partition)
        run_id = self.context.control.start_run(self.name, logical_date)

        try:
            extracted = self.extract(run)
            transformed = self.transform(extracted, run)
            validated, quarantined = self._validate(transformed, run)
            written = self.load(validated, run)
        except Exception as exc:
            self.context.control.finish_run(run_id, RunStatus.FAILED, error=str(exc))
            log.error("pipeline_failed", error=str(exc), error_type=type(exc).__name__)
            raise

        stats = RunStats(
            rows_read=extracted.height,
            rows_written=written,
            rows_quarantined=quarantined,
        )
        self.context.control.finish_run(run_id, RunStatus.SUCCESS, stats)
        log.info(
            "pipeline_succeeded",
            rows_read=stats.rows_read,
            rows_written=stats.rows_written,
            rows_quarantined=stats.rows_quarantined,
        )
        return stats

    # -- helpers -------------------------------------------------------------
    def _validate(self, frame: pl.DataFrame, run: RunContext) -> tuple[pl.DataFrame, int]:
        """Validate against `schema`, diverting failures to quarantine."""
        if self.schema is None:
            return frame, 0

        result = validate(frame, self.schema)

        if result.rows_invalid:
            if result.rejection_rate > self.max_rejection_rate:
                msg = (
                    f"{self.name}: {result.rows_invalid} of {result.rows_in} rows failed "
                    f"validation ({result.rejection_rate:.1%}), above the "
                    f"{self.max_rejection_rate:.0%} threshold. "
                    f"Failing checks: {result.failure_summary.to_dicts()}"
                )
                raise ValueError(msg)
            self._quarantine(result, run)

        return result.valid, result.rows_invalid

    def _quarantine(self, result: ValidationResult, run: RunContext) -> None:
        """Persist rejected rows and why they were rejected.

        The rows alone are not enough to act on: a thousand quarantined records
        are a mystery until you know which check failed and how often. The
        summary lands beside them so the answer is one read away rather than a
        re-run with a debugger.
        """
        directory = self.context.hdfs.zone_path(
            LakeZone.QUARANTINE, self.dataset, f"logical_date={run.partition}"
        )
        self.context.hdfs.remove(directory)
        self.context.hdfs.write_parquet(result.invalid, f"{directory}/rejected.parquet")

        report = result.failure_summary.with_columns(
            pl.lit(self.name).alias("pipeline"),
            pl.lit(run.partition).alias("logical_date"),
            pl.lit(result.rows_in).alias("rows_in"),
            pl.lit(result.rows_invalid).alias("rows_rejected"),
            pl.lit(round(result.rejection_rate, 6)).alias("rejection_rate"),
        )
        self.context.hdfs.write_parquet(report, f"{directory}/report.parquet")

        logger.warning(
            "rows_quarantined",
            pipeline=self.name,
            rows=result.rows_invalid,
            rows_in=result.rows_in,
            rejection_rate=round(result.rejection_rate, 4),
            checks=result.failure_summary.select("check").to_series().to_list(),
            path=directory,
        )

    def _partition_path(self, zone: LakeZone, key: str, run: RunContext) -> str:
        """Directory for this run's partition of the dataset."""
        return self.context.hdfs.zone_path(zone, self.dataset, f"{key}={run.partition}")
