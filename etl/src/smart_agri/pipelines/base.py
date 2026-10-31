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
from smart_agri.quality import (
    DEFAULT_VOLUME_RULE,
    CheckCollector,
    CheckResult,
    CheckType,
    Outcome,
    Severity,
    VolumeRule,
    evaluate_volume,
    outcome_for,
)
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    import uuid
    from collections.abc import Sequence
    from datetime import date

    import pandera.polars as pa

logger = get_logger(__name__)

#: Unmatched join keys kept in a check's details: enough to start a diagnosis
#: without turning one result row into a copy of the orphaned data.
_SAMPLE_KEYS = 5


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
            self._control = ControlStore(self.settings.etl_meta)
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

    #: The ClickHouse table `load` writes, if any. Setting it is what turns on
    #: the post-load reconciliation.
    warehouse_table: str | None = None

    #: The band this run's row count must fall in, relative to recent runs.
    #: `None` for a pipeline whose batches legitimately vary — an incremental
    #: extract is empty whenever the source is quiet.
    volume_rule: VolumeRule | None = DEFAULT_VOLUME_RULE

    def __init__(self, context: PipelineContext | None = None) -> None:
        self.context = context or PipelineContext()
        #: Results of the current run. Replaced at the start of each `run`; built
        #: here too so a step exercised on its own still has somewhere to record.
        self.checks = CheckCollector(self.name)

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
        """Execute the pipeline, recording the outcome and every check result.

        Data quality gates sit either side of `load`: an error-severity breach
        before it means nothing is written, one after it means downstream stages
        never start. Either way the results are persisted before the run is
        closed, so a failed run always says which check stopped it.

        A failure is logged and re-raised: the Airflow task must fail, and the
        run-log row must say why.
        """
        run = RunContext(logical_date=logical_date, pipeline=self.name)
        log = logger.bind(pipeline=self.name, logical_date=run.partition)
        self.checks = CheckCollector(self.name, enforce=self.context.settings.dq.enforce)
        run_id = self.context.control.start_run(self.name, logical_date)
        rows_read = written = quarantined = 0

        try:
            extracted = self.extract(run)
            rows_read = extracted.height
            transformed = self.transform(extracted, run)
            validated, quarantined = self._validate(transformed, run)
            self._pre_load_checks(validated, run)
            self.checks.raise_for_errors()
            written = self.load(validated, run)
            self._post_load_checks(written, run)
            self.checks.raise_for_errors()
            self.context.control.record_checks(run_id, self.name, logical_date, self.checks.results)
        except Exception as exc:
            self._persist_checks_best_effort(run_id, run)
            partial = RunStats(rows_read, written, quarantined)
            self.context.control.finish_run(run_id, RunStatus.FAILED, partial, error=str(exc))
            log.error("pipeline_failed", error=str(exc), error_type=type(exc).__name__)
            raise

        stats = RunStats(rows_read=rows_read, rows_written=written, rows_quarantined=quarantined)
        self.context.control.finish_run(run_id, RunStatus.SUCCESS, stats)
        log.info(
            "pipeline_succeeded",
            rows_read=stats.rows_read,
            rows_written=stats.rows_written,
            rows_quarantined=stats.rows_quarantined,
        )
        return stats

    # -- data quality hooks ----------------------------------------------------
    def _pre_load_checks(self, frame: pl.DataFrame, run: RunContext) -> None:  # noqa: ARG002
        """Checks on the validated frame. An error here means nothing is written.

        Compares the row count with recent runs, when the pipeline has a rule.
        Before the load on purpose: a broken extract must not overwrite the lake.
        """
        rule = self.volume_rule
        if rule is None:
            return
        history = self.context.control.recent_runs(self.name, limit=rule.history)
        self.checks.record(
            evaluate_volume(
                rule,
                frame.height,
                [past.rows_written for past in history],
                dataset=self.dataset,
            )
        )

    def _post_load_checks(self, written: int, run: RunContext) -> None:  # noqa: ARG002
        """Checks on what `load` wrote. An error here stops downstream stages.

        Reconciles the warehouse table, when the pipeline loads one.
        """
        if self.warehouse_table is not None:
            self._reconcile_warehouse(self.warehouse_table, written)

    def _reconcile_warehouse(self, table: str, written: int) -> None:
        """The warehouse must hold exactly what was loaded — now, and after merges.

        `row_count` catches an insert that landed short or a concurrent run that
        truncated the table underneath this one. `unique_keys` catches what a
        plain count cannot: a ReplacingMergeTree keeps rows that share a sort key
        until a background merge silently collapses them, so a load with
        duplicate keys reads correctly now and loses rows later.
        """
        sink = self.context.clickhouse
        landed = sink.count(table)
        self._record_reconciliation("reconciliation.row_count", table, landed, written)
        if sink.engine(table) == "ReplacingMergeTree":
            distinct = sink.count(table, final=True)
            self._record_reconciliation("reconciliation.unique_keys", table, distinct, written)

    def _record_reconciliation(self, name: str, table: str, observed: int, expected: int) -> None:
        self.checks.record(
            CheckResult(
                check_name=name,
                check_type=CheckType.RECONCILIATION,
                severity=Severity.ERROR,
                outcome=outcome_for(breached=observed != expected, severity=Severity.ERROR),
                dataset=self.dataset,
                target=table,
                observed_value=observed,
                threshold=expected,
                rows_evaluated=expected,
                rows_failed=abs(expected - observed),
            )
        )

    def _persist_checks_best_effort(self, run_id: uuid.UUID, run: RunContext) -> None:
        """Record results on the failure path without masking the real error.

        If etl-meta-db is what failed, this fails too; the original exception is
        the one the operator needs, so this one is logged and swallowed.
        """
        try:
            self.context.control.record_checks(
                run_id, self.name, run.logical_date, self.checks.results
            )
        except Exception as exc:  # the original failure must propagate, not this
            logger.error(
                "dq_results_not_recorded",
                pipeline=self.name,
                results=len(self.checks.results),
                error=str(exc),
            )

    # -- counted joins and filters -------------------------------------------
    def _inner_join(
        self,
        left: pl.DataFrame,
        right: pl.DataFrame,
        *,
        on: str | Sequence[str],
        target: str,
        max_drop_rate: float | None = 0.0,
        severity: Severity = Severity.WARN,
    ) -> pl.DataFrame:
        """An inner join that records how many left rows found no match.

        An inner join is how an orphan fact is kept off a dashboard — and also
        how a broken parent extract silently empties a mart. The count is what
        tells the two apart. The default flags any orphan as a warning;
        `max_drop_rate=None` only measures, for a join that drops by design.
        """
        keys = [on] if isinstance(on, str) else list(on)
        orphans = left.join(right.select(keys).unique(), on=keys, how="anti")
        self.checks.evaluate_rate(
            f"join.{target}",
            CheckType.JOIN,
            failed=orphans.height,
            total=left.height,
            max_rate=max_drop_rate,
            severity=severity,
            dataset=self.dataset,
            target=target,
            details={
                "on": keys,
                "sample_keys": orphans.select(keys).unique().head(_SAMPLE_KEYS).to_dicts(),
            },
        )
        return left.join(right, on=keys, how="inner")

    def _filter(
        self,
        frame: pl.DataFrame,
        condition: pl.Expr,
        *,
        name: str,
        max_drop_rate: float | None = None,
        severity: Severity = Severity.WARN,
    ) -> pl.DataFrame:
        """A filter that records how many rows it removed.

        Measured only by default: most filters drop rows by design, and a
        threshold belongs on the ones where a sudden rise means a source broke.
        """
        kept = frame.filter(condition)
        self.checks.evaluate_rate(
            f"filter.{name}",
            CheckType.FILTER,
            failed=frame.height - kept.height,
            total=frame.height,
            max_rate=max_drop_rate,
            severity=severity,
            dataset=self.dataset,
            target=name,
        )
        return kept

    def _dedupe_on(self, frame: pl.DataFrame, keys: Sequence[str]) -> pl.DataFrame:
        """Keep the last row per key, and record how many were duplicates."""
        unique = frame.unique(subset=list(keys), keep="last")
        self.checks.evaluate_rate(
            f"dedupe.{'+'.join(keys)}",
            CheckType.DEDUPE,
            failed=frame.height - unique.height,
            total=frame.height,
            max_rate=None,
            severity=Severity.WARN,
            dataset=self.dataset,
            target="+".join(keys),
        )
        return unique

    # -- helpers -------------------------------------------------------------
    def _validate(self, frame: pl.DataFrame, run: RunContext) -> tuple[pl.DataFrame, int]:
        """Validate against `schema`, diverting failures to quarantine.

        Records one result per failing (column, check) and one for the overall
        rejection rate. The rate is the gate: above `max_rejection_rate` the run
        fails and nothing is quarantined, because the frame as a whole is wrong.
        """
        if self.schema is None:
            return frame, 0

        result = validate(frame, self.schema)

        for failure in result.failure_summary.iter_rows(named=True):
            self.checks.record(
                CheckResult(
                    check_name=f"schema.{failure['column']}.{failure['check']}",
                    check_type=CheckType.SCHEMA,
                    severity=Severity.WARN,
                    outcome=Outcome.WARNED,
                    dataset=self.dataset,
                    target=failure["column"],
                    observed_value=failure["failed_rows"] / result.rows_in,
                    rows_evaluated=result.rows_in,
                    rows_failed=failure["failed_rows"],
                    details={"schema": self.schema.name, "check": failure["check"]},
                )
            )

        gate = self.checks.evaluate_rate(
            "rejection_rate",
            CheckType.REJECTION_RATE,
            failed=result.rows_invalid,
            total=result.rows_in,
            max_rate=self.max_rejection_rate,
            severity=Severity.ERROR,
            dataset=self.dataset,
            details={"schema": self.schema.name},
        )
        if gate.outcome is Outcome.FAILED:
            self.checks.raise_for_errors()

        if result.rows_invalid:
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
