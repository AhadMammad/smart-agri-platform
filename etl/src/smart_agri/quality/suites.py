"""Data quality suites: checks that run on their own, outside any pipeline.

A pipeline checks what it holds in memory. Two questions it cannot answer:

- **Freshness** — did it run at all? A paused or broken DAG never reaches its
  own checks, so staleness is judged from outside, on a schedule of its own.
- **Reconciliation** — does the warehouse still agree with the source? That
  crosses Postgres, the watermarks and ClickHouse, which no single stage owns.

Each suite run opens its own run-log row (`dq.<suite>`), so its results hang
off a run id exactly as a pipeline's do.

Imported lazily by the CLI, never by `smart_agri.quality`: the suites read the
pipeline registry, and the pipelines import `smart_agri.quality`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING

from smart_agri.io.control import RunStats, RunStatus
from smart_agri.pipelines import DOMAIN_STAGES, get_pipeline
from smart_agri.pipelines.bronze import INCREMENTAL_SPECS, SNAPSHOT_SPECS
from smart_agri.pipelines.load import LoadDimensionPipeline, LoadFactPipeline
from smart_agri.pipelines.silver_specs import SILVER_SPECS_BY_DATASET
from smart_agri.quality.results import (
    CheckCollector,
    CheckResult,
    CheckType,
    Outcome,
    Severity,
    outcome_for,
)
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import date

    from smart_agri.pipelines.base import PipelineContext

logger = get_logger(__name__)


# --- freshness ---------------------------------------------------------------
#: Every domain DAG runs daily; two hours of slack absorbs a slow run or a
#: retry before the next one is due.
DEFAULT_MAX_AGE = timedelta(hours=26)

#: Domains whose cadence differs from daily. Empty today; the hook exists so a
#: change of schedule is one line here rather than a false alarm every day.
MAX_AGE_BY_DOMAIN: Mapping[str, timedelta] = {}


@dataclass(frozen=True, slots=True)
class FreshnessSpec:
    """How recently a pipeline must have succeeded."""

    pipeline: str
    domain: str
    max_age: timedelta = DEFAULT_MAX_AGE
    severity: Severity = Severity.WARN


def freshness_specs() -> tuple[FreshnessSpec, ...]:
    """One SLA per pipeline in each domain's final stage.

    The final stage, because it only succeeds once everything before it has:
    if it is fresh, the domain is.
    """
    return tuple(
        FreshnessSpec(
            pipeline=pipeline,
            domain=domain,
            max_age=MAX_AGE_BY_DOMAIN.get(domain, DEFAULT_MAX_AGE),
        )
        for domain, stages in DOMAIN_STAGES.items()
        for pipeline in stages[-1]
    )


def check_freshness(context: PipelineContext, *, now: datetime | None = None) -> list[CheckResult]:
    """Hours since each final-stage pipeline last succeeded, against its SLA."""
    specs = freshness_specs()
    now = now or datetime.now(UTC)
    last = context.control.last_success([spec.pipeline for spec in specs])

    results: list[CheckResult] = []
    for spec in specs:
        finished = last.get(spec.pipeline)
        limit_h = spec.max_age.total_seconds() / 3600
        details: dict[str, object] = {"domain": spec.domain}
        if finished is None:
            age_h = None
            breached = True
            details["reason"] = "never succeeded"
        else:
            age_h = (now - finished).total_seconds() / 3600
            breached = age_h > limit_h
            details["last_success"] = finished.isoformat()
        results.append(
            CheckResult(
                check_name=f"freshness.{spec.pipeline}",
                check_type=CheckType.FRESHNESS,
                severity=spec.severity,
                outcome=outcome_for(breached=breached, severity=spec.severity),
                dataset=spec.pipeline.split(".", 1)[-1],
                target=spec.pipeline,
                observed_value=None if age_h is None else round(age_h, 2),
                threshold=limit_h,
                details=details,
            )
        )
    return results


# --- reconciliation ----------------------------------------------------------
#: Share of source rows the warehouse may lack before it is flagged. Silver
#: drops rows by design — orphans, filtered sensors — so the warehouse may
#: legitimately hold slightly fewer rows than the source.
DEFAULT_SHORTFALL_TOLERANCE = 0.02


@dataclass(frozen=True, slots=True)
class SourcePair:
    """A warehouse table and the source table its rows come from."""

    load_pipeline: str
    warehouse_table: str
    source_table: str
    #: Incremental sources are bounded by their watermark, so rows written since
    #: the last extract are not counted as missing.
    watermark_pipeline: str | None = None
    watermark_column: str | None = None


def source_pairs(domain: str) -> tuple[SourcePair, ...]:
    """Every load in `domain` that copies a Postgres table into ClickHouse.

    Derived from the specs, so a new table is reconciled as soon as it is
    loaded. Loads with no Postgres origin — weather, the Gold marts — are skipped.
    """
    snapshot = {spec.dataset: spec for spec in SNAPSHOT_SPECS}
    incremental = {spec.dataset: spec for spec in INCREMENTAL_SPECS}

    pairs: list[SourcePair] = []
    for stage in DOMAIN_STAGES[domain]:
        for name in stage:
            if not name.startswith("load."):
                continue
            pipeline = get_pipeline(name)
            if not isinstance(pipeline, LoadDimensionPipeline | LoadFactPipeline):
                continue
            silver = SILVER_SPECS_BY_DATASET.get(pipeline.dataset)
            if silver is None or pipeline.warehouse_table is None:
                continue
            if silver.bronze in incremental:
                spec = incremental[silver.bronze]
                pairs.append(
                    SourcePair(
                        load_pipeline=name,
                        warehouse_table=pipeline.warehouse_table,
                        source_table=spec.source_table,
                        watermark_pipeline=f"bronze.{spec.dataset}",
                        watermark_column=spec.watermark_column,
                    )
                )
            elif silver.bronze in snapshot:
                pairs.append(
                    SourcePair(
                        load_pipeline=name,
                        warehouse_table=pipeline.warehouse_table,
                        source_table=snapshot[silver.bronze].source_table,
                    )
                )
    return tuple(pairs)


def reconcile_domain(
    domain: str,
    context: PipelineContext,
    *,
    tolerance: float = DEFAULT_SHORTFALL_TOLERANCE,
) -> list[CheckResult]:
    """Compare each warehouse table with its source, two checks per table.

    - `no_duplicates` (error): more rows in the warehouse than in the source can
      only mean a row was loaded twice.
    - `completeness` (warn): a shortfall beyond `tolerance` means rows went
      missing somewhere between the source and the warehouse.
    """
    results: list[CheckResult] = []
    for pair in source_pairs(domain):
        watermark = None
        if pair.watermark_pipeline is not None:
            watermark = context.control.get_watermark(pair.watermark_pipeline)
            if watermark is None:
                results.append(_never_extracted(pair))
                continue

        source = context.postgres.count(
            pair.source_table, column=pair.watermark_column, at_most=watermark
        )
        sink = context.clickhouse
        final = sink.engine(pair.warehouse_table) == "ReplacingMergeTree"
        warehouse = sink.count(pair.warehouse_table, final=final)

        details: dict[str, object] = {
            "source_table": pair.source_table,
            "source_rows": source,
            "warehouse_rows": warehouse,
        }
        if watermark is not None:
            details["watermark"] = watermark.isoformat()

        shortfall = 0.0 if source == 0 else max(source - warehouse, 0) / source
        results.append(
            _pair_result(
                pair,
                "no_duplicates",
                Severity.ERROR,
                warehouse > source,
                warehouse,
                source,
                details,
            )
        )
        results.append(
            _pair_result(
                pair,
                "completeness",
                Severity.WARN,
                shortfall > tolerance,
                shortfall,
                tolerance,
                details,
            )
        )
    return results


def _pair_result(
    pair: SourcePair,
    check: str,
    severity: Severity,
    breached: bool,
    observed: float,
    threshold: float,
    details: Mapping[str, object],
) -> CheckResult:
    return CheckResult(
        check_name=f"reconcile.{pair.warehouse_table}.{check}",
        check_type=CheckType.RECONCILIATION,
        severity=severity,
        outcome=outcome_for(breached=breached, severity=severity),
        dataset=pair.warehouse_table,
        target=pair.source_table,
        observed_value=observed,
        threshold=threshold,
        details=details,
    )


def _never_extracted(pair: SourcePair) -> CheckResult:
    return CheckResult(
        check_name=f"reconcile.{pair.warehouse_table}.completeness",
        check_type=CheckType.RECONCILIATION,
        severity=Severity.WARN,
        outcome=Outcome.SKIPPED,
        dataset=pair.warehouse_table,
        target=pair.source_table,
        details={"reason": f"{pair.watermark_pipeline} has no watermark yet"},
    )


# --- the suites --------------------------------------------------------------
def _reconciled_domains() -> tuple[str, ...]:
    return tuple(domain for domain in DOMAIN_STAGES if source_pairs(domain))


def dq_suites() -> dict[str, Callable[[PipelineContext], list[CheckResult]]]:
    """Every suite, by the name `smart-agri dq run <name>` takes."""
    from smart_agri.quality.export import export_operational_data

    suites: dict[str, Callable[[PipelineContext], list[CheckResult]]] = {
        "freshness": check_freshness,
        "export": export_operational_data,
    }
    for domain in _reconciled_domains():
        suites[f"reconcile.{domain}"] = partial(reconcile_domain, domain)
    return suites


def domain_quality_suites() -> dict[str, tuple[str, ...]]:
    """The suites each domain DAG runs after its last stage.

    Mirrored in `airflow/dags/common/pipelines.py`; `test_dag_pipeline_sync.py`
    keeps the two identical.
    """
    return {domain: (f"reconcile.{domain}",) for domain in _reconciled_domains()}


def run_suite(
    name: str, logical_date: date, context: PipelineContext, *, enforce: bool | None = None
) -> tuple[CheckResult, ...]:
    """Run one suite as its own entry in the run log, and persist its results.

    Raises `DataQualityError` if an error-severity check failed, after the
    results are recorded — exactly as a pipeline does.
    """
    suites = dq_suites()
    if name not in suites:
        valid = ", ".join(sorted(suites))
        msg = f"unknown data quality suite {name!r}; valid suites: {valid}"
        raise KeyError(msg)

    pipeline = f"dq.{name}"
    enforce = context.settings.dq.enforce if enforce is None else enforce
    collector = CheckCollector(pipeline, enforce=enforce)
    run_id = context.control.start_run(pipeline, logical_date)

    try:
        for result in suites[name](context):
            collector.record(result)
        context.control.record_checks(run_id, pipeline, logical_date, collector.results)
        collector.raise_for_errors()
    except Exception as exc:
        context.control.finish_run(run_id, RunStatus.FAILED, error=str(exc))
        logger.error("dq_suite_failed", suite=name, error=str(exc))
        raise

    evaluated = len(collector.results)
    context.control.finish_run(run_id, RunStatus.SUCCESS, RunStats(rows_read=evaluated))
    logger.info(
        "dq_suite_succeeded",
        suite=name,
        checks=evaluated,
        not_passed=sum(r.outcome is not Outcome.PASSED for r in collector.results),
    )
    return collector.results


def summarise(results: Sequence[CheckResult]) -> str:
    """`passed=12 warned=1 …`, in a fixed order, omitting zero counts."""
    counts = {outcome: 0 for outcome in Outcome}
    for result in results:
        counts[result.outcome] += 1
    return " ".join(f"{outcome.value}={n}" for outcome, n in counts.items() if n)
