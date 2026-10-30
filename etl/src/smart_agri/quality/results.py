"""The data quality result model: what a check found, and what happens next.

Every check — a schema rule inside a pipeline, an orphan count at a join, a
freshness SLA run on its own — produces the same `CheckResult`, so one table, one
dashboard and one OpenMetadata push can carry all of them.

Severity decides the consequence, not the check:

- `error`: a breach fails the run, and Airflow then blocks every stage downstream.
- `warn`: a breach is recorded and shown, and the run carries on.

A check without a threshold is a measurement: it always passes, and exists so
the number has a history.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class Severity(StrEnum):
    """Mirrors the CHECK constraint on etl_control.dq_check_result.severity."""

    ERROR = "error"
    WARN = "warn"


class Outcome(StrEnum):
    """Mirrors the CHECK constraint on etl_control.dq_check_result.outcome."""

    PASSED = "passed"
    WARNED = "warned"
    FAILED = "failed"
    SKIPPED = "skipped"


class CheckType(StrEnum):
    """Mirrors the CHECK constraint on etl_control.dq_check_result.check_type."""

    SCHEMA = "schema"
    REJECTION_RATE = "rejection_rate"
    JOIN = "join"
    FILTER = "filter"
    DEDUPE = "dedupe"
    VOLUME = "volume"
    RECONCILIATION = "reconciliation"
    FRESHNESS = "freshness"


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One evaluation of one check, in one run."""

    check_name: str
    check_type: CheckType
    severity: Severity
    outcome: Outcome
    dataset: str | None = None
    #: What was checked, beyond the dataset: a column, a joined dataset, a table.
    target: str | None = None
    observed_value: float | None = None
    threshold: float | None = None
    rows_evaluated: int | None = None
    rows_failed: int | None = None
    details: Mapping[str, object] = field(default_factory=dict)

    def __str__(self) -> str:
        observed = "" if self.observed_value is None else f" observed={self.observed_value:g}"
        threshold = "" if self.threshold is None else f" threshold={self.threshold:g}"
        return f"[{self.outcome.value}] {self.check_name}{observed}{threshold}"


class DataQualityError(ValueError):
    """One or more error-severity checks failed; the run must not continue.

    A `ValueError`, because that is what a breached rejection threshold raised
    before this model existed, and callers already handle it.
    """

    def __init__(self, pipeline: str, failures: Sequence[CheckResult]) -> None:
        self.pipeline = pipeline
        self.failures = tuple(failures)
        lines = "; ".join(str(result) for result in self.failures)
        super().__init__(f"{pipeline}: {len(self.failures)} data quality check(s) failed: {lines}")


def outcome_for(*, breached: bool, severity: Severity) -> Outcome:
    """The outcome of a check that did or did not cross its threshold."""
    if not breached:
        return Outcome.PASSED
    return Outcome.FAILED if severity is Severity.ERROR else Outcome.WARNED


class CheckCollector:
    """Accumulates the results of one run, and decides when the run must stop.

    `enforce=False` is the backfill escape hatch: an error-severity breach is
    recorded as `warned`, flagged in its details, and does not stop the run.
    """

    def __init__(self, pipeline: str, *, enforce: bool = True) -> None:
        self.pipeline = pipeline
        self.enforce = enforce
        self._results: list[CheckResult] = []

    @property
    def results(self) -> tuple[CheckResult, ...]:
        return tuple(self._results)

    def record(self, result: CheckResult) -> CheckResult:
        if not self.enforce and result.outcome is Outcome.FAILED:
            result = replace(
                result,
                outcome=Outcome.WARNED,
                details={**result.details, "downgraded": "enforcement disabled"},
            )
        self._results.append(result)
        return result

    def evaluate_rate(
        self,
        check_name: str,
        check_type: CheckType,
        *,
        failed: int,
        total: int,
        max_rate: float | None,
        severity: Severity,
        dataset: str | None = None,
        target: str | None = None,
        details: Mapping[str, object] | None = None,
    ) -> CheckResult:
        """Record `failed / total` against `max_rate`.

        `max_rate=None` records the rate as a measurement that always passes.
        An empty input has a rate of zero: nothing evaluated, nothing failed.
        """
        rate = 0.0 if total == 0 else failed / total
        breached = max_rate is not None and rate > max_rate
        return self.record(
            CheckResult(
                check_name=check_name,
                check_type=check_type,
                severity=severity,
                outcome=outcome_for(breached=breached, severity=severity),
                dataset=dataset,
                target=target,
                observed_value=rate,
                threshold=max_rate,
                rows_evaluated=total,
                rows_failed=failed,
                details=dict(details or {}),
            )
        )

    def errors(self) -> tuple[CheckResult, ...]:
        return tuple(r for r in self._results if r.outcome is Outcome.FAILED)

    def raise_for_errors(self) -> None:
        """Stop the run if any error-severity check failed."""
        failures = self.errors()
        if failures:
            raise DataQualityError(self.pipeline, failures)
