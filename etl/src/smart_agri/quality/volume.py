"""Volume: is this run's row count plausible, given the runs before it?

A schema can be perfectly satisfied by a frame that is a tenth of its usual
size — a source that half-failed, a filter that started matching everything.
Nothing else catches that, so each run's count is compared with the median of
its recent successful runs.

The median rather than the mean, so a single backfill or a single empty day
does not move the baseline.
"""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import TYPE_CHECKING

from smart_agri.quality.results import CheckResult, CheckType, Outcome, Severity, outcome_for

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class VolumeRule:
    """The band a run's row count must fall in, relative to recent history."""

    low: float = 0.5
    """Below `low * median` is a breach: the input shrank."""

    high: float = 2.0
    """Above `high * median` is a breach: the input grew, or rows duplicated."""

    min_history: int = 5
    """Fewer successful runs than this is no baseline at all; the check is skipped."""

    history: int = 14
    """How many recent successful runs make up the baseline."""

    severity: Severity = Severity.WARN


#: The default for a pipeline that rereads the same whole dataset every run.
DEFAULT_VOLUME_RULE = VolumeRule()

CHECK_NAME = "volume.rows"


def evaluate_volume(
    rule: VolumeRule, current: int, history: Sequence[int], *, dataset: str
) -> CheckResult:
    """Compare `current` with the median of `history` (newest first)."""
    baseline = list(history[: rule.history])
    details: dict[str, object] = {
        "low": rule.low,
        "high": rule.high,
        "history_runs": len(baseline),
    }

    if len(baseline) < rule.min_history:
        return CheckResult(
            CHECK_NAME,
            CheckType.VOLUME,
            rule.severity,
            Outcome.SKIPPED,
            dataset=dataset,
            rows_evaluated=current,
            details={**details, "reason": f"fewer than {rule.min_history} successful runs"},
        )

    typical = float(median(baseline))
    details["median"] = typical
    if typical == 0:
        # A dataset that has always been empty: any rows at all are news, but
        # there is no ratio to judge them by.
        breached = current > 0
        ratio = None
    else:
        ratio = current / typical
        breached = not rule.low <= ratio <= rule.high

    return CheckResult(
        CHECK_NAME,
        CheckType.VOLUME,
        rule.severity,
        outcome_for(breached=breached, severity=rule.severity),
        dataset=dataset,
        observed_value=ratio,
        rows_evaluated=current,
        details=details,
    )
