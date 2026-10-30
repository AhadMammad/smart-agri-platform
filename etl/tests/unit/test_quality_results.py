"""Tests for the data quality result model.

Severity is the whole contract with Airflow: an `error` breach must stop the
run so downstream stages never see the data, and a `warn` breach must never
stop it. A slip in either direction is either a silent bad load or a pipeline
that halts on noise.
"""

from __future__ import annotations

import pytest

from smart_agri.quality import (
    CheckCollector,
    CheckResult,
    CheckType,
    DataQualityError,
    Outcome,
    Severity,
    outcome_for,
)


def _rate(
    collector: CheckCollector,
    *,
    failed: int,
    total: int = 100,
    max_rate: float | None = 0.1,
    severity: Severity = Severity.ERROR,
) -> CheckResult:
    return collector.evaluate_rate(
        "join.dim_field",
        CheckType.JOIN,
        failed=failed,
        total=total,
        max_rate=max_rate,
        severity=severity,
        dataset="fact_irrigation",
        target="dim_field",
    )


class TestOutcomeFor:
    @pytest.mark.parametrize(
        ("breached", "severity", "expected"),
        [
            (False, Severity.ERROR, Outcome.PASSED),
            (False, Severity.WARN, Outcome.PASSED),
            (True, Severity.ERROR, Outcome.FAILED),
            (True, Severity.WARN, Outcome.WARNED),
        ],
    )
    def test_severity_decides_the_outcome_of_a_breach(
        self, breached: bool, severity: Severity, expected: Outcome
    ) -> None:
        assert outcome_for(breached=breached, severity=severity) is expected


class TestEvaluateRate:
    def test_a_rate_within_the_threshold_passes(self) -> None:
        result = _rate(CheckCollector("p"), failed=10)

        assert result.outcome is Outcome.PASSED
        assert result.observed_value == pytest.approx(0.1)
        assert (result.rows_failed, result.rows_evaluated) == (10, 100)

    def test_a_rate_above_an_error_threshold_fails(self) -> None:
        assert _rate(CheckCollector("p"), failed=11).outcome is Outcome.FAILED

    def test_a_rate_above_a_warn_threshold_warns(self) -> None:
        result = _rate(CheckCollector("p"), failed=50, severity=Severity.WARN)
        assert result.outcome is Outcome.WARNED

    def test_without_a_threshold_the_rate_is_only_measured(self) -> None:
        """A filter that drops 90% of rows by design must not be flagged."""
        result = _rate(CheckCollector("p"), failed=90, max_rate=None)

        assert result.outcome is Outcome.PASSED
        assert result.observed_value == pytest.approx(0.9)
        assert result.threshold is None

    def test_an_empty_input_has_a_zero_rate(self) -> None:
        """No division by zero on a caught-up incremental batch."""
        result = _rate(CheckCollector("p"), failed=0, total=0, max_rate=0.0)

        assert result.outcome is Outcome.PASSED
        assert result.observed_value == 0.0

    def test_a_zero_threshold_fails_on_a_single_row(self) -> None:
        assert _rate(CheckCollector("p"), failed=1, max_rate=0.0).outcome is Outcome.FAILED

    def test_the_result_is_kept_by_the_collector(self) -> None:
        collector = CheckCollector("p")
        result = _rate(collector, failed=1)
        assert collector.results == (result,)


class TestRaiseForErrors:
    def test_only_error_failures_stop_the_run(self) -> None:
        collector = CheckCollector("silver.fact_irrigation")
        _rate(collector, failed=50, severity=Severity.WARN)
        _rate(collector, failed=1)

        collector.raise_for_errors()  # neither breach is an error failure

    def test_an_error_failure_raises_with_every_failing_check(self) -> None:
        collector = CheckCollector("silver.fact_irrigation")
        _rate(collector, failed=50)
        _rate(collector, failed=60)
        _rate(collector, failed=70, severity=Severity.WARN)

        with pytest.raises(DataQualityError) as raised:
            collector.raise_for_errors()

        assert raised.value.pipeline == "silver.fact_irrigation"
        assert len(raised.value.failures) == 2
        assert "2 data quality check(s) failed" in str(raised.value)
        assert "[failed] join.dim_field observed=0.5 threshold=0.1" in str(raised.value)

    def test_the_error_is_a_value_error(self) -> None:
        """Callers that handled the old rejection-threshold ValueError still do."""
        assert issubclass(DataQualityError, ValueError)


class TestEnforcementDisabled:
    def test_an_error_breach_is_downgraded_and_does_not_stop_the_run(self) -> None:
        """The escape hatch for a backfill over known-bad history."""
        collector = CheckCollector("p", enforce=False)

        result = _rate(collector, failed=50)

        assert result.outcome is Outcome.WARNED
        assert result.details["downgraded"] == "enforcement disabled"
        collector.raise_for_errors()

    def test_passing_checks_are_left_alone(self) -> None:
        result = _rate(CheckCollector("p", enforce=False), failed=1)
        assert result.outcome is Outcome.PASSED
        assert "downgraded" not in result.details


class TestCheckResultStr:
    def test_measurement_without_values_renders_cleanly(self) -> None:
        result = CheckResult(
            "freshness.weather", CheckType.FRESHNESS, Severity.WARN, Outcome.SKIPPED
        )
        assert str(result) == "[skipped] freshness.weather"
