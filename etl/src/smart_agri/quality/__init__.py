"""Data quality: check results, their severity, and when a run must stop."""

from __future__ import annotations

from smart_agri.quality.results import (
    CheckCollector,
    CheckResult,
    CheckType,
    DataQualityError,
    Outcome,
    Severity,
    outcome_for,
)

__all__ = [
    "CheckCollector",
    "CheckResult",
    "CheckType",
    "DataQualityError",
    "Outcome",
    "Severity",
    "outcome_for",
]
