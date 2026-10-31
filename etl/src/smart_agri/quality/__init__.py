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
from smart_agri.quality.volume import DEFAULT_VOLUME_RULE, VolumeRule, evaluate_volume

__all__ = [
    "DEFAULT_VOLUME_RULE",
    "CheckCollector",
    "CheckResult",
    "CheckType",
    "DataQualityError",
    "Outcome",
    "Severity",
    "VolumeRule",
    "evaluate_volume",
    "outcome_for",
]
