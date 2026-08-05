"""Validation with quarantine.

A pipeline that drops bad rows silently is worse than one that fails: the
numbers stay plausible while quietly being wrong. Every Silver and Gold
validation therefore splits its input into rows that pass and rows that do not,
writes the failures to `quarantine/`, and reports both counts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandera.errors
import polars as pl

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    import pandera.polars as pa

logger = get_logger(__name__)

_INDEX_COLUMN = "__row_index"


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of validating a frame against a schema."""

    valid: pl.DataFrame
    invalid: pl.DataFrame
    failure_summary: pl.DataFrame
    """One row per distinct (column, check) that failed, with a count."""

    @property
    def rows_in(self) -> int:
        return self.valid.height + self.invalid.height

    @property
    def rows_valid(self) -> int:
        return self.valid.height

    @property
    def rows_invalid(self) -> int:
        return self.invalid.height

    @property
    def rejection_rate(self) -> float:
        return 0.0 if self.rows_in == 0 else self.rows_invalid / self.rows_in


def validate(frame: pl.DataFrame, schema: pa.DataFrameSchema) -> ValidationResult:
    """Split a frame into rows that satisfy the schema and rows that do not.

    Uses pandera's lazy mode so every failing check is collected in one pass
    rather than aborting on the first, which is what makes the quarantine report
    useful for diagnosing a source problem.

    Structural failures — a missing column, a wrong dtype — are not row-level
    and cannot be quarantined; those are re-raised, because no subset of the
    data would be correct.
    """
    if frame.is_empty():
        return ValidationResult(frame, frame.clear(), _empty_summary())

    indexed = frame.with_row_index(_INDEX_COLUMN)

    try:
        schema.validate(indexed.drop(_INDEX_COLUMN), lazy=True)
    except pandera.errors.SchemaErrors as errors:
        failures = _failure_cases(errors)
        bad_indices = _row_indices(failures)

        if bad_indices is None:
            # No row-level indices: the schema failed structurally.
            logger.error("validation_structural_failure", schema=schema.name)
            raise

        invalid = indexed.filter(pl.col(_INDEX_COLUMN).is_in(bad_indices)).drop(_INDEX_COLUMN)
        valid = indexed.filter(~pl.col(_INDEX_COLUMN).is_in(bad_indices)).drop(_INDEX_COLUMN)
        summary = _summarise(failures)

        logger.warning(
            "validation_rejected_rows",
            schema=schema.name,
            rows_in=frame.height,
            rows_invalid=invalid.height,
            checks=summary.select("check").to_series().to_list(),
        )
        return ValidationResult(valid, invalid, summary)

    return ValidationResult(frame, frame.clear(), _empty_summary())


def _failure_cases(errors: pandera.errors.SchemaErrors) -> pl.DataFrame:
    """Normalise pandera's failure cases into a Polars frame.

    pandera returns pandas or polars depending on the backend, so the shape is
    normalised once here rather than at every call site.
    """
    cases = errors.failure_cases
    if isinstance(cases, pl.DataFrame):
        return cases
    return pl.from_pandas(cases)


def _row_indices(failures: pl.DataFrame) -> list[int] | None:
    """Row indices of failing records, or None if the failures are structural."""
    if "index" not in failures.columns:
        return None
    indices = failures["index"].drop_nulls()
    if indices.is_empty():
        return None
    return [int(value) for value in indices.unique().to_list()]


def _summarise(failures: pl.DataFrame) -> pl.DataFrame:
    """Count failures per column and check."""
    columns = [c for c in ("column", "check") if c in failures.columns]
    if not columns:
        return _empty_summary()
    return (
        failures.group_by(columns)
        .agg(pl.len().alias("failed_rows"))
        .sort("failed_rows", descending=True)
    )


def _empty_summary() -> pl.DataFrame:
    return pl.DataFrame(schema={"column": pl.String, "check": pl.String, "failed_rows": pl.UInt32})
