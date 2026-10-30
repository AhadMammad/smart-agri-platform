"""Tests for `validate()`, the split between quarantinable rows and broken frames.

The split has one rule that matters more than the rest: a row-level failure is
quarantined, a structural one fails the run. The case that went wrong was a
frame with *both* — a wrong-dtype or missing column alongside an out-of-range
value. The row-level failure was quarantined and every other row passed as
"valid", carrying the broken column into Silver.
"""

from __future__ import annotations

import pandera.errors
import pandera.polars as pa
import polars as pl
import pytest
from pandera.engines import polars_engine

from smart_agri.contracts import (
    SILVER_DIM_PLANTING,
    SILVER_FACT_MACHINE_FAULT,
    SILVER_FACT_MACHINE_OPERATION,
    row_rule,
    validate,
)
from smart_agri.contracts.validation import ROW_RULE_COLUMN

SCHEMA = pa.DataFrameSchema(
    {
        "id": pa.Column(polars_engine.Int64, unique=True),
        "moisture": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 100)),
        "depth_cm": pa.Column(polars_engine.Int64, nullable=True),
        "status": pa.Column(polars_engine.String, pa.Check.isin(["active", "idle"])),
    },
    strict=True,
    name="probe_schema",
)


def _frame(**overrides: list[object]) -> pl.DataFrame:
    columns: dict[str, list[object]] = {
        "id": [1, 2, 3],
        "moisture": [10.0, 20.0, 30.0],
        "depth_cm": [5, 10, None],
        "status": ["active", "idle", "active"],
    }
    columns.update(overrides)
    return pl.DataFrame(columns)


class TestRowLevelFailures:
    def test_a_clean_frame_passes_whole(self) -> None:
        result = validate(_frame(), SCHEMA)

        assert result.rows_valid == 3
        assert result.rows_invalid == 0
        assert result.failure_summary.is_empty()

    @pytest.mark.parametrize(
        ("overrides", "check"),
        [
            ({"moisture": [10.0, 120.0, 30.0]}, "in_range(0, 100)"),
            ({"moisture": [10.0, None, 30.0]}, "not_nullable"),
            ({"status": ["active", "broken", "active"]}, "isin(['active', 'idle'])"),
        ],
        ids=["range", "null", "isin"],
    )
    def test_only_the_offending_row_is_quarantined(
        self, overrides: dict[str, list[object]], check: str
    ) -> None:
        result = validate(_frame(**overrides), SCHEMA)

        assert result.invalid["id"].to_list() == [2]
        assert result.valid["id"].to_list() == [1, 3]
        assert result.failure_summary["check"].to_list() == [check]

    def test_every_row_sharing_a_duplicate_key_is_quarantined(self) -> None:
        """Neither copy can be trusted as the real one."""
        result = validate(_frame(id=[1, 1, 3]), SCHEMA)

        assert result.invalid.height == 2
        assert result.valid["id"].to_list() == [3]

    def test_the_summary_counts_failures_per_column_and_check(self) -> None:
        result = validate(_frame(moisture=[-1.0, 120.0, 30.0]), SCHEMA)

        summary = result.failure_summary.to_dicts()
        assert summary == [{"column": "moisture", "check": "in_range(0, 100)", "failed_rows": 2}]
        assert result.rejection_rate == pytest.approx(2 / 3)

    def test_the_valid_and_invalid_frames_keep_the_input_columns(self) -> None:
        """The internal row index must never leak into the lake or quarantine."""
        result = validate(_frame(moisture=[10.0, 120.0, 30.0]), SCHEMA)

        assert result.valid.columns == _frame().columns
        assert result.invalid.columns == _frame().columns


class TestStructuralFailures:
    @pytest.mark.parametrize(
        "frame",
        [
            _frame(depth_cm=["a", "b", "c"]),
            _frame().drop("status"),
            _frame().with_columns(pl.lit(0).alias("unexpected")),
        ],
        ids=["wrong-dtype", "missing-column", "extra-column"],
    )
    def test_a_structural_failure_fails_the_run(self, frame: pl.DataFrame) -> None:
        with pytest.raises(pandera.errors.SchemaErrors):
            validate(frame, SCHEMA)

    @pytest.mark.parametrize(
        "frame",
        [
            _frame(moisture=[10.0, 120.0, 30.0], depth_cm=["a", "b", "c"]),
            _frame(moisture=[10.0, 120.0, 30.0]).drop("status"),
        ],
        ids=["wrong-dtype", "missing-column"],
    )
    def test_a_row_failure_does_not_mask_a_structural_one(self, frame: pl.DataFrame) -> None:
        """Regression: the row-level failure was quarantined and the rest of
        the frame — with the broken column — passed as valid."""
        with pytest.raises(pandera.errors.SchemaErrors):
            validate(frame, SCHEMA)


class TestEmptyInput:
    def test_an_empty_frame_is_valid_and_keeps_its_columns(self) -> None:
        empty = _frame().clear()

        result = validate(empty, SCHEMA)

        assert result.rows_in == 0
        assert result.rejection_rate == 0.0
        assert result.valid.columns == empty.columns


TIMED = pa.DataFrameSchema(
    {
        "id": pa.Column(polars_engine.Int64, unique=True),
        "started": pa.Column(polars_engine.Int64),
        "finished": pa.Column(polars_engine.Int64, nullable=True),
    },
    checks=[row_rule("finished_after_started", pl.col("finished") >= pl.col("started"))],
    strict=True,
    name="timed",
)


class TestRowRules:
    def test_only_the_row_breaking_the_rule_is_quarantined(self) -> None:
        frame = pl.DataFrame({"id": [1, 2, 3], "started": [1, 5, 2], "finished": [2, 4, 2]})

        result = validate(frame, TIMED)

        assert result.invalid["id"].to_list() == [2]
        assert result.valid["id"].to_list() == [1, 3]

    def test_failures_are_reported_against_the_row_not_the_schema(self) -> None:
        """pandera names the schema as the failing 'column'; the report must not."""
        frame = pl.DataFrame({"id": [1, 2], "started": [5, 5], "finished": [4, 3]})

        summary = validate(frame, TIMED).failure_summary.to_dicts()

        assert summary == [
            {"column": ROW_RULE_COLUMN, "check": "finished_after_started", "failed_rows": 2}
        ]

    def test_a_null_comparison_passes(self) -> None:
        """An open interval is the nullable column's business, not the rule's."""
        frame = pl.DataFrame(
            {"id": [1], "started": [5], "finished": [None]},
            schema={"id": pl.Int64, "started": pl.Int64, "finished": pl.Int64},
        )
        assert validate(frame, TIMED).rows_invalid == 0

    def test_a_row_rule_and_a_column_check_on_one_row_quarantine_it_once(self) -> None:
        frame = pl.DataFrame({"id": [1, 1, 3], "started": [5, 1, 1], "finished": [4, 2, 2]})

        result = validate(frame, TIMED)

        assert sorted(result.invalid["id"].to_list()) == [1, 1]
        assert set(result.failure_summary["check"].to_list()) == {
            "finished_after_started",
            "field_uniqueness",
        }


class TestSilverRowRules:
    """The rules guard real contracts; dropping one would be silent otherwise."""

    @pytest.mark.parametrize(
        ("schema", "rule"),
        [
            (SILVER_FACT_MACHINE_OPERATION, "finished_after_started"),
            (SILVER_DIM_PLANTING, "harvest_after_planting"),
            (SILVER_FACT_MACHINE_FAULT, "resolved_after_occurred"),
        ],
    )
    def test_the_contract_carries_its_row_rule(self, schema: pa.DataFrameSchema, rule: str) -> None:
        assert rule in {check.name for check in schema.checks}
