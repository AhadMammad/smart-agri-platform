"""Tests for the copy of etl-meta-db into ClickHouse.

The same columns are declared three times — the Liquibase changelog that
creates them in Postgres, the export query that reads them, and the ClickHouse
DDL that receives them — and only the last two meet at runtime, where a
mismatch fails the hourly export with an insert error. So the three are diffed
here, and the copy itself is checked for losing rows.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from tests.fakes import FakeClickHouseSink, FakePostgresSource
from tests.unit.test_clickhouse_ddl import _declared_tables

from smart_agri.pipelines.base import PipelineContext
from smart_agri.quality import Outcome
from smart_agri.quality.export import EXPORTS, export_operational_data

LIQUIBASE = Path(__file__).resolve().parents[3] / "liquibase" / "etl-meta" / "changes"
SOURCE_TABLE = {"ops_run_log": "run_log", "ops_dq_check_result": "dq_check_result"}


def _selected(query: str) -> list[str]:
    """Output column names of an export SELECT, one column per line."""
    select = query.split("FROM", 1)[0].replace("SELECT", "", 1)
    names = []
    for raw in select.splitlines():
        line = raw.strip().rstrip(",")
        if not line:
            continue
        alias = re.search(r"\bAS\s+(\w+)$", line)
        names.append(alias.group(1) if alias else line)
    return names


def _changelog_columns(table: str) -> list[str]:
    for path in sorted(LIQUIBASE.glob("*.xml")):
        for element in ET.parse(path).getroot().iter():
            if element.tag.endswith("createTable") and element.get("tableName") == table:
                return [c.get("name", "") for c in element.iter() if c.tag.endswith("column")]
    msg = f"etl_control.{table} not created by any etl-meta changeset"
    raise AssertionError(msg)


class TestColumnsAgree:
    @pytest.mark.parametrize("table", sorted(EXPORTS))
    def test_the_export_reads_exactly_the_warehouse_columns_in_order(self, table: str) -> None:
        """`insert_arrow` maps by position; a reordered column lands in the wrong place."""
        assert _selected(EXPORTS[table]) == _declared_tables()[table]

    @pytest.mark.parametrize("table", sorted(EXPORTS))
    def test_every_postgres_column_is_exported(self, table: str) -> None:
        """A column added to the changelog and not to the export is silently dropped."""
        assert set(_selected(EXPORTS[table])) == set(_changelog_columns(SOURCE_TABLE[table]))


def _run_log() -> pl.DataFrame:
    started = datetime(2026, 8, 1, 6, tzinfo=UTC)
    return pl.DataFrame(
        {
            "run_id": ["a", "b"],
            "pipeline": ["load.dim_farm", "dq.freshness"],
            "logical_date": [date(2026, 8, 1)] * 2,
            "status": ["success", "success"],
            "rows_read": [5, 43],
            "rows_written": [5, 0],
            "rows_quarantined": [0, 0],
            "started_at": [started, started],
            "finished_at": [started, started],
            "error_message": [None, None],
        }
    )


def _results() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "result_id": [1],
            "run_id": ["a"],
            "check_name": ["reconciliation.row_count"],
            "outcome": ["passed"],
        }
    )


@pytest.fixture
def etl_meta() -> FakePostgresSource:
    return FakePostgresSource(
        {"etl_control.run_log": _run_log(), "etl_control.dq_check_result": _results()}
    )


@pytest.fixture
def clickhouse() -> FakeClickHouseSink:
    return FakeClickHouseSink()


@pytest.fixture
def context(etl_meta: FakePostgresSource, clickhouse: FakeClickHouseSink) -> PipelineContext:
    return PipelineContext(
        etl_meta=etl_meta,  # type: ignore[arg-type]
        clickhouse=clickhouse,  # type: ignore[arg-type]
    )


class TestExport:
    def test_both_tables_are_replaced_with_the_postgres_rows(
        self, context: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        results = export_operational_data(context)

        assert clickhouse.tables["ops_run_log"].height == 2
        assert clickhouse.tables["ops_dq_check_result"].height == 1
        assert {r.dataset: r.outcome for r in results} == {
            "ops_run_log": Outcome.PASSED,
            "ops_dq_check_result": Outcome.PASSED,
        }

    def test_a_copy_that_lands_short_fails(
        self, context: PipelineContext, clickhouse: FakeClickHouseSink
    ) -> None:
        """A results dashboard missing results would be the least trustworthy chart."""

        class Lossy(FakeClickHouseSink):
            def replace_table(self, table: str, frame: pl.DataFrame) -> int:
                self.tables[table] = frame.head(0)
                return frame.height

        lossy = Lossy()
        context._clickhouse = lossy  # type: ignore[assignment]

        results = {r.dataset: r for r in export_operational_data(context)}

        assert results["ops_run_log"].outcome is Outcome.FAILED
        assert results["ops_run_log"].rows_failed == 2

    def test_reading_etl_meta_never_touches_the_source(self) -> None:
        """The source database is read-only to the ETL and holds none of this."""
        context = PipelineContext()
        assert context.settings.etl_meta.host == "etl-meta-db"
        assert context.etl_meta._settings.host == "etl-meta-db"  # type: ignore[attr-defined]
