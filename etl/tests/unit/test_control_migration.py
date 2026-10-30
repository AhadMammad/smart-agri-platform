"""Tests for the one-shot move of `etl_control` into etl-meta-db.

The copy runs once against a live deployment, right before a changeset drops the
source's copy of the schema. A bug here loses every watermark — and a lost
watermark silently turns the next incremental run into a full re-extract — so
the behaviour is pinned against an in-memory database that applies the real
conflict rules, not a mock that accepts any statement.
"""

from __future__ import annotations

import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from smart_agri.io.control_migration import (
    CONTROL_TABLES,
    ControlCopyError,
    ControlTable,
    copy_control_tables,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

LIQUIBASE = Path(__file__).resolve().parents[3] / "liquibase"
_TABLES = {table.name: table for table in CONTROL_TABLES}


@dataclass
class _Cursor:
    rows: list[tuple[Any, ...]]

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[Any]:
        return list(self.rows)


@dataclass
class FakeControlDb:
    """An `etl_control` schema in memory, understanding the module's SQL.

    `drop_inserts` simulates a target that silently loses rows, the failure the
    post-copy verification exists to catch.
    """

    has_schema: bool = True
    tables: dict[str, dict[Any, tuple[Any, ...]]] = field(
        default_factory=lambda: {name: {} for name in _TABLES}
    )
    drop_inserts: bool = False
    commits: int = 0

    def execute(self, query: str, params: Sequence[Any] | None = None) -> _Cursor:
        if query.startswith("SELECT to_regclass"):
            return _Cursor([(self.has_schema,)])
        for name, table in _TABLES.items():
            if query == table.select_sql:
                return _Cursor(list(self.tables[name].values()))
            if query == f"SELECT count(*) FROM etl_control.{name}":
                return _Cursor([(len(self.tables[name]),)])
            if query == table.insert_sql:
                assert params is not None
                self._insert(table, tuple(params))
                return _Cursor([])
        msg = f"unexpected SQL: {query}"
        raise AssertionError(msg)

    def commit(self) -> None:
        self.commits += 1

    def _insert(self, table: ControlTable, row: tuple[Any, ...]) -> None:
        if self.drop_inserts:
            return
        rows = self.tables[table.name]
        key = row[0]
        if key not in rows:
            rows[key] = row
        elif table.name == "watermark":
            # ON CONFLICT DO UPDATE SET watermark_value = GREATEST(old, new)
            old = rows[key]
            marks = [m for m in (old[3], row[3]) if m is not None]
            rows[key] = (*old[:3], max(marks) if marks else None, old[4])
        # run_log: ON CONFLICT DO NOTHING

    def add(self, table: str, row: tuple[Any, ...]) -> None:
        self.tables[table][row[0]] = row


def _ts(day: int) -> datetime:
    return datetime(2026, 8, day, tzinfo=UTC)


def _watermark(pipeline: str, day: int) -> tuple[Any, ...]:
    return (pipeline, "agri.sensor_reading", "updated_at", _ts(day), _ts(day))


def _run(pipeline: str) -> tuple[Any, ...]:
    return (uuid.uuid4(), pipeline, date(2026, 8, 1), "success", 10, 10, 0, _ts(1), _ts(1), None)


@pytest.fixture
def source() -> FakeControlDb:
    db = FakeControlDb()
    db.add("watermark", _watermark("bronze.sensor_reading", 5))
    db.add("run_log", _run("bronze.sensor_reading"))
    db.add("run_log", _run("silver.fact_sensor_reading"))
    return db


class TestCopyControlTables:
    def test_every_row_lands_in_the_target(self, source: FakeControlDb) -> None:
        target = FakeControlDb()

        report = copy_control_tables(source, target)

        assert report.copied == {"watermark": 1, "run_log": 2}
        assert target.tables == source.tables

    def test_a_second_run_changes_nothing(self, source: FakeControlDb) -> None:
        """`make migrate` runs the copy on every deploy, not just the first."""
        target = FakeControlDb()
        copy_control_tables(source, target)
        before = {name: dict(rows) for name, rows in target.tables.items()}

        copy_control_tables(source, target)

        assert target.tables == before

    def test_a_fresh_clone_has_nothing_to_copy(self) -> None:
        """No etl_control in the source is the normal state after the move."""
        target = FakeControlDb()

        report = copy_control_tables(FakeControlDb(has_schema=False), target)

        assert report.skipped
        assert "nothing to copy" in report.summary()
        assert all(not rows for rows in target.tables.values())

    def test_a_newer_watermark_in_the_target_is_kept(self, source: FakeControlDb) -> None:
        """A run against the new database before the copy must not be rewound,
        or the next incremental run re-extracts rows already loaded."""
        target = FakeControlDb()
        target.add("watermark", _watermark("bronze.sensor_reading", 9))

        copy_control_tables(source, target)

        assert target.tables["watermark"]["bronze.sensor_reading"][3] == _ts(9)

    def test_an_older_watermark_in_the_target_is_advanced(self, source: FakeControlDb) -> None:
        target = FakeControlDb()
        target.add("watermark", _watermark("bronze.sensor_reading", 2))

        copy_control_tables(source, target)

        assert target.tables["watermark"]["bronze.sensor_reading"][3] == _ts(5)

    def test_missing_rows_in_the_target_fail_the_copy(self, source: FakeControlDb) -> None:
        """The failure is what stops `make migrate` before the source drop."""
        with pytest.raises(ControlCopyError, match="refusing to continue"):
            copy_control_tables(source, FakeControlDb(drop_inserts=True))

    def test_summary_lists_rows_per_table(self, source: FakeControlDb) -> None:
        report = copy_control_tables(source, FakeControlDb())
        assert report.summary() == "watermark=1, run_log=2"


def _changelog_columns(path: Path) -> dict[str, tuple[str, ...]]:
    columns: dict[str, tuple[str, ...]] = {}
    for element in ET.parse(path).getroot().iter():
        if element.tag.endswith("createTable") and element.get("schemaName") == "etl_control":
            name = element.get("tableName")
            assert name is not None
            columns[name] = tuple(
                c.get("name", "") for c in element.iter() if c.tag.endswith("column")
            )
    return columns


class TestColumnsMatchTheChangelogs:
    """The copy names its columns explicitly; both schemas must have exactly those."""

    @pytest.mark.parametrize(
        "path",
        [
            LIQUIBASE / "changelog" / "changes" / "005-etl-control.xml",
            LIQUIBASE / "etl-meta" / "changes" / "001-etl-control.xml",
        ],
        ids=["source", "etl-meta"],
    )
    def test_copied_columns_are_the_table_columns(self, path: Path) -> None:
        declared = _changelog_columns(path)
        assert declared == {table.name: table.columns for table in CONTROL_TABLES}
