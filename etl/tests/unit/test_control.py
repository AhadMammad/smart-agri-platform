"""Tests for the data quality half of the control store.

`dq_check_result` is written by Python and constrained by SQL, and the two are
declared in different places: the column list and enums here, the table and its
CHECK constraints in the etl-meta changelog. A drift between them would not fail
a single unit test — the fake stores anything — but would fail every pipeline
run on the first insert. So the pair is diffed here.
"""

from __future__ import annotations

import json
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from psycopg.types.json import Jsonb

from smart_agri.config import EtlMetaSettings
from smart_agri.io.control import DQ_RESULT_COLUMNS, ControlStore, RunRecord, dq_result_row
from smart_agri.quality import CheckResult, CheckType, Outcome, Severity

if TYPE_CHECKING:
    from collections.abc import Sequence
    from enum import StrEnum

CHANGELOG = (
    Path(__file__).resolve().parents[3]
    / "liquibase"
    / "etl-meta"
    / "changes"
    / "002-data-quality.xml"
)
DB_DEFAULTED = {"result_id", "checked_at"}

RESULT = CheckResult(
    check_name="join.dim_field",
    check_type=CheckType.JOIN,
    severity=Severity.WARN,
    outcome=Outcome.WARNED,
    dataset="fact_irrigation",
    target="dim_field",
    observed_value=0.25,
    threshold=0.0,
    rows_evaluated=4,
    rows_failed=1,
    details={"sample_keys": [7], "as_of": date(2026, 8, 1)},
)


def _table_columns() -> list[str]:
    root = ET.parse(CHANGELOG).getroot()
    table = next(e for e in root.iter() if e.tag.endswith("createTable"))
    return [c.get("name", "") for c in table.iter() if c.tag.endswith("column")]


def _check_values(constraint: str) -> set[str]:
    sql = CHANGELOG.read_text()
    match = re.search(rf"{constraint}\s+CHECK \(\w+ IN \(([^)]*)\)\)", sql)
    assert match, f"{constraint} not found in {CHANGELOG.name}"
    return set(re.findall(r"'(\w+)'", match.group(1)))


class TestSchemaDrift:
    def test_inserted_columns_are_the_table_columns(self) -> None:
        assert set(DQ_RESULT_COLUMNS) == set(_table_columns()) - DB_DEFAULTED

    @pytest.mark.parametrize(
        ("constraint", "enum"),
        [
            ("ck_dq_check_type", CheckType),
            ("ck_dq_severity", Severity),
            ("ck_dq_outcome", Outcome),
        ],
    )
    def test_check_constraints_accept_exactly_the_enum_values(
        self, constraint: str, enum: type[StrEnum]
    ) -> None:
        assert _check_values(constraint) == {member.value for member in enum}

    def test_the_changelog_is_included_by_the_master(self) -> None:
        master = (CHANGELOG.parent.parent / "db.changelog-master.xml").read_text()
        assert f"changes/{CHANGELOG.name}" in master


class TestResultRow:
    def test_fields_map_to_columns_in_order(self) -> None:
        run_id = uuid.uuid4()
        row = dict(
            zip(
                DQ_RESULT_COLUMNS,
                dq_result_row(run_id, "silver.fact_irrigation", date(2026, 8, 1), RESULT),
                strict=True,
            )
        )

        assert row["run_id"] == run_id
        assert row["pipeline"] == "silver.fact_irrigation"
        assert (row["check_type"], row["severity"], row["outcome"]) == ("join", "warn", "warned")
        assert (row["rows_evaluated"], row["rows_failed"]) == (4, 1)

    def test_details_are_json_even_with_dates_in_them(self) -> None:
        """A non-JSON value in free-form details must not fail the whole run."""
        details = dq_result_row(uuid.uuid4(), "p", date(2026, 8, 1), RESULT)[-1]

        assert isinstance(details, Jsonb)
        assert json.loads(details.dumps(details.obj)) == {
            "as_of": "2026-08-01",
            "sample_keys": [7],
        }


class _RecordingConnection:
    """A psycopg connection that records statements and returns canned rows."""

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, Any]] = []
        self.many: list[tuple[str, Sequence[Any]]] = []
        self.commits = 0

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> _RecordingConnection:
        self.executed.append((query, params))
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def cursor(self) -> _RecordingConnection:
        return self

    def executemany(self, query: str, rows: Sequence[Any]) -> None:
        self.many.append((query, rows))

    def commit(self) -> None:
        self.commits += 1


@pytest.fixture
def store() -> ControlStore:
    return ControlStore(EtlMetaSettings())


def _connect(monkeypatch: pytest.MonkeyPatch, conn: _RecordingConnection) -> list[str]:
    import psycopg

    uris: list[str] = []

    def connect(uri: str, **_: object) -> _RecordingConnection:
        uris.append(uri)
        return conn

    monkeypatch.setattr(psycopg, "connect", connect)
    return uris


class TestRecordChecks:
    def test_every_result_is_inserted_in_one_transaction(
        self, store: ControlStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _RecordingConnection()
        uris = _connect(monkeypatch, conn)

        store.record_checks(uuid.uuid4(), "p", date(2026, 8, 1), [RESULT, RESULT])

        (query, rows), *_ = conn.many
        assert query.startswith("INSERT INTO etl_control.dq_check_result (run_id, pipeline,")
        assert query.count("%s") == len(DQ_RESULT_COLUMNS)
        assert len(rows) == 2
        assert conn.commits == 1
        assert uris == [EtlMetaSettings().uri]

    def test_no_results_opens_no_connection(
        self, store: ControlStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uris = _connect(monkeypatch, _RecordingConnection())
        store.record_checks(uuid.uuid4(), "p", date(2026, 8, 1), [])
        assert uris == []


class TestRecentRuns:
    def test_reads_only_successful_runs_newest_first(
        self, store: ControlStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = _RecordingConnection(
            rows=[(date(2026, 8, 2), 10, 9, 1), (date(2026, 8, 1), 8, 8, 0)]
        )
        _connect(monkeypatch, conn)

        runs = store.recent_runs("silver.fact_irrigation", limit=5)

        assert runs == [
            RunRecord(date(2026, 8, 2), 10, 9, 1),
            RunRecord(date(2026, 8, 1), 8, 8, 0),
        ]
        query, params = conn.executed[0]
        assert "status = %s" in query
        assert "ORDER BY started_at DESC" in query
        assert params == ("silver.fact_irrigation", "success", 5)


class TestLastSuccess:
    def test_maps_each_pipeline_to_its_latest_successful_finish(
        self, store: ControlStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        finished = datetime(2026, 8, 1, 7, tzinfo=UTC)
        conn = _RecordingConnection(rows=[("load.fact_harvest", finished), ("load.x", None)])
        _connect(monkeypatch, conn)

        latest = store.last_success(["load.fact_harvest", "load.x"])

        assert latest == {"load.fact_harvest": finished}
        query, params = conn.executed[0]
        assert "pipeline = ANY(%s)" in query
        assert params == ("success", ["load.fact_harvest", "load.x"])

    def test_no_pipelines_opens_no_connection(
        self, store: ControlStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        assert _connect(monkeypatch, _RecordingConnection()) == []
        assert store.last_success([]) == {}
