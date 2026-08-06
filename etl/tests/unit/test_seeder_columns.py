"""Checks the seeder only writes columns the database actually has.

This exists because of a bug it would have caught: `_insert_telemetry` passed
its whole frame to COPY, including the `machine_code` and `operation_key`
natural keys used for the surrogate-id lookup. Neither is a column on
`agri.machine_telemetry`, so the insert failed with

    column "machine_code" of relation "machine_telemetry" does not exist

Every other unit test passed, because the in-memory fake stores whatever
columns it is handed. The real schema is the only thing that knows better — so
it is read from the Liquibase changelogs and compared here.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tests.fakes import FakePostgresSource

from smart_agri.generator import GeneratorConfig
from smart_agri.generator.seeder import TABLES_IN_DEPENDENCY_ORDER, DatasetSeeder

if TYPE_CHECKING:
    from collections.abc import Mapping

CHANGELOG_DIR = Path(__file__).resolve().parents[3] / "liquibase" / "changelog" / "changes"
_NS = {"lb": "http://www.liquibase.org/xml/ns/dbchangelog"}

TINY = GeneratorConfig(
    n_farms=2,
    fields_per_farm=2,
    sensors_per_field=1,
    machines_per_farm=3,
    start_date=date(2025, 8, 1),
    end_date=date(2026, 8, 1),
    reading_interval_minutes=720,
)


def _schema_columns() -> dict[str, set[str]]:
    """Column names per table, read from the `createTable` elements."""
    tables: dict[str, set[str]] = {}
    for path in sorted(CHANGELOG_DIR.glob("*.xml")):
        for element in ET.parse(path).getroot().iter():
            if not element.tag.endswith("createTable"):
                continue
            schema = element.get("schemaName", "public")
            name = element.get("tableName")
            if name is None:
                continue
            columns = {
                column.get("name")
                for column in element.iter()
                if column.tag.endswith("column") and column.get("name")
            }
            tables[f"{schema}.{name}"] = {c for c in columns if c}

    # Columns added later by raw ALTER statements would be missed by the
    # element walk above; none exist today, but catch it if that changes.
    for path in sorted(CHANGELOG_DIR.glob("*.xml")):
        for match in re.finditer(
            r"ALTER TABLE (\S+) ADD COLUMN (\w+)", path.read_text(), re.IGNORECASE
        ):
            tables.setdefault(match.group(1), set()).add(match.group(2))

    return tables


@pytest.fixture(scope="module")
def schema() -> Mapping[str, set[str]]:
    columns = _schema_columns()
    if not columns:
        pytest.skip("no Liquibase changelogs found")
    return columns


@pytest.fixture(scope="module")
def inserts() -> list[tuple[str, tuple[str, ...]]]:
    postgres = FakePostgresSource()
    DatasetSeeder(postgres, TINY).seed()  # type: ignore[arg-type]
    return postgres.copied_columns


class TestSeederColumns:
    def test_every_seeded_table_is_in_the_schema(
        self, inserts: list[tuple[str, tuple[str, ...]]], schema: Mapping[str, set[str]]
    ) -> None:
        unknown = {table for table, _ in inserts} - set(schema)
        assert not unknown, f"seeder writes to unknown tables: {sorted(unknown)}"

    def test_no_insert_uses_a_column_the_table_lacks(
        self, inserts: list[tuple[str, tuple[str, ...]]], schema: Mapping[str, set[str]]
    ) -> None:
        problems: list[str] = []
        for table, columns in inserts:
            missing = set(columns) - schema.get(table, set())
            if missing:
                problems.append(f"{table}: {sorted(missing)}")
        assert not problems, "columns not present in the schema:\n" + "\n".join(problems)

    def test_natural_keys_never_reach_the_database(
        self, inserts: list[tuple[str, tuple[str, ...]]]
    ) -> None:
        """The generator refers to everything by natural key; those keys are for
        resolving surrogate ids and must not be inserted."""
        natural_keys = {"machine_code", "operation_key", "sensor_code", "planting_key"}
        for table, columns in inserts:
            leaked = natural_keys & set(columns)
            # `*_code` columns are legitimate on the dimension tables that own
            # them; only the fact inserts are checked here.
            if table in {"agri.machine_telemetry", "agri.sensor_reading"}:
                assert not leaked, f"{table} inserts natural keys: {sorted(leaked)}"

    def test_required_columns_are_supplied(
        self, inserts: list[tuple[str, tuple[str, ...]]], schema: Mapping[str, set[str]]
    ) -> None:
        """Each insert must at least carry its foreign keys, or the rows would
        land orphaned."""
        expected_keys = {
            "agri.field": {"farm_id"},
            "agri.sensor": {"field_id"},
            "agri.sensor_reading": {"sensor_id"},
            "agri.planting": {"field_id", "variety_id"},
            "agri.machine": {"farm_id"},
            "agri.machine_operation": {"machine_id", "field_id"},
            "agri.machine_telemetry": {"machine_id"},
            "agri.harvest": {"planting_id", "field_id"},
        }
        by_table = dict(inserts)
        for table, keys in expected_keys.items():
            assert keys <= set(by_table.get(table, ())), f"{table} is missing {keys}"

    def test_every_declared_table_receives_an_insert(
        self, inserts: list[tuple[str, tuple[str, ...]]]
    ) -> None:
        written = {table for table, _ in inserts}
        assert set(TABLES_IN_DEPENDENCY_ORDER) == written
