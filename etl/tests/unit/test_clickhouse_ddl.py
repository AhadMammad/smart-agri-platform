"""Keeps the ClickHouse DDL in step with the contracts it is loaded from.

Every warehouse table's column list is written twice: once as a pandera schema
in `contracts/schemas.py`, and once as SQL in `clickhouse/ddl/`. `metastore.py`
avoids that duplication for Hive by generating the DDL from the contracts, but
ClickHouse's DDL carries engine, partition key and sort order too, so it stays
hand-written.

Hand-written and unguarded is how a mart gains a column that the load then fails
on at 2 a.m. — or worse, silently drops. This test parses the SQL and compares
it with the schema each table is loaded from.

The load inserts Arrow by column name, so it is the *set* of names that has to
agree; declaration order is free.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from smart_agri.contracts import SILVER_SCHEMAS
from smart_agri.pipelines.bronze import INCREMENTAL_SPECS, SNAPSHOT_SPECS
from smart_agri.pipelines.gold import GoldFieldSoilDailyPipeline
from smart_agri.pipelines.gold_marts import (
    GoldFieldCropHealthDailyPipeline,
    GoldFieldIrrigationDailyPipeline,
    GoldMachineDailyPipeline,
    GoldPlantingEconomicsPipeline,
)
from smart_agri.pipelines.load import DIM_LOAD_SPECS, FACT_LOAD_SPECS, GOLD_LOAD_SPECS
from smart_agri.pipelines.silver_specs import SILVER_SPECS_BY_DATASET
from smart_agri.pipelines.weather import (
    GoldDimDatePipeline,
    GoldFieldWeatherDailyPipeline,
    SilverFactWeatherDailyPipeline,
)

DDL_ROOT = Path(__file__).resolve().parents[3] / "clickhouse" / "ddl"

#: Gold datasets are not spec-driven, so the schema comes off the pipeline class
#: that produces them. Reading the class attribute avoids constructing a
#: pipeline, which would need settings and a context.
GOLD_SCHEMAS = {
    cls.dataset: cls.schema
    for cls in (
        GoldFieldSoilDailyPipeline,
        GoldFieldCropHealthDailyPipeline,
        GoldFieldIrrigationDailyPipeline,
        GoldMachineDailyPipeline,
        GoldPlantingEconomicsPipeline,
        GoldDimDatePipeline,
        GoldFieldWeatherDailyPipeline,
    )
}

_CREATE_TABLE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)^\)",
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)


def _columns_in(block: str) -> list[str]:
    """Column names from the body of a CREATE TABLE, ignoring comments.

    A column line starts with the name. Nested parentheses appear in types
    (`Nullable(Float64)`, `DateTime64(3, 'UTC')`) but never at the start of a
    line, so leading-token extraction is enough.
    """
    names = []
    for raw in block.splitlines():
        line = raw.strip()
        if not line or line.startswith("--"):
            continue
        match = re.match(r"(\w+)\s+\S", line)
        if match:
            names.append(match.group(1))
    return names


def _declared_tables() -> dict[str, list[str]]:
    tables: dict[str, list[str]] = {}
    for path in sorted(DDL_ROOT.rglob("*.sql")):
        for name, block in _CREATE_TABLE.findall(path.read_text()):
            tables[name] = _columns_in(block)
    return tables


@pytest.fixture(scope="module")
def declared() -> dict[str, list[str]]:
    if not DDL_ROOT.is_dir():
        pytest.skip(f"DDL not present at {DDL_ROOT}")
    tables = _declared_tables()
    assert tables, "no CREATE TABLE statements found — the parser has drifted"
    return tables


def _silver_schema(dataset: str):
    """The Silver contract for a dataset, from wherever it is declared."""
    if dataset in SILVER_SPECS_BY_DATASET:
        return SILVER_SPECS_BY_DATASET[dataset].schema
    if dataset in SILVER_SCHEMAS:
        return SILVER_SCHEMAS[dataset]
    if dataset == SilverFactWeatherDailyPipeline.dataset:
        return SilverFactWeatherDailyPipeline.schema
    msg = f"no Silver contract found for {dataset}"
    raise AssertionError(msg)


class TestEveryLoadedTableIsDeclared:
    """A load naming a table that no DDL creates fails only at runtime."""

    def test_every_dimension_table_exists(self, declared: dict[str, list[str]]) -> None:
        missing = [spec.table for spec in DIM_LOAD_SPECS if spec.table not in declared]
        assert not missing, f"dimension tables with no DDL: {missing}"

    def test_every_fact_table_exists(self, declared: dict[str, list[str]]) -> None:
        missing = [spec.table for spec in FACT_LOAD_SPECS if spec.table not in declared]
        assert not missing, f"fact tables with no DDL: {missing}"

    def test_every_gold_table_exists(self, declared: dict[str, list[str]]) -> None:
        missing = [spec.table for spec in GOLD_LOAD_SPECS if spec.table not in declared]
        assert not missing, f"gold tables with no DDL: {missing}"


class TestColumnsMatchTheContract:
    """The failure this prevents: a column added to a contract but not to the
    warehouse, which surfaces as a failed insert or a silently absent metric."""

    @pytest.mark.parametrize("spec", DIM_LOAD_SPECS, ids=lambda s: s.table)
    def test_dimension_columns_match_silver(
        self, spec: object, declared: dict[str, list[str]]
    ) -> None:
        expected = set(_silver_schema(spec.dataset).columns)  # type: ignore[attr-defined]
        assert set(declared[spec.table]) == expected  # type: ignore[attr-defined]

    @pytest.mark.parametrize("spec", FACT_LOAD_SPECS, ids=lambda s: s.table)
    def test_fact_columns_match_silver(self, spec: object, declared: dict[str, list[str]]) -> None:
        expected = set(_silver_schema(spec.dataset).columns)  # type: ignore[attr-defined]
        assert set(declared[spec.table]) == expected  # type: ignore[attr-defined]

    @pytest.mark.parametrize("spec", GOLD_LOAD_SPECS, ids=lambda s: s.table)
    def test_gold_columns_match_the_aggregate(
        self, spec: object, declared: dict[str, list[str]]
    ) -> None:
        schema = GOLD_SCHEMAS[spec.dataset]  # type: ignore[attr-defined]
        assert schema is not None
        assert set(declared[spec.table]) == set(schema.columns)  # type: ignore[attr-defined]


class TestFactLoadSpecsAreCoherent:
    def test_dedupe_key_exists_in_the_contract(self) -> None:
        """Deduplicating on a column that is not there would raise at load time,
        after the extract has already been paid for."""
        for spec in FACT_LOAD_SPECS:
            columns = set(_silver_schema(spec.dataset).columns)
            assert spec.dedupe_on in columns, f"{spec.dataset}: {spec.dedupe_on} not in contract"
            assert spec.sort_by in columns, f"{spec.dataset}: {spec.sort_by} not in contract"

    def test_facts_load_from_incremental_sources(self) -> None:
        """A fact load reads every partition because Silver lands one file per
        batch. A snapshot dataset read that way would be counted twice."""
        incremental = {spec.dataset for spec in INCREMENTAL_SPECS}
        for spec in FACT_LOAD_SPECS:
            silver = SILVER_SPECS_BY_DATASET[spec.dataset]
            assert silver.bronze in incremental, f"{spec.dataset} is not incrementally sourced"

    def test_dimensions_load_from_snapshot_sources(self) -> None:
        snapshot = {spec.dataset for spec in SNAPSHOT_SPECS}
        for spec in DIM_LOAD_SPECS:
            silver = SILVER_SPECS_BY_DATASET[spec.dataset]
            assert silver.bronze in snapshot, f"{spec.dataset} is not snapshot-sourced"
