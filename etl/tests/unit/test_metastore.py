"""The generated Hive DDL must describe the lake the pipelines actually write."""

from __future__ import annotations

import polars as pl
import pytest

from smart_agri.config import HdfsSettings, LakeZone
from smart_agri.metastore import (
    DATABASE,
    TableSpec,
    create_table_ddl,
    hive_type,
    registration_script,
    table_specs,
)
from smart_agri.pipelines.bronze import (
    INCREMENTAL_SPECS,
    INGEST_PARTITION_KEY,
    SNAPSHOT_PARTITION_KEY,
    SNAPSHOT_SPECS,
)
from smart_agri.pipelines.silver_specs import SILVER_SPECS


@pytest.fixture
def settings() -> HdfsSettings:
    return HdfsSettings()


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (pl.Int8, "TINYINT"),
        (pl.Int16, "SMALLINT"),
        (pl.Int32, "INT"),
        (pl.Int64, "BIGINT"),
        (pl.Float32, "FLOAT"),
        (pl.Float64, "DOUBLE"),
        (pl.Boolean, "BOOLEAN"),
        (pl.Date, "DATE"),
        (pl.String, "STRING"),
    ],
)
def test_hive_type_maps_the_common_dtypes(dtype: object, expected: str) -> None:
    assert hive_type(dtype) == expected


def test_hive_type_accepts_instances_as_well_as_classes() -> None:
    """Pandera hands back instantiated dtypes, not just the classes."""
    assert hive_type(pl.Int64()) == "BIGINT"
    assert hive_type(pl.Datetime(time_unit="us")) == "TIMESTAMP"


def test_hive_type_collapses_what_hive_cannot_express() -> None:
    """Hive has no unsigned integers and no timezone-aware timestamp."""
    assert hive_type(pl.UInt32) == "BIGINT"
    assert hive_type(pl.UInt64) == "BIGINT"
    assert hive_type(pl.Datetime(time_unit="us", time_zone="UTC")) == "TIMESTAMP"


def test_hive_type_falls_back_to_string_for_the_unknown() -> None:
    """A coarse catalog entry beats a registration step that refuses to run."""
    assert hive_type(pl.Struct({"a": pl.Int64})) == "STRING"
    assert hive_type(None) == "STRING"


def test_every_bronze_and_silver_dataset_is_registered() -> None:
    names = {table.name for table in table_specs()}

    for spec in (*SNAPSHOT_SPECS, *INCREMENTAL_SPECS):
        assert f"bronze_{spec.dataset}" in names
    for spec in SILVER_SPECS:
        assert f"silver_{spec.dataset}" in names

    assert len(names) == len(table_specs()), "table names must be unique"


def test_table_specs_carry_the_partition_key_of_their_strategy() -> None:
    """Snapshots partition by snapshot date, incrementals by ingest date."""
    by_name = {table.name: table for table in table_specs()}

    for spec in SNAPSHOT_SPECS:
        assert by_name[f"bronze_{spec.dataset}"].partition_key == SNAPSHOT_PARTITION_KEY
    for spec in INCREMENTAL_SPECS:
        assert by_name[f"bronze_{spec.dataset}"].partition_key == INGEST_PARTITION_KEY
    for spec in SILVER_SPECS:
        assert by_name[f"silver_{spec.dataset}"].partition_key == spec.partition_key


def test_table_location_points_at_its_zone(settings: HdfsSettings) -> None:
    by_name = {table.name: table for table in table_specs()}

    assert by_name["bronze_farm"].location(settings) == settings.zone_uri(LakeZone.BRONZE, "farm")
    assert by_name["silver_dim_farm"].location(settings) == settings.zone_uri(
        LakeZone.SILVER, "dim_farm"
    )


def test_every_location_is_a_fully_qualified_hdfs_uri(settings: HdfsSettings) -> None:
    """A scheme-less location resolves against Hive's own filesystem, not the lake."""
    for table in table_specs():
        assert table.location(settings).startswith("hdfs://")
        assert create_table_ddl(table, settings).count("LOCATION 'hdfs://") == 1


def test_ddl_declares_external_partitioned_parquet(settings: HdfsSettings) -> None:
    table = next(t for t in table_specs() if t.name == "bronze_farm")
    ddl = create_table_ddl(table, settings)

    assert ddl.startswith(f"CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.bronze_farm (")
    assert "STORED AS PARQUET" in ddl
    assert f"PARTITIONED BY (`{table.partition_key}` STRING)" in ddl
    assert f"LOCATION '{table.location(settings)}'" in ddl


def test_ddl_columns_match_the_contract(settings: HdfsSettings) -> None:
    """The catalog cannot describe a shape the contract does not validate."""
    table = next(t for t in table_specs() if t.name == "bronze_farm")
    ddl = create_table_ddl(table, settings)

    for name in table.schema.columns:
        if name != table.partition_key:
            assert f"`{name}`" in ddl


def test_ddl_never_declares_the_partition_column_twice(settings: HdfsSettings) -> None:
    """Hive rejects a table that names a partition column in the body as well."""
    for table in table_specs():
        if table.partition_key not in table.schema.columns:
            continue
        body, _, partitions = create_table_ddl(table, settings).partition("PARTITIONED BY")
        assert f"`{table.partition_key}`" not in body
        assert f"`{table.partition_key}`" in partitions


def test_ddl_uses_the_contract_dtype(settings: HdfsSettings) -> None:
    schema = next(t for t in table_specs() if t.name == "bronze_farm").schema
    ddl = create_table_ddl(
        TableSpec(
            name="bronze_farm",
            zone=LakeZone.BRONZE,
            dataset="farm",
            partition_key=SNAPSHOT_PARTITION_KEY,
            schema=schema,
        ),
        settings,
    )

    assert "`farm_id` BIGINT" in ddl
    assert "`latitude` DOUBLE" in ddl
    assert "`farm_code` STRING" in ddl


def test_script_creates_the_database_before_any_table(settings: HdfsSettings) -> None:
    script = registration_script(settings)

    assert script.index(f"CREATE DATABASE IF NOT EXISTS {DATABASE}") < script.index(
        "CREATE EXTERNAL TABLE"
    )


def test_script_repairs_every_table_it_creates(settings: HdfsSettings) -> None:
    """Without MSCK the catalog lists tables with no partitions — so no data."""
    script = registration_script(settings)

    for table in table_specs():
        assert f"MSCK REPAIR TABLE {DATABASE}.{table.name};" in script


def test_script_repairs_only_after_every_table_exists(settings: HdfsSettings) -> None:
    script = registration_script(settings)

    assert script.index("MSCK REPAIR TABLE") > script.rindex("CREATE EXTERNAL TABLE")


def test_script_is_safe_to_re_run(settings: HdfsSettings) -> None:
    """Registration runs on every deploy, so nothing in it may fail on a second pass."""
    script = registration_script(settings)

    for statement in script.split(";"):
        if statement.strip().startswith("CREATE"):
            assert "IF NOT EXISTS" in statement


def test_script_statements_are_terminated(settings: HdfsSettings) -> None:
    """beeline splits on semicolons — an unterminated tail would be dropped."""
    script = registration_script(settings)

    assert script.endswith(";\n")
    assert script.count(";") == len(table_specs()) * 2 + 1


def test_catalog_covers_every_bronze_and_silver_pipeline() -> None:
    """The catalog must not drift from the pipelines that write the lake.

    The weather Bronze datasets have no extraction spec — they are pipeline
    classes — so they are declared by hand in `metastore`. This is the guard
    that catches the next dataset added the same way.
    """
    from smart_agri.pipelines import pipeline_names

    registered = {table.name for table in table_specs()}
    written = {
        name.replace(".", "_", 1)
        for name in pipeline_names()
        if name.startswith(("bronze.", "silver."))
    }

    assert written - registered == set(), "pipelines writing datasets Hive does not know about"


def test_weather_datasets_are_registered() -> None:
    """Weather has no extraction spec, so it is the easiest thing to leave out."""
    names = {table.name for table in table_specs()}

    assert {
        "bronze_weather_archive",
        "bronze_weather_forecast",
        "silver_fact_weather_daily",
    } <= names
