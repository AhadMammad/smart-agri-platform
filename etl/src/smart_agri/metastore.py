"""Register the lake zones as Hive external tables.

The Metastore is the catalog over the Parquet the pipelines write, and the
direct on-ramp to Iceberg: an Iceberg migration converts tables that are already
catalogued rather than starting from an unregistered pile of files.

Nothing in the current pipeline reads through it — Polars addresses Parquet
paths directly — so this exists to make the lake discoverable and to keep that
migration a storage-layer swap.

The DDL is generated here rather than hand-written because the schemas already
declare the columns. Generating it from the same pandera contracts the pipelines
validate against means the catalog cannot describe a shape the lake does not
have. It is emitted as SQL and run through HiveServer2 with beeline, which keeps
a Thrift client out of the ETL image.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.config import LakeZone
from smart_agri.pipelines.bronze import (
    INCREMENTAL_SPECS,
    INGEST_PARTITION_KEY,
    SNAPSHOT_PARTITION_KEY,
    SNAPSHOT_SPECS,
)
from smart_agri.pipelines.silver_specs import SILVER_SPECS

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pandera.polars as pa

    from smart_agri.config import HdfsSettings

DATABASE = "agri_lake"

#: Polars dtype to Hive type. Hive has no unsigned integers and no timezone-aware
#: timestamp, so both collapse onto the nearest thing it does have.
_HIVE_TYPES: Mapping[type[pl.DataType], str] = {
    pl.Int8: "TINYINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INT",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INT",
    pl.UInt32: "BIGINT",
    pl.UInt64: "BIGINT",
    pl.Float32: "FLOAT",
    pl.Float64: "DOUBLE",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMP",
    pl.String: "STRING",
    pl.Utf8: "STRING",
}

#: Checked in order against the dtype's printed name, for anything the exact
#: table above misses. `datetime` precedes `date` because `Datetime` matches both.
_HIVE_TYPE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("datetime", "TIMESTAMP"),
    ("date", "DATE"),
    ("int", "BIGINT"),
    ("uint", "BIGINT"),
    ("float", "DOUBLE"),
    ("bool", "BOOLEAN"),
    ("str", "STRING"),
    ("utf8", "STRING"),
)

_FALLBACK_HIVE_TYPE = "STRING"


@dataclass(frozen=True, slots=True)
class TableSpec:
    """One external table to register."""

    name: str
    zone: LakeZone
    dataset: str
    partition_key: str
    schema: pa.DataFrameSchema

    def location(self, settings: HdfsSettings) -> str:
        return settings.zone_path(self.zone, self.dataset)


def hive_type(dtype: object) -> str:
    """Hive column type for a pandera/polars dtype.

    Unknown types fall back to STRING rather than raising: a catalog entry that
    is slightly coarse is more useful than a registration step that refuses to
    run because one column has an exotic type.
    """
    for polars_type, hive in _HIVE_TYPES.items():
        try:
            if dtype is polars_type or isinstance(dtype, polars_type):
                return hive
        except TypeError:  # not a class — compare by name below
            continue

    # Fall back to the dtype's printed name. `datetime` is checked before
    # `date` because `Datetime` starts with both.
    name = str(dtype).lower()
    for prefix, hive in _HIVE_TYPE_PREFIXES:
        if name.startswith(prefix):
            return hive
    return _FALLBACK_HIVE_TYPE


def table_specs() -> tuple[TableSpec, ...]:
    """Every Bronze and Silver dataset, as a table to register."""
    tables: list[TableSpec] = []

    for snapshot in SNAPSHOT_SPECS:
        tables.append(
            TableSpec(
                name=f"bronze_{snapshot.dataset}",
                zone=LakeZone.BRONZE,
                dataset=snapshot.dataset,
                partition_key=SNAPSHOT_PARTITION_KEY,
                schema=snapshot.schema,
            )
        )
    for incremental in INCREMENTAL_SPECS:
        tables.append(
            TableSpec(
                name=f"bronze_{incremental.dataset}",
                zone=LakeZone.BRONZE,
                dataset=incremental.dataset,
                partition_key=INGEST_PARTITION_KEY,
                schema=incremental.schema,
            )
        )
    for silver in SILVER_SPECS:
        tables.append(
            TableSpec(
                name=f"silver_{silver.dataset}",
                zone=LakeZone.SILVER,
                dataset=silver.dataset,
                partition_key=silver.partition_key,
                schema=silver.schema,
            )
        )

    return tuple(tables)


def create_table_ddl(table: TableSpec, settings: HdfsSettings) -> str:
    """`CREATE EXTERNAL TABLE` for one dataset.

    External, so dropping the table never deletes the Parquet — the lake is the
    record and the catalog is a description of it. `IF NOT EXISTS` so the whole
    script is safe to re-run on every deploy.
    """
    columns = ",\n".join(
        f"  `{name}` {hive_type(column.dtype.type if column.dtype else None)}"
        for name, column in table.schema.columns.items()
        # The partition column lives in the path, and Hive rejects a table that
        # declares it in both places.
        if name != table.partition_key
    )

    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{table.name} (\n"
        f"{columns}\n"
        f")\n"
        f"PARTITIONED BY (`{table.partition_key}` STRING)\n"
        f"STORED AS PARQUET\n"
        f"LOCATION '{table.location(settings)}'"
    )


def registration_script(settings: HdfsSettings) -> str:
    """The full DDL script: database, tables, and partition discovery.

    `MSCK REPAIR TABLE` is what makes the catalog reflect what the pipelines
    actually wrote — every run adds a partition directory, and without the
    repair Hive would list a table with no data in it.
    """
    parts = [
        f"CREATE DATABASE IF NOT EXISTS {DATABASE}",
        *(create_table_ddl(table, settings) for table in table_specs()),
        *(f"MSCK REPAIR TABLE {DATABASE}.{table.name}" for table in table_specs()),
    ]
    return ";\n\n".join(parts) + ";\n"
