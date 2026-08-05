"""ClickHouse, the serving layer.

Gold Parquet is read from HDFS into Polars and inserted here as Arrow.
ClickHouse's own `hdfs()` table function is deliberately unused: it depends on
an optional `libhdfs3` build that is not reliably present in the official
images, and it would move transformation logic out of tested Python and into
SQL strings.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import polars as pl

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from clickhouse_connect.driver.client import Client

    from smart_agri.config import ClickHouseSettings

logger = get_logger(__name__)

# Table names reach SQL by interpolation — clickhouse-connect has no identifier
# placeholder — so they are validated rather than trusted.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_identifier(name: str, kind: str = "table") -> str:
    if not _IDENTIFIER.match(name):
        msg = f"invalid {kind} name: {name!r}"
        raise ValueError(msg)
    return name


class ClickHouseSink:
    """Writes analytical tables and runs queries against them."""

    def __init__(self, settings: ClickHouseSettings) -> None:
        self._settings = settings
        self._client: Client | None = None

    @property
    def client(self) -> Client:
        """Lazily-opened client, so constructing a sink opens no socket."""
        if self._client is None:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                host=self._settings.host,
                port=self._settings.http_port,
                username=self._settings.user,
                password=self._settings.password.get_secret_value(),
                database=self._settings.db,
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()  # type: ignore[no-untyped-call]
            self._client = None

    def __enter__(self) -> ClickHouseSink:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- statements ----------------------------------------------------------
    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> None:
        self.client.command(sql, parameters=parameters)

    def execute_script(self, script: str) -> int:
        """Run a multi-statement SQL script.

        Statements are split on semicolons at end of line, which is enough for
        the DDL files in `clickhouse/ddl/` and avoids pulling in a SQL parser.
        Comment-only fragments are skipped.

        Returns the number of statements executed.
        """
        statements = [s.strip() for s in re.split(r";\s*(?:\n|$)", script) if s.strip()]
        executed = 0
        for statement in statements:
            if all(
                line.strip().startswith("--") or not line.strip() for line in statement.splitlines()
            ):
                continue
            self.client.command(statement)
            executed += 1
        return executed

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> pl.DataFrame:
        """Run a query and return a Polars DataFrame."""
        arrow_table = self.client.query_arrow(sql, parameters=parameters)
        frame = pl.from_arrow(arrow_table)
        return frame if isinstance(frame, pl.DataFrame) else frame.to_frame()

    def scalar(self, sql: str, parameters: dict[str, Any] | None = None) -> Any:
        result = self.client.query(sql, parameters=parameters)
        return result.result_rows[0][0] if result.result_rows else None

    # -- loading -------------------------------------------------------------
    def insert_frame(self, table: str, frame: pl.DataFrame) -> int:
        """Append a DataFrame to a table.

        Returns the number of rows inserted.
        """
        _check_identifier(table)
        if frame.is_empty():
            logger.warning("clickhouse_insert_skipped", table=table, reason="empty frame")
            return 0

        self.client.insert_arrow(table, frame.to_arrow())
        logger.info("clickhouse_insert", table=table, rows=frame.height)
        return frame.height

    def replace_table(self, table: str, frame: pl.DataFrame) -> int:
        """Replace a table's entire contents.

        Used for dimensions, which are small and snapshot-extracted. `TRUNCATE`
        then insert rather than a swap: at these sizes the brief empty window is
        irrelevant, and it keeps the operation a single obvious step.
        """
        _check_identifier(table)
        self.execute(f"TRUNCATE TABLE IF EXISTS {table}")
        return self.insert_frame(table, frame)

    def delete_partition(self, table: str, partition: str) -> None:
        """Drop one partition so a re-run replaces rather than duplicates.

        This is what makes fact loads idempotent: ClickHouse MergeTree has no
        upsert, so the partition for the logical date is dropped before it is
        rewritten.
        """
        _check_identifier(table)
        self.client.command(
            f"ALTER TABLE {table} DROP PARTITION %(partition)s",
            parameters={"partition": partition},
        )
        logger.info("clickhouse_drop_partition", table=table, partition=partition)

    def table_exists(self, table: str) -> bool:
        _check_identifier(table)
        return bool(
            self.scalar(
                "SELECT count() FROM system.tables " "WHERE database = %(db)s AND name = %(table)s",
                {"db": self._settings.db, "table": table},
            )
        )

    def count(self, table: str) -> int:
        _check_identifier(table)
        return int(self.scalar(f"SELECT count() FROM {table}") or 0)

    def columns(self, table: str) -> Sequence[str]:
        _check_identifier(table)
        frame = self.query(
            "SELECT name FROM system.columns "
            "WHERE database = %(db)s AND table = %(table)s ORDER BY position",
            {"db": self._settings.db, "table": table},
        )
        return [] if frame.is_empty() else frame["name"].to_list()
