"""PostgreSQL access.

Two drivers, each for what it is good at:

* **ADBC** for bulk reads — it hands Arrow straight to Polars with no row-by-row
  Python conversion, which is what makes a multi-million-row extract tolerable.
* **psycopg** for control operations and COPY — small transactional statements
  and the fastest bulk-insert path Postgres offers.
"""

from __future__ import annotations

import csv
import io
from typing import TYPE_CHECKING, Any

import polars as pl
import psycopg

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from smart_agri.config import PostgresSettings

logger = get_logger(__name__)


class PostgresSource:
    """Reads business data out of, and bulk-loads seed data into, Postgres."""

    def __init__(self, settings: PostgresSettings) -> None:
        self._settings = settings

    # -- reads ---------------------------------------------------------------
    def read_query(self, sql: str, params: Sequence[Any] | None = None) -> pl.DataFrame:
        """Run a query and return the result as a Polars DataFrame.

        ADBC has no client-side parameter binding for arbitrary statements, so
        parameters are bound through a prepared statement on the server and the
        Arrow result is read back in one go.
        """
        import adbc_driver_postgresql.dbapi as pg_dbapi

        with pg_dbapi.connect(self._settings.uri) as conn, conn.cursor() as cur:
            cur.execute(sql, parameters=list(params) if params else None)
            table = cur.fetch_arrow_table()

        frame = pl.from_arrow(table)
        result = frame if isinstance(frame, pl.DataFrame) else frame.to_frame()
        logger.debug("postgres_read", rows=result.height, columns=result.width)
        return result

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        """Fetch a single value, or None when the query returns no rows."""
        with psycopg.connect(self._settings.uri) as conn:
            row = conn.execute(sql, params).fetchone()
        return row[0] if row else None

    # -- writes --------------------------------------------------------------
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Run a statement in its own transaction."""
        with psycopg.connect(self._settings.uri) as conn:
            conn.execute(sql, params)
            conn.commit()

    def copy_frame(self, frame: pl.DataFrame, table: str, columns: Sequence[str]) -> int:
        """Bulk-insert a DataFrame with COPY.

        COPY rather than executemany because seeding the `large` profile inserts
        tens of millions of rows, where the difference is minutes against hours.

        Args:
            frame: Rows to insert. Only `columns` are read from it.
            table: Schema-qualified target, e.g. `agri.sensor_reading`.
            columns: Target column names, in order.

        Returns:
            Number of rows written.
        """
        if frame.is_empty():
            return 0

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        for row in frame.select(columns).iter_rows():
            writer.writerow(["" if value is None else value for value in row])
        buffer.seek(0)

        column_list = ", ".join(columns)
        statement = f"COPY {table} ({column_list}) FROM STDIN WITH (FORMAT csv, NULL '')"

        with psycopg.connect(self._settings.uri) as conn:
            with conn.cursor().copy(statement) as copy:
                copy.write(buffer.getvalue())
            conn.commit()

        logger.info("postgres_copy", table=table, rows=frame.height)
        return frame.height

    def truncate(self, tables: Iterable[str]) -> None:
        """Empty tables and reset their identity sequences.

        A single statement so foreign keys never block the order of removal.
        """
        target = ", ".join(tables)
        self.execute(f"TRUNCATE TABLE {target} RESTART IDENTITY CASCADE")
        logger.info("postgres_truncate", tables=target)
