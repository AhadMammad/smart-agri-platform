"""One-shot move of `etl_control` from the source database to etl-meta-db.

Phases 2-8 kept the ETL's bookkeeping inside the OLTP source. Phase 9 moves it to
its own database; this copies what a running deployment already has, so the
incremental pipelines keep their watermarks and the run history survives.

It is idempotent and safe to run on every `make migrate`:
- a fresh clone has no `etl_control` in the source, so there is nothing to copy;
- a second run inserts nothing new, because run ids conflict and watermarks only
  ever move forwards;
- once the source changelog has dropped the old schema, it is a no-op.

`make migrate` runs it after the etl-meta-db changelog and *before* the source
changelog, whose changeset 011 drops the old schema. The drop therefore never
runs ahead of the copy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger(__name__)


class _Cursor(Protocol):
    def fetchone(self) -> Any: ...
    def fetchall(self) -> list[Any]: ...


class Connection(Protocol):
    """The slice of a psycopg connection this module uses."""

    def execute(self, query: str, params: Sequence[Any] | None = None) -> _Cursor: ...
    def commit(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ControlTable:
    """A table to move, and how a row already present in the target is merged."""

    name: str
    columns: tuple[str, ...]
    on_conflict: str

    @property
    def select_sql(self) -> str:
        return f"SELECT {', '.join(self.columns)} FROM etl_control.{self.name}"

    @property
    def insert_sql(self) -> str:
        placeholders = ", ".join(["%s"] * len(self.columns))
        return (
            f"INSERT INTO etl_control.{self.name} ({', '.join(self.columns)}) "
            f"VALUES ({placeholders}) {self.on_conflict}"
        )


CONTROL_TABLES: tuple[ControlTable, ...] = (
    ControlTable(
        name="watermark",
        columns=("pipeline", "source_table", "watermark_column", "watermark_value", "updated_at"),
        # A pipeline may already have run against the new database. Keep the
        # later mark either way, exactly as `ControlStore.set_watermark` does.
        on_conflict=(
            "ON CONFLICT (pipeline) DO UPDATE SET watermark_value = GREATEST("
            "etl_control.watermark.watermark_value, EXCLUDED.watermark_value)"
        ),
    ),
    ControlTable(
        name="run_log",
        columns=(
            "run_id",
            "pipeline",
            "logical_date",
            "status",
            "rows_read",
            "rows_written",
            "rows_quarantined",
            "started_at",
            "finished_at",
            "error_message",
        ),
        on_conflict="ON CONFLICT (run_id) DO NOTHING",
    ),
)

_SOURCE_HAS_SCHEMA = "SELECT to_regclass('etl_control.run_log') IS NOT NULL"


@dataclass(frozen=True, slots=True)
class CopyReport:
    """Rows found in the source, per table. Empty when there was nothing to copy."""

    copied: dict[str, int]

    @property
    def skipped(self) -> bool:
        return not self.copied

    def summary(self) -> str:
        if self.skipped:
            return "source has no etl_control schema — nothing to copy"
        return ", ".join(f"{table}={rows}" for table, rows in self.copied.items())


class ControlCopyError(RuntimeError):
    """The target is missing rows the source has; the source must not be dropped."""


def copy_control_tables(source: Connection, target: Connection) -> CopyReport:
    """Copy every `etl_control` row from `source` into `target`, then verify.

    Raises `ControlCopyError` if any table in the target ends up with fewer rows
    than the source — the caller (`make migrate`) then stops before the source
    changelog drops the old schema.
    """
    exists = source.execute(_SOURCE_HAS_SCHEMA).fetchone()
    if not exists or not exists[0]:
        logger.info("etl_control_copy_skipped", reason="source has no etl_control")
        return CopyReport(copied={})

    copied: dict[str, int] = {}
    for table in CONTROL_TABLES:
        rows = source.execute(table.select_sql).fetchall()
        for row in rows:
            target.execute(table.insert_sql, tuple(row))
        target.commit()
        copied[table.name] = len(rows)

        landed = target.execute(f"SELECT count(*) FROM etl_control.{table.name}").fetchone()
        landed_rows = int(landed[0]) if landed else 0
        if landed_rows < len(rows):
            msg = (
                f"etl_control.{table.name}: source has {len(rows)} rows, "
                f"etl-meta-db has {landed_rows} — refusing to continue"
            )
            raise ControlCopyError(msg)
        logger.info("etl_control_copied", table=table.name, rows=len(rows), target=landed_rows)

    return CopyReport(copied=copied)
