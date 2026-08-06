"""In-memory doubles for the io layer.

These let every pipeline be tested against real Polars frames without a running
stack, which is what keeps the unit suite fast enough to run on each save. The
integration suite covers the real connectors.

Each fake reproduces the behaviour a pipeline actually depends on — path
semantics, watermark monotonicity, replace-vs-append — rather than recording
calls, so a test failure means the pipeline is wrong rather than that a mock
expectation drifted.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import polars as pl

from smart_agri.config import HdfsSettings
from smart_agri.io.control import RunStats, RunStatus

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from datetime import date, datetime

    from smart_agri.config import LakeZone


class FakeHdfsStore:
    """Lake backed by a dict keyed on path."""

    def __init__(self, settings: HdfsSettings | None = None) -> None:
        self._settings = settings or HdfsSettings()
        self.files: dict[str, pl.DataFrame] = {}
        self.removed: list[str] = []

    def zone_path(self, zone: LakeZone, *parts: str) -> str:
        return self._settings.zone_path(zone, *parts)

    def write_parquet(self, frame: pl.DataFrame, path: str) -> int:
        self.files[path] = frame
        return frame.height

    def read_parquet(self, path: str) -> pl.DataFrame:
        try:
            return self.files[path]
        except KeyError:
            msg = f"lake path does not exist: {path}"
            raise FileNotFoundError(msg) from None

    def read_parquet_dir(self, path: str, *, missing_ok: bool = False) -> pl.DataFrame:
        prefix = path.rstrip("/") + "/"
        matches = [self.files[key] for key in sorted(self.files) if key.startswith(prefix)]
        if not matches:
            if missing_ok:
                return pl.DataFrame()
            msg = f"no parquet files under: {path}"
            raise FileNotFoundError(msg)
        return pl.concat(matches, how="vertical_relaxed")

    def exists(self, path: str) -> bool:
        prefix = path.rstrip("/") + "/"
        return path in self.files or any(key.startswith(prefix) for key in self.files)

    def list_files(self, path: str, suffix: str | None = None) -> Sequence[str]:
        prefix = path.rstrip("/") + "/"
        found = [key for key in sorted(self.files) if key.startswith(prefix)]
        return [key for key in found if suffix is None or key.endswith(suffix)]

    def makedirs(self, path: str) -> None:
        return None

    def remove(self, path: str, *, recursive: bool = True) -> None:
        self.removed.append(path)
        prefix = path.rstrip("/") + "/"
        for key in [k for k in self.files if k == path or k.startswith(prefix)]:
            del self.files[key]


class FakePostgresSource:
    """Source that answers queries from a table-name lookup."""

    def __init__(self, tables: dict[str, pl.DataFrame] | None = None) -> None:
        self.tables = tables or {}
        self.queries: list[tuple[str, Sequence[Any] | None]] = []
        self.copied: list[tuple[str, int]] = []
        #: (table, columns) for every insert, so a test can check the
        #: seeder only writes columns the real schema actually has.
        self.copied_columns: list[tuple[str, tuple[str, ...]]] = []
        self.truncated: list[str] = []

    def read_query(self, sql: str, params: Sequence[Any] | None = None) -> pl.DataFrame:
        """Resolve the query by finding a known table name inside it.

        Longest name first, because table names nest: `agri.sensor` is a prefix
        of `agri.sensor_reading`, and matching the short one would silently
        return the wrong frame.
        """
        self.queries.append((sql, params))
        for name in sorted(self.tables, key=len, reverse=True):
            if name in sql:
                return self._apply_watermark(self.tables[name], sql, params)
        msg = f"no fake table matches query: {sql}"
        raise KeyError(msg)

    @staticmethod
    def _apply_watermark(
        frame: pl.DataFrame, sql: str, params: Sequence[Any] | None
    ) -> pl.DataFrame:
        """Honour a `WHERE <col> > $1` clause so incremental tests are real."""
        if not params or "WHERE" not in sql.upper():
            return frame
        column = sql.split("WHERE")[1].split(">")[0].strip()
        if column not in frame.columns:
            return frame
        return frame.filter(pl.col(column) > params[0])

    def copy_frame(self, frame: pl.DataFrame, table: str, columns: Sequence[str]) -> int:
        """Insert rows, assigning a surrogate key the way Postgres would.

        The identity column matters: the seeder inserts each level and then
        reads the generated ids back to resolve the next level's foreign keys.
        A fake that skipped this would make that whole mechanism untested.
        """
        self.copied.append((table, frame.height))
        self.copied_columns.append((table, tuple(columns)))
        selected = frame.select(columns)

        identity = self._identity_column(table)
        if identity and identity not in selected.columns:
            start = 1 + (self.tables[table].height if table in self.tables else 0)
            selected = selected.with_columns(
                pl.Series(identity, range(start, start + selected.height), dtype=pl.Int64)
            )

        existing = self.tables.get(table)
        self.tables[table] = (
            selected
            if existing is None
            else pl.concat([existing, selected], how="vertical_relaxed")
        )
        return frame.height

    @staticmethod
    def _identity_column(table: str) -> str | None:
        """`agri.sensor_reading` -> `reading_id`, matching the real schema.

        Every table the seeder writes needs an entry: a missing one silently
        skips key assignment and the next level's foreign-key lookup fails.
        """
        leaf = table.split(".")[-1]
        return {
            "crop_variety": "variety_id",
            "farm": "farm_id",
            "field": "field_id",
            "sensor": "sensor_id",
            "sensor_reading": "reading_id",
            "planting": "planting_id",
            "irrigation_event": "irrigation_id",
            "input_application": "application_id",
            "harvest": "harvest_id",
            "field_cost": "cost_id",
            "machine": "machine_id",
            "machine_operation": "operation_id",
            "machine_telemetry": "telemetry_id",
            "machine_fault": "fault_id",
            "field_index_observation": "observation_id",
        }.get(leaf)

    def truncate(self, tables: Iterable[str]) -> None:
        for table in tables:
            self.truncated.append(table)
            self.tables.pop(table, None)


class FakeControlStore:
    """Watermarks and run log held in memory."""

    def __init__(self) -> None:
        self.watermarks: dict[str, datetime | None] = {}
        self.runs: list[dict[str, Any]] = []

    def get_watermark(self, pipeline: str) -> datetime | None:
        return self.watermarks.get(pipeline)

    def set_watermark(
        self, pipeline: str, source_table: str, watermark_column: str, value: datetime
    ) -> None:
        """Never moves backwards, matching the SQL `GREATEST` guard."""
        del source_table, watermark_column
        current = self.watermarks.get(pipeline)
        self.watermarks[pipeline] = value if current is None else max(current, value)

    def reset_watermark(self, pipeline: str) -> None:
        self.watermarks[pipeline] = None

    def start_run(self, pipeline: str, logical_date: date) -> uuid.UUID:
        run_id = uuid.uuid4()
        self.runs.append(
            {
                "run_id": run_id,
                "pipeline": pipeline,
                "logical_date": logical_date,
                "status": RunStatus.RUNNING,
                "stats": None,
                "error": None,
            }
        )
        return run_id

    def finish_run(
        self,
        run_id: uuid.UUID,
        status: RunStatus,
        stats: RunStats | None = None,
        error: str | None = None,
    ) -> None:
        for run in self.runs:
            if run["run_id"] == run_id:
                run.update(status=status, stats=stats or RunStats(), error=error)
                return
        msg = f"unknown run_id {run_id}"
        raise KeyError(msg)

    @property
    def last_run(self) -> dict[str, Any]:
        return self.runs[-1]


class FakeClickHouseSink:
    """Warehouse tables held as frames."""

    def __init__(self) -> None:
        self.tables: dict[str, pl.DataFrame] = {}
        self.dropped_partitions: list[tuple[str, str]] = []
        self.statements: list[str] = []

    def insert_frame(self, table: str, frame: pl.DataFrame) -> int:
        if frame.is_empty():
            return 0
        existing = self.tables.get(table)
        self.tables[table] = (
            frame if existing is None else pl.concat([existing, frame], how="vertical_relaxed")
        )
        return frame.height

    def replace_table(self, table: str, frame: pl.DataFrame) -> int:
        self.tables[table] = frame
        return frame.height

    def delete_partition(self, table: str, partition: str) -> None:
        self.dropped_partitions.append((table, partition))

    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> None:
        del parameters
        self.statements.append(sql)

    def count(self, table: str) -> int:
        frame = self.tables.get(table)
        return 0 if frame is None else frame.height

    def table_exists(self, table: str) -> bool:
        return table in self.tables
