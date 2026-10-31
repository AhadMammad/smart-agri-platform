"""Pipeline bookkeeping: watermarks, the run log and data quality results.

State lives in `etl_control` in etl-meta-db rather than in Airflow Variables, so it
is queryable by anyone, survives an Airflow reset, and — the reason that matters
most here — can be tested without an Airflow instance.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from typing import TYPE_CHECKING

import psycopg
from psycopg.types.json import Jsonb

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date, datetime

    from smart_agri.config import PostgresSettings
    from smart_agri.quality import CheckResult

logger = get_logger(__name__)


class RunStatus(StrEnum):
    """Mirrors the CHECK constraint on etl_control.run_log."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RunStats:
    """Row counts a pipeline reports when it finishes."""

    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0


@dataclass(frozen=True, slots=True)
class RunRecord:
    """A finished run, as the volume check reads its history."""

    logical_date: date
    rows_read: int
    rows_written: int
    rows_quarantined: int


#: Columns `record_checks` writes, in order. `result_id` and `checked_at` take
#: their database defaults. `test_control.py` diffs this against the changelog.
DQ_RESULT_COLUMNS: tuple[str, ...] = (
    "run_id",
    "pipeline",
    "logical_date",
    "check_name",
    "check_type",
    "dataset",
    "target",
    "severity",
    "outcome",
    "observed_value",
    "threshold",
    "rows_evaluated",
    "rows_failed",
    "details",
)

# Check details are free-form; a date or Decimal in them must not fail the run.
_dumps = partial(json.dumps, default=str, sort_keys=True)


def dq_result_row(
    run_id: uuid.UUID, pipeline: str, logical_date: date, result: CheckResult
) -> tuple[object, ...]:
    """One `dq_check_result` row, in `DQ_RESULT_COLUMNS` order."""
    return (
        run_id,
        pipeline,
        logical_date,
        result.check_name,
        result.check_type.value,
        result.dataset,
        result.target,
        result.severity.value,
        result.outcome.value,
        result.observed_value,
        result.threshold,
        result.rows_evaluated,
        result.rows_failed,
        Jsonb(dict(result.details), dumps=_dumps),
    )


class ControlStore:
    """Reads and writes the ETL control tables."""

    def __init__(self, settings: PostgresSettings) -> None:
        self._settings = settings

    # -- watermarks ----------------------------------------------------------
    def get_watermark(self, pipeline: str) -> datetime | None:
        """High-water mark for a pipeline.

        `None` means the pipeline has never completed, which callers must treat
        as "extract everything" rather than as an error.
        """
        with psycopg.connect(self._settings.uri) as conn:
            row = conn.execute(
                "SELECT watermark_value FROM etl_control.watermark WHERE pipeline = %s",
                (pipeline,),
            ).fetchone()
        return row[0] if row else None

    def set_watermark(
        self,
        pipeline: str,
        source_table: str,
        watermark_column: str,
        value: datetime,
    ) -> None:
        """Advance the watermark, never backwards.

        The `GREATEST` guard means a re-run over an older window cannot rewind
        the mark and cause the next run to re-extract rows already loaded.
        """
        with psycopg.connect(self._settings.uri) as conn:
            conn.execute(
                """
                INSERT INTO etl_control.watermark
                    (pipeline, source_table, watermark_column, watermark_value, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (pipeline) DO UPDATE SET
                    source_table     = EXCLUDED.source_table,
                    watermark_column = EXCLUDED.watermark_column,
                    watermark_value  = GREATEST(
                        etl_control.watermark.watermark_value, EXCLUDED.watermark_value
                    ),
                    updated_at       = now()
                """,
                (pipeline, source_table, watermark_column, value),
            )
            conn.commit()
        logger.info("watermark_set", pipeline=pipeline, value=value.isoformat())

    def reset_watermark(self, pipeline: str) -> None:
        """Clear a watermark so the next run performs a full extract."""
        with psycopg.connect(self._settings.uri) as conn:
            conn.execute(
                "UPDATE etl_control.watermark SET watermark_value = NULL WHERE pipeline = %s",
                (pipeline,),
            )
            conn.commit()
        logger.info("watermark_reset", pipeline=pipeline)

    # -- run log -------------------------------------------------------------
    def start_run(self, pipeline: str, logical_date: date) -> uuid.UUID:
        """Open a run-log entry and return its id."""
        run_id = uuid.uuid4()
        with psycopg.connect(self._settings.uri) as conn:
            conn.execute(
                """
                INSERT INTO etl_control.run_log (run_id, pipeline, logical_date, status)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, pipeline, logical_date, RunStatus.RUNNING.value),
            )
            conn.commit()
        return run_id

    def finish_run(
        self,
        run_id: uuid.UUID,
        status: RunStatus,
        stats: RunStats | None = None,
        error: str | None = None,
    ) -> None:
        """Close a run-log entry with its outcome and row counts."""
        stats = stats or RunStats()
        with psycopg.connect(self._settings.uri) as conn:
            conn.execute(
                """
                UPDATE etl_control.run_log
                   SET status           = %s,
                       rows_read        = %s,
                       rows_written     = %s,
                       rows_quarantined = %s,
                       finished_at      = now(),
                       error_message    = %s
                 WHERE run_id = %s
                """,
                (
                    status.value,
                    stats.rows_read,
                    stats.rows_written,
                    stats.rows_quarantined,
                    error,
                    run_id,
                ),
            )
            conn.commit()

    def recent_runs(self, pipeline: str, limit: int = 14) -> list[RunRecord]:
        """The latest successful runs of a pipeline, newest first."""
        with psycopg.connect(self._settings.uri) as conn:
            rows = conn.execute(
                """
                SELECT logical_date, rows_read, rows_written, rows_quarantined
                  FROM etl_control.run_log
                 WHERE pipeline = %s AND status = %s
                 ORDER BY started_at DESC
                 LIMIT %s
                """,
                (pipeline, RunStatus.SUCCESS.value, limit),
            ).fetchall()
        return [RunRecord(*row) for row in rows]

    def last_success(self, pipelines: Sequence[str]) -> dict[str, datetime]:
        """When each pipeline last finished successfully. Absent if it never has."""
        if not pipelines:
            return {}
        with psycopg.connect(self._settings.uri) as conn:
            rows = conn.execute(
                """
                SELECT pipeline, max(finished_at)
                  FROM etl_control.run_log
                 WHERE status = %s AND pipeline = ANY(%s)
                 GROUP BY pipeline
                """,
                (RunStatus.SUCCESS.value, list(pipelines)),
            ).fetchall()
        return {pipeline: finished for pipeline, finished in rows if finished is not None}

    # -- data quality ----------------------------------------------------------
    def record_checks(
        self,
        run_id: uuid.UUID,
        pipeline: str,
        logical_date: date,
        results: Sequence[CheckResult],
    ) -> None:
        """Persist every check result of one run, in one transaction."""
        if not results:
            return
        placeholders = ", ".join(["%s"] * len(DQ_RESULT_COLUMNS))
        query = (
            f"INSERT INTO etl_control.dq_check_result ({', '.join(DQ_RESULT_COLUMNS)}) "
            f"VALUES ({placeholders})"
        )
        rows = [dq_result_row(run_id, pipeline, logical_date, r) for r in results]
        with psycopg.connect(self._settings.uri) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, rows)
            conn.commit()
