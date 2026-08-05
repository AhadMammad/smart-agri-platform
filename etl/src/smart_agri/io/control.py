"""Pipeline bookkeeping: watermarks and the run log.

State lives in `etl_control` in Postgres rather than in Airflow Variables, so it
is queryable by anyone, survives an Airflow reset, and — the reason that matters
most here — can be tested without an Airflow instance.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import psycopg

from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from datetime import date, datetime

    from smart_agri.config import PostgresSettings

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
