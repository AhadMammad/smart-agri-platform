"""Copy the run log and every check result from etl-meta-db into ClickHouse.

Superset reads only ClickHouse, so the data quality dashboard needs the
results there. Postgres stays the source of truth; this replaces the copies
wholesale each time, which at these sizes is simpler than an incremental sync
and can never drift.

Runs as the `export` suite, from the hourly data_quality DAG only. One writer
on one schedule: `replace_table` is a TRUNCATE then an INSERT, and two
concurrent exports would interleave.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from smart_agri.quality.results import CheckResult, CheckType, Severity, outcome_for

if TYPE_CHECKING:
    from smart_agri.pipelines.base import PipelineContext


#: ClickHouse table, then the SELECT that produces exactly its columns.
#: UUIDs and JSONB become text in SQL, because the Arrow driver has no faithful
#: mapping for either; `test_quality_export.py` diffs these columns against the
#: Liquibase changelogs and the ClickHouse DDL.
EXPORTS: dict[str, str] = {
    "ops_run_log": """
        SELECT run_id::text AS run_id,
               pipeline,
               logical_date,
               status,
               coalesce(rows_read, 0)        AS rows_read,
               coalesce(rows_written, 0)     AS rows_written,
               coalesce(rows_quarantined, 0) AS rows_quarantined,
               started_at,
               finished_at,
               error_message
          FROM etl_control.run_log
         ORDER BY started_at
    """,
    "ops_dq_check_result": """
        SELECT result_id,
               run_id::text  AS run_id,
               pipeline,
               logical_date,
               check_name,
               check_type,
               dataset,
               target,
               severity,
               outcome,
               observed_value,
               threshold,
               rows_evaluated,
               rows_failed,
               details::text AS details,
               checked_at
          FROM etl_control.dq_check_result
         ORDER BY result_id
    """,
}


def export_operational_data(context: PipelineContext) -> list[CheckResult]:
    """Replace each ops table, then confirm it holds what was read.

    The confirmation is the same row-count reconciliation every load gets: a
    dashboard of data quality results that silently lost some would be the
    least trustworthy chart on the platform.
    """
    results: list[CheckResult] = []
    for table, query in EXPORTS.items():
        frame = context.etl_meta.read_query(query)
        written = context.clickhouse.replace_table(table, frame)
        landed = context.clickhouse.count(table)
        results.append(
            CheckResult(
                check_name="reconciliation.row_count",
                check_type=CheckType.RECONCILIATION,
                severity=Severity.ERROR,
                outcome=outcome_for(
                    breached=landed != frame.height or written != frame.height,
                    severity=Severity.ERROR,
                ),
                dataset=table,
                target=table,
                observed_value=landed,
                threshold=frame.height,
                rows_evaluated=frame.height,
                rows_failed=abs(frame.height - landed),
            )
        )
    return results
