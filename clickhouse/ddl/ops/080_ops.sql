-- Operational data: the run log and every data quality result (Phase 9).
--
-- The source of truth is etl-meta-db (Postgres); these are copies, replaced
-- wholesale by `smart-agri dq run export` from the hourly data_quality DAG,
-- so Superset can chart them beside the business data. Nothing here is written
-- by a pipeline directly, and nothing should be: one writer, one schedule.
--
-- The columns mirror etl_control.run_log and etl_control.dq_check_result;
-- test_quality_export.py diffs all three declarations.

CREATE TABLE IF NOT EXISTS ops_run_log
(
    run_id           String,
    pipeline         LowCardinality(String),
    logical_date     Date,
    status           LowCardinality(String),
    rows_read        Int64,
    rows_written     Int64,
    rows_quarantined Int64,
    started_at       DateTime64(3, 'UTC'),
    finished_at      Nullable(DateTime64(3, 'UTC')),
    error_message    Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (pipeline, started_at)
COMMENT 'Every pipeline and suite run, copied from etl-meta-db';


CREATE TABLE IF NOT EXISTS ops_dq_check_result
(
    result_id      Int64,
    run_id         String,
    pipeline       LowCardinality(String),
    logical_date   Date,
    check_name     String,
    check_type     LowCardinality(String),
    dataset        Nullable(String),
    target         Nullable(String),
    severity       LowCardinality(String),
    outcome        LowCardinality(String),
    observed_value Nullable(Float64),
    threshold      Nullable(Float64),
    rows_evaluated Nullable(Int64),
    rows_failed    Nullable(Int64),
    details        String,
    checked_at     DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(checked_at)
ORDER BY (pipeline, check_name, checked_at)
COMMENT 'Every data quality check result, copied from etl-meta-db';


-- The current state of every check: its most recent result. What the status
-- table on the dashboard shows, and what a "how are we doing" question means.
CREATE OR REPLACE VIEW v_dq_latest AS
SELECT
    pipeline,
    check_name,
    argMax(check_type, checked_at)     AS check_type,
    argMax(dataset, checked_at)        AS dataset,
    argMax(target, checked_at)         AS target,
    argMax(severity, checked_at)       AS severity,
    argMax(outcome, checked_at)        AS outcome,
    -- Triage order for the status table: alphabetical would put `warned` last.
    multiIf(outcome = 'failed', 0, outcome = 'warned', 1, outcome = 'skipped', 2, 3)
                                       AS outcome_rank,
    round(argMax(observed_value, checked_at), 4) AS observed_value,
    argMax(threshold, checked_at)      AS threshold,
    argMax(rows_failed, checked_at)    AS rows_failed,
    argMax(rows_evaluated, checked_at) AS rows_evaluated,
    -- Not aliased `checked_at`: ClickHouse resolves an alias before a column,
    -- so every argMax above would then nest this aggregate inside itself.
    max(checked_at)                    AS last_checked_at
FROM ops_dq_check_result
GROUP BY pipeline, check_name;


-- Outcomes per day, for the trend chart. Every outcome is counted, passes
-- included, so a quiet day shows as a full bar rather than an empty one.
CREATE OR REPLACE VIEW v_dq_daily AS
SELECT
    toDate(checked_at) AS check_date,
    check_type,
    outcome,
    count()            AS checks,
    sum(coalesce(rows_failed, 0)) AS rows_failed
FROM ops_dq_check_result
GROUP BY check_date, check_type, outcome;


-- Hours since each pipeline last succeeded, beside how often it fails. Read
-- straight from the run log, so it agrees with the freshness suite.
CREATE OR REPLACE VIEW v_pipeline_freshness AS
SELECT
    pipeline,
    splitByChar('.', pipeline)[1]                       AS stage,
    maxIf(finished_at, status = 'success')              AS last_success_at,
    round(dateDiff('minute', maxIf(finished_at, status = 'success'), now64(3, 'UTC')) / 60, 1)
                                                        AS hours_since_success,
    countIf(status = 'success')                         AS successful_runs,
    countIf(status = 'failed')                          AS failed_runs,
    argMax(rows_written, started_at)                    AS last_rows_written
FROM ops_run_log
GROUP BY pipeline;
