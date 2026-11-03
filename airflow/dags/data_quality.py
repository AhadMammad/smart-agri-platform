"""Data quality checks that no domain DAG can run for itself.

Freshness asks whether each domain has succeeded recently enough. It runs here,
on its own hourly schedule, because the failure it exists to catch — a paused
or broken DAG — is exactly the one where that DAG's own checks never run.

Each task is `smart-agri dq run <suite>`, recorded as a `dq.<suite>` run with
its results in etl-meta-db.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from common.etl_task import etl_task
from common.pipelines import SCHEDULED_QUALITY_SUITES

with DAG(
    dag_id="data_quality",
    description="Hourly freshness checks across every domain",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
        "execution_timeout": pendulum.duration(minutes=10),
    },
    tags=["data-quality", "phase-9"],
    doc_md=__doc__,
) as dag:
    for suite in SCHEDULED_QUALITY_SUITES:
        etl_task(
            task_id=suite.replace(".", "_"),
            command=["dq", "run", suite, "--date", "{{ ds }}"],
        )
