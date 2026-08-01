"""Platform health check.

Phase 1's exit criterion, expressed as a DAG. Running it proves three things at
once: the ETL image is launchable, DockerOperator can reach the host daemon
through the mounted socket, and a task container can resolve and reach every
backing service on the platform network.

It is intentionally the simplest possible DAG — if this one fails, no pipeline
built in a later phase can work.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from common.etl_task import etl_task

with DAG(
    dag_id="platform_healthcheck",
    description="Verify every backing service is reachable from a task container",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["platform", "phase-1"],
) as dag:
    etl_task(
        task_id="check_services",
        # A generous timeout: HDFS can be slow to answer while the DataNode is
        # still registering with the NameNode after a cold start.
        command=["doctor", "--timeout", "30"],
    )
