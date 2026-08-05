"""The soil-sensor slice, end to end.

Postgres → Bronze → Silver → Gold → ClickHouse, one DockerOperator task per
pipeline, grouped into the four lake stages. This is the DAG that proves the
whole platform works; every later domain follows the same shape.

Each task runs `smart-agri run <pipeline> --date {{ ds }}` in a fresh container.
Because the logical date comes from Airflow rather than the wall clock, a
backfill for an old date produces exactly the partitions that date should own.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from common.etl_task import etl_task
from common.pipelines import SOIL_SENSOR_STAGES, STAGE_LABELS, task_id_for

with DAG(
    dag_id="soil_sensor_daily",
    description="Soil sensor readings from Postgres through the lake into ClickHouse",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    # Backfills are run deliberately with `airflow dags backfill`, never by a
    # DAG replaying months of history the first time it is unpaused.
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        # A stuck task holds the only slot this DAG has; fail rather than block.
        "execution_timeout": pendulum.duration(minutes=30),
    },
    tags=["soil-sensor", "field-health", "phase-2"],
    doc_md=__doc__,
) as dag:
    previous_group: TaskGroup | None = None

    for label, stage in zip(STAGE_LABELS, SOIL_SENSOR_STAGES, strict=True):
        with TaskGroup(group_id=label) as group:
            for pipeline in stage:
                etl_task(
                    task_id=task_id_for(pipeline),
                    command=["run", pipeline, "--date", "{{ ds }}"],
                )

        if previous_group is not None:
            previous_group >> group
        previous_group = group
