"""Shared task graph for the two weather DAGs.

`weather_daily` and `weather_backfill` run the same pipelines in the same order;
only their cadence and their intent differ. Building the graph once here means
adding a weather stage cannot leave the two DAGs disagreeing about what the
chain is.
"""

from __future__ import annotations

from airflow.utils.task_group import TaskGroup
from common.etl_task import etl_task
from common.pipelines import WEATHER_STAGE_LABELS, WEATHER_STAGES, task_id_for


def build_weather_stages(execution_date: str = "{{ ds }}") -> None:
    """Create the weather TaskGroups and wire them in order.

    Must be called inside an open `DAG` context.

    Args:
        execution_date: Templated logical date handed to each task. The default
            is the DAG run's own date; the backfill overrides it so every task
            in a manual run shares one window rather than each resolving its own.
    """
    previous: TaskGroup | None = None

    for label, stage in zip(WEATHER_STAGE_LABELS, WEATHER_STAGES, strict=True):
        with TaskGroup(group_id=label) as group:
            for pipeline in stage:
                etl_task(
                    task_id=task_id_for(pipeline),
                    command=["run", pipeline, "--date", execution_date],
                )

        if previous is not None:
            previous >> group
        previous = group
