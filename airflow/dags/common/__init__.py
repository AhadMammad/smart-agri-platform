"""Shared DAG building blocks.

Airflow puts the dags folder on sys.path, so DAG files import this as
`from common.etl_task import etl_task`.
"""

from common.etl_task import etl_environment, etl_task
from common.pipelines import (
    SOIL_SENSOR_STAGES,
    STAGE_LABELS,
    WEATHER_STAGE_LABELS,
    WEATHER_STAGES,
    task_id_for,
)

__all__ = [
    "SOIL_SENSOR_STAGES",
    "STAGE_LABELS",
    "WEATHER_STAGES",
    "WEATHER_STAGE_LABELS",
    "etl_environment",
    "etl_task",
    "task_id_for",
]
