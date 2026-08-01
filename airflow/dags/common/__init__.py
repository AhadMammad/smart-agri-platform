"""Shared DAG building blocks.

Airflow puts the dags folder on sys.path, so DAG files import this as
`from common.etl_task import etl_task`.
"""

from common.etl_task import etl_environment, etl_task

__all__ = ["etl_environment", "etl_task"]
