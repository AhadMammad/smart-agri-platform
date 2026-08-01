"""smart-agri — ETL application for the smart agriculture analytics platform.

Data flows Postgres -> HDFS Parquet lake (bronze/silver/gold) -> ClickHouse.
Airflow invokes this package's CLI inside a container, one container per task.
"""

from __future__ import annotations

__version__ = "0.1.0"
