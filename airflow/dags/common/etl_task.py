"""Factory for the DockerOperator tasks that every pipeline is built from.

Airflow never imports the ETL package. It launches the `smart-agri-etl` image
with a command vector, so orchestration and transformation stay independently
deployable and independently testable.

Centralising the operator construction here means the settings that are easy to
get wrong — the network, the tmp-dir mount, the forwarded environment — are
decided once rather than per DAG.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from airflow.providers.docker.operators.docker import DockerOperator

if TYPE_CHECKING:
    from collections.abc import Sequence

# Every variable the ETL container needs to reach the backing services. Airflow
# receives these from Compose and passes them straight through; nothing is
# duplicated in a DAG file.
_FORWARDED_ENV_VARS: tuple[str, ...] = (
    "SMART_AGRI_ENV",
    "SMART_AGRI_LOG_LEVEL",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_HTTP_PORT",
    "CLICKHOUSE_DB",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "HDFS_NAMENODE_HOST",
    "HDFS_NAMENODE_HTTP_PORT",
    "HDFS_USER",
    "HDFS_LAKE_ROOT",
    "HIVE_METASTORE_HOST",
    "HIVE_METASTORE_PORT",
)


def etl_environment(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Build the env block for a task container from Airflow's own environment."""
    env = {name: os.environ[name] for name in _FORWARDED_ENV_VARS if name in os.environ}
    if extra:
        env.update(extra)
    return env


def etl_task(
    task_id: str,
    command: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    **operator_kwargs: Any,
) -> DockerOperator:
    """Return a DockerOperator that runs one `smart-agri` subcommand.

    Args:
        task_id: Airflow task id.
        command: Arguments appended to the image's `smart-agri` entrypoint,
            e.g. `["doctor", "--timeout", "30"]`.
        env: Extra environment variables, merged over the forwarded set.
        operator_kwargs: Passed through to DockerOperator for the rare task that
            needs to override a default.
    """
    defaults: dict[str, Any] = {
        "image": os.environ.get("ETL_IMAGE", "smart-agri-etl:local"),
        "api_version": "auto",
        "docker_url": "unix://var/run/docker.sock",
        # Task containers must join the platform network, otherwise service
        # hostnames such as `namenode` do not resolve.
        "network_mode": os.environ.get("ETL_TASK_NETWORK", "smart-agri_agri-net"),
        "auto_remove": "success",
        # DockerOperator otherwise bind-mounts a host temp directory into the
        # task. With sibling containers that host path does not exist inside the
        # Airflow worker, and the task fails before it starts.
        "mount_tmp_dir": False,
        # The image is built locally and never pushed, so a pull would fail.
        "force_pull": False,
        "tty": False,
        "retrieve_output": False,
    }
    defaults.update(operator_kwargs)

    return DockerOperator(
        task_id=task_id,
        command=list(command),
        environment=etl_environment(env),
        **defaults,
    )
