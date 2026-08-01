"""DAG integrity tests.

Run inside the Airflow container (`make test-dags`), because Airflow itself is
the only place the DAG-parsing dependencies exist — the ETL application must
stay free of them.

These catch the failure mode that costs the most time: a DAG that imports fine
in an editor but breaks the scheduler for every other DAG in the folder.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_FOLDER = Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"))


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


class TestDagBag:
    def test_no_import_errors(self, dagbag: DagBag) -> None:
        assert not dagbag.import_errors, (
            "DAG import failures:\n"
            + "\n".join(f"  {path}: {err}" for path, err in dagbag.import_errors.items())
        )

    def test_at_least_one_dag_is_loaded(self, dagbag: DagBag) -> None:
        """Guards against a silently empty dags folder — a mounted-volume typo
        looks identical to 'no errors' otherwise."""
        assert dagbag.dags, f"no DAGs discovered under {DAGS_FOLDER}"

    def test_platform_healthcheck_is_present(self, dagbag: DagBag) -> None:
        assert "platform_healthcheck" in dagbag.dags


class TestDagConventions:
    def test_every_dag_has_an_owner_and_retries(self, dagbag: DagBag) -> None:
        for dag_id, dag in dagbag.dags.items():
            assert dag.default_args.get("owner"), f"{dag_id} has no owner"
            assert dag.default_args.get("retries") is not None, f"{dag_id} sets no retries"

    def test_every_dag_is_tagged(self, dagbag: DagBag) -> None:
        """Tags are how the four analytics domains stay navigable in the UI once
        later phases add a DAG per domain."""
        for dag_id, dag in dagbag.dags.items():
            assert dag.tags, f"{dag_id} has no tags"

    def test_no_dag_has_catchup_enabled(self, dagbag: DagBag) -> None:
        """Backfills are run deliberately via the backfill tooling, never by a
        DAG quietly replaying months of history on first unpause."""
        for dag_id, dag in dagbag.dags.items():
            assert dag.catchup is False, f"{dag_id} has catchup enabled"

    def test_no_task_cycles(self, dagbag: DagBag) -> None:
        for dag in dagbag.dags.values():
            dag.validate()

    def test_task_ids_are_unique_within_each_dag(self, dagbag: DagBag) -> None:
        for dag_id, dag in dagbag.dags.items():
            task_ids = [t.task_id for t in dag.tasks]
            assert len(task_ids) == len(set(task_ids)), f"{dag_id} has duplicate task ids"


class TestEtlTaskFactory:
    def test_tasks_do_not_mount_a_host_temp_dir(self, dagbag: DagBag) -> None:
        """With sibling containers the host temp path does not exist inside the
        worker, and the task fails before the command ever runs."""
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                if type(task).__name__ == "DockerOperator":
                    assert task.mount_tmp_dir is False, (
                        f"{dag_id}.{task.task_id} would mount a host temp dir"
                    )

    def test_tasks_join_the_platform_network(self, dagbag: DagBag) -> None:
        expected = os.environ.get("ETL_TASK_NETWORK", "smart-agri_agri-net")
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                if type(task).__name__ == "DockerOperator":
                    assert task.network_mode == expected, (
                        f"{dag_id}.{task.task_id} is not on the platform network"
                    )
