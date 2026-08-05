"""Keeps the DAG's pipeline list in sync with the registry.

Airflow carries no ETL dependencies, so `airflow/dags/common/pipelines.py` holds
its own copy of the execution order. That duplication is only safe if something
checks it: rename a pipeline in the registry and the DAG would keep asking for
a name that no longer exists, failing at 2 a.m. rather than in CI.

The DAG module is read and parsed rather than imported, because importing it
would pull in Airflow.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from smart_agri.pipelines import SOIL_SENSOR_STAGES, pipeline_names

DAG_PIPELINES_FILE = (
    Path(__file__).resolve().parents[3] / "airflow" / "dags" / "common" / "pipelines.py"
)


def _literal_from_module(path: Path, name: str) -> object:
    """Read one module-level literal assignment without importing the module."""
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == name and node.value is not None:
                return ast.literal_eval(node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    msg = f"{name} not found in {path}"
    raise AssertionError(msg)


@pytest.fixture(scope="module")
def dag_stages() -> tuple[tuple[str, ...], ...]:
    if not DAG_PIPELINES_FILE.exists():
        pytest.skip(f"DAG file not present at {DAG_PIPELINES_FILE}")
    value = _literal_from_module(DAG_PIPELINES_FILE, "SOIL_SENSOR_STAGES")
    return tuple(tuple(stage) for stage in value)  # type: ignore[union-attr]


class TestStagesMatchRegistry:
    def test_dag_stages_are_identical_to_the_registry(
        self, dag_stages: tuple[tuple[str, ...], ...]
    ) -> None:
        assert dag_stages == SOIL_SENSOR_STAGES

    def test_every_dag_pipeline_is_registered(
        self, dag_stages: tuple[tuple[str, ...], ...]
    ) -> None:
        """The failure this exists to prevent: a DAG task naming a pipeline the
        ETL image cannot build."""
        referenced = {name for stage in dag_stages for name in stage}
        unknown = referenced - set(pipeline_names())
        assert not unknown, f"DAG references unregistered pipelines: {sorted(unknown)}"

    def test_stage_labels_cover_every_stage(self, dag_stages: tuple[tuple[str, ...], ...]) -> None:
        labels = _literal_from_module(DAG_PIPELINES_FILE, "STAGE_LABELS")
        assert len(labels) == len(dag_stages)  # type: ignore[arg-type]


class TestRegistryCoverage:
    def test_every_registered_pipeline_runs_in_some_stage(self) -> None:
        """An orphaned pipeline is either dead code or a forgotten DAG task."""
        staged = {name for stage in SOIL_SENSOR_STAGES for name in stage}
        assert set(pipeline_names()) == staged

    def test_stages_contain_no_duplicates(self) -> None:
        flat = [name for stage in SOIL_SENSOR_STAGES for name in stage]
        assert len(flat) == len(set(flat))

    def test_stages_are_ordered_bronze_silver_gold_load(self) -> None:
        """Each stage may only depend on zones already built."""
        expected_prefix = ("bronze", "silver", "gold", "load")
        for prefix, stage in zip(expected_prefix, SOIL_SENSOR_STAGES, strict=True):
            for name in stage:
                assert name.startswith(f"{prefix}."), f"{name} is not in the {prefix} stage"
