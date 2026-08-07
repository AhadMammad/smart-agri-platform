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

from smart_agri.pipelines import (
    DOMAIN_STAGES,
    SOIL_SENSOR_STAGES,
    WEATHER_STAGES,
    pipeline_names,
)

DAG_PIPELINES_FILE = (
    Path(__file__).resolve().parents[3] / "airflow" / "dags" / "common" / "pipelines.py"
)


def _module_constants(path: Path) -> dict[str, object]:
    """Evaluate a module's top-level constant assignments without importing it.

    Importing would pull in Airflow. `ast.literal_eval` alone is not enough
    either: `DOMAIN_STAGES` refers to the other constants by name, so the
    assignments are walked in order and earlier names are resolved from what has
    already been evaluated.
    """
    namespace: dict[str, object] = {}

    def evaluate(node: ast.expr) -> object:
        if isinstance(node, ast.Name):
            if node.id not in namespace:
                msg = f"{node.id} is referenced before it is defined"
                raise KeyError(msg)
            return namespace[node.id]
        if isinstance(node, ast.Tuple):
            return tuple(evaluate(item) for item in node.elts)
        if isinstance(node, ast.List):
            return [evaluate(item) for item in node.elts]
        if isinstance(node, ast.Dict):
            return {
                evaluate(key): evaluate(value)
                for key, value in zip(node.keys, node.values, strict=True)
                if key is not None
            }
        return ast.literal_eval(node)

    for node in ast.parse(path.read_text(), filename=str(path)).body:
        targets: list[str] = []
        value: ast.expr | None = None

        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets, value = [node.target.id], node.value
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            value = node.value

        if targets and value is not None:
            try:
                resolved = evaluate(value)
            except (ValueError, KeyError, TypeError):
                continue  # not a constant; nothing this test needs
            for name in targets:
                namespace[name] = resolved

    return namespace


def _literal_from_module(path: Path, name: str) -> object:
    """Read one module-level constant, resolving references to earlier ones."""
    constants = _module_constants(path)
    if name not in constants:
        msg = f"{name} not found in {path}"
        raise AssertionError(msg)
    return constants[name]


def _stages(name: str) -> tuple[tuple[str, ...], ...]:
    if not DAG_PIPELINES_FILE.exists():
        pytest.skip(f"DAG file not present at {DAG_PIPELINES_FILE}")
    value = _literal_from_module(DAG_PIPELINES_FILE, name)
    return tuple(tuple(stage) for stage in value)  # type: ignore[union-attr]


@pytest.fixture(scope="module")
def dag_stages() -> tuple[tuple[str, ...], ...]:
    return _stages("SOIL_SENSOR_STAGES")


@pytest.fixture(scope="module")
def dag_weather_stages() -> tuple[tuple[str, ...], ...]:
    return _stages("WEATHER_STAGES")


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


class TestWeatherStagesMatchRegistry:
    def test_dag_weather_stages_are_identical_to_the_registry(
        self, dag_weather_stages: tuple[tuple[str, ...], ...]
    ) -> None:
        assert dag_weather_stages == WEATHER_STAGES

    def test_every_weather_pipeline_is_registered(
        self, dag_weather_stages: tuple[tuple[str, ...], ...]
    ) -> None:
        referenced = {name for stage in dag_weather_stages for name in stage}
        unknown = referenced - set(pipeline_names())
        assert not unknown, f"weather DAG references unregistered pipelines: {sorted(unknown)}"

    def test_weather_reruns_its_own_source_extracts(self) -> None:
        """A weather DAG must not depend on the soil DAG having run first, so it
        re-extracts the farm and field snapshots its joins need."""
        first_stage = WEATHER_STAGES[0]
        assert "bronze.farm" in first_stage
        assert "bronze.field" in first_stage


class TestDomainStagesMatchRegistry:
    """Every Phase 5 domain exists on both sides and agrees.

    The DAG module carries its own copy of the domain map because Airflow holds
    no ETL dependencies. Six domains multiply the chance of drift, so each one
    is compared rather than just the two the earlier tests cover.
    """

    @pytest.fixture(scope="class")
    def dag_domains(self) -> dict[str, tuple[tuple[str, ...], ...]]:
        value = _literal_from_module(DAG_PIPELINES_FILE, "DOMAIN_STAGES")
        return {
            name: tuple(tuple(stage) for stage in stages)
            for name, stages in value.items()  # type: ignore[union-attr]
        }

    def test_the_same_domains_exist_on_both_sides(
        self, dag_domains: dict[str, tuple[tuple[str, ...], ...]]
    ) -> None:
        assert set(dag_domains) == set(DOMAIN_STAGES)

    def test_every_domain_has_identical_stages(
        self, dag_domains: dict[str, tuple[tuple[str, ...], ...]]
    ) -> None:
        for domain, stages in DOMAIN_STAGES.items():
            assert dag_domains[domain] == stages, f"{domain} has drifted"

    def test_every_domain_pipeline_is_registered(
        self, dag_domains: dict[str, tuple[tuple[str, ...], ...]]
    ) -> None:
        registered = set(pipeline_names())
        for domain, stages in dag_domains.items():
            unknown = {name for stage in stages for name in stage} - registered
            assert not unknown, f"{domain} references unregistered: {sorted(unknown)}"

    def test_phase_5_domains_have_two_stages(
        self, dag_domains: dict[str, tuple[tuple[str, ...], ...]]
    ) -> None:
        """The shared factory labels them `bronze` then `silver`, and raises if
        the label count does not match the stage count."""
        for domain in ("reference", "operations", "machinery", "imagery"):
            assert len(dag_domains[domain]) == 2, f"{domain} is not Bronze-then-Silver"


class TestRegistryCoverage:
    def test_every_registered_pipeline_runs_in_some_stage(self) -> None:
        """An orphaned pipeline is either dead code or a forgotten DAG task."""
        staged = {name for stages in DOMAIN_STAGES.values() for stage in stages for name in stage}
        assert set(pipeline_names()) == staged

    def test_every_domain_stage_names_a_registered_pipeline(self) -> None:
        registered = set(pipeline_names())
        for domain, stages in DOMAIN_STAGES.items():
            unknown = {n for stage in stages for n in stage} - registered
            assert not unknown, f"{domain} names unregistered pipelines: {sorted(unknown)}"

    def test_every_domain_runs_bronze_before_silver(self) -> None:
        """A Silver stage may only follow the Bronze it reads."""
        for domain, stages in DOMAIN_STAGES.items():
            seen_silver = False
            for stage in stages:
                prefixes = {name.split(".")[0] for name in stage}
                if "silver" in prefixes:
                    seen_silver = True
                elif "bronze" in prefixes:
                    assert not seen_silver, f"{domain} extracts Bronze after Silver"

    def test_stages_contain_no_duplicates(self) -> None:
        for stages in (SOIL_SENSOR_STAGES, WEATHER_STAGES):
            flat = [name for stage in stages for name in stage]
            assert len(flat) == len(set(flat))

    def test_weather_stages_are_ordered_by_zone(self) -> None:
        """Each stage may only depend on zones already built."""
        expected = ("bronze", "bronze", "silver", "gold", "load")
        for prefix, stage in zip(expected, WEATHER_STAGES, strict=True):
            for name in stage:
                assert name.startswith(f"{prefix}."), f"{name} is not in a {prefix} stage"

    def test_stages_are_ordered_bronze_silver_gold_load(self) -> None:
        """Each stage may only depend on zones already built."""
        expected_prefix = ("bronze", "silver", "gold", "load")
        for prefix, stage in zip(expected_prefix, SOIL_SENSOR_STAGES, strict=True):
            for name in stage:
                assert name.startswith(f"{prefix}."), f"{name} is not in the {prefix} stage"
