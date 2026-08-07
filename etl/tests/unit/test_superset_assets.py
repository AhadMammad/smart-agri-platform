"""Keeps the Superset assets consistent with each other and with ClickHouse.

Dashboards-as-code only works if the code is checked. A dashboard is a web of
UUID references — dashboard to chart, chart to dataset, dataset to database, and
filters to dataset columns — and Superset resolves none of them until import,
where a broken reference produces either a silent no-op or an unhelpful
traceback deep in the importer.

Worse, the assets restate the warehouse: every dataset column and every metric
expression names ClickHouse columns that live in `clickhouse/ddl/`. A renamed
mart column leaves a chart that imports cleanly and then renders an error where
a number should be.

So this walks the whole graph statically:

* every chart a dashboard places exists, and every chart is placed somewhere
* every dataset a chart uses exists, and every column and metric it names is
  declared on that dataset
* every dataset column exists in the ClickHouse DDL, and every metric expression
  references only real columns
* UUIDs are unique across the bundle

None of it needs a running Superset, so it runs in the same `make check` as
everything else.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
import yaml

ASSETS = Path(__file__).resolve().parents[3] / "superset" / "assets"
DDL_ROOT = Path(__file__).resolve().parents[3] / "clickhouse" / "ddl"

#: Columns Superset itself supplies or that a chart may name without the dataset
#: declaring them.
_VIRTUAL_COLUMNS = {"count"}

#: Labels that repeat across parents, so they identify nothing on their own.
#: Field names are generated per farm — "Upper Block 1" exists on both EG-001 and
#: MA-001 — so filtering or grouping on one silently merges two fields on two
#: continents and shows their average. They are display columns; the `_code`
#: variants are the identifiers.
_NON_UNIQUE_LABELS = {"field_name", "farm_name"}


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def _docs(kind: str) -> dict[Path, dict[str, Any]]:
    return {path: _load(path) for path in sorted((ASSETS / kind).rglob("*.yaml"))}


pytestmark = pytest.mark.skipif(not ASSETS.is_dir(), reason=f"no assets at {ASSETS}")


@pytest.fixture(scope="module")
def datasets() -> dict[Path, dict[str, Any]]:
    return _docs("datasets")


@pytest.fixture(scope="module")
def charts() -> dict[Path, dict[str, Any]]:
    return _docs("charts")


@pytest.fixture(scope="module")
def dashboards() -> dict[Path, dict[str, Any]]:
    return _docs("dashboards")


@pytest.fixture(scope="module")
def dataset_by_uuid(datasets: dict[Path, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {doc["uuid"]: doc for doc in datasets.values()}


# --- ClickHouse columns ------------------------------------------------------

_CREATE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW|MATERIALIZED\s+VIEW)"
    r"(?:\s+IF\s+NOT\s+EXISTS)?\s+(\w+)",
    re.IGNORECASE,
)


def _ddl_relations() -> dict[str, set[str]]:
    """Column names per ClickHouse relation, from the DDL.

    Tables are parsed from their column block. Views are parsed from their
    SELECT aliases and bare column references, which is approximate — so view
    checks below are membership-only and never assert completeness.
    """
    relations: dict[str, set[str]] = {}
    for path in sorted(DDL_ROOT.rglob("*.sql")):
        text = path.read_text()
        for statement in text.split(";"):
            match = _CREATE.search(statement)
            if not match:
                continue
            kind, name = match.group(1).upper(), match.group(2)
            if kind == "TABLE":
                relations[name] = _table_columns(statement)
            else:
                relations[name] = _view_columns(statement)
    return relations


def _table_columns(statement: str) -> set[str]:
    body = statement[statement.index("(") + 1 :]
    columns = set()
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("--") or line.startswith(")"):
            continue
        if re.match(r"(ENGINE|ORDER|PARTITION|PRIMARY|COMMENT|SETTINGS|TTL)\b", line, re.I):
            break
        found = re.match(r"(\w+)\s+\S", line)
        if found:
            columns.add(found.group(1))
    return columns


def _view_columns(statement: str) -> set[str]:
    """Every alias and identifier a view could plausibly expose.

    Deliberately generous: this is used only to confirm a named column *could*
    exist, so over-collecting risks a missed error, while under-collecting would
    produce false failures on legitimate SQL.
    """
    names = set(re.findall(r"\bAS\s+(\w+)", statement, re.IGNORECASE))
    names |= set(re.findall(r"\b(\w+)\b", statement))
    return names


@pytest.fixture(scope="module")
def ddl_relations() -> dict[str, set[str]]:
    if not DDL_ROOT.is_dir():
        pytest.skip(f"no DDL at {DDL_ROOT}")
    relations = _ddl_relations()
    assert relations, "no relations parsed — the DDL parser has drifted"
    return relations


# --- structural integrity ----------------------------------------------------


class TestUuidsAreUniqueAndResolvable:
    def test_no_uuid_is_reused(
        self,
        datasets: dict[Path, dict[str, Any]],
        charts: dict[Path, dict[str, Any]],
        dashboards: dict[Path, dict[str, Any]],
    ) -> None:
        """A duplicated UUID makes the importer overwrite one asset with another,
        which looks like a chart that silently changed its own definition."""
        seen = Counter(
            doc["uuid"] for group in (datasets, charts, dashboards) for doc in group.values()
        )
        duplicates = {uuid: count for uuid, count in seen.items() if count > 1}
        assert not duplicates, f"UUIDs used more than once: {duplicates}"

    def test_every_chart_points_at_a_real_dataset(
        self, charts: dict[Path, dict[str, Any]], dataset_by_uuid: dict[str, dict[str, Any]]
    ) -> None:
        for path, doc in charts.items():
            assert doc["dataset_uuid"] in dataset_by_uuid, f"{path.name}: unknown dataset_uuid"

    def test_chart_datasource_matches_its_dataset(self, charts: dict[Path, dict[str, Any]]) -> None:
        """`params.datasource` carries the dataset UUID a second time. The two
        disagreeing is how a chart renders another chart's data."""
        for path, doc in charts.items():
            declared = doc["params"]["datasource"]
            assert (
                declared == f"{doc['dataset_uuid']}__table"
            ), f"{path.name}: datasource {declared} does not match dataset_uuid"

    def test_every_dataset_points_at_the_database(
        self, datasets: dict[Path, dict[str, Any]]
    ) -> None:
        database = _load(ASSETS / "databases" / "clickhouse_agri.yaml")
        for path, doc in datasets.items():
            assert doc["database_uuid"] == database["uuid"], f"{path.name}: wrong database_uuid"


class TestDashboardsReferenceRealCharts:
    def test_every_placed_chart_exists(
        self, dashboards: dict[Path, dict[str, Any]], charts: dict[Path, dict[str, Any]]
    ) -> None:
        known = {doc["uuid"]: doc["slice_name"] for doc in charts.values()}
        for path, doc in dashboards.items():
            for node in doc["position"].values():
                if not isinstance(node, dict) or node.get("type") != "CHART":
                    continue
                uuid = node["meta"]["uuid"]
                assert uuid in known, f"{path.name} places unknown chart {uuid}"

    def test_placed_chart_names_match_the_chart(
        self, dashboards: dict[Path, dict[str, Any]], charts: dict[Path, dict[str, Any]]
    ) -> None:
        """`sliceName` is what the dashboard renders before the chart loads. A
        stale one mislabels the tile for as long as the query takes."""
        known = {doc["uuid"]: doc["slice_name"] for doc in charts.values()}
        for path, doc in dashboards.items():
            for node in doc["position"].values():
                if not isinstance(node, dict) or node.get("type") != "CHART":
                    continue
                meta = node["meta"]
                assert (
                    meta["sliceName"] == known[meta["uuid"]]
                ), f"{path.name}: {meta['sliceName']!r} is not the chart's name"

    def test_every_chart_is_placed_on_some_dashboard(
        self, dashboards: dict[Path, dict[str, Any]], charts: dict[Path, dict[str, Any]]
    ) -> None:
        """An orphaned chart is either dead or a dashboard edit left unfinished."""
        placed = {
            node["meta"]["uuid"]
            for doc in dashboards.values()
            for node in doc["position"].values()
            if isinstance(node, dict) and node.get("type") == "CHART"
        }
        orphaned = {doc["slice_name"] for doc in charts.values() if doc["uuid"] not in placed}
        assert not orphaned, f"charts on no dashboard: {sorted(orphaned)}"

    def test_every_row_child_is_declared(self, dashboards: dict[Path, dict[str, Any]]) -> None:
        """A row naming a node that does not exist renders an empty band."""
        for path, doc in dashboards.items():
            position = doc["position"]
            for node_id, node in position.items():
                if not isinstance(node, dict):
                    continue
                for child in node.get("children", []):
                    assert child in position, f"{path.name}: {node_id} names missing {child}"

    def test_filters_target_columns_the_dataset_declares(
        self, dashboards: dict[Path, dict[str, Any]], dataset_by_uuid: dict[str, dict[str, Any]]
    ) -> None:
        """A native filter on a column that is not there silently offers no
        values, which reads as "no data" rather than as a broken filter."""
        for path, doc in dashboards.items():
            for filter_config in doc["metadata"].get("native_filter_configuration", []):
                for target in filter_config.get("targets", []):
                    dataset = dataset_by_uuid.get(target["datasetUuid"])
                    assert dataset is not None, f"{path.name}: filter on unknown dataset"
                    column = target.get("column", {}).get("name")
                    if column is None:  # a time filter targets the dataset, not a column
                        continue
                    declared = {item["column_name"] for item in dataset["columns"]}
                    assert column in declared, (
                        f"{path.name}: filter {filter_config['name']!r} targets "
                        f"{column!r}, absent from {dataset['table_name']}"
                    )


class TestNothingIsKeyedOnAnAmbiguousLabel:
    """Filtering or grouping on a name that repeats across farms merges rows
    that belong to different places, and reports the average as if it were one
    field. It produces a plausible number, which is why it survived a phase."""

    def test_no_filter_targets_a_non_unique_label(
        self, dashboards: dict[Path, dict[str, Any]]
    ) -> None:
        for path, doc in dashboards.items():
            for filter_config in doc["metadata"].get("native_filter_configuration", []):
                for target in filter_config.get("targets", []):
                    column = target.get("column", {}).get("name")
                    assert column not in _NON_UNIQUE_LABELS, (
                        f"{path.name}: filter {filter_config['name']!r} targets "
                        f"{column!r}, which repeats across farms — use the code"
                    )

    def test_no_chart_aggregates_on_a_non_unique_label(
        self, charts: dict[Path, dict[str, Any]]
    ) -> None:
        """Naming one in `all_columns` is fine: a table row shows the label
        beside its farm. Grouping by one is not."""
        for path, doc in charts.items():
            params = doc["params"]
            grouped = set(params.get("groupby") or [])
            if isinstance(params.get("x_axis"), str):
                grouped.add(params["x_axis"])
            offending = grouped & _NON_UNIQUE_LABELS
            assert (
                not offending
            ), f"{path.name} groups by {sorted(offending)}, which repeats across farms"


class TestChartsReferenceRealColumnsAndMetrics:
    @staticmethod
    def _referenced(params: dict[str, Any]) -> tuple[set[str], set[str]]:
        """Column and metric names a chart's params name, ignoring ad-hoc SQL."""
        columns: set[str] = set()
        metrics: set[str] = set()

        for key in ("groupby", "groupby_b", "all_columns", "columns"):
            columns |= {item for item in params.get(key) or [] if isinstance(item, str)}
        for key in ("x_axis", "granularity_sqla"):
            value = params.get(key)
            if isinstance(value, str):
                columns.add(value)
        for key in ("metrics", "metrics_b"):
            metrics |= {item for item in params.get(key) or [] if isinstance(item, str)}
        value = params.get("timeseries_limit_metric")
        if isinstance(value, str):
            metrics.add(value)

        for raw in params.get("order_by_cols") or []:
            found = re.match(r'\["(\w+)"', raw)
            if found:
                columns.add(found.group(1))

        return columns, metrics

    def test_every_named_column_is_declared_on_the_dataset(
        self, charts: dict[Path, dict[str, Any]], dataset_by_uuid: dict[str, dict[str, Any]]
    ) -> None:
        for path, doc in charts.items():
            dataset = dataset_by_uuid[doc["dataset_uuid"]]
            declared = {item["column_name"] for item in dataset["columns"]} | _VIRTUAL_COLUMNS
            columns, _ = self._referenced(doc["params"])
            missing = columns - declared
            assert (
                not missing
            ), f"{path.name} names {sorted(missing)}, absent from {dataset['table_name']}"

    def test_every_named_metric_is_declared_on_the_dataset(
        self, charts: dict[Path, dict[str, Any]], dataset_by_uuid: dict[str, dict[str, Any]]
    ) -> None:
        for path, doc in charts.items():
            dataset = dataset_by_uuid[doc["dataset_uuid"]]
            declared = {item["metric_name"] for item in dataset["metrics"]}
            _, metrics = self._referenced(doc["params"])
            missing = metrics - declared
            assert not missing, (
                f"{path.name} uses metrics {sorted(missing)}, absent from "
                f"{dataset['table_name']}"
            )

    def test_the_time_column_is_marked_as_one(
        self, charts: dict[Path, dict[str, Any]], dataset_by_uuid: dict[str, dict[str, Any]]
    ) -> None:
        """`granularity_sqla` on a column with `is_dttm: false` makes Superset
        apply a time grain to something that is not a time."""
        for path, doc in charts.items():
            column = doc["params"].get("granularity_sqla")
            if not column:
                continue
            dataset = dataset_by_uuid[doc["dataset_uuid"]]
            temporal = {item["column_name"] for item in dataset["columns"] if item["is_dttm"]}
            assert (
                column in temporal
            ), f"{path.name}: granularity_sqla {column!r} is not a temporal column"


class TestDatasetsMatchClickHouse:
    def test_every_dataset_names_a_real_relation(
        self, datasets: dict[Path, dict[str, Any]], ddl_relations: dict[str, set[str]]
    ) -> None:
        for path, doc in datasets.items():
            assert (
                doc["table_name"] in ddl_relations
            ), f"{path.name}: {doc['table_name']} is created by no DDL"

    def test_every_dataset_column_exists_in_clickhouse(
        self, datasets: dict[Path, dict[str, Any]], ddl_relations: dict[str, set[str]]
    ) -> None:
        """The failure this prevents: a renamed mart column leaving a chart that
        imports cleanly and then renders an error where a number should be."""
        for path, doc in datasets.items():
            available = ddl_relations[doc["table_name"]]
            named = {item["column_name"] for item in doc["columns"]}
            missing = named - available
            assert not missing, f"{path.name}: {sorted(missing)} not in {doc['table_name']}"

    def test_metric_expressions_reference_real_columns(
        self, datasets: dict[Path, dict[str, Any]], ddl_relations: dict[str, set[str]]
    ) -> None:
        """Identifiers inside a metric's SQL are checked too — they are the part
        no other test looks at, and `SUM(old_name)` fails only at query time."""
        sql_words = {
            "SUM",
            "AVG",
            "COUNT",
            "MIN",
            "MAX",
            "DISTINCT",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "NULLIF",
            "IS",
            "NULL",
            "NOT",
            "AND",
            "OR",
            "IF",
        }
        for path, doc in datasets.items():
            available = ddl_relations[doc["table_name"]]
            for metric in doc["metrics"]:
                # String literals first: `WHEN moisture_stress = 'dry'` names a
                # value, not a column, and matching on it would be a false alarm.
                expression = re.sub(r"'[^']*'", " ", metric["expression"])
                identifiers = set(re.findall(r"\b([a-z_][a-z0-9_]*)\b", expression))
                unknown = identifiers - available - {word.lower() for word in sql_words}
                assert not unknown, (
                    f"{path.name}: metric {metric['metric_name']!r} references "
                    f"{sorted(unknown)}, absent from {doc['table_name']}"
                )


class TestBundleIsImportable:
    def test_chart_params_are_mappings(self, charts: dict[Path, dict[str, Any]]) -> None:
        """Superset's v1 importer rejects a JSON-encoded string with an error
        that names neither the file nor the field."""
        for path, doc in charts.items():
            assert isinstance(doc["params"], dict), f"{path.name}: params is not a mapping"

    def test_the_bundle_declares_itself_a_dashboard_export(self) -> None:
        metadata = _load(ASSETS / "metadata.yaml")
        assert metadata["type"] == "Dashboard"
        assert metadata["version"] == "1.0.0"

    def test_every_dashboard_has_a_unique_slug(
        self, dashboards: dict[Path, dict[str, Any]]
    ) -> None:
        slugs = Counter(doc["slug"] for doc in dashboards.values())
        assert not [slug for slug, count in slugs.items() if count > 1]

    def test_all_four_domains_have_a_dashboard(
        self, dashboards: dict[Path, dict[str, Any]]
    ) -> None:
        """The Phase 7 exit criterion, as an assertion."""
        assert {doc["slug"] for doc in dashboards.values()} == {
            "field-soil-health",
            "irrigation-water",
            "machinery-fleet",
            "yield-economics",
        }
