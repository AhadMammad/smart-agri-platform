"""Coherence checks over every declared Silver spec.

A spec is data, so it can be wrong in ways a type checker cannot see: a dedupe
key that is not in the schema, a rename whose target nothing selects, a Bronze
dataset that is never extracted. Each of those would fail at run time on the VM
rather than here, so they are checked directly.
"""

from __future__ import annotations

import pytest

from smart_agri.pipelines.bronze import INCREMENTAL_SPECS, SNAPSHOT_SPECS
from smart_agri.pipelines.silver_specs import SILVER_SPECS

BRONZE_DATASETS = {spec.dataset for spec in (*SNAPSHOT_SPECS, *INCREMENTAL_SPECS)}
INCREMENTAL_DATASETS = {spec.dataset for spec in INCREMENTAL_SPECS}

IDS = [spec.dataset for spec in SILVER_SPECS]


@pytest.mark.parametrize("spec", SILVER_SPECS, ids=IDS)
class TestSpecCoherence:
    def test_the_bronze_source_is_extracted(self, spec) -> None:  # type: ignore[no-untyped-def]
        """A Silver spec reading a dataset nothing lands would fail only when
        the DAG runs."""
        assert spec.bronze in BRONZE_DATASETS

    def test_the_partition_matches_the_bronze_strategy(self, spec) -> None:  # type: ignore[no-untyped-def]
        """Reading a snapshot partition from an incremental source — or the
        reverse — finds nothing and silently produces an empty Silver table."""
        assert spec.incremental == (spec.bronze in INCREMENTAL_DATASETS)

    def test_joins_reference_extracted_datasets(self, spec) -> None:  # type: ignore[no-untyped-def]
        for join in spec.joins:
            assert join.dataset in BRONZE_DATASETS, f"{spec.dataset} joins {join.dataset}"

    def test_joins_are_snapshots(self, spec) -> None:  # type: ignore[no-untyped-def]
        """A join reads the whole current picture, so its source must be a
        snapshot — an incremental partition holds only the latest batch."""
        for join in spec.joins:
            assert (
                join.dataset not in INCREMENTAL_DATASETS
            ), f"{spec.dataset} joins the incremental dataset {join.dataset}"

    def test_the_dedupe_key_is_in_the_schema(self, spec) -> None:  # type: ignore[no-untyped-def]
        """Deduplication runs after the column selection, so a key that is not
        selected raises rather than deduplicating."""
        missing = set(spec.dedupe_on) - set(spec.schema.columns)
        assert not missing, f"{spec.dataset} dedupes on unselected {sorted(missing)}"

    def test_derived_columns_are_selected(self, spec) -> None:  # type: ignore[no-untyped-def]
        """A derivation the schema does not name is computed and thrown away."""
        unused = set(spec.derive) - set(spec.schema.columns)
        assert not unused, f"{spec.dataset} derives unused {sorted(unused)}"

    def test_rename_targets_are_selected(self, spec) -> None:  # type: ignore[no-untyped-def]
        unused = set(spec.rename.values()) - set(spec.schema.columns)
        assert not unused, f"{spec.dataset} renames to unselected {sorted(unused)}"

    def test_the_schema_is_strict(self, spec) -> None:  # type: ignore[no-untyped-def]
        """Silver is the conformed layer; an unexpected column arriving here
        should fail rather than flow on unnoticed."""
        assert spec.schema.strict is True


class TestSpecSet:
    def test_dataset_names_are_unique(self) -> None:
        names = [spec.dataset for spec in SILVER_SPECS]
        assert len(names) == len(set(names))

    def test_every_bronze_source_reaches_silver(self) -> None:
        """Phase 5's exit criterion: every source table present in both zones.

        `weather_archive` and `weather_forecast` are excluded — they are merged
        by a dedicated pipeline rather than conformed one-to-one.
        """
        conformed = {spec.bronze for spec in SILVER_SPECS}
        weather = {"weather_archive", "weather_forecast"}
        assert BRONZE_DATASETS - weather == conformed

    def test_every_fact_carries_the_farm_key(self) -> None:
        """Every analytics question is eventually sliced by farm or region;
        joining back through the field at query time is slower and easier to get
        wrong than carrying the key."""
        for spec in SILVER_SPECS:
            if spec.dataset.startswith("fact_"):
                assert "farm_id" in spec.schema.columns, spec.dataset

    def test_every_timestamped_fact_carries_a_date(self) -> None:
        """ClickHouse partitions on it and every dashboard groups by it."""
        for spec in SILVER_SPECS:
            timestamps = [c for c in spec.schema.columns if c.endswith(("_ts", "_at"))]
            if spec.dataset.startswith("fact_") and timestamps:
                dates = [c for c in spec.schema.columns if c.endswith("_date")]
                assert dates, f"{spec.dataset} has {timestamps} but no date column"
