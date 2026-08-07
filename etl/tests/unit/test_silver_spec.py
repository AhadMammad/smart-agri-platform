"""Tests for the declarative Silver framework.

Eighteen datasets share one pipeline, so a fault here is a fault in all of them.
These exercise the framework against synthetic frames; the specs themselves are
checked in `test_silver_specs.py`.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandera.polars as pa
import polars as pl
import pytest
from pandera.engines import polars_engine
from tests.fakes import FakeClickHouseSink, FakeControlStore, FakeHdfsStore, FakePostgresSource

from smart_agri.config import LakeZone
from smart_agri.pipelines.base import PipelineContext
from smart_agri.pipelines.bronze import (
    DATA_FILE,
    INGEST_PARTITION_KEY,
    SNAPSHOT_PARTITION_KEY,
)
from smart_agri.pipelines.silver_spec import JoinSpec, SilverPipeline, SilverSpec

LOGICAL_DATE = date(2026, 8, 1)
PARTITION = LOGICAL_DATE.isoformat()

SIMPLE_SCHEMA = pa.DataFrameSchema(
    {
        "widget_id": pa.Column(polars_engine.Int64, unique=True),
        "widget_code": pa.Column(polars_engine.String),
        "colour": pa.Column(polars_engine.String),
    },
    strict=True,
    name="silver_widget",
)


@pytest.fixture
def hdfs() -> FakeHdfsStore:
    return FakeHdfsStore()


@pytest.fixture
def context(hdfs: FakeHdfsStore) -> PipelineContext:
    return PipelineContext(
        postgres=FakePostgresSource(),  # type: ignore[arg-type]
        hdfs=hdfs,  # type: ignore[arg-type]
        control=FakeControlStore(),  # type: ignore[arg-type]
        clickhouse=FakeClickHouseSink(),  # type: ignore[arg-type]
    )


def put_bronze(
    hdfs: FakeHdfsStore, dataset: str, frame: pl.DataFrame, *, incremental: bool = False
) -> None:
    key = INGEST_PARTITION_KEY if incremental else SNAPSHOT_PARTITION_KEY
    path = hdfs.zone_path(LakeZone.BRONZE, dataset, f"{key}={PARTITION}", DATA_FILE)
    hdfs.files[path] = frame


def silver_of(hdfs: FakeHdfsStore, spec: SilverSpec) -> pl.DataFrame:
    path = hdfs.zone_path(
        LakeZone.SILVER, spec.dataset, f"{spec.partition_key}={PARTITION}", DATA_FILE
    )
    return hdfs.files[path]


def run(spec: SilverSpec, context: PipelineContext) -> None:
    SilverPipeline(spec, context).run(LOGICAL_DATE)


class TestCleaning:
    def test_whitespace_and_case_are_normalised(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame(
                {
                    "widget_id": [1, 2],
                    "widget_code": ["  W-1  ", "W-2"],
                    "colour": [" RED ", "Blue"],
                }
            ),
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=SIMPLE_SCHEMA,
            trim=("widget_code",),
            lower=("colour",),
        )
        run(spec, context)

        result = silver_of(hdfs, spec)
        assert result["widget_code"].to_list() == ["W-1", "W-2"]
        assert result["colour"].to_list() == ["red", "blue"]

    def test_upper_also_strips(self, context: PipelineContext, hdfs: FakeHdfsStore) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame({"widget_id": [1], "widget_code": ["w-1"], "colour": [" eg "]}),
        )
        spec = SilverSpec(
            dataset="widget", bronze="widget", schema=SIMPLE_SCHEMA, upper=("colour",)
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["colour"].item() == "EG"


class TestRenaming:
    def test_columns_are_renamed_to_their_analytical_names(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame({"widget_id": [1], "code": ["W-1"], "colour": ["red"]}),
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=SIMPLE_SCHEMA,
            rename={"code": "widget_code"},
        )
        run(spec, context)
        assert "widget_code" in silver_of(hdfs, spec).columns

    def test_a_rename_for_an_absent_column_is_ignored(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Specs are shared across sources that vary slightly; a rename that
        does not apply must not break the run."""
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame({"widget_id": [1], "widget_code": ["W-1"], "colour": ["red"]}),
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=SIMPLE_SCHEMA,
            rename={"not_here": "widget_code"},
        )
        run(spec, context)
        assert silver_of(hdfs, spec).height == 1


class TestJoins:
    def test_a_key_is_brought_in_from_another_dataset(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(hdfs, "widget", pl.DataFrame({"widget_id": [1, 2], "box_id": [10, 11]}))
        put_bronze(hdfs, "box", pl.DataFrame({"box_id": [10, 11], "colour": ["red", "blue"]}))

        schema = pa.DataFrameSchema(
            {
                "widget_id": pa.Column(polars_engine.Int64),
                "box_id": pa.Column(polars_engine.Int64),
                "colour": pa.Column(polars_engine.String),
            },
            strict=True,
            name="silver_widget_joined",
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=schema,
            joins=(JoinSpec(dataset="box", on=("box_id",), columns=("colour",)),),
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["colour"].to_list() == ["red", "blue"]

    def test_a_row_whose_parent_is_missing_is_dropped(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Inner join by design: an orphan carried through with a null key would
        reach a dashboard as a phantom row."""
        put_bronze(hdfs, "widget", pl.DataFrame({"widget_id": [1, 2], "box_id": [10, 99]}))
        put_bronze(hdfs, "box", pl.DataFrame({"box_id": [10], "colour": ["red"]}))

        schema = pa.DataFrameSchema(
            {
                "widget_id": pa.Column(polars_engine.Int64),
                "box_id": pa.Column(polars_engine.Int64),
                "colour": pa.Column(polars_engine.String),
            },
            strict=True,
            name="silver_widget_orphan",
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=schema,
            joins=(JoinSpec(dataset="box", on=("box_id",), columns=("colour",)),),
        )
        run(spec, context)

        result = silver_of(hdfs, spec)
        assert result["widget_id"].to_list() == [1]

    def test_a_missing_join_column_fails_loudly(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(hdfs, "widget", pl.DataFrame({"widget_id": [1], "box_id": [10]}))
        put_bronze(hdfs, "box", pl.DataFrame({"box_id": [10]}))

        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=SIMPLE_SCHEMA,
            joins=(JoinSpec(dataset="box", on=("box_id",), columns=("colour",)),),
        )
        with pytest.raises(KeyError, match="missing join columns"):
            run(spec, context)

    def test_the_facts_own_column_survives_a_clash(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """When both sides carry a column, the joined value wins — that is the
        point of the join, and a `_right` suffix would break the selection."""
        put_bronze(
            hdfs, "widget", pl.DataFrame({"widget_id": [1], "box_id": [10], "colour": ["old"]})
        )
        put_bronze(hdfs, "box", pl.DataFrame({"box_id": [10], "colour": ["new"]}))

        schema = pa.DataFrameSchema(
            {
                "widget_id": pa.Column(polars_engine.Int64),
                "box_id": pa.Column(polars_engine.Int64),
                "colour": pa.Column(polars_engine.String),
            },
            strict=True,
            name="silver_widget_clash",
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=schema,
            joins=(JoinSpec(dataset="box", on=("box_id",), columns=("colour",)),),
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["colour"].item() == "new"


class TestFiltersAndDerivations:
    def test_filters_exclude_rows(self, context: PipelineContext, hdfs: FakeHdfsStore) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame(
                {
                    "widget_id": [1, 2, 3],
                    "widget_code": ["a", "b", "c"],
                    "colour": ["red", "scrap", "blue"],
                }
            ),
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=SIMPLE_SCHEMA,
            filters=(pl.col("colour") != "scrap",),
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["widget_id"].to_list() == [1, 3]

    def test_derivations_see_the_cast_type(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Casts run before derivations, so a date derived from a timestamp gets
        a real timestamp rather than whatever the driver returned."""
        put_bronze(
            hdfs,
            "event",
            pl.DataFrame(
                {
                    "event_id": [1],
                    "event_ts": [datetime(2026, 8, 1, 14, 30, tzinfo=UTC)],
                }
            ),
        )
        schema = pa.DataFrameSchema(
            {
                "event_id": pa.Column(polars_engine.Int64),
                "event_date": pa.Column(polars_engine.Date),
            },
            strict=True,
            name="silver_event",
        )
        spec = SilverSpec(
            dataset="event",
            bronze="event",
            schema=schema,
            derive={"event_date": pl.col("event_ts").dt.date()},
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["event_date"].item() == date(2026, 8, 1)


class TestSelectionAndDedupe:
    def test_audit_columns_do_not_reach_silver(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """Carrying `updated_at` invites a chart to group by an ingestion
        timestamp and call it a business date."""
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame(
                {
                    "widget_id": [1],
                    "widget_code": ["W-1"],
                    "colour": ["red"],
                    "created_at": [datetime(2026, 8, 1, tzinfo=UTC)],
                    "updated_at": [datetime(2026, 8, 1, tzinfo=UTC)],
                }
            ),
        )
        spec = SilverSpec(dataset="widget", bronze="widget", schema=SIMPLE_SCHEMA)
        run(spec, context)
        assert set(silver_of(hdfs, spec).columns) == set(SIMPLE_SCHEMA.columns)

    def test_a_missing_column_names_itself(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        """The error has to say which column and where to look, or a
        spec-driven framework becomes impossible to debug."""
        put_bronze(hdfs, "widget", pl.DataFrame({"widget_id": [1], "widget_code": ["W-1"]}))
        spec = SilverSpec(dataset="widget", bronze="widget", schema=SIMPLE_SCHEMA)

        with pytest.raises(KeyError, match="colour"):
            run(spec, context)

    def test_duplicates_are_collapsed_on_the_natural_key(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame(
                {
                    "widget_id": [1, 1, 2],
                    "widget_code": ["a", "a", "b"],
                    "colour": ["red", "red", "blue"],
                }
            ),
        )
        spec = SilverSpec(
            dataset="widget",
            bronze="widget",
            schema=SIMPLE_SCHEMA,
            dedupe_on=("widget_id",),
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["widget_id"].to_list() == [1, 2]


class TestPartitioning:
    def test_incremental_specs_read_the_ingest_partition(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame({"widget_id": [1], "widget_code": ["a"], "colour": ["red"]}),
            incremental=True,
        )
        spec = SilverSpec(dataset="widget", bronze="widget", schema=SIMPLE_SCHEMA, incremental=True)
        run(spec, context)
        assert spec.partition_key == INGEST_PARTITION_KEY
        assert silver_of(hdfs, spec).height == 1

    def test_rerunning_replaces_rather_than_appending(
        self, context: PipelineContext, hdfs: FakeHdfsStore
    ) -> None:
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame({"widget_id": [1], "widget_code": ["a"], "colour": ["red"]}),
        )
        spec = SilverSpec(dataset="widget", bronze="widget", schema=SIMPLE_SCHEMA)

        run(spec, context)
        first = silver_of(hdfs, spec)
        run(spec, context)

        assert silver_of(hdfs, spec).equals(first)

    def test_output_is_deterministic(self, context: PipelineContext, hdfs: FakeHdfsStore) -> None:
        """Sorted on the primary key, so a re-run writes identical Parquet."""
        put_bronze(
            hdfs,
            "widget",
            pl.DataFrame(
                {
                    "widget_id": [3, 1, 2],
                    "widget_code": ["c", "a", "b"],
                    "colour": ["x", "y", "z"],
                }
            ),
        )
        spec = SilverSpec(
            dataset="widget", bronze="widget", schema=SIMPLE_SCHEMA, dedupe_on=("widget_id",)
        )
        run(spec, context)
        assert silver_of(hdfs, spec)["widget_id"].to_list() == [1, 2, 3]
