"""Tests for the io layer.

`HdfsStore` is exercised against fsspec's in-memory filesystem, so the real
path, listing and concatenation logic runs without a NameNode. The driver calls
themselves are covered by the integration suite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import fsspec
import polars as pl
import pytest

from smart_agri.config import ClickHouseSettings, HdfsSettings, LakeZone
from smart_agri.io.clickhouse import ClickHouseSink, _check_identifier
from smart_agri.io.hdfs import HdfsStore

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def store() -> Iterator[HdfsStore]:
    """An HdfsStore backed by an isolated in-memory filesystem."""
    memory = fsspec.filesystem("memory")
    memory.store.clear()
    memory.pseudo_dirs.clear()

    hdfs = HdfsStore(HdfsSettings())
    hdfs._fs = memory
    yield hdfs

    memory.store.clear()
    memory.pseudo_dirs.clear()


def _frame(n: int, offset: int = 0) -> pl.DataFrame:
    return pl.DataFrame({"x": list(range(offset, offset + n))})


class TestHdfsPaths:
    def test_zone_path_delegates_to_settings(self, store: HdfsStore) -> None:
        assert store.zone_path(LakeZone.BRONZE, "farm") == "/lake/bronze/farm"

    @pytest.mark.parametrize(
        ("entry", "expected"),
        [("lake/bronze/a.parquet", "/lake/bronze/a.parquet"), ("/already/abs", "/already/abs")],
    )
    def test_listing_entries_are_normalised_to_absolute(self, entry: str, expected: str) -> None:
        """WebHDFS listings drop the leading slash depending on the call."""
        assert HdfsStore._absolute(entry) == expected


class TestHdfsRoundTrip:
    def test_write_then_read_a_single_file(self, store: HdfsStore) -> None:
        rows = store.write_parquet(_frame(3), "/lake/bronze/farm/data.parquet")
        assert rows == 3
        assert store.read_parquet("/lake/bronze/farm/data.parquet").height == 3

    def test_write_creates_parent_directories(self, store: HdfsStore) -> None:
        store.write_parquet(_frame(1), "/lake/gold/deep/nested/path/data.parquet")
        assert store.exists("/lake/gold/deep/nested/path/data.parquet")

    def test_read_dir_concatenates_every_file(self, store: HdfsStore) -> None:
        store.write_parquet(_frame(2, 0), "/lake/bronze/r/ingest_date=2026-08-01/a.parquet")
        store.write_parquet(_frame(3, 100), "/lake/bronze/r/ingest_date=2026-08-01/b.parquet")

        result = store.read_parquet_dir("/lake/bronze/r")
        assert result.height == 5

    def test_read_dir_is_ordered_deterministically(self, store: HdfsStore) -> None:
        """Aggregate output is compared in tests, so read order must be stable."""
        store.write_parquet(_frame(1, 20), "/lake/bronze/r/z.parquet")
        store.write_parquet(_frame(1, 10), "/lake/bronze/r/a.parquet")
        assert store.read_parquet_dir("/lake/bronze/r")["x"].to_list() == [10, 20]

    def test_read_dir_ignores_non_parquet_files(self, store: HdfsStore) -> None:
        store.write_parquet(_frame(2), "/lake/bronze/r/data.parquet")
        with store.fs.open("/lake/bronze/r/_SUCCESS", "wb") as handle:
            handle.write(b"")
        assert store.read_parquet_dir("/lake/bronze/r").height == 2


class TestHdfsMissingPaths:
    def test_missing_path_raises_by_default(self, store: HdfsStore) -> None:
        with pytest.raises(FileNotFoundError, match="does not exist"):
            store.read_parquet_dir("/lake/bronze/absent")

    def test_missing_path_is_empty_when_allowed(self, store: HdfsStore) -> None:
        """A pipeline may legitimately run on a date with no data."""
        assert store.read_parquet_dir("/lake/bronze/absent", missing_ok=True).is_empty()

    def test_directory_without_parquet_raises_by_default(self, store: HdfsStore) -> None:
        with store.fs.open("/lake/bronze/r/_SUCCESS", "wb") as handle:
            handle.write(b"")
        with pytest.raises(FileNotFoundError, match="no parquet files"):
            store.read_parquet_dir("/lake/bronze/r")

    def test_directory_without_parquet_is_empty_when_allowed(self, store: HdfsStore) -> None:
        with store.fs.open("/lake/bronze/r/_SUCCESS", "wb") as handle:
            handle.write(b"")
        assert store.read_parquet_dir("/lake/bronze/r", missing_ok=True).is_empty()


class TestHdfsRemoval:
    def test_remove_deletes_a_directory_tree(self, store: HdfsStore) -> None:
        store.write_parquet(_frame(1), "/lake/bronze/r/d=1/data.parquet")
        store.remove("/lake/bronze/r/d=1")
        assert not store.exists("/lake/bronze/r/d=1/data.parquet")

    def test_removing_an_absent_path_is_a_no_op(self, store: HdfsStore) -> None:
        store.remove("/lake/bronze/never-existed")

    def test_list_files_filters_by_suffix(self, store: HdfsStore) -> None:
        store.write_parquet(_frame(1), "/lake/bronze/r/a.parquet")
        with store.fs.open("/lake/bronze/r/b.txt", "wb") as handle:
            handle.write(b"x")
        assert len(store.list_files("/lake/bronze/r", ".parquet")) == 1
        assert len(store.list_files("/lake/bronze/r")) == 2

    def test_list_files_on_a_missing_path_is_empty(self, store: HdfsStore) -> None:
        assert store.list_files("/lake/bronze/absent") == []


class TestClickHouseIdentifiers:
    @pytest.mark.parametrize("name", ["dim_farm", "fact_sensor_reading", "_private", "t1"])
    def test_valid_identifiers_pass(self, name: str) -> None:
        assert _check_identifier(name) == name

    @pytest.mark.parametrize(
        "name",
        ["dim farm", "dim-farm", "1table", "db.table", "t; DROP TABLE x", "", "t'"],
    )
    def test_invalid_identifiers_are_rejected(self, name: str) -> None:
        """Table names reach SQL by interpolation, so they are validated rather
        than trusted."""
        with pytest.raises(ValueError, match="invalid table name"):
            _check_identifier(name)


class _FakeClient:
    """Minimal stand-in for clickhouse_connect's Client."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, int]] = []

    def command(self, sql: str, parameters: dict[str, Any] | None = None) -> None:
        del parameters
        self.commands.append(sql.strip())

    def insert_arrow(self, table: str, arrow_table: Any) -> None:
        self.inserts.append((table, arrow_table.num_rows))


@pytest.fixture
def sink() -> tuple[ClickHouseSink, _FakeClient]:
    client = _FakeClient()
    instance = ClickHouseSink(ClickHouseSettings())
    instance._client = client  # type: ignore[assignment]
    return instance, client


class TestClickHouseScripts:
    def test_statements_are_split_on_semicolons(
        self, sink: tuple[ClickHouseSink, _FakeClient]
    ) -> None:
        instance, client = sink
        count = instance.execute_script("CREATE TABLE a (x Int64);\nCREATE TABLE b (y Int64);\n")
        assert count == 2
        assert len(client.commands) == 2

    def test_comment_only_fragments_are_skipped(
        self, sink: tuple[ClickHouseSink, _FakeClient]
    ) -> None:
        """The DDL files lead with comment blocks; those are not statements."""
        script = "-- a comment\n-- another\n;\nCREATE TABLE a (x Int64);\n"
        assert sink[0].execute_script(script) == 1

    def test_trailing_statement_without_a_semicolon_still_runs(
        self, sink: tuple[ClickHouseSink, _FakeClient]
    ) -> None:
        assert sink[0].execute_script("CREATE TABLE a (x Int64)") == 1

    def test_empty_script_runs_nothing(self, sink: tuple[ClickHouseSink, _FakeClient]) -> None:
        assert sink[0].execute_script("\n\n   \n") == 0


class TestClickHouseLoading:
    def test_empty_frame_is_not_inserted(self, sink: tuple[ClickHouseSink, _FakeClient]) -> None:
        instance, client = sink
        assert instance.insert_frame("dim_farm", pl.DataFrame()) == 0
        assert client.inserts == []

    def test_insert_returns_the_row_count(self, sink: tuple[ClickHouseSink, _FakeClient]) -> None:
        instance, client = sink
        assert instance.insert_frame("dim_farm", _frame(4)) == 4
        assert client.inserts == [("dim_farm", 4)]

    def test_replace_truncates_before_inserting(
        self, sink: tuple[ClickHouseSink, _FakeClient]
    ) -> None:
        instance, client = sink
        instance.replace_table("dim_farm", _frame(2))
        assert client.commands[0].startswith("TRUNCATE TABLE IF EXISTS dim_farm")
        assert client.inserts == [("dim_farm", 2)]

    def test_replace_rejects_an_unsafe_table_name(
        self, sink: tuple[ClickHouseSink, _FakeClient]
    ) -> None:
        with pytest.raises(ValueError, match="invalid table name"):
            sink[0].replace_table("dim_farm; DROP TABLE dim_field", _frame(1))

    def test_drop_partition_is_parameterised(
        self, sink: tuple[ClickHouseSink, _FakeClient]
    ) -> None:
        """The partition value is bound, not interpolated."""
        instance, client = sink
        instance.delete_partition("fact_sensor_reading", "202608")
        assert "%(partition)s" in client.commands[0]
        assert "202608" not in client.commands[0]


class TestClickHouseLifecycle:
    def test_context_manager_closes_the_client(self) -> None:
        closed: list[bool] = []

        class Closing(_FakeClient):
            def close(self) -> None:
                closed.append(True)

        instance = ClickHouseSink(ClickHouseSettings())
        instance._client = Closing()  # type: ignore[assignment]

        with instance:
            pass

        assert closed == [True]

    def test_client_is_not_opened_on_construction(self) -> None:
        assert ClickHouseSink(ClickHouseSettings())._client is None
