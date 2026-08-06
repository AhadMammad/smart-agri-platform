"""Tests for unwrapping the driver's Arrow extension types.

The bug this covers stopped every Postgres extract dead: ADBC returns
`NUMERIC` as `arrow.opaque` wrapping a string, and `pl.from_arrow` refuses it —

    ComputeError: cannot create series from Extension("arrow.opaque", Utf8, ...)

Nearly every table has a NUMERIC column, so nothing read at all.
"""

from __future__ import annotations

import polars as pl
import pyarrow as pa
import pytest

from smart_agri.io.postgres import normalise_arrow


def _opaque_numeric(values: list[str | None]) -> pa.ChunkedArray:
    """An array shaped exactly as adbc-driver-postgresql returns NUMERIC."""
    opaque = pa.opaque(pa.string(), "numeric", "PostgreSQL")
    storage = pa.array(values, type=pa.string())
    return pa.chunked_array([pa.ExtensionArray.from_storage(opaque, storage)])


class TestNormaliseArrow:
    def test_plain_tables_pass_through_unchanged(self) -> None:
        table = pa.table({"farm_id": pa.array([1, 2]), "name": pa.array(["a", "b"])})
        assert normalise_arrow(table) is table

    def test_opaque_numeric_becomes_a_float(self) -> None:
        table = pa.Table.from_arrays(
            [pa.chunked_array([pa.array([1, 2])]), _opaque_numeric(["30.9", "11.7"])],
            names=["farm_id", "latitude"],
        )
        normalised = normalise_arrow(table)

        assert normalised.schema.field("latitude").type == pa.float64()
        assert normalised.column("latitude").to_pylist() == [30.9, 11.7]

    def test_the_result_converts_to_polars(self) -> None:
        """The whole point: the original raises inside pl.from_arrow."""
        table = pa.Table.from_arrays(
            [pa.chunked_array([pa.array([1])]), _opaque_numeric(["120.50"])],
            names=["farm_id", "area_ha"],
        )
        with pytest.raises(Exception, match="[Ee]xtension|opaque"):
            pl.from_arrow(table)

        frame = pl.from_arrow(normalise_arrow(table))
        assert isinstance(frame, pl.DataFrame)
        assert frame["area_ha"].dtype == pl.Float64
        assert frame["area_ha"].item() == pytest.approx(120.50)

    def test_nulls_survive(self) -> None:
        """A missing measurement is a null, not a zero."""
        table = pa.Table.from_arrays([_opaque_numeric(["1.5", None, "3.5"])], names=["soil_ph"])
        frame = pl.from_arrow(normalise_arrow(table))
        assert frame["soil_ph"].null_count() == 1  # type: ignore[index]

    def test_other_columns_are_untouched(self) -> None:
        table = pa.Table.from_arrays(
            [
                pa.chunked_array([pa.array(["EG-001"])]),
                _opaque_numeric(["30.9"]),
                pa.chunked_array([pa.array([True])]),
            ],
            names=["farm_code", "latitude", "active"],
        )
        normalised = normalise_arrow(table)
        assert normalised.schema.field("farm_code").type == pa.string()
        assert normalised.schema.field("active").type == pa.bool_()

    def test_an_unknown_extension_falls_back_to_its_storage(self) -> None:
        """A type with no mapping must still be readable rather than raising."""
        opaque = pa.opaque(pa.string(), "some_future_type", "PostgreSQL")
        storage = pa.array(["x"], type=pa.string())
        table = pa.Table.from_arrays(
            [pa.chunked_array([pa.ExtensionArray.from_storage(opaque, storage)])],
            names=["odd"],
        )
        assert normalise_arrow(table).schema.field("odd").type == pa.string()

    def test_multiple_chunks_are_handled(self) -> None:
        opaque = pa.opaque(pa.string(), "numeric", "PostgreSQL")
        chunks = [
            pa.ExtensionArray.from_storage(opaque, pa.array(["1.0"], type=pa.string())),
            pa.ExtensionArray.from_storage(opaque, pa.array(["2.0"], type=pa.string())),
        ]
        table = pa.Table.from_arrays([pa.chunked_array(chunks)], names=["value"])
        assert normalise_arrow(table).column("value").to_pylist() == [1.0, 2.0]
