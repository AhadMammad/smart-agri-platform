"""Keeps the Python reference catalogue in step with the Liquibase changelogs.

The generator needs regions, soil classes and crops without a database, so they
exist twice: as Python constants and as INSERT statements in
`liquibase/changelog/changes/`. That duplication is only safe if something
checks it — a base temperature edited on one side and not the other would skew
every yield in the dataset while every test still passed.

The changelogs are parsed rather than executed, so no database is involved.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from smart_agri.generator.agronomy import CROPS, CROPS_BY_CODE
from smart_agri.generator.regions import ALL_REGIONS
from smart_agri.generator.signals import _MOISTURE_BAND

if TYPE_CHECKING:
    from collections.abc import Sequence

CHANGELOG_DIR = Path(__file__).resolve().parents[3] / "liquibase" / "changelog" / "changes"

# Matches one `('a', 'b', 12.0, ...)` tuple from a multi-row VALUES clause.
_ROW = re.compile(r"\(([^()]*)\)")


def _values_rows(path: Path, table: str) -> list[list[str]]:
    """Extract the VALUES tuples of an `INSERT INTO <table>` from a changelog."""
    text = path.read_text()
    marker = f"INSERT INTO {table}"
    if marker not in text:
        pytest.fail(f"no INSERT INTO {table} in {path.name}")

    after = text.split(marker, 1)[1]
    body = after.split("VALUES", 1)[1].split(";", 1)[0]
    # Drop comment lines, which may themselves contain parentheses.
    body = "\n".join(line for line in body.splitlines() if not line.strip().startswith("--"))

    rows: list[list[str]] = []
    for match in _ROW.finditer(body):
        cells = [cell.strip().strip("'") for cell in match.group(1).split(",")]
        rows.append(cells)
    return rows


def _as_float(value: str) -> float:
    return float(value)


class TestRegionCatalogue:
    @pytest.fixture(scope="class")
    def rows(self) -> Sequence[list[str]]:
        return _values_rows(CHANGELOG_DIR / "006-reference.xml", "agri.region")

    def test_same_regions_on_both_sides(self, rows: Sequence[list[str]]) -> None:
        assert {row[0] for row in rows} == {region.code for region in ALL_REGIONS}

    def test_names_and_countries_match(self, rows: Sequence[list[str]]) -> None:
        by_code = {region.code: region for region in ALL_REGIONS}
        for code, name, country_code, country_name, _macro, rainfall in rows:
            region = by_code[code]
            assert region.name == name
            assert region.country_code == country_code
            assert region.country_name == country_name
            assert region.rainfall.value == rainfall

    def test_macro_region_matches_the_hemisphere_grouping(self, rows: Sequence[list[str]]) -> None:
        """A farm's region drives its crop pool, so a mislabelled macro-region
        would put cocoa in Tunisia."""
        from smart_agri.generator.regions import NORTH_AFRICA

        north = {region.code for region in NORTH_AFRICA}
        for code, _name, _cc, _cn, macro, _rain in rows:
            expected = "North Africa" if code in north else "West Africa"
            assert macro == expected, code


class TestSoilCatalogue:
    @pytest.fixture(scope="class")
    def rows(self) -> Sequence[list[str]]:
        return _values_rows(CHANGELOG_DIR / "006-reference.xml", "agri.soil_type")

    def test_same_soil_classes_on_both_sides(self, rows: Sequence[list[str]]) -> None:
        assert {row[0] for row in rows} == {soil.value for soil in _MOISTURE_BAND}

    def test_moisture_bands_match_the_signal_model(self, rows: Sequence[list[str]]) -> None:
        """These bands are what the generator scales its moisture curve into;
        a mismatch would make the database describe data it did not produce."""
        by_value = {soil.value: band for soil, band in _MOISTURE_BAND.items()}
        for code, _name, low, high, _drainage in rows:
            expected_low, expected_high = by_value[code]
            assert _as_float(low) == pytest.approx(expected_low)
            assert _as_float(high) == pytest.approx(expected_high)


class TestCropCatalogue:
    @pytest.fixture(scope="class")
    def rows(self) -> Sequence[list[str]]:
        return _values_rows(CHANGELOG_DIR / "007-crop.xml", "agri.crop")

    def test_same_crops_on_both_sides(self, rows: Sequence[list[str]]) -> None:
        assert {row[0] for row in rows} == set(CROPS_BY_CODE)

    def test_every_agronomic_value_matches(self, rows: Sequence[list[str]]) -> None:
        """Base temperature and degree-day requirement drive the yield model, so
        a divergence here changes the data without changing any test."""
        for cells in rows:
            (
                code,
                name,
                category,
                is_perennial,
                base_temp,
                gdd,
                cycle_days,
                water_mm,
                peak_ndvi,
            ) = cells
            crop = CROPS_BY_CODE[code]

            assert crop.name == name
            assert crop.category == category
            assert crop.is_perennial is (is_perennial == "true")
            assert crop.base_temp_c == pytest.approx(_as_float(base_temp))
            assert crop.gdd_to_maturity == int(gdd)
            assert crop.cycle_days == int(cycle_days)
            assert crop.water_demand_mm == int(water_mm)
            assert crop.peak_ndvi == pytest.approx(_as_float(peak_ndvi))

    def test_row_count_matches(self, rows: Sequence[list[str]]) -> None:
        assert len(rows) == len(CROPS)


class TestCatalogueSelfConsistency:
    def test_every_region_crop_exists_in_the_catalogue(self) -> None:
        """A region naming a crop that is not in the catalogue would raise a
        KeyError deep inside generation rather than here."""
        for region in ALL_REGIONS:
            unknown = set(region.crops) - set(CROPS_BY_CODE)
            assert not unknown, f"{region.code} names unknown crops: {sorted(unknown)}"

    def test_every_region_soil_type_has_a_moisture_band(self) -> None:
        for region in ALL_REGIONS:
            for soil in region.soil_types:
                assert soil in _MOISTURE_BAND, f"{region.code}: {soil} has no band"

    def test_every_region_offers_at_least_one_annual_crop(self) -> None:
        """A region of only perennials could never rotate, and the planting
        generator would have nothing to choose from in an arable field."""
        for region in ALL_REGIONS:
            annuals = [c for c in region.crops if not CROPS_BY_CODE[c].is_perennial]
            assert annuals, f"{region.code} has no annual crops"
