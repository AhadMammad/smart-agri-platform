"""Tests for the synthetic data generator."""

from __future__ import annotations

import itertools
import math
from datetime import UTC, date, datetime

import polars as pl
import pytest

from smart_agri.domain import SensorStatus, SoilType
from smart_agri.generator import DatasetGenerator, GeneratorConfig, get_profile
from smart_agri.generator.config import PROFILES
from smart_agri.generator.generator import sample_reading_model
from smart_agri.generator.regions import (
    ALL_REGIONS,
    KANO_PLAINS,
    NILE_DELTA,
    NORTH_AFRICA,
    WEST_AFRICA,
    RainfallPattern,
    Region,
)
from smart_agri.generator.signals import (
    SoilProfile,
    battery_pct,
    soil_ec_ds_m,
    soil_moisture_pct,
    wetness_index,
)

TINY = GeneratorConfig(
    n_farms=3,
    fields_per_farm=2,
    sensors_per_field=2,
    start_date=date(2026, 7, 1),
    end_date=date(2026, 7, 2),
    reading_interval_minutes=360,
)


class TestGeneratorConfig:
    def test_end_date_before_start_date_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="end_date"):
            GeneratorConfig(start_date=date(2026, 8, 1), end_date=date(2026, 7, 1))

    def test_derived_counts(self) -> None:
        config = GeneratorConfig(
            n_farms=2,
            fields_per_farm=3,
            sensors_per_field=4,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            reading_interval_minutes=60,
        )
        assert config.n_sensors == 24
        assert config.readings_per_sensor == 24  # a single inclusive day
        assert config.estimated_readings == 576

    @pytest.mark.parametrize("name", sorted(PROFILES))
    def test_every_profile_is_valid(self, name: str) -> None:
        assert get_profile(name).estimated_readings > 0

    def test_unknown_profile_lists_the_valid_ones(self) -> None:
        with pytest.raises(KeyError, match="large, medium, small"):
            get_profile("enormous")


class TestRegions:
    def test_north_and_west_africa_are_both_represented(self) -> None:
        assert len(NORTH_AFRICA) == 3
        assert len(WEST_AFRICA) == 3
        assert set(ALL_REGIONS) == set(NORTH_AFRICA) | set(WEST_AFRICA)

    @pytest.mark.parametrize("region", ALL_REGIONS, ids=lambda r: r.code)
    def test_bounding_boxes_are_plausible_for_africa(self, region: Region) -> None:
        """A coordinate outside Africa would make Open-Meteo return weather that
        contradicts the soil signal in Phase 4."""
        assert -35 <= region.lat_min < region.lat_max <= 38
        assert -18 <= region.lon_min < region.lon_max <= 52

    @pytest.mark.parametrize("region", ALL_REGIONS, ids=lambda r: r.code)
    def test_each_region_declares_soils_and_crops(self, region: Region) -> None:
        assert region.soil_types
        assert region.crops

    def test_inverted_bounding_box_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="inverted bounding box"):
            Region(
                code="XX",
                name="Bad",
                country_code="XX",
                country_name="Nowhere",
                lat_min=10.0,
                lat_max=5.0,
                lon_min=0.0,
                lon_max=1.0,
                rainfall=RainfallPattern.UNIMODAL,
                soil_types=(SoilType.SANDY,),
                crops=("maize",),
            )


class TestDeterminism:
    def test_same_seed_produces_identical_dimensions(self) -> None:
        first = DatasetGenerator(TINY)
        second = DatasetGenerator(TINY)
        assert first.farms() == second.farms()

    def test_same_seed_produces_identical_readings(self) -> None:
        def build() -> pl.DataFrame:
            gen = DatasetGenerator(TINY)
            farms = gen.farms()
            fields = gen.fields(farms)
            return gen.readings(gen.sensors(fields), fields, farms)

        assert build().equals(build())

    def test_different_seeds_produce_different_data(self) -> None:
        other = TINY.model_copy(update={"seed": TINY.seed + 1})
        assert DatasetGenerator(TINY).farms() != DatasetGenerator(other).farms()


class TestDimensions:
    def test_farms_are_spread_across_regions(self) -> None:
        """Round-robin rather than random assignment, so a small run still
        touches both North and West Africa."""
        gen = DatasetGenerator(GeneratorConfig(n_farms=6))
        assert len({farm.region for farm in gen.farms()}) == 6

    def test_farm_coordinates_fall_inside_their_region(self) -> None:
        gen = DatasetGenerator(GeneratorConfig(n_farms=12))
        by_name = {region.name: region for region in ALL_REGIONS}
        for farm in gen.farms():
            region = by_name[farm.region]
            assert region.lat_min <= farm.latitude <= region.lat_max
            assert region.lon_min <= farm.longitude <= region.lon_max

    def test_codes_are_unique_at_every_level(self) -> None:
        gen = DatasetGenerator(TINY)
        farms = gen.farms()
        fields = gen.fields(farms)
        sensors = gen.sensors(fields)

        for items, attribute in (
            (farms, "farm_code"),
            (fields, "field_code"),
            (sensors, "sensor_code"),
        ):
            codes = [getattr(item, attribute) for item in items]
            assert len(codes) == len(set(codes)), f"duplicate {attribute}"

    def test_field_codes_nest_under_their_farm(self) -> None:
        gen = DatasetGenerator(TINY)
        farms = gen.farms()
        for field in gen.fields(farms):
            assert field.field_code.startswith(field.farm_code)

    def test_sensors_are_installed_before_the_reading_window(self) -> None:
        """Otherwise a probe appears to report readings before it existed."""
        gen = DatasetGenerator(TINY)
        sensors = gen.sensors(gen.fields(gen.farms()))
        assert all(s.installed_on < TINY.start_date for s in sensors)

    def test_fleet_contains_unhealthy_sensors(self) -> None:
        """The Silver filtering rules are only worth testing if the generated
        fleet actually contains faulty and decommissioned units."""
        gen = DatasetGenerator(GeneratorConfig(n_farms=20, fields_per_farm=5))
        statuses = {s.status for s in gen.sensors(gen.fields(gen.farms()))}
        assert SensorStatus.FAULTY in statuses
        assert SensorStatus.DECOMMISSIONED in statuses


class TestReadings:
    @pytest.fixture(scope="class")
    def readings(self) -> pl.DataFrame:
        gen = DatasetGenerator(TINY)
        farms = gen.farms()
        fields = gen.fields(farms)
        return gen.readings(gen.sensors(fields), fields, farms)

    def test_schema_matches_the_declared_types(self, readings: pl.DataFrame) -> None:
        assert readings["reading_ts"].dtype == pl.Datetime(time_unit="us", time_zone="UTC")
        assert readings["soil_moisture_pct"].dtype == pl.Float64

    def test_timestamps_are_timezone_aware_utc(self, readings: pl.DataFrame) -> None:
        assert readings["reading_ts"].dtype.time_zone == "UTC"  # type: ignore[union-attr]

    def test_decommissioned_sensors_emit_nothing(self) -> None:
        config = GeneratorConfig(
            n_farms=10,
            fields_per_farm=4,
            sensors_per_field=2,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            reading_interval_minutes=720,
        )
        gen = DatasetGenerator(config)
        farms = gen.farms()
        fields = gen.fields(farms)
        sensors = gen.sensors(fields)
        frame = gen.readings(sensors, fields, farms)

        dead = {s.sensor_code for s in sensors if s.status is SensorStatus.DECOMMISSIONED}
        assert dead, "expected the fixture to contain decommissioned sensors"
        assert not set(frame["sensor_code"].to_list()) & dead

    def test_values_stay_inside_physical_bounds(self, readings: pl.DataFrame) -> None:
        """These are the same bounds the Silver contract enforces; a generator
        that breached them would quarantine its own data."""
        bounds = {
            "soil_moisture_pct": (0, 100),
            "soil_temperature_c": (-20, 70),
            "soil_ph": (0, 14),
            "soil_ec_ds_m": (0, 20),
            "battery_pct": (0, 100),
        }
        for column, (low, high) in bounds.items():
            series = readings[column].drop_nulls()
            assert series.min() >= low, column
            assert series.max() <= high, column

    def test_some_channels_are_missing(self, readings: pl.DataFrame) -> None:
        """Real probes drop channels, and the contracts must be exercised."""
        assert readings["soil_moisture_pct"].null_count() > 0

    def test_rows_satisfy_the_domain_model(self, readings: pl.DataFrame) -> None:
        for row in readings.head(50).to_dicts():
            sample_reading_model(row)

    def test_unknown_region_is_reported_clearly(self) -> None:
        gen = DatasetGenerator(TINY, regions=[NILE_DELTA])
        farm = gen.farms()[0].model_copy(update={"region": "Atlantis"})
        with pytest.raises(LookupError, match="Atlantis"):
            gen._region_for(farm)


class TestSignals:
    def test_wetness_is_always_a_fraction(self) -> None:
        for region in ALL_REGIONS:
            values = [
                wetness_index(region, datetime(2026, month, 15, 12, tzinfo=UTC))
                for month in range(1, 13)
            ]
            assert all(0.0 <= v <= 1.0 for v in values), region.code

    def test_sahelian_region_has_a_pronounced_dry_season(self) -> None:
        """A flat curve would make every seasonal chart meaningless."""
        values = [
            wetness_index(KANO_PLAINS, datetime(2026, month, 15, 12, tzinfo=UTC))
            for month in range(1, 13)
        ]
        assert max(values) - min(values) > 0.5

    def test_irrigated_region_stays_wet_year_round(self) -> None:
        values = [
            wetness_index(NILE_DELTA, datetime(2026, month, 15, 12, tzinfo=UTC))
            for month in range(1, 13)
        ]
        assert min(values) > 0.5
        assert max(values) - min(values) < 0.25

    def test_clay_holds_more_water_than_sand(self) -> None:
        from random import Random

        moment = datetime(2026, 8, 15, 6, tzinfo=UTC)

        def mean_for(soil: SoilType) -> float:
            rng = Random(1)
            profile = SoilProfile(soil, base_ph=7.0, moisture_offset=0.0, temperature_offset=0.0)
            return (
                sum(soil_moisture_pct(profile, KANO_PLAINS, moment, rng) for _ in range(200)) / 200
            )

        assert mean_for(SoilType.CLAY) > mean_for(SoilType.SANDY)

    def test_conductivity_rises_with_moisture(self) -> None:
        """The correlation a dashboard plotting both is meant to reveal."""
        from random import Random

        rng = Random(2)
        profile = SoilProfile(SoilType.LOAMY, 6.5, 0.0, 0.0)
        assert soil_ec_ds_m(45.0, profile, rng) > soil_ec_ds_m(8.0, profile, rng)

    def test_battery_recovers_rather_than_only_draining(self) -> None:
        from random import Random

        rng = Random(3)
        cycle = 24
        levels = [battery_pct(step, cycle, rng) for step in range(cycle * 2)]
        assert levels[cycle] > levels[cycle - 1], "expected a solar recharge at cycle boundary"
        assert all(0.0 <= level <= 100.0 for level in levels)

    def test_moisture_dips_during_the_afternoon(self) -> None:
        """Evapotranspiration peaks mid-afternoon, so moisture bottoms out then."""
        from random import Random

        profile = SoilProfile(SoilType.LOAMY, 6.5, 0.0, 0.0)

        def mean_at(hour: int) -> float:
            rng = Random(4)
            moment = datetime(2026, 8, 15, hour, tzinfo=UTC)
            return (
                sum(soil_moisture_pct(profile, KANO_PLAINS, moment, rng) for _ in range(300)) / 300
            )

        assert mean_at(15) < mean_at(3)

    def test_soil_temperature_peaks_in_the_afternoon(self) -> None:
        """Soil at depth lags the air, so the peak sits after solar noon."""
        from random import Random

        from smart_agri.generator.signals import soil_temperature_c

        profile = SoilProfile(SoilType.LOAMY, 6.5, 0.0, 0.0)

        def mean_at(hour: int) -> float:
            rng = Random(6)
            moment = datetime(2026, 8, 15, hour, tzinfo=UTC)
            return (
                sum(soil_temperature_c(profile, KANO_PLAINS, moment, rng) for _ in range(300)) / 300
            )

        assert mean_at(15) > mean_at(3)

    def test_wetness_curves_are_continuous(self) -> None:
        """A discontinuity would show up as a cliff in every seasonal chart."""
        for region in ALL_REGIONS:
            values = [
                wetness_index(region, datetime(2026, 1, 1, tzinfo=UTC).replace(month=m))
                for m in range(1, 13)
            ]
            deltas = [abs(b - a) for a, b in itertools.pairwise(values)]
            assert max(deltas) < 0.6, region.code

    def test_no_nan_or_infinity_is_produced(self) -> None:
        from random import Random

        rng = Random(5)
        profile = SoilProfile(SoilType.SILT, 6.8, 0.0, 0.0)
        for month in range(1, 13):
            moment = datetime(2026, month, 10, 9, tzinfo=UTC)
            value = soil_moisture_pct(profile, NILE_DELTA, moment, rng)
            assert math.isfinite(value)
