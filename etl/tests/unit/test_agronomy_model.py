"""Tests for the agronomic model and the correlations it exists to produce.

Phase 3's value is not that the tables have rows — it is that the rows relate to
each other the way an agronomist would expect. Yield must respond to water and
heat, NDVI must trace canopy development, irrigation must show up in the soil
series, and machine work must land on the days the crop calendar calls for.

Each of those is asserted here, because each is a property that could silently
degrade into noise without any structural test noticing.
"""

from __future__ import annotations

import itertools
import statistics as st
from datetime import UTC, date, datetime, timedelta

import pytest

from smart_agri.domain import PlantingStatus
from smart_agri.generator import CROPS_BY_CODE, DatasetGenerator, GeneratorConfig
from smart_agri.generator.agronomy import (
    GrowthStage,
    canopy_evi,
    canopy_ndvi,
    canopy_ndwi,
    growing_degree_days,
    growth_stage,
    yield_factor,
)
from smart_agri.generator.regions import KANO_PLAINS, NILE_DELTA
from smart_agri.generator.signals import irrigation_boost_pct, mean_air_temp_c

# Large enough for distributions to be meaningful, coarse enough to stay fast.
SEASON = GeneratorConfig(
    n_farms=12,
    fields_per_farm=3,
    sensors_per_field=1,
    machines_per_farm=3,
    start_date=date(2025, 8, 1),
    end_date=date(2026, 8, 1),
    reading_interval_minutes=720,
)


@pytest.fixture(scope="module")
def dataset():  # type: ignore[no-untyped-def]
    return DatasetGenerator(SEASON).build()


# --- growth model ------------------------------------------------------------
class TestGrowthStages:
    @pytest.mark.parametrize(
        ("progress", "expected"),
        [
            (0.0, GrowthStage.EMERGENCE),
            (0.30, GrowthStage.VEGETATIVE),
            (0.55, GrowthStage.FLOWERING),
            (0.80, GrowthStage.GRAIN_FILL),
            (1.0, GrowthStage.MATURITY),
        ],
    )
    def test_stage_for_progress(self, progress: float, expected: GrowthStage) -> None:
        assert growth_stage(progress) is expected

    def test_progress_is_clamped(self) -> None:
        assert growth_stage(-5.0) is GrowthStage.EMERGENCE
        assert growth_stage(99.0) is GrowthStage.MATURITY


class TestGrowingDegreeDays:
    def test_no_accumulation_below_base_temperature(self) -> None:
        assert growing_degree_days(mean_temp_c=8.0, base_temp_c=10.0) == 0.0

    def test_accumulates_above_base(self) -> None:
        assert growing_degree_days(mean_temp_c=22.0, base_temp_c=10.0) == 12.0

    def test_heat_is_capped(self) -> None:
        """Growth does not keep accelerating with heat; an uncapped sum would
        give the hottest fields implausible yields."""
        assert growing_degree_days(45.0, 10.0, cap_c=30.0) == 20.0


class TestCanopyIndices:
    def test_annual_ndvi_traces_an_arc(self) -> None:
        crop = CROPS_BY_CODE["maize"]
        at_sowing = canopy_ndvi(crop, 0.0)
        at_peak = canopy_ndvi(crop, 0.60)
        at_maturity = canopy_ndvi(crop, 1.0)

        assert at_sowing < 0.2, "bare soil at sowing"
        assert at_peak == pytest.approx(crop.peak_ndvi, abs=0.01)
        assert at_maturity < at_peak, "senescence must bring the canopy down"

    def test_annual_ndvi_rises_monotonically_to_the_peak(self) -> None:
        crop = CROPS_BY_CODE["wheat"]
        values = [canopy_ndvi(crop, p / 100) for p in range(0, 61)]
        assert all(b >= a for a, b in itertools.pairwise(values))

    def test_perennial_ndvi_never_falls_to_bare_soil(self) -> None:
        """An orchard keeps its canopy year round; a dip to bare soil would look
        like the trees had been removed."""
        crop = CROPS_BY_CODE["olive"]
        values = [canopy_ndvi(crop, p / 100) for p in range(101)]
        assert min(values) > 0.4

    @pytest.mark.parametrize("crop_code", ["maize", "cotton", "cocoa", "cassava"])
    def test_indices_stay_within_physical_bounds(self, crop_code: str) -> None:
        crop = CROPS_BY_CODE[crop_code]
        for step in range(101):
            ndvi = canopy_ndvi(crop, step / 100)
            assert -1.0 <= ndvi <= 1.0
            assert -1.0 <= canopy_ndwi(ndvi, 0.5) <= 1.0
            assert -1.0 <= canopy_evi(ndvi) <= 1.0

    def test_water_stress_pulls_ndwi_down(self) -> None:
        """This is what distinguishes a dry canopy from a sparse one."""
        assert canopy_ndwi(0.7, water_stress=0.8) < canopy_ndwi(0.7, water_stress=0.0)


class TestYieldFactor:
    def test_full_water_and_heat_reaches_potential(self) -> None:
        assert yield_factor(1.0, 1.0, "moderate") == pytest.approx(1.0)

    def test_water_shortage_reduces_yield(self) -> None:
        assert yield_factor(0.5, 1.0, "low") < yield_factor(1.0, 1.0, "low")

    def test_heat_shortage_reduces_yield(self) -> None:
        assert yield_factor(1.0, 0.6, "low") < yield_factor(1.0, 1.0, "low")

    def test_drought_tolerance_softens_a_water_deficit(self) -> None:
        dry = 0.6
        assert yield_factor(dry, 1.0, "high") > yield_factor(dry, 1.0, "low")

    def test_both_limits_apply_together(self) -> None:
        """Multiplicative, not additive: abundant heat cannot compensate for
        water the crop never received."""
        assert yield_factor(0.4, 1.0, "low") < 0.5

    def test_result_is_always_a_fraction(self) -> None:
        for water in (0.0, 0.3, 0.8, 1.5):
            for heat in (0.0, 0.4, 1.0, 2.0):
                assert 0.0 <= yield_factor(water, heat, "moderate") <= 1.0


class TestAirTemperature:
    def test_sahel_is_warmer_and_less_seasonal_than_the_maghreb(self) -> None:
        january = datetime(2026, 1, 15, tzinfo=UTC)
        july = datetime(2026, 7, 15, tzinfo=UTC)

        sahel_swing = mean_air_temp_c(KANO_PLAINS, july) - mean_air_temp_c(KANO_PLAINS, january)
        delta_swing = mean_air_temp_c(NILE_DELTA, july) - mean_air_temp_c(NILE_DELTA, january)

        assert mean_air_temp_c(KANO_PLAINS, january) > mean_air_temp_c(NILE_DELTA, january)
        assert sahel_swing < delta_swing

    def test_temperatures_are_plausible_for_africa(self) -> None:
        """An overstated annual swing starves winter-sown cereals of
        degree-days and makes every Mediterranean planting fail."""
        for month in range(1, 13):
            moment = datetime(2026, month, 15, tzinfo=UTC)
            for region in (KANO_PLAINS, NILE_DELTA):
                assert 5.0 < mean_air_temp_c(region, moment) < 40.0


# --- irrigation coupling -----------------------------------------------------
class TestIrrigationBoost:
    def test_boost_is_largest_immediately_after_watering(self) -> None:
        applied = datetime(2026, 7, 1, 6, tzinfo=UTC)
        events = [(applied, 20.0)]
        at_once = irrigation_boost_pct(events, applied)
        a_day_later = irrigation_boost_pct(events, applied + timedelta(days=1))
        assert at_once > a_day_later > 0

    def test_boost_decays_to_nothing(self) -> None:
        applied = datetime(2026, 7, 1, 6, tzinfo=UTC)
        events = [(applied, 20.0)]
        assert irrigation_boost_pct(events, applied + timedelta(days=10)) == 0.0

    def test_deeper_application_lifts_moisture_further(self) -> None:
        applied = datetime(2026, 7, 1, 6, tzinfo=UTC)
        assert irrigation_boost_pct([(applied, 40.0)], applied) > irrigation_boost_pct(
            [(applied, 10.0)], applied
        )

    def test_future_events_are_ignored(self) -> None:
        """A reading must not be raised by water applied after it was taken."""
        moment = datetime(2026, 7, 1, 6, tzinfo=UTC)
        future = [(moment + timedelta(days=1), 30.0)]
        assert irrigation_boost_pct(future, moment) == 0.0


# --- dataset-level correlations ----------------------------------------------
class TestPlantingCalendar:
    def test_crops_are_sown_into_seasons_they_can_finish(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """The bug this pins: cotton has a base temperature of 15 °C, so a
        November sowing in the Nile Delta banks almost no degree-days and grades
        out as a total failure."""
        ratios = [
            plan.gdd_accumulated / plan.crop.gdd_to_maturity
            for plan in dataset.plans
            if plan.harvest is not None
        ]
        assert min(ratios) > 0.55, "a crop was sown into a season it cannot finish"

    def test_warm_and_cool_season_crops_are_separated(self, dataset) -> None:  # type: ignore[no-untyped-def]
        months_by_crop: dict[str, set[int]] = {}
        for plan in dataset.plans:
            months_by_crop.setdefault(plan.crop.crop_code, set()).add(
                plan.planting.planted_on.month
            )

        # Cotton is a summer crop; it must never appear in the winter window.
        winter = {11, 12, 1}
        assert not (months_by_crop.get("cotton", set()) & winter)

    def test_perennial_fields_keep_their_crop(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """An olive grove does not become a maize field mid-year."""
        crops_by_field: dict[str, set[str]] = {}
        for plan in dataset.plans:
            crops_by_field.setdefault(plan.planting.field_code, set()).add(plan.crop.crop_code)

        for field_code, crops in crops_by_field.items():
            perennials = {c for c in crops if CROPS_BY_CODE[c].is_perennial}
            if perennials:
                assert crops == perennials, f"{field_code} mixes an orchard with arable crops"


class TestWaterAndYield:
    def test_yield_responds_to_water_received(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """The relationship the water-use dashboard exists to show.

        It only holds because irrigation deliberately falls short of the full
        deficit for some plantings — if every field received exactly its demand,
        water would never limit yield and this would be flat.
        """
        pairs = sorted(
            (
                plan.water_received_mm / plan.crop.water_demand_mm,
                plan.harvest.yield_t_ha / plan.variety.yield_potential_t_ha,
            )
            for plan in dataset.plans
            if plan.harvest is not None and plan.harvest.yield_t_ha > 0
        )
        assert len(pairs) >= 10, "not enough completed harvests to compare"

        half = len(pairs) // 2
        driest = st.mean(y for _, y in pairs[:half])
        wettest = st.mean(y for _, y in pairs[half:])
        assert wettest > driest, "yield does not respond to water"

    def test_water_received_actually_varies(self, dataset) -> None:  # type: ignore[no-untyped-def]
        ratios = [plan.water_received_mm / plan.crop.water_demand_mm for plan in dataset.plans]
        assert max(ratios) - min(ratios) > 0.2, "irrigation adequacy has no spread"

    def test_failed_plantings_still_produce_a_harvest_row(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """Dropping them would make average yield look better than it was."""
        failed = [p for p in dataset.plans if p.planting.status is PlantingStatus.FAILED]
        for plan in failed:
            assert plan.harvest is not None
            assert plan.harvest.yield_tonnes == 0.0

    def test_revenue_follows_yield_and_price(self, dataset) -> None:  # type: ignore[no-untyped-def]
        for plan in dataset.plans:
            if plan.harvest is None:
                continue
            expected = plan.harvest.yield_tonnes * plan.harvest.price_usd_per_tonne
            assert plan.harvest.revenue_usd == pytest.approx(expected, abs=0.02)


class TestIrrigationSchedule:
    def test_arid_regions_irrigate_more_than_humid_ones(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """Rain supplies most of a Ghanaian crop's water and almost none of an
        Egyptian one's, so the schedules must differ sharply."""
        depth_by_pattern: dict[str, list[float]] = {}
        for plan in dataset.plans:
            total = sum(e.depth_mm for e in plan.irrigation)
            depth_by_pattern.setdefault(plan.region.rainfall.value, []).append(
                total / plan.crop.water_demand_mm
            )

        arid = st.mean(depth_by_pattern.get("irrigated_arid", [0.0]))
        humid = st.mean(depth_by_pattern.get("bimodal", [0.0]))
        assert arid > humid

    def test_irrigation_falls_inside_the_crop_cycle(self, dataset) -> None:  # type: ignore[no-untyped-def]
        for plan in dataset.plans:
            for event in plan.irrigation:
                assert event.event_ts.date() >= plan.planting.planted_on
                assert event.event_ts.date() <= plan.harvest_date + timedelta(days=1)

    def test_energy_scales_with_volume(self, dataset) -> None:  # type: ignore[no-untyped-def]
        for plan in dataset.plans:
            for event in plan.irrigation:
                assert event.energy_kwh >= 0
                if event.water_volume_m3 > 0:
                    assert event.energy_kwh > 0


class TestMachineryAlignment:
    def test_operations_land_on_the_crop_calendar(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """A combine works a field on the day it is harvested, not at random."""
        plans_by_key = {p.key: p for p in dataset.plans}
        checked = 0
        for record in dataset.operations:
            plan = plans_by_key.get(record.operation.planting_key or "")
            if plan is None:
                continue
            checked += 1
            offset = (record.operation.started_at.date() - plan.planting.planted_on).days
            assert -14 <= offset <= plan.variety.maturity_days + 7
        assert checked > 0

    def test_harvesting_uses_a_harvester_where_one_exists(self, dataset) -> None:  # type: ignore[no-untyped-def]
        machines = {m.machine_code: m for m in dataset.machines}
        harvest_ops = [
            r for r in dataset.operations if r.operation.operation_type.value == "harvesting"
        ]
        used = {machines[r.operation.machine_code].machine_type.value for r in harvest_ops}
        assert "combine_harvester" in used

    def test_telemetry_is_emitted_while_working_and_while_parked(self, dataset) -> None:  # type: ignore[no-untyped-def]
        working = dataset.telemetry.filter(dataset.telemetry["engine_running"])
        parked = dataset.telemetry.filter(~dataset.telemetry["engine_running"])
        assert working.height > 0
        assert parked.height > 0

    def test_parked_machines_burn_no_fuel(self, dataset) -> None:  # type: ignore[no-untyped-def]
        parked = dataset.telemetry.filter(~dataset.telemetry["engine_running"])
        assert parked["fuel_rate_l_per_h"].max() == 0.0
        assert parked["engine_rpm"].max() == 0

    def test_some_working_samples_are_idle(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """Idle ratio is a Phase 6 metric; a fleet that is never idle makes it
        trivially zero."""
        working = dataset.telemetry.filter(dataset.telemetry["engine_running"])
        assert 0.0 < working["is_idle"].mean() < 0.5

    def test_retired_machines_emit_nothing(self, dataset) -> None:  # type: ignore[no-untyped-def]
        retired = {m.machine_code for m in dataset.machines if m.status.value == "retired"}
        if retired:
            reporting = set(dataset.telemetry["machine_code"].unique().to_list())
            assert not (retired & reporting)

    def test_some_faults_remain_open(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """An open fault is what a maintenance-due signal keys off."""
        assert any(f.resolved_at is None for f in dataset.faults)

    def test_resolved_faults_carry_downtime_and_cost(self, dataset) -> None:  # type: ignore[no-untyped-def]
        for fault in dataset.faults:
            if fault.resolved_at is not None:
                assert fault.downtime_hours is not None
                assert fault.repair_cost_usd is not None
                assert fault.resolved_at >= fault.occurred_at


class TestImagerySeries:
    def test_cloudy_passes_are_recorded_without_indices(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """Dropping them would hide gaps a real series genuinely has."""
        cloudy = [o for o in dataset.observations if not o.usable]
        assert cloudy, "no cloudy passes generated"
        for observation in cloudy:
            assert observation.ndvi is None
            assert observation.ndwi is None
            assert observation.evi is None

    def test_ndvi_peaks_mid_cycle_rather_than_at_the_ends(self, dataset) -> None:  # type: ignore[no-untyped-def]
        plan = max(dataset.plans, key=lambda p: len(p.irrigation))
        series = sorted(
            ((o.observed_on - plan.planting.planted_on).days / plan.variety.maturity_days, o.ndvi)
            for o in dataset.observations
            if o.planting_key == plan.key and o.usable and o.ndvi is not None
        )
        assert len(series) >= 8

        peak_progress = max(series, key=lambda item: item[1] or 0.0)[0]
        assert 0.35 < peak_progress < 0.85

    def test_field_and_date_and_source_are_unique(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """Mirrors uq_observation_field_date_source; a duplicate would fail the
        insert rather than any earlier check."""
        keys = [(o.field_code, o.observed_on, o.source.value) for o in dataset.observations]
        assert len(keys) == len(set(keys))


class TestCostAccounting:
    def test_costs_are_booked_against_real_categories(self, dataset) -> None:  # type: ignore[no-untyped-def]
        categories = {c.cost_category.value for c in dataset.costs}
        assert {"seed", "fertilizer", "irrigation", "labour"} <= categories

    def test_input_cost_matches_quantity_times_unit_cost(self, dataset) -> None:  # type: ignore[no-untyped-def]
        for application in dataset.inputs:
            expected = application.quantity_kg * application.unit_cost_usd_per_kg
            assert application.total_cost_usd == pytest.approx(expected, abs=0.02)

    def test_fertiliser_cost_tracks_the_applications(self, dataset) -> None:  # type: ignore[no-untyped-def]
        """Costs are derived from what actually happened on the field, not
        drawn independently — that is what makes cost per hectare meaningful."""
        for plan in dataset.plans:
            applied = sum(
                a.total_cost_usd for a in plan.inputs if a.input_type.value == "fertilizer"
            )
            booked = sum(
                c.amount_usd
                for c in dataset.costs
                if c.planting_key == plan.key and c.cost_category.value == "fertilizer"
            )
            if applied > 0 and booked > 0:
                assert booked == pytest.approx(applied, rel=0.01)
