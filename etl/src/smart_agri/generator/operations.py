"""Farm operations: plantings, irrigation, inputs, harvests and costs.

The generation order here is not arbitrary. A planting fixes the crop, the
window and the water demand; irrigation is then scheduled to cover whatever the
rain does not supply; inputs land at the growth stages that call for them; and
the harvest falls out of the water actually received and the degree-days
actually accumulated. Costs are the sum of what all of that consumed.

Generating in that order is what makes the economics dashboard show real
relationships — water use against yield, cost per hectare against margin —
rather than four independent random series.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING

from smart_agri.domain import (
    CostCategory,
    FieldCost,
    Harvest,
    InputApplication,
    InputType,
    IrrigationEvent,
    IrrigationMethod,
    Planting,
    PlantingStatus,
    QualityGrade,
    WaterSource,
)
from smart_agri.generator.agronomy import (
    CROPS_BY_CODE,
    VARIETIES_BY_CROP,
    Crop,
    Variety,
    growing_degree_days,
    planting_months,
    yield_factor,
)
from smart_agri.generator.signals import mean_air_temp_c, rainfall_share

if TYPE_CHECKING:
    from collections.abc import Sequence
    from random import Random

    from smart_agri.domain import Farm, Field_
    from smart_agri.generator.config import GeneratorConfig
    from smart_agri.generator.regions import Region

# Typical application depth per irrigation event, in millimetres. Drip delivers
# little and often; flood delivers a lot and rarely.
_EVENT_DEPTH_MM: dict[IrrigationMethod, float] = {
    IrrigationMethod.DRIP: 8.0,
    IrrigationMethod.SPRINKLER: 15.0,
    IrrigationMethod.PIVOT: 20.0,
    IrrigationMethod.FURROW: 40.0,
    IrrigationMethod.FLOOD: 60.0,
}

# Pumping energy per cubic metre delivered, in kWh. Pressurised systems cost
# more to run than gravity-fed ones.
_ENERGY_KWH_PER_M3: dict[IrrigationMethod, float] = {
    IrrigationMethod.DRIP: 0.28,
    IrrigationMethod.SPRINKLER: 0.42,
    IrrigationMethod.PIVOT: 0.38,
    IrrigationMethod.FURROW: 0.12,
    IrrigationMethod.FLOOD: 0.08,
}

# A crop is only sown into a season that banks between these shares of its
# degree-day requirement. The lower bound stops cotton going in at the start
# of a Mediterranean winter; the upper bound stops wheat going into Egyptian
# high summer, which banks far more heat than the crop can use.
_MIN_VIABLE_GDD_RATIO = 0.85
_MAX_VIABLE_GDD_RATIO = 1.80

# Share of fields managed as perennial orchards rather than arable rotation.
_ORCHARD_SHARE = 0.30
# A rain calendar with more than this many windows has a distinct dry season.
_SEASONS_WITH_DRY_WINDOW = 2
# Fraction of attainable yield at which each quality grade begins.
_GRADE_A_FLOOR = 0.80
_GRADE_B_FLOOR = 0.55
_GRADE_C_FLOOR = 0.25

_ELECTRICITY_USD_PER_KWH = 0.11
_SEED_USD_PER_KG = 1.8

# Farm-gate price per tonne, by crop category.
_PRICE_USD_PER_TONNE: dict[str, float] = {
    "cereal": 260.0,
    "legume": 720.0,
    "root": 130.0,
    "vegetable": 340.0,
    "fibre": 1_650.0,
    "tree_crop": 980.0,
}

_FERTILISER_PRODUCTS = ("NPK 15-15-15", "Urea 46%", "DAP 18-46-0", "CAN 27%")
_PROTECTION_PRODUCTS = ("Glyphosate 480", "Lambda-cyhalothrin", "Mancozeb 80WP", "Atrazine 500")

_INPUT_UNIT_COST_USD_PER_KG: dict[InputType, float] = {
    InputType.FERTILIZER: 0.62,
    InputType.HERBICIDE: 7.40,
    InputType.PESTICIDE: 12.80,
    InputType.FUNGICIDE: 9.50,
    InputType.LIME: 0.09,
}


@dataclass(slots=True)
class PlantingPlan:
    """A planting together with everything derived from it.

    Carried as one object so the machinery and imagery generators can align to
    the same dates rather than each inventing their own calendar.
    """

    key: str
    """Natural key — the seeder resolves it to the surrogate planting_id."""

    planting: Planting
    crop: Crop
    variety: Variety
    region: Region
    field_area_ha: float

    irrigation: list[IrrigationEvent] = dataclass_field(default_factory=list)
    inputs: list[InputApplication] = dataclass_field(default_factory=list)
    harvest: Harvest | None = None

    water_received_mm: float = 0.0
    gdd_accumulated: float = 0.0

    @property
    def harvest_date(self) -> date:
        return self.planting.expected_harvest_on


class OperationsGenerator:
    """Produces the operational record for a set of fields."""

    def __init__(self, config: GeneratorConfig, rng: Random) -> None:
        self._config = config
        self._rng = rng
        # (region, crop, month) -> projected degree-day ratio.
        self._gdd_cache: dict[tuple[str, str, int], float] = {}

    # -- plantings -----------------------------------------------------------
    def plantings(
        self,
        fields: Sequence[Field_],
        farms: Sequence[Farm],
        regions_by_farm: dict[str, Region],
    ) -> list[PlantingPlan]:
        """One plan per field per season within the window.

        Annual crops rotate between seasons, which is both realistic and what
        gives the yield dashboard more than one crop to compare. A perennial
        field keeps its crop and produces one cycle a year.
        """
        del farms
        plans: list[PlantingPlan] = []

        for field in fields:
            region = regions_by_farm[field.farm_code]
            crop_pool = [CROPS_BY_CODE[code] for code in region.crops]
            perennials = [c for c in crop_pool if c.is_perennial]
            annuals = [c for c in crop_pool if not c.is_perennial]

            # A field is either an orchard or under arable rotation, decided
            # once — an olive grove does not become a maize field mid-year.
            is_orchard = bool(perennials) and self._rng.random() < _ORCHARD_SHARE
            fixed_crop = self._rng.choice(perennials) if is_orchard else None

            for planted_on in self._planting_dates(region, is_orchard=is_orchard):
                crop = fixed_crop or self._choose_crop(region, annuals or crop_pool, planted_on)
                variety = self._rng.choice(VARIETIES_BY_CROP[crop.crop_code])
                plans.append(self._build_plan(field, region, crop, variety, planted_on))

        return plans

    def _choose_crop(self, region: Region, candidates: Sequence[Crop], planted_on: date) -> Crop:
        """Pick a crop the season can actually finish.

        Sowing a warm-season crop into the winter window is not a small error:
        cotton has a base temperature of 15 °C, so a November planting in the
        Nile Delta accumulates almost no degree-days and grades out as a total
        failure. Filtering candidates by projected heat units keeps the calendar
        agronomically honest — winter cereals in the cool window, cotton and
        vegetables in the warm one — without hard-coding a crop-to-month table.
        """
        viable = [
            crop
            for crop in candidates
            if _MIN_VIABLE_GDD_RATIO
            <= self._projected_gdd_ratio(region, crop, planted_on)
            <= _MAX_VIABLE_GDD_RATIO
        ]
        if viable:
            return self._rng.choice(viable)

        # Nothing sits in the band. Take whichever crop lands closest to a
        # perfect heat match rather than a random one, so the data stays
        # plausible instead of pretending the season suits nothing.
        return min(
            candidates,
            key=lambda crop: abs(self._projected_gdd_ratio(region, crop, planted_on) - 1.0),
        )

    def _projected_gdd_ratio(self, region: Region, crop: Crop, planted_on: date) -> float:
        """Degree-days a cycle starting on this date would bank, over what it needs.

        Memoised on the month rather than the exact day: the seasonal curve
        barely moves within a month, and without the cache this would dominate
        generation time on the larger profiles.
        """
        cache_key = (region.code, crop.crop_code, planted_on.month)
        cached = self._gdd_cache.get(cache_key)
        if cached is not None:
            return cached

        total = 0.0
        anchor = date(planted_on.year, planted_on.month, 15)
        for offset in range(crop.cycle_days):
            moment = datetime.combine(anchor + timedelta(days=offset), time(hour=12), tzinfo=UTC)
            total += growing_degree_days(mean_air_temp_c(region, moment), crop.base_temp_c)

        ratio = total / crop.gdd_to_maturity
        self._gdd_cache[cache_key] = ratio
        return ratio

    def _planting_dates(self, region: Region, *, is_orchard: bool) -> list[date]:
        """Sowing dates inside the window, following the region's rain calendar."""
        months = planting_months(region.rainfall)
        if is_orchard:
            # A perennial cycle is annual; use the first window of each year.
            months = months[:1]

        dates: list[date] = []
        for year in range(self._config.start_date.year, self._config.end_date.year + 1):
            for month in months:
                day = self._rng.randint(1, 26)
                candidate = date(year, month, day)
                if self._config.start_date <= candidate <= self._config.end_date:
                    dates.append(candidate)
        return sorted(dates)

    def _build_plan(
        self, field: Field_, region: Region, crop: Crop, variety: Variety, planted_on: date
    ) -> PlantingPlan:
        cycle_days = variety.maturity_days
        expected_harvest = planted_on + timedelta(days=cycle_days)
        season_label = self._season_label(planted_on, region)
        area = round(float(field.area_ha) * self._rng.uniform(0.85, 1.0), 2)

        planting = Planting(
            field_code=field.field_code,
            variety_code=variety.variety_code,
            season=season_label,
            planted_on=planted_on,
            expected_harvest_on=expected_harvest,
            area_ha=area,
            seed_rate_kg_ha=round(self._seed_rate(crop), 2),
            status=PlantingStatus.GROWING,
        )

        return PlantingPlan(
            key=f"{field.field_code}|{season_label}|{planted_on.isoformat()}",
            planting=planting,
            crop=crop,
            variety=variety,
            region=region,
            field_area_ha=area,
        )

    def _season_label(self, planted_on: date, region: Region) -> str:
        months = planting_months(region.rainfall)
        if len(months) == 1 or planted_on.month == months[0]:
            suffix = "main"
        elif planted_on.month == months[-1] and len(months) > _SEASONS_WITH_DRY_WINDOW:
            suffix = "dry"
        else:
            suffix = "second"
        return f"{planted_on.year}-{suffix}"

    def _seed_rate(self, crop: Crop) -> float:
        return {
            "cereal": self._rng.uniform(90, 180),
            "legume": self._rng.uniform(60, 110),
            "root": self._rng.uniform(1_800, 2_600),
            "vegetable": self._rng.uniform(0.3, 0.8),
            "fibre": self._rng.uniform(18, 30),
            "tree_crop": self._rng.uniform(0.05, 0.2),
        }[crop.category]

    # -- irrigation ----------------------------------------------------------
    def irrigate(self, plan: PlantingPlan) -> list[IrrigationEvent]:
        """Schedule irrigation to cover the deficit rain leaves behind.

        Events are weighted toward flowering and grain fill, when crop water
        demand peaks — so a moisture chart shows watering clustered where an
        agronomist would expect it.
        """
        rain_fraction = rainfall_share(plan.region.rainfall)
        rain_mm = plan.crop.water_demand_mm * rain_fraction * self._rng.uniform(0.75, 1.15)
        deficit_mm = max(0.0, plan.crop.water_demand_mm - rain_mm)

        # Farms rarely deliver the full deficit — allocations are capped,
        # boreholes run slow, pumps fail and diesel costs money. Scaling by an
        # adequacy factor is what makes water genuinely limiting for some
        # plantings and not others, and therefore what makes yield respond to
        # water at all.
        adequacy = self._rng.uniform(
            self._config.irrigation_adequacy_min, self._config.irrigation_adequacy_max
        )
        deficit_mm *= adequacy

        method = self._irrigation_method(plan)
        source = self._water_source(plan.region)
        depth_per_event = _EVENT_DEPTH_MM[method]
        n_events = int(deficit_mm / depth_per_event)

        cycle_days = plan.variety.maturity_days
        events: list[IrrigationEvent] = []
        applied_mm = 0.0

        for _ in range(n_events):
            # Beta-ish draw peaking around 0.55 of the cycle.
            progress = min(0.98, max(0.02, self._rng.betavariate(2.4, 2.0)))
            offset_days = int(progress * cycle_days)
            event_day = plan.planting.planted_on + timedelta(days=offset_days)
            if event_day > self._config.end_date:
                continue

            depth = round(depth_per_event * self._rng.uniform(0.85, 1.15), 2)
            volume = round(depth * plan.field_area_ha * 10.0, 2)  # 1 mm/ha = 10 m3
            energy = round(volume * _ENERGY_KWH_PER_M3[method], 2)

            events.append(
                IrrigationEvent(
                    field_code=plan.planting.field_code,
                    planting_key=plan.key,
                    # Irrigation runs early, before the day's evaporative peak.
                    event_ts=datetime.combine(
                        event_day, time(hour=self._rng.randint(4, 8)), tzinfo=UTC
                    ),
                    method=method,
                    water_volume_m3=volume,
                    depth_mm=depth,
                    duration_minutes=max(15, int(volume / self._rng.uniform(20, 60))),
                    energy_kwh=energy,
                    water_source=source,
                )
            )
            applied_mm += depth

        plan.irrigation = sorted(events, key=lambda e: e.event_ts)
        plan.water_received_mm = round(rain_mm + applied_mm, 2)
        return plan.irrigation

    def _irrigation_method(self, plan: PlantingPlan) -> IrrigationMethod:
        if plan.crop.crop_code in {"rice", "rice_upland"}:
            return IrrigationMethod.FLOOD
        if plan.crop.is_perennial:
            return IrrigationMethod.DRIP
        if plan.crop.category == "vegetable":
            return IrrigationMethod.DRIP
        return self._rng.choice(
            [IrrigationMethod.SPRINKLER, IrrigationMethod.FURROW, IrrigationMethod.PIVOT]
        )

    def _water_source(self, region: Region) -> WaterSource:
        if region.code == "EG-DELTA":
            return WaterSource.CANAL
        return self._rng.choice([WaterSource.BOREHOLE, WaterSource.RIVER, WaterSource.RESERVOIR])

    # -- inputs --------------------------------------------------------------
    def apply_inputs(self, plan: PlantingPlan) -> list[InputApplication]:
        """Fertiliser and crop protection, placed at the stages that call for them."""
        cycle = plan.variety.maturity_days
        schedule: list[tuple[float, InputType]] = [
            (0.02, InputType.FERTILIZER),  # basal, at sowing
            (0.10, InputType.HERBICIDE),  # early weed control
            (0.35, InputType.FERTILIZER),  # top dressing through vegetative growth
            (0.55, InputType.PESTICIDE),  # flowering, when pest pressure peaks
        ]
        if plan.crop.category in {"vegetable", "tree_crop"}:
            schedule.append((0.70, InputType.FUNGICIDE))

        applications: list[InputApplication] = []
        for progress, input_type in schedule:
            applied_on = plan.planting.planted_on + timedelta(days=int(progress * cycle))
            if applied_on > self._config.end_date:
                continue

            rate = round(self._input_rate(input_type), 3)
            quantity = round(rate * plan.field_area_ha, 3)
            # Round the unit price *before* multiplying. Deriving the total from
            # the unrounded value would leave a row that does not reconcile with
            # itself, and any cost dashboard recomputing spend from rate times
            # price would disagree with the stored total.
            unit_cost = round(
                _INPUT_UNIT_COST_USD_PER_KG[input_type] * self._rng.uniform(0.9, 1.15), 4
            )

            applications.append(
                InputApplication(
                    field_code=plan.planting.field_code,
                    planting_key=plan.key,
                    applied_on=applied_on,
                    input_type=input_type,
                    product_name=self._product_name(input_type),
                    rate_kg_ha=rate,
                    quantity_kg=quantity,
                    unit_cost_usd_per_kg=unit_cost,
                    total_cost_usd=round(quantity * unit_cost, 2),
                    application_method=self._rng.choice(
                        ["broadcast", "banded", "foliar_spray", "fertigation"]
                    ),
                )
            )

        plan.inputs = applications
        return applications

    def _input_rate(self, input_type: InputType) -> float:
        return {
            InputType.FERTILIZER: self._rng.uniform(80, 220),
            InputType.HERBICIDE: self._rng.uniform(1.2, 3.5),
            InputType.PESTICIDE: self._rng.uniform(0.4, 1.6),
            InputType.FUNGICIDE: self._rng.uniform(1.0, 2.8),
            InputType.LIME: self._rng.uniform(400, 1_200),
        }[input_type]

    def _product_name(self, input_type: InputType) -> str:
        pool = _FERTILISER_PRODUCTS if input_type is InputType.FERTILIZER else _PROTECTION_PRODUCTS
        return self._rng.choice(pool)

    # -- harvest -------------------------------------------------------------
    def harvest(self, plan: PlantingPlan) -> Harvest | None:
        """Yield from the water received and the degree-days accumulated.

        Returns None when the cycle has not finished inside the window — a
        planting still in the ground is not a missing harvest, and the pipeline
        must not treat it as one.
        """
        harvested_on = plan.harvest_date + timedelta(days=self._rng.randint(-4, 10))
        if harvested_on > self._config.end_date:
            return None

        plan.gdd_accumulated = self._accumulate_gdd(plan)

        if self._rng.random() < self._config.crop_failure_ratio:
            return self._failed_harvest(plan, harvested_on)

        water_ratio = plan.water_received_mm / plan.crop.water_demand_mm
        gdd_ratio = plan.gdd_accumulated / plan.crop.gdd_to_maturity
        achieved = yield_factor(water_ratio, gdd_ratio, plan.variety.drought_tolerance)

        yield_t_ha = round(
            plan.variety.yield_potential_t_ha * achieved * self._rng.uniform(0.93, 1.07), 3
        )
        total_tonnes = round(yield_t_ha * plan.field_area_ha, 3)
        price = round(_PRICE_USD_PER_TONNE[plan.crop.category] * self._rng.uniform(0.85, 1.2), 2)

        plan.planting = plan.planting.model_copy(update={"status": PlantingStatus.HARVESTED})
        plan.harvest = Harvest(
            planting_key=plan.key,
            field_code=plan.planting.field_code,
            harvested_on=harvested_on,
            area_harvested_ha=plan.field_area_ha,
            yield_tonnes=total_tonnes,
            yield_t_ha=yield_t_ha,
            moisture_pct=round(self._rng.uniform(11.0, 19.0), 2),
            quality_grade=self._grade(achieved),
            price_usd_per_tonne=price,
            revenue_usd=round(total_tonnes * price, 2),
        )
        return plan.harvest

    def _failed_harvest(self, plan: PlantingPlan, harvested_on: date) -> Harvest:
        """A failed crop still produces a record — with zero yield.

        Dropping the row instead would make average yield look better than it
        was, which is exactly the kind of quiet bias a dashboard must not have.
        """
        plan.planting = plan.planting.model_copy(update={"status": PlantingStatus.FAILED})
        plan.harvest = Harvest(
            planting_key=plan.key,
            field_code=plan.planting.field_code,
            harvested_on=harvested_on,
            area_harvested_ha=plan.field_area_ha,
            yield_tonnes=0.0,
            yield_t_ha=0.0,
            moisture_pct=None,
            quality_grade=QualityGrade.REJECT,
            price_usd_per_tonne=0.0,
            revenue_usd=0.0,
        )
        return plan.harvest

    def _accumulate_gdd(self, plan: PlantingPlan) -> float:
        """Sum daily growing-degree days across the cycle."""
        total = 0.0
        for offset in range(plan.variety.maturity_days):
            day = plan.planting.planted_on + timedelta(days=offset)
            moment = datetime.combine(day, time(hour=12), tzinfo=UTC)
            total += growing_degree_days(
                mean_air_temp_c(plan.region, moment), plan.crop.base_temp_c
            )
        return round(total, 1)

    def _grade(self, achieved: float) -> QualityGrade:
        if achieved >= _GRADE_A_FLOOR:
            return QualityGrade.A
        if achieved >= _GRADE_B_FLOOR:
            return QualityGrade.B
        if achieved >= _GRADE_C_FLOOR:
            return QualityGrade.C
        return QualityGrade.REJECT

    # -- costs ---------------------------------------------------------------
    def costs(self, plan: PlantingPlan, machinery_cost_usd: float) -> list[FieldCost]:
        """Book everything the planting consumed.

        Derived from the actual inputs, irrigation energy and machine fuel
        rather than drawn independently, so cost per hectare genuinely tracks
        what happened on the field.
        """
        field_code = plan.planting.field_code
        planted_on = plan.planting.planted_on
        entries: list[FieldCost] = []

        def book(category: CostCategory, when: date, amount: float, note: str) -> None:
            if amount <= 0 or when > self._config.end_date:
                return
            entries.append(
                FieldCost(
                    field_code=field_code,
                    planting_key=plan.key,
                    cost_date=when,
                    cost_category=category,
                    amount_usd=round(amount, 2),
                    description=note,
                )
            )

        seed_cost = float(plan.planting.seed_rate_kg_ha) * plan.field_area_ha * _SEED_USD_PER_KG
        book(CostCategory.SEED, planted_on, seed_cost, f"{plan.variety.name} seed")

        fertiliser = sum(
            a.total_cost_usd for a in plan.inputs if a.input_type is InputType.FERTILIZER
        )
        protection = sum(
            a.total_cost_usd for a in plan.inputs if a.input_type is not InputType.FERTILIZER
        )
        book(CostCategory.FERTILIZER, planted_on, fertiliser, "Fertiliser applications")
        book(
            CostCategory.CROP_PROTECTION,
            planted_on,
            protection,
            "Herbicide, pesticide and fungicide",
        )

        energy_kwh = sum(e.energy_kwh for e in plan.irrigation)
        book(
            CostCategory.IRRIGATION,
            planted_on,
            energy_kwh * _ELECTRICITY_USD_PER_KWH,
            f"Pumping energy for {len(plan.irrigation)} irrigation events",
        )

        book(CostCategory.MACHINERY, planted_on, machinery_cost_usd, "Machine fuel and running")

        labour = plan.field_area_ha * self._rng.uniform(45, 120)
        book(CostCategory.LABOUR, planted_on, labour, "Field labour")

        return entries
