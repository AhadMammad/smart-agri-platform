"""Vegetation indices derived from satellite and drone passes.

NDVI follows the crop's own growth curve rather than being drawn at random, so
a field's index series rises through canopy closure and falls through
senescence — the shape the field-health dashboard exists to show. Water stress
pulls NDWI down independently, which is what separates a dry canopy from a
sparse one.

Only derived values are produced; no rasters. See docs/architecture.md.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

from smart_agri.domain import FieldIndexObservation, ImagerySource
from smart_agri.generator.agronomy import canopy_evi, canopy_ndvi, canopy_ndwi
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence
    from random import Random

    from smart_agri.domain import Field_
    from smart_agri.generator.config import GeneratorConfig
    from smart_agri.generator.operations import PlantingPlan

logger = get_logger(__name__)

_SATELLITE_PLATFORMS = ("Sentinel-2", "Landsat-9", "PlanetScope")
_DRONE_PLATFORMS = ("DJI Mavic 3M", "senseFly eBee X", "Parrot Bluegrass")

# Ground sample distance in metres, by platform class.
_SATELLITE_RESOLUTION_M = {"Sentinel-2": 10.0, "Landsat-9": 30.0, "PlanetScope": 3.0}
_DRONE_RESOLUTION_M = 0.05

# Bare soil between crop cycles still reflects something.
_FALLOW_NDVI = 0.13


class ImageryGenerator:
    """Produces the per-field index series."""

    def __init__(self, config: GeneratorConfig, rng: Random) -> None:
        self._config = config
        self._rng = rng

    def observations(
        self, fields: Sequence[Field_], plans: Sequence[PlantingPlan]
    ) -> list[FieldIndexObservation]:
        """Satellite passes on a fixed revisit cycle, plus a few drone flights."""
        plans_by_field: dict[str, list[PlantingPlan]] = {}
        for plan in plans:
            plans_by_field.setdefault(plan.planting.field_code, []).append(plan)

        result: list[FieldIndexObservation] = []
        for field in fields:
            field_plans = sorted(
                plans_by_field.get(field.field_code, []),
                key=lambda p: p.planting.planted_on,
            )
            result.extend(self._satellite_series(field, field_plans))
            result.extend(self._drone_flights(field, field_plans))

        logger.info("imagery_generated", fields=len(fields), observations=len(result))
        return result

    # -- satellite -----------------------------------------------------------
    def _satellite_series(
        self, field: Field_, plans: Sequence[PlantingPlan]
    ) -> list[FieldIndexObservation]:
        platform = self._rng.choice(_SATELLITE_PLATFORMS)
        resolution = _SATELLITE_RESOLUTION_M[platform]

        result: list[FieldIndexObservation] = []
        observed_on = self._config.start_date + timedelta(
            days=self._rng.randint(0, self._config.imagery_interval_days - 1)
        )

        while observed_on <= self._config.end_date:
            cloudy = self._rng.random() < self._config.cloudy_observation_ratio
            result.append(
                self._observe(
                    field=field,
                    plans=plans,
                    observed_on=observed_on,
                    source=ImagerySource.SATELLITE,
                    platform=platform,
                    resolution_m=resolution,
                    cloudy=cloudy,
                )
            )
            observed_on += timedelta(days=self._config.imagery_interval_days)

        return result

    # -- drone ---------------------------------------------------------------
    def _drone_flights(
        self, field: Field_, plans: Sequence[PlantingPlan]
    ) -> list[FieldIndexObservation]:
        """A handful of flights per cycle, timed to the stages that matter.

        Drones fly when someone decides to look — around canopy closure and
        again before harvest — not on a satellite's fixed cadence.
        """
        result: list[FieldIndexObservation] = []
        platform = self._rng.choice(_DRONE_PLATFORMS)

        for plan in plans:
            cycle = plan.variety.maturity_days
            for index in range(self._config.drone_flights_per_season):
                progress = (index + 1) / (self._config.drone_flights_per_season + 1)
                observed_on = plan.planting.planted_on + timedelta(days=int(progress * cycle))
                if not (self._config.start_date <= observed_on <= self._config.end_date):
                    continue

                result.append(
                    self._observe(
                        field=field,
                        plans=plans,
                        observed_on=observed_on,
                        source=ImagerySource.DRONE,
                        platform=platform,
                        resolution_m=_DRONE_RESOLUTION_M,
                        # A drone flies under the cloud base, so a pass is
                        # almost never lost to cloud.
                        cloudy=False,
                    )
                )

        return self._deduplicate(result)

    @staticmethod
    def _deduplicate(
        observations: Sequence[FieldIndexObservation],
    ) -> list[FieldIndexObservation]:
        """Enforce the (field, date, source) uniqueness the table requires.

        Two overlapping plantings can schedule a flight on the same day; without
        this the insert would violate uq_observation_field_date_source.
        """
        seen: set[tuple[str, date, str]] = set()
        unique: list[FieldIndexObservation] = []
        for observation in observations:
            key = (observation.field_code, observation.observed_on, observation.source.value)
            if key not in seen:
                seen.add(key)
                unique.append(observation)
        return unique

    # -- shared --------------------------------------------------------------
    def _observe(
        self,
        *,
        field: Field_,
        plans: Sequence[PlantingPlan],
        observed_on: date,
        source: ImagerySource,
        platform: str,
        resolution_m: float,
        cloudy: bool,
    ) -> FieldIndexObservation:
        plan = self._active_plan(plans, observed_on)

        if cloudy:
            # The pass happened and is recorded, but yields no index. Dropping
            # it would hide gaps that a real series genuinely has.
            return FieldIndexObservation(
                field_code=field.field_code,
                planting_key=plan.key if plan else None,
                observed_on=observed_on,
                source=source,
                platform=platform,
                cloud_cover_pct=round(self._rng.uniform(65, 100), 2),
                resolution_m=resolution_m,
                usable=False,
            )

        if plan is None:
            ndvi = _FALLOW_NDVI + self._rng.gauss(0, 0.02)
            stress = 0.0
        else:
            progress = self._progress(plan, observed_on)
            ndvi = canopy_ndvi(plan.crop, progress) + self._rng.gauss(0, 0.025)
            stress = self._water_stress(plan)

        ndvi = round(max(-1.0, min(1.0, ndvi)), 4)

        return FieldIndexObservation(
            field_code=field.field_code,
            planting_key=plan.key if plan else None,
            observed_on=observed_on,
            source=source,
            platform=platform,
            ndvi=ndvi,
            ndwi=canopy_ndwi(ndvi, stress),
            evi=canopy_evi(ndvi),
            cloud_cover_pct=round(self._rng.uniform(0, 35), 2),
            resolution_m=resolution_m,
            usable=True,
        )

    @staticmethod
    def _active_plan(plans: Sequence[PlantingPlan], observed_on: date) -> PlantingPlan | None:
        """The planting in the ground on this date, if any."""
        for plan in plans:
            if plan.planting.planted_on <= observed_on <= plan.harvest_date:
                return plan
        return None

    @staticmethod
    def _progress(plan: PlantingPlan, observed_on: date) -> float:
        cycle = max(1, plan.variety.maturity_days)
        elapsed = (observed_on - plan.planting.planted_on).days
        return min(1.0, max(0.0, elapsed / cycle))

    @staticmethod
    def _water_stress(plan: PlantingPlan) -> float:
        """How far short of its water demand the crop has fallen, in 0..1."""
        if plan.crop.water_demand_mm <= 0:
            return 0.0
        ratio = plan.water_received_mm / plan.crop.water_demand_mm
        return round(min(1.0, max(0.0, 1.0 - ratio)), 4)
