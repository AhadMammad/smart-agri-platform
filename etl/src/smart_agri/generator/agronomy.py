"""Crop catalogue and growth model.

This is the Python side of the reference data in
`liquibase/changelog/changes/007-crop.xml`. The generator needs the catalogue
without a database, so it is duplicated here; `tests/unit/test_reference_catalogue.py`
parses the changelog and asserts the two agree, so a value edited on one side
fails CI rather than silently skewing the data.

The growth model is what makes the later dashboards meaningful. Yield follows
accumulated growing-degree days and water received, NDVI follows canopy
development, and irrigation follows crop water demand — so the correlations an
agronomist expects to see are actually present.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from smart_agri.generator.regions import RainfallPattern


class GrowthStage(StrEnum):
    """Phenological stages, ordered by position in the cycle."""

    EMERGENCE = "emergence"
    VEGETATIVE = "vegetative"
    FLOWERING = "flowering"
    GRAIN_FILL = "grain_fill"
    MATURITY = "maturity"


@dataclass(frozen=True, slots=True)
class Crop:
    """A cultivated species.

    Field names and values mirror `agri.crop` exactly.
    """

    crop_code: str
    name: str
    category: str
    is_perennial: bool
    base_temp_c: float
    """Growth below this temperature contributes no degree-days."""

    gdd_to_maturity: int
    cycle_days: int
    water_demand_mm: int
    peak_ndvi: float


# Order and values must match the INSERT in 007-crop.xml.
CROPS: tuple[Crop, ...] = (
    # --- North Africa ---
    Crop("durum_wheat", "Durum wheat", "cereal", False, 4.0, 1900, 150, 450, 0.780),
    Crop("wheat", "Wheat", "cereal", False, 4.0, 1800, 145, 430, 0.790),
    Crop("cotton", "Cotton", "fibre", False, 15.0, 2400, 175, 800, 0.760),
    Crop("rice", "Rice", "cereal", False, 10.0, 2100, 140, 1200, 0.810),
    Crop("tomato", "Tomato", "vegetable", False, 10.0, 1500, 110, 600, 0.820),
    Crop("sugar_beet", "Sugar beet", "root", False, 3.0, 2200, 180, 650, 0.850),
    Crop("olive", "Olive", "tree_crop", True, 8.0, 2800, 365, 500, 0.700),
    Crop("citrus", "Citrus", "tree_crop", True, 12.5, 3000, 365, 900, 0.740),
    Crop("date_palm", "Date palm", "tree_crop", True, 10.0, 3400, 365, 1800, 0.650),
    # --- West Africa ---
    Crop("maize", "Maize", "cereal", False, 10.0, 1600, 120, 550, 0.830),
    Crop("sorghum", "Sorghum", "cereal", False, 10.0, 1700, 130, 450, 0.760),
    Crop("millet", "Millet", "cereal", False, 10.0, 1400, 100, 350, 0.720),
    Crop("groundnut", "Groundnut", "legume", False, 10.0, 1500, 115, 500, 0.780),
    Crop("cowpea", "Cowpea", "legume", False, 11.0, 1300, 90, 350, 0.750),
    Crop("cassava", "Cassava", "root", False, 12.0, 3200, 330, 700, 0.800),
    Crop("rice_upland", "Upland rice", "cereal", False, 10.0, 1900, 125, 900, 0.790),
    Crop("cocoa", "Cocoa", "tree_crop", True, 15.0, 3600, 365, 1500, 0.860),
    Crop("oil_palm", "Oil palm", "tree_crop", True, 15.0, 3800, 365, 1700, 0.880),
)

CROPS_BY_CODE: dict[str, Crop] = {crop.crop_code: crop for crop in CROPS}


@dataclass(frozen=True, slots=True)
class Variety:
    """A named cultivar. Mirrors `agri.crop_variety`, which the seeder writes."""

    variety_code: str
    crop_code: str
    name: str
    maturity_days: int
    yield_potential_t_ha: float
    drought_tolerance: str


# Above this seasonal requirement a crop is thirsty enough that even an
# improved line cannot be called drought tolerant.
_THIRSTY_CROP_MM = 700


def _varieties() -> tuple[Variety, ...]:
    """Two cultivars per crop: an established one and an improved one.

    The improved line matures a little sooner, yields more, and tolerates
    drought better — enough contrast for a variety comparison to be worth
    charting without inventing a whole seed catalogue.
    """
    # Attainable yields by category, in tonnes per hectare.
    baseline: dict[str, float] = {
        "cereal": 4.5,
        "legume": 2.2,
        "root": 22.0,
        "vegetable": 55.0,
        "fibre": 3.2,
        "tree_crop": 9.0,
    }

    result: list[Variety] = []
    for crop in CROPS:
        base_yield = baseline[crop.category]
        result.append(
            Variety(
                variety_code=f"{crop.crop_code}-local",
                crop_code=crop.crop_code,
                name=f"{crop.name} (local)",
                maturity_days=crop.cycle_days,
                yield_potential_t_ha=round(base_yield * 0.85, 2),
                drought_tolerance="moderate" if crop.water_demand_mm < _THIRSTY_CROP_MM else "low",
            )
        )
        result.append(
            Variety(
                variety_code=f"{crop.crop_code}-improved",
                crop_code=crop.crop_code,
                name=f"{crop.name} (improved)",
                maturity_days=max(60, int(crop.cycle_days * 0.92)),
                yield_potential_t_ha=round(base_yield * 1.25, 2),
                drought_tolerance="high" if crop.water_demand_mm < _THIRSTY_CROP_MM else "moderate",
            )
        )
    return tuple(result)


VARIETIES: tuple[Variety, ...] = _varieties()
VARIETIES_BY_CROP: dict[str, tuple[Variety, ...]] = {
    crop.crop_code: tuple(v for v in VARIETIES if v.crop_code == crop.crop_code) for crop in CROPS
}


# --- planting calendar -------------------------------------------------------
#
# Month in which the main season is sown, by rainfall pattern. Planting tracks
# water availability, so a Sahelian farm sows at the onset of the rains while a
# Mediterranean one sows into the winter wet season.
PLANTING_MONTHS: dict[RainfallPattern, tuple[int, ...]] = {
    RainfallPattern.MEDITERRANEAN: (11, 12),
    RainfallPattern.UNIMODAL: (5, 6),
    RainfallPattern.BIMODAL: (3, 4, 9),
    # Irrigation removes the constraint, so two cycles a year are normal.
    RainfallPattern.IRRIGATED_ARID: (5, 11),
}


def planting_months(pattern: RainfallPattern) -> tuple[int, ...]:
    return PLANTING_MONTHS[pattern]


# --- growth ------------------------------------------------------------------
#
# Fraction of the cycle at which each stage ends. Roughly a cereal's calendar,
# applied to every annual for simplicity; Phase 6 can refine per category if a
# dashboard ever needs stage-level precision.
_STAGE_BOUNDARIES: tuple[tuple[float, GrowthStage], ...] = (
    (0.12, GrowthStage.EMERGENCE),
    (0.45, GrowthStage.VEGETATIVE),
    (0.65, GrowthStage.FLOWERING),
    (0.88, GrowthStage.GRAIN_FILL),
    (1.01, GrowthStage.MATURITY),
)


def growth_stage(progress: float) -> GrowthStage:
    """Stage for a position in the cycle, where `progress` runs 0 to 1."""
    clamped = min(1.0, max(0.0, progress))
    for boundary, stage in _STAGE_BOUNDARIES:
        if clamped < boundary:
            return stage
    return GrowthStage.MATURITY


def growing_degree_days(mean_temp_c: float, base_temp_c: float, cap_c: float = 30.0) -> float:
    """Degree-days accumulated in one day.

    Capped above, because growth does not keep accelerating with heat — an
    uncapped sum would have the hottest fields produce implausible yields.
    """
    effective = min(mean_temp_c, cap_c)
    return max(0.0, effective - base_temp_c)


def canopy_ndvi(crop: Crop, progress: float) -> float:
    """NDVI for a point in the crop cycle.

    Annuals trace the familiar arc: bare soil at sowing, a rise through canopy
    closure to a peak around three-fifths of the cycle, then senescence. A flat
    or random NDVI would make the vegetation-health charts meaningless, which is
    the whole point of generating it this way.

    Perennials keep a high evergreen baseline with a mild seasonal flush.
    """
    if crop.is_perennial:
        seasonal = 0.5 - 0.5 * math.cos(2 * math.pi * progress)
        return crop.peak_ndvi * (0.82 + 0.18 * seasonal)

    clamped = min(1.0, max(0.0, progress))
    bare_soil = 0.14
    peak_at = 0.60

    if clamped <= peak_at:
        # Sigmoid rise: slow to emerge, fast through canopy closure.
        shaped = clamped / peak_at
        growth = shaped**2 * (3 - 2 * shaped)  # smoothstep
        return bare_soil + (crop.peak_ndvi - bare_soil) * growth

    # Senescence: a steeper fall than the rise, ending on standing dry matter.
    shaped = (clamped - peak_at) / (1.0 - peak_at)
    residual = 0.22
    return float(crop.peak_ndvi - (crop.peak_ndvi - residual) * shaped**1.6)


def canopy_ndwi(ndvi: float, water_stress: float) -> float:
    """Water index, derived from canopy vigour and how dry the crop is.

    NDWI tracks canopy water content, so it moves with NDVI but drops further
    under stress — which is what separates a dry field from a bare one.
    """
    return round(max(-1.0, min(1.0, ndvi * 0.72 - 0.35 * water_stress)), 4)


def canopy_evi(ndvi: float) -> float:
    """Enhanced vegetation index.

    EVI is less prone to saturating in dense canopy than NDVI, so it sits
    slightly lower at the top of the range and tracks it closely elsewhere.
    """
    return round(max(-1.0, min(1.0, 2.5 * ndvi / (ndvi + 2.4))), 4)


def yield_factor(water_ratio: float, gdd_ratio: float, drought_tolerance: str) -> float:
    """Fraction of attainable yield actually achieved, in 0..1.

    Both water and heat units are limiting: a crop that got half the water it
    needed does not reach full yield however warm the season was. The two are
    combined multiplicatively for that reason, and a drought-tolerant variety
    loses less to a water deficit.
    """
    tolerance_relief = {"low": 0.0, "moderate": 0.12, "high": 0.25}[drought_tolerance]

    water_term = min(1.0, water_ratio + tolerance_relief * max(0.0, 1.0 - water_ratio))

    # Saturates once the crop has banked ~90% of the degree-days it needs, and
    # falls away below that as grain fill is cut short. A harsher curve would
    # make any season slightly cooler than nominal read as a total failure.
    heat_term = min(1.0, max(0.0, (gdd_ratio - 0.45) / 0.45))

    return round(min(1.0, max(0.0, water_term * heat_term)), 4)
