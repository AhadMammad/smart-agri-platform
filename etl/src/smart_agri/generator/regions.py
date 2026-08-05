"""Real agricultural regions of North and West Africa.

Coordinates are drawn from bounding boxes over genuine farming areas so that
Open-Meteo returns plausible weather for every generated farm in Phase 4. A
random point on the globe would mostly land in ocean or desert and produce
weather that contradicts the soil signal.

Each region also carries a rainfall season, which drives the soil-moisture
model in `signals.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from smart_agri.domain import SoilType


class RainfallPattern(StrEnum):
    """Shape of the annual rainfall curve."""

    MEDITERRANEAN = "mediterranean"
    """Winter rain, dry summer. North African coast."""

    UNIMODAL = "unimodal"
    """One long wet season. Sahelian and Sudanian West Africa."""

    BIMODAL = "bimodal"
    """Two wet seasons. Humid coastal West Africa."""

    IRRIGATED_ARID = "irrigated_arid"
    """Negligible rain; soil moisture is driven almost entirely by irrigation."""


@dataclass(frozen=True, slots=True)
class Region:
    """A farming area with the properties the generator needs."""

    code: str
    name: str
    country_code: str
    country_name: str
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    rainfall: RainfallPattern
    soil_types: tuple[SoilType, ...]
    crops: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.lat_min >= self.lat_max or self.lon_min >= self.lon_max:
            msg = f"region {self.code} has an inverted bounding box"
            raise ValueError(msg)


# --- North Africa ------------------------------------------------------------
NILE_DELTA = Region(
    code="EG-DELTA",
    name="Nile Delta",
    country_code="EG",
    country_name="Egypt",
    lat_min=30.40,
    lat_max=31.40,
    lon_min=30.40,
    lon_max=31.80,
    rainfall=RainfallPattern.IRRIGATED_ARID,
    soil_types=(SoilType.CLAY, SoilType.CLAY_LOAM, SoilType.SILT),
    crops=("wheat", "cotton", "rice", "tomato"),
)

GHARB = Region(
    code="MA-GHARB",
    name="Gharb Plain",
    country_code="MA",
    country_name="Morocco",
    lat_min=34.00,
    lat_max=34.90,
    lon_min=-6.50,
    lon_max=-5.70,
    rainfall=RainfallPattern.MEDITERRANEAN,
    soil_types=(SoilType.LOAMY, SoilType.CLAY_LOAM, SoilType.SILT),
    crops=("durum_wheat", "sugar_beet", "citrus", "olive"),
)

CAP_BON = Region(
    code="TN-CAPBON",
    name="Cap Bon",
    country_code="TN",
    country_name="Tunisia",
    lat_min=36.30,
    lat_max=37.00,
    lon_min=10.30,
    lon_max=11.00,
    rainfall=RainfallPattern.MEDITERRANEAN,
    soil_types=(SoilType.SANDY_LOAM, SoilType.LOAMY, SoilType.CLAY_LOAM),
    crops=("olive", "citrus", "tomato", "durum_wheat"),
)

# --- West Africa -------------------------------------------------------------
KANO_PLAINS = Region(
    code="NG-KANO",
    name="Kano Plains",
    country_code="NG",
    country_name="Nigeria",
    lat_min=11.20,
    lat_max=12.20,
    lon_min=8.10,
    lon_max=9.10,
    rainfall=RainfallPattern.UNIMODAL,
    soil_types=(SoilType.SANDY, SoilType.SANDY_LOAM, SoilType.LOAMY),
    crops=("maize", "sorghum", "groundnut", "cowpea"),
)

ASHANTI = Region(
    code="GH-ASHANTI",
    name="Ashanti Belt",
    country_code="GH",
    country_name="Ghana",
    lat_min=6.30,
    lat_max=7.20,
    lon_min=-2.10,
    lon_max=-1.30,
    rainfall=RainfallPattern.BIMODAL,
    soil_types=(SoilType.LOAMY, SoilType.CLAY_LOAM, SoilType.SANDY_LOAM),
    crops=("cocoa", "maize", "cassava", "oil_palm"),
)

SINE_SALOUM = Region(
    code="SN-SALOUM",
    name="Sine-Saloum",
    country_code="SN",
    country_name="Senegal",
    lat_min=13.80,
    lat_max=14.60,
    lon_min=-16.40,
    lon_max=-15.60,
    rainfall=RainfallPattern.UNIMODAL,
    soil_types=(SoilType.SANDY, SoilType.SANDY_LOAM),
    crops=("groundnut", "millet", "maize", "cowpea"),
)

NORTH_AFRICA: tuple[Region, ...] = (NILE_DELTA, GHARB, CAP_BON)
WEST_AFRICA: tuple[Region, ...] = (KANO_PLAINS, ASHANTI, SINE_SALOUM)
ALL_REGIONS: tuple[Region, ...] = NORTH_AFRICA + WEST_AFRICA

REGIONS_BY_CODE: dict[str, Region] = {region.code: region for region in ALL_REGIONS}
