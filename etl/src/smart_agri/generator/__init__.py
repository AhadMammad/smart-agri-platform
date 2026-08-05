"""Synthetic data generator for North and West African farms."""

from __future__ import annotations

from smart_agri.generator.agronomy import (
    CROPS,
    CROPS_BY_CODE,
    VARIETIES,
    VARIETIES_BY_CROP,
    Crop,
    GrowthStage,
    Variety,
)
from smart_agri.generator.config import PROFILES, GeneratorConfig, get_profile
from smart_agri.generator.generator import (
    READING_SCHEMA,
    TELEMETRY_SCHEMA,
    Dataset,
    DatasetGenerator,
)
from smart_agri.generator.operations import PlantingPlan
from smart_agri.generator.regions import (
    ALL_REGIONS,
    NORTH_AFRICA,
    REGIONS_BY_CODE,
    WEST_AFRICA,
    RainfallPattern,
    Region,
)

__all__ = [
    "ALL_REGIONS",
    "CROPS",
    "CROPS_BY_CODE",
    "NORTH_AFRICA",
    "PROFILES",
    "READING_SCHEMA",
    "REGIONS_BY_CODE",
    "TELEMETRY_SCHEMA",
    "VARIETIES",
    "VARIETIES_BY_CROP",
    "WEST_AFRICA",
    "Crop",
    "Dataset",
    "DatasetGenerator",
    "GeneratorConfig",
    "GrowthStage",
    "PlantingPlan",
    "RainfallPattern",
    "Region",
    "Variety",
    "get_profile",
]
