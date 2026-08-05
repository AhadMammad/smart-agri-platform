"""Synthetic data generator for North and West African farms."""

from __future__ import annotations

from smart_agri.generator.config import PROFILES, GeneratorConfig, get_profile
from smart_agri.generator.generator import READING_SCHEMA, DatasetGenerator
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
    "NORTH_AFRICA",
    "PROFILES",
    "READING_SCHEMA",
    "REGIONS_BY_CODE",
    "WEST_AFRICA",
    "DatasetGenerator",
    "GeneratorConfig",
    "RainfallPattern",
    "Region",
    "get_profile",
]
