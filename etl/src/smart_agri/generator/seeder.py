"""Loads a generated dataset into Postgres.

Kept separate from the generator so the generator stays pure and testable
without a database, and so the same dataset could be written somewhere else
without touching generation logic.

Surrogate keys are assigned by Postgres, so each level is inserted and then read
back to resolve natural keys to generated ids before the next level is written.
The generator refers to everything by natural key precisely so this layer is the
only place that has to know about surrogate ids at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from smart_agri.generator.generator import DatasetGenerator
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from smart_agri.generator.config import GeneratorConfig
    from smart_agri.generator.generator import Dataset
    from smart_agri.io import PostgresSource

logger = get_logger(__name__)

# Child-first, so foreign keys never block the truncate. `crop`, `region` and
# `soil_type` are absent deliberately: those are Liquibase-managed reference
# data, and farm.region carries a foreign key onto one of them.
TABLES_IN_DEPENDENCY_ORDER: tuple[str, ...] = (
    "agri.sensor_reading",
    "agri.machine_telemetry",
    "agri.machine_fault",
    "agri.machine_operation",
    "agri.field_index_observation",
    "agri.field_cost",
    "agri.harvest",
    "agri.input_application",
    "agri.irrigation_event",
    "agri.planting",
    "agri.machine",
    "agri.sensor",
    "agri.field",
    "agri.farm",
    "agri.crop_variety",
)


@dataclass(frozen=True, slots=True)
class SeedResult:
    """Rows written per table."""

    counts: dict[str, int]

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    def summary(self) -> str:
        widest = max(len(name) for name in self.counts)
        lines = [f"  {name:<{widest}}  {count:>12,}" for name, count in self.counts.items()]
        lines.append(f"  {'total':<{widest}}  {self.total:>12,}")
        return "\n".join(lines)


class DatasetSeeder:
    """Generates a dataset and writes it to the source database."""

    def __init__(self, postgres: PostgresSource, config: GeneratorConfig | None = None) -> None:
        self._postgres = postgres
        self._generator = DatasetGenerator(config)
        self.config = self._generator.config

    def seed(self, *, truncate: bool = True) -> SeedResult:
        """Populate the source database.

        Args:
            truncate: Empty the tables first. Default because re-seeding on top
                of existing data would violate the natural-key uniqueness
                constraints rather than fail cleanly.
        """
        if truncate:
            self._postgres.truncate(TABLES_IN_DEPENDENCY_ORDER)

        dataset = self._generator.build()
        counts: dict[str, int] = {}

        # --- reference and dimensions ---
        counts["crop_variety"] = self._insert_varieties(dataset)
        variety_ids = self._id_map("agri.crop_variety", "variety_code", "variety_id")

        counts["farm"] = self._insert_farms(dataset)
        farm_ids = self._id_map("agri.farm", "farm_code", "farm_id")

        counts["field"] = self._insert_fields(dataset, farm_ids)
        field_ids = self._id_map("agri.field", "field_code", "field_id")

        counts["sensor"] = self._insert_sensors(dataset, field_ids)
        sensor_ids = self._id_map("agri.sensor", "sensor_code", "sensor_id")

        # --- operations ---
        counts["planting"] = self._insert_plantings(dataset, field_ids, variety_ids)
        planting_ids = self._planting_id_map(field_ids)

        counts["irrigation_event"] = self._insert_irrigation(dataset, field_ids, planting_ids)
        counts["input_application"] = self._insert_inputs(dataset, field_ids, planting_ids)
        counts["harvest"] = self._insert_harvests(dataset, field_ids, planting_ids)
        counts["field_cost"] = self._insert_costs(dataset, field_ids, planting_ids)

        # --- machinery ---
        counts["machine"] = self._insert_machines(dataset, farm_ids)
        machine_ids = self._id_map("agri.machine", "machine_code", "machine_id")

        counts["machine_operation"] = self._insert_operations(
            dataset, machine_ids, field_ids, planting_ids
        )
        operation_ids = self._operation_id_map(machine_ids)

        counts["machine_telemetry"] = self._insert_telemetry(dataset, machine_ids, operation_ids)
        counts["machine_fault"] = self._insert_faults(dataset, machine_ids)

        # --- imagery and telemetry ---
        counts["field_index_observation"] = self._insert_observations(
            dataset, field_ids, planting_ids
        )
        counts["sensor_reading"] = self._insert_readings(dataset, sensor_ids)

        result = SeedResult(counts=counts)
        logger.info("seed_complete", total=result.total, **counts)
        return result

    # -- reference and dimensions -------------------------------------------
    def _insert_varieties(self, dataset: Dataset) -> int:
        frame = pl.DataFrame(
            [
                {
                    "variety_code": v.variety_code,
                    "crop_code": v.crop_code,
                    "name": v.name,
                    "maturity_days": v.maturity_days,
                    "yield_potential_t_ha": v.yield_potential_t_ha,
                    "drought_tolerance": v.drought_tolerance,
                }
                for v in dataset.varieties
            ]
        )
        return self._copy(frame, "agri.crop_variety")

    def _insert_farms(self, dataset: Dataset) -> int:
        frame = pl.DataFrame(
            [
                {
                    "farm_code": f.farm_code,
                    "name": f.name,
                    "country_code": f.country_code,
                    "region": f.region,
                    "latitude": float(f.latitude),
                    "longitude": float(f.longitude),
                    "area_ha": float(f.area_ha),
                }
                for f in dataset.farms
            ]
        )
        return self._copy(frame, "agri.farm")

    def _insert_fields(self, dataset: Dataset, farm_ids: dict[str, int]) -> int:
        frame = pl.DataFrame(
            [
                {
                    "field_code": f.field_code,
                    "farm_id": farm_ids[f.farm_code],
                    "name": f.name,
                    "area_ha": float(f.area_ha),
                    "soil_type": f.soil_type.value,
                }
                for f in dataset.fields
            ]
        )
        return self._copy(frame, "agri.field")

    def _insert_sensors(self, dataset: Dataset, field_ids: dict[str, int]) -> int:
        frame = pl.DataFrame(
            [
                {
                    "sensor_code": s.sensor_code,
                    "field_id": field_ids[s.field_code],
                    "sensor_type": s.sensor_type,
                    "model": s.model,
                    "depth_cm": s.depth_cm,
                    "installed_on": s.installed_on.isoformat(),
                    "status": s.status.value,
                }
                for s in dataset.sensors
            ]
        )
        return self._copy(frame, "agri.sensor")

    # -- operations ----------------------------------------------------------
    def _insert_plantings(
        self, dataset: Dataset, field_ids: dict[str, int], variety_ids: dict[str, int]
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "field_id": field_ids[p.planting.field_code],
                    "variety_id": variety_ids[p.planting.variety_code],
                    "season": p.planting.season,
                    "planted_on": p.planting.planted_on.isoformat(),
                    "expected_harvest_on": p.planting.expected_harvest_on.isoformat(),
                    "area_ha": p.planting.area_ha,
                    "seed_rate_kg_ha": p.planting.seed_rate_kg_ha,
                    "status": p.planting.status.value,
                }
                for p in dataset.plans
            ]
        )
        return self._copy(frame, "agri.planting")

    def _insert_irrigation(
        self, dataset: Dataset, field_ids: dict[str, int], planting_ids: dict[str, int]
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "field_id": field_ids[e.field_code],
                    "planting_id": planting_ids.get(e.planting_key or ""),
                    "event_ts": e.event_ts,
                    "method": e.method.value,
                    "water_volume_m3": e.water_volume_m3,
                    "depth_mm": e.depth_mm,
                    "duration_minutes": e.duration_minutes,
                    "energy_kwh": e.energy_kwh,
                    "water_source": e.water_source.value,
                }
                for e in dataset.irrigation
            ]
        )
        return self._copy(frame, "agri.irrigation_event")

    def _insert_inputs(
        self, dataset: Dataset, field_ids: dict[str, int], planting_ids: dict[str, int]
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "field_id": field_ids[a.field_code],
                    "planting_id": planting_ids.get(a.planting_key or ""),
                    "applied_on": a.applied_on.isoformat(),
                    "input_type": a.input_type.value,
                    "product_name": a.product_name,
                    "rate_kg_ha": a.rate_kg_ha,
                    "quantity_kg": a.quantity_kg,
                    "unit_cost_usd_per_kg": a.unit_cost_usd_per_kg,
                    "total_cost_usd": a.total_cost_usd,
                    "application_method": a.application_method,
                }
                for a in dataset.inputs
            ]
        )
        return self._copy(frame, "agri.input_application")

    def _insert_harvests(
        self, dataset: Dataset, field_ids: dict[str, int], planting_ids: dict[str, int]
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "planting_id": planting_ids[h.planting_key],
                    "field_id": field_ids[h.field_code],
                    "harvested_on": h.harvested_on.isoformat(),
                    "area_harvested_ha": h.area_harvested_ha,
                    "yield_tonnes": h.yield_tonnes,
                    "yield_t_ha": h.yield_t_ha,
                    "moisture_pct": h.moisture_pct,
                    "quality_grade": h.quality_grade.value,
                    "price_usd_per_tonne": h.price_usd_per_tonne,
                    "revenue_usd": h.revenue_usd,
                }
                for h in dataset.harvests
            ]
        )
        return self._copy(frame, "agri.harvest")

    def _insert_costs(
        self, dataset: Dataset, field_ids: dict[str, int], planting_ids: dict[str, int]
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "field_id": field_ids[c.field_code],
                    "planting_id": planting_ids.get(c.planting_key or ""),
                    "cost_date": c.cost_date.isoformat(),
                    "cost_category": c.cost_category.value,
                    "amount_usd": c.amount_usd,
                    "description": c.description,
                }
                for c in dataset.costs
            ]
        )
        return self._copy(frame, "agri.field_cost")

    # -- machinery -----------------------------------------------------------
    def _insert_machines(self, dataset: Dataset, farm_ids: dict[str, int]) -> int:
        frame = pl.DataFrame(
            [
                {
                    "machine_code": m.machine_code,
                    "farm_id": farm_ids[m.farm_code],
                    "machine_type": m.machine_type.value,
                    "manufacturer": m.manufacturer,
                    "model": m.model,
                    "year_built": m.year_built,
                    "rated_power_hp": m.rated_power_hp,
                    "purchase_date": m.purchase_date.isoformat(),
                    "engine_hours_at_purchase": m.engine_hours_at_purchase,
                    "status": m.status.value,
                }
                for m in dataset.machines
            ]
        )
        return self._copy(frame, "agri.machine")

    def _insert_operations(
        self,
        dataset: Dataset,
        machine_ids: dict[str, int],
        field_ids: dict[str, int],
        planting_ids: dict[str, int],
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "machine_id": machine_ids[r.operation.machine_code],
                    "field_id": field_ids[r.operation.field_code],
                    "planting_id": planting_ids.get(r.operation.planting_key or ""),
                    "operation_type": r.operation.operation_type.value,
                    "started_at": r.operation.started_at,
                    "finished_at": r.operation.finished_at,
                    "area_covered_ha": r.operation.area_covered_ha,
                    "fuel_used_litres": r.operation.fuel_used_litres,
                    "distance_km": r.operation.distance_km,
                    "operator_name": r.operation.operator_name,
                }
                for r in dataset.operations
            ]
        )
        return self._copy(frame, "agri.machine_operation")

    def _insert_telemetry(
        self, dataset: Dataset, machine_ids: dict[str, int], operation_ids: dict[str, int]
    ) -> int:
        if dataset.telemetry.is_empty():
            return 0

        resolved = dataset.telemetry.with_columns(
            pl.col("machine_code")
            .replace_strict(machine_ids, return_dtype=pl.Int64)
            .alias("machine_id"),
            pl.col("operation_key")
            .replace_strict(operation_ids, default=None, return_dtype=pl.Int64)
            .alias("operation_id"),
        )
        # Select explicitly: `machine_code` and `operation_key` are the natural
        # keys used for the lookup above and have no column in the table, so
        # copying the whole frame fails with "column does not exist".
        return self._copy(
            resolved.select(
                "machine_id",
                "operation_id",
                "reading_ts",
                "engine_hours",
                "engine_running",
                "is_idle",
                "fuel_level_pct",
                "fuel_rate_l_per_h",
                "engine_temp_c",
                "engine_rpm",
                "speed_kmh",
                "latitude",
                "longitude",
            ),
            "agri.machine_telemetry",
        )

    def _insert_faults(self, dataset: Dataset, machine_ids: dict[str, int]) -> int:
        frame = pl.DataFrame(
            [
                {
                    "machine_id": machine_ids[f.machine_code],
                    "occurred_at": f.occurred_at,
                    "fault_code": f.fault_code,
                    "severity": f.severity.value,
                    "description": f.description,
                    "resolved_at": f.resolved_at,
                    "downtime_hours": f.downtime_hours,
                    "repair_cost_usd": f.repair_cost_usd,
                }
                for f in dataset.faults
            ]
        )
        return self._copy(frame, "agri.machine_fault")

    # -- imagery and telemetry -----------------------------------------------
    def _insert_observations(
        self, dataset: Dataset, field_ids: dict[str, int], planting_ids: dict[str, int]
    ) -> int:
        frame = pl.DataFrame(
            [
                {
                    "field_id": field_ids[o.field_code],
                    "planting_id": planting_ids.get(o.planting_key or ""),
                    "observed_on": o.observed_on.isoformat(),
                    "source": o.source.value,
                    "platform": o.platform,
                    "ndvi": o.ndvi,
                    "ndwi": o.ndwi,
                    "evi": o.evi,
                    "cloud_cover_pct": o.cloud_cover_pct,
                    "resolution_m": o.resolution_m,
                    "usable": o.usable,
                }
                for o in dataset.observations
            ]
        )
        return self._copy(frame, "agri.field_index_observation")

    def _insert_readings(self, dataset: Dataset, sensor_ids: dict[str, int]) -> int:
        if dataset.readings.is_empty():
            return 0

        resolved = dataset.readings.with_columns(
            pl.col("sensor_code")
            .replace_strict(sensor_ids, return_dtype=pl.Int64)
            .alias("sensor_id")
        )
        return self._copy(
            resolved.select(
                "sensor_id",
                "reading_ts",
                "soil_moisture_pct",
                "soil_temperature_c",
                "soil_ph",
                "soil_ec_ds_m",
                "battery_pct",
            ),
            "agri.sensor_reading",
        )

    # -- helpers -------------------------------------------------------------
    def _copy(self, frame: pl.DataFrame, table: str) -> int:
        """Bulk-insert every column of the frame, in its declared order."""
        if frame.is_empty():
            return 0
        return self._postgres.copy_frame(frame, table, frame.columns)

    def _id_map(self, table: str, code_column: str, id_column: str) -> dict[str, int]:
        """Read back the surrogate keys Postgres assigned, keyed by natural key."""
        frame = self._postgres.read_query(f"SELECT {code_column}, {id_column} FROM {table}")
        if frame.is_empty():
            return {}
        return dict(zip(frame[code_column].to_list(), frame[id_column].to_list(), strict=True))

    def _planting_id_map(self, field_ids: dict[str, int]) -> dict[str, int]:
        """Map the generator's planting key to the surrogate planting_id.

        `agri.planting` has no natural-key column, so the key is reassembled
        from the columns that do identify a cycle: its field, season and sowing
        date.
        """
        frame = self._postgres.read_query(
            "SELECT planting_id, field_id, season, planted_on FROM agri.planting"
        )
        if frame.is_empty():
            return {}

        field_code_by_id = {value: key for key, value in field_ids.items()}
        return {
            "{}|{}|{}".format(
                field_code_by_id[row["field_id"]], row["season"], row["planted_on"]
            ): row["planting_id"]
            for row in frame.iter_rows(named=True)
        }

    def _operation_id_map(self, machine_ids: dict[str, int]) -> dict[str, int]:
        """Map the generator's operation key to the surrogate operation_id."""
        frame = self._postgres.read_query(
            "SELECT operation_id, machine_id, started_at FROM agri.machine_operation"
        )
        if frame.is_empty():
            return {}

        machine_code_by_id = {value: key for key, value in machine_ids.items()}
        return {
            self._operation_key(machine_code_by_id[row["machine_id"]], row["started_at"]): row[
                "operation_id"
            ]
            for row in frame.iter_rows(named=True)
        }

    @staticmethod
    def _operation_key(machine_code: str, started_at: Any) -> str:
        """Rebuild the key the machinery generator used.

        Postgres returns a timezone-aware datetime; the generator built the key
        from `datetime.isoformat()`, so the two agree as long as both are UTC.
        """
        return f"{machine_code}|{started_at.isoformat()}"


__all__ = ["TABLES_IN_DEPENDENCY_ORDER", "DatasetSeeder", "SeedResult"]


def table_names() -> Sequence[str]:
    """Every table the seeder writes, in dependency order (children first)."""
    return TABLES_IN_DEPENDENCY_ORDER
