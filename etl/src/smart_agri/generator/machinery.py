"""The machinery fleet: machines, field operations, telemetry and faults.

Operations are anchored to the planting calendar rather than scattered at
random — a combine works a field on the day it is harvested, a planter on the
day it is sown. Telemetry is then emitted densely *during* those operations and
as a sparse heartbeat when parked, which is both what a real fleet does and
what keeps the row count proportional to work actually performed.

That anchoring is what makes machine utilisation, fuel per hectare and idle
ratio worth charting in Phase 6.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.domain import (
    FaultSeverity,
    Machine,
    MachineFault,
    MachineOperation,
    MachineStatus,
    MachineType,
    OperationType,
)
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence
    from random import Random

    from smart_agri.domain import Farm
    from smart_agri.generator.config import GeneratorConfig
    from smart_agri.generator.operations import PlantingPlan

logger = get_logger(__name__)

_MANUFACTURERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("John Deere", ("6120M", "7R 230", "S780")),
    ("Massey Ferguson", ("MF 5710", "MF 7726", "MF Activa 7345")),
    ("New Holland", ("T6.145", "T7.270", "CX8.80")),
    ("CLAAS", ("Arion 630", "Axion 850", "Lexion 6800")),
    ("Kubota", ("M7-172", "M5-111", "DC-70")),
)

# Fuel burn per hour by machine type, in litres.
_FUEL_L_PER_HOUR: dict[MachineType, float] = {
    MachineType.TRACTOR: 14.0,
    MachineType.COMBINE_HARVESTER: 32.0,
    MachineType.SPRAYER: 11.0,
    MachineType.PLANTER: 13.0,
    MachineType.TILLAGE: 19.0,
    MachineType.IRRIGATION_PUMP: 6.0,
    MachineType.TELEHANDLER: 9.0,
}

# Hectares an operation covers per hour of work.
_HA_PER_HOUR: dict[OperationType, float] = {
    OperationType.TILLAGE: 2.4,
    OperationType.PLANTING: 3.2,
    OperationType.SPRAYING: 8.0,
    OperationType.FERTILISING: 6.5,
    OperationType.HARVESTING: 2.0,
    OperationType.TRANSPORT: 12.0,
    OperationType.MOWING: 4.0,
}

# Which machine type performs which operation.
_MACHINE_FOR_OPERATION: dict[OperationType, MachineType] = {
    OperationType.TILLAGE: MachineType.TILLAGE,
    OperationType.PLANTING: MachineType.PLANTER,
    OperationType.SPRAYING: MachineType.SPRAYER,
    OperationType.FERTILISING: MachineType.SPRAYER,
    OperationType.HARVESTING: MachineType.COMBINE_HARVESTER,
    OperationType.TRANSPORT: MachineType.TRACTOR,
    OperationType.MOWING: MachineType.TRACTOR,
}

_FAULT_CATALOGUE: tuple[tuple[str, FaultSeverity, str], ...] = (
    ("ENG-101", FaultSeverity.WARNING, "Coolant temperature above threshold"),
    ("ENG-204", FaultSeverity.CRITICAL, "Oil pressure loss detected"),
    ("FUE-310", FaultSeverity.WARNING, "Fuel filter restriction"),
    ("HYD-412", FaultSeverity.WARNING, "Hydraulic pressure out of range"),
    ("HYD-455", FaultSeverity.CRITICAL, "Hydraulic pump failure"),
    ("ELE-520", FaultSeverity.INFO, "Battery voltage low at start"),
    ("ELE-533", FaultSeverity.WARNING, "Alternator output unstable"),
    ("TRN-601", FaultSeverity.CRITICAL, "Transmission overheating"),
    ("SEN-702", FaultSeverity.INFO, "GPS signal degraded"),
    ("BRK-810", FaultSeverity.WARNING, "Brake wear indicator triggered"),
)

# Share of the fleet in each non-working state at any moment.
_RETIRED_SHARE = 0.05
_MAINTENANCE_SHARE = 0.15
# No single operation runs longer than a working day. Without a cap a very
# large field produces a twenty-hour shift that overruns into the next
# morning's parked heartbeat.
_MAX_OPERATION_HOURS = 10.0
# A machine needs turnaround between jobs.
_TURNAROUND_MINUTES = 30
# Roughly one telemetry sample in eight is a headland turn or a pause.
_IDLE_SAMPLE_SHARE = 0.12
# Below this the operator refuels before the next job.
_REFUEL_THRESHOLD_PCT = 25.0
# Share of faults still open when the window ends, so a maintenance-due
# signal has something to key off.
_UNRESOLVED_FAULT_SHARE = 0.20

_OPERATOR_NAMES = (
    "A. Mensah",
    "F. Diallo",
    "K. Osei",
    "M. El Amrani",
    "N. Traoré",
    "S. Ben Salah",
    "Y. Adeyemi",
    "H. Ndiaye",
    "T. Boateng",
    "R. Cherif",
)

TELEMETRY_SCHEMA: dict[str, pl.DataType] = {
    "machine_code": pl.String(),
    "operation_key": pl.String(),
    "reading_ts": pl.Datetime(time_unit="us", time_zone="UTC"),
    "engine_hours": pl.Float64(),
    "engine_running": pl.Boolean(),
    "is_idle": pl.Boolean(),
    "fuel_level_pct": pl.Float64(),
    "fuel_rate_l_per_h": pl.Float64(),
    "engine_temp_c": pl.Float64(),
    "engine_rpm": pl.Int32(),
    "speed_kmh": pl.Float64(),
    "latitude": pl.Float64(),
    "longitude": pl.Float64(),
}


@dataclass(slots=True)
class OperationRecord:
    """A machine operation plus the natural key the seeder resolves."""

    key: str
    operation: MachineOperation


class MachineryGenerator:
    """Produces the fleet and everything it does."""

    def __init__(self, config: GeneratorConfig, rng: Random) -> None:
        self._config = config
        self._rng = rng

    # -- fleet ---------------------------------------------------------------
    def machines(self, farms: Sequence[Farm]) -> list[Machine]:
        """A fleet per farm, always including the types the work requires.

        The first machines cover planting, harvesting and spraying, so no
        planting is ever left without a machine that can work it.
        """
        essential = (MachineType.TRACTOR, MachineType.PLANTER, MachineType.COMBINE_HARVESTER)
        optional = (
            MachineType.SPRAYER,
            MachineType.TILLAGE,
            MachineType.IRRIGATION_PUMP,
            MachineType.TELEHANDLER,
        )

        result: list[Machine] = []
        for farm in farms:
            for index in range(self._config.machines_per_farm):
                machine_type = (
                    essential[index] if index < len(essential) else self._rng.choice(optional)
                )
                manufacturer, models = self._rng.choice(_MANUFACTURERS)
                year = self._rng.randint(2012, self._config.start_date.year)

                result.append(
                    Machine(
                        machine_code=f"{farm.farm_code}-M{index + 1:02d}",
                        farm_code=farm.farm_code,
                        machine_type=machine_type,
                        manufacturer=manufacturer,
                        model=self._rng.choice(models),
                        year_built=year,
                        rated_power_hp=self._rated_power(machine_type),
                        purchase_date=self._purchase_date(year),
                        engine_hours_at_purchase=round(self._rng.uniform(0, 2_500), 1),
                        status=self._draw_status(),
                    )
                )
        return result

    def _rated_power(self, machine_type: MachineType) -> int:
        return {
            MachineType.TRACTOR: self._rng.randint(90, 260),
            MachineType.COMBINE_HARVESTER: self._rng.randint(250, 480),
            MachineType.SPRAYER: self._rng.randint(110, 220),
            MachineType.PLANTER: self._rng.randint(80, 180),
            MachineType.TILLAGE: self._rng.randint(120, 300),
            MachineType.IRRIGATION_PUMP: self._rng.randint(20, 90),
            MachineType.TELEHANDLER: self._rng.randint(70, 150),
        }[machine_type]

    def _purchase_date(self, year_built: int) -> date:
        return date(year_built, self._rng.randint(1, 12), self._rng.randint(1, 28))

    def _draw_status(self) -> MachineStatus:
        roll = self._rng.random()
        if roll < _RETIRED_SHARE:
            return MachineStatus.RETIRED
        if roll < _MAINTENANCE_SHARE:
            return MachineStatus.MAINTENANCE
        return MachineStatus.ACTIVE

    # -- operations ----------------------------------------------------------
    def operations(
        self, plans: Sequence[PlantingPlan], machines: Sequence[Machine]
    ) -> list[OperationRecord]:
        """Field work anchored to each planting's own calendar."""
        by_farm: dict[str, list[Machine]] = {}
        for machine in machines:
            if machine.status is not MachineStatus.RETIRED:
                by_farm.setdefault(machine.farm_code, []).append(machine)

        # Every candidate is collected first and assigned in chronological
        # order. Scheduling plan by plan would hand the same machine two fields
        # on the same morning — physically impossible, and it produced duplicate
        # telemetry that violated uq_telemetry_machine_ts.
        candidates: list[tuple[date, str, OperationType, PlantingPlan, str]] = []
        for plan in plans:
            farm_code = plan.planting.field_code.rsplit("-F", 1)[0]
            if farm_code not in by_farm:
                continue

            for operation_type, offset_days in self._schedule(plan):
                work_day = plan.planting.planted_on + timedelta(days=offset_days)
                if not (self._config.start_date <= work_day <= self._config.end_date):
                    continue
                candidates.append((work_day, plan.key, operation_type, plan, farm_code))

        # Sorted on the work day plus stable tiebreaks, so the assignment — and
        # therefore every downstream RNG draw — stays deterministic.
        candidates.sort(key=lambda item: (item[0], item[1], item[2].value))

        free_at: dict[str, datetime] = {}
        records: list[OperationRecord] = []
        for work_day, _key, operation_type, plan, farm_code in candidates:
            machine = self._pick_machine(by_farm[farm_code], operation_type)
            record = self._build_operation(plan, machine, operation_type, work_day, free_at)
            free_at[machine.machine_code] = record.operation.finished_at + timedelta(
                minutes=_TURNAROUND_MINUTES
            )
            records.append(record)

        return records

    def _schedule(self, plan: PlantingPlan) -> list[tuple[OperationType, int]]:
        """Operations relative to the sowing date."""
        cycle = plan.variety.maturity_days
        schedule = [
            (OperationType.TILLAGE, -7),
            (OperationType.PLANTING, 0),
            (OperationType.SPRAYING, int(0.10 * cycle)),
            (OperationType.FERTILISING, int(0.35 * cycle)),
            (OperationType.SPRAYING, int(0.55 * cycle)),
            (OperationType.HARVESTING, cycle),
            (OperationType.TRANSPORT, cycle + 1),
        ]
        if plan.crop.is_perennial:
            # An orchard is mown rather than tilled and sown.
            schedule = [
                (OperationType.MOWING, int(0.20 * cycle)),
                (OperationType.SPRAYING, int(0.40 * cycle)),
                (OperationType.HARVESTING, cycle),
                (OperationType.TRANSPORT, cycle + 1),
            ]
        return schedule

    def _pick_machine(self, fleet: Sequence[Machine], operation_type: OperationType) -> Machine:
        """Prefer the right tool, but fall back to whatever the farm owns."""
        preferred = _MACHINE_FOR_OPERATION[operation_type]
        matching = [m for m in fleet if m.machine_type is preferred]
        return self._rng.choice(matching or list(fleet))

    def _build_operation(
        self,
        plan: PlantingPlan,
        machine: Machine,
        operation_type: OperationType,
        work_day: date,
        free_at: dict[str, datetime] | None = None,
    ) -> OperationRecord:
        rate = _HA_PER_HOUR[operation_type] * self._rng.uniform(0.8, 1.2)
        hours = min(_MAX_OPERATION_HOURS, max(0.5, plan.field_area_ha / rate))

        preferred = datetime.combine(work_day, time(hour=self._rng.randint(6, 10)), tzinfo=UTC)
        busy_until = (free_at or {}).get(machine.machine_code)
        started = max(preferred, busy_until) if busy_until else preferred
        finished = started + timedelta(hours=hours)
        fuel = round(
            _FUEL_L_PER_HOUR[machine.machine_type] * hours * self._rng.uniform(0.85, 1.2), 2
        )

        operation = MachineOperation(
            machine_code=machine.machine_code,
            field_code=plan.planting.field_code,
            planting_key=plan.key,
            operation_type=operation_type,
            started_at=started,
            finished_at=finished,
            area_covered_ha=round(plan.field_area_ha, 2),
            fuel_used_litres=fuel,
            distance_km=round(plan.field_area_ha * self._rng.uniform(1.8, 3.4), 2),
            operator_name=self._rng.choice(_OPERATOR_NAMES),
        )
        key = f"{machine.machine_code}|{started.isoformat()}"
        return OperationRecord(key=key, operation=operation)

    # -- telemetry -----------------------------------------------------------
    def telemetry(
        self,
        machines: Sequence[Machine],
        operations: Sequence[OperationRecord],
        farm_coordinates: dict[str, tuple[float, float]],
    ) -> pl.DataFrame:
        """Dense samples while working, a daily heartbeat while parked.

        Sampling only during real work is what keeps this proportional to the
        fleet's activity instead of to the length of the window — a parked
        machine emitting every fifteen minutes for a year would dominate the
        dataset and tell you nothing.
        """
        interval = timedelta(minutes=self._config.telemetry_interval_minutes)
        by_machine: dict[str, list[OperationRecord]] = {}
        for record in operations:
            by_machine.setdefault(record.operation.machine_code, []).append(record)

        rows: list[dict[str, object]] = []
        for machine in machines:
            if machine.status is MachineStatus.RETIRED:
                continue

            latitude, longitude = farm_coordinates[machine.farm_code]
            engine_hours = machine.engine_hours_at_purchase
            fuel_level = self._rng.uniform(55, 100)

            machine_operations = sorted(
                by_machine.get(machine.machine_code, []),
                key=lambda r: r.operation.started_at,
            )

            for record in machine_operations:
                operation = record.operation
                moment = operation.started_at
                while moment < operation.finished_at:
                    # Roughly one sample in eight is a headland turn or a pause.
                    idle = self._rng.random() < _IDLE_SAMPLE_SHARE
                    burn = _FUEL_L_PER_HOUR[machine.machine_type] * (0.25 if idle else 1.0)
                    step_hours = interval.total_seconds() / 3600
                    engine_hours += step_hours
                    fuel_level = max(8.0, fuel_level - burn * step_hours / 4.0)

                    rows.append(
                        {
                            "machine_code": machine.machine_code,
                            "operation_key": record.key,
                            "reading_ts": moment,
                            "engine_hours": round(engine_hours, 2),
                            "engine_running": True,
                            "is_idle": idle,
                            "fuel_level_pct": round(fuel_level, 2),
                            "fuel_rate_l_per_h": round(burn * self._rng.uniform(0.9, 1.1), 2),
                            "engine_temp_c": round(
                                (72.0 if idle else 88.0) + self._rng.gauss(0, 3.0), 2
                            ),
                            "engine_rpm": int((900 if idle else 1_950) + self._rng.gauss(0, 120)),
                            "speed_kmh": round(0.0 if idle else self._rng.uniform(4.0, 12.0), 2),
                            "latitude": round(latitude + self._rng.gauss(0, 0.004), 6),
                            "longitude": round(longitude + self._rng.gauss(0, 0.004), 6),
                        }
                    )
                    moment += interval

                fuel_level = self._maybe_refuel(fuel_level)

            busy_windows = [
                (r.operation.started_at, r.operation.finished_at) for r in machine_operations
            ]
            rows.extend(
                self._parked_heartbeats(machine, engine_hours, latitude, longitude, busy_windows)
            )

        frame = pl.DataFrame(rows, schema=TELEMETRY_SCHEMA)

        # `uq_telemetry_machine_ts` makes (machine, timestamp) unique, so a
        # collision is an insert failure rather than a wrong number. Scheduling
        # already prevents overlapping operations; this is the invariant made
        # explicit, and it complains rather than quietly dropping rows.
        deduplicated = frame.unique(subset=["machine_code", "reading_ts"], keep="first")
        if deduplicated.height != frame.height:
            logger.warning(
                "telemetry_duplicate_timestamps",
                dropped=frame.height - deduplicated.height,
                detail="a machine emitted two readings for one instant",
            )

        logger.info("telemetry_generated", machines=len(machines), rows=deduplicated.height)
        return deduplicated.sort("machine_code", "reading_ts")

    def _maybe_refuel(self, fuel_level: float) -> float:
        return self._rng.uniform(80, 100) if fuel_level < _REFUEL_THRESHOLD_PCT else fuel_level

    def _parked_heartbeats(
        self,
        machine: Machine,
        engine_hours: float,
        latitude: float,
        longitude: float,
        busy_windows: Sequence[tuple[datetime, datetime]] = (),
    ) -> list[dict[str, object]]:
        """One reading a day while the machine sits in the yard.

        Skipped on days the machine is still working at that hour. A long job
        can run past midnight, and emitting a "parked" reading for a machine
        that is mid-field would both contradict itself and collide with the
        operation's own sample on `uq_telemetry_machine_ts`.
        """
        rows: list[dict[str, object]] = []
        day = self._config.start_date
        while day <= self._config.end_date:
            moment = datetime.combine(day, time(hour=3), tzinfo=UTC)
            if any(start <= moment < finish for start, finish in busy_windows):
                day += timedelta(days=1)
                continue
            rows.append(
                {
                    "machine_code": machine.machine_code,
                    "operation_key": None,
                    "reading_ts": moment,
                    "engine_hours": round(engine_hours, 2),
                    "engine_running": False,
                    "is_idle": False,
                    "fuel_level_pct": round(self._rng.uniform(20, 95), 2),
                    "fuel_rate_l_per_h": 0.0,
                    "engine_temp_c": round(self._rng.uniform(14, 32), 2),
                    "engine_rpm": 0,
                    "speed_kmh": 0.0,
                    "latitude": round(latitude, 6),
                    "longitude": round(longitude, 6),
                }
            )
            day += timedelta(days=1)
        return rows

    # -- faults --------------------------------------------------------------
    def faults(self, machines: Sequence[Machine]) -> list[MachineFault]:
        """Occasional trouble codes, some still open at the end of the window.

        Leaving a few unresolved is deliberate: an open fault is what a
        maintenance-due signal keys off, and a dataset where everything is
        already closed would make that logic untestable.
        """
        window_years = max(0.25, self._config.window_days / 365.0)
        result: list[MachineFault] = []

        for machine in machines:
            if machine.status is MachineStatus.RETIRED:
                continue

            expected = self._config.faults_per_machine_year * window_years
            # Older machines break down more often.
            age_factor = 1.0 + 0.06 * max(0, self._config.start_date.year - machine.year_built)
            count = int(max(0, self._rng.gauss(expected * age_factor, expected * 0.4)))

            for _ in range(count):
                offset = self._rng.randint(0, max(0, self._config.window_days - 1))
                occurred = datetime.combine(
                    self._config.start_date + timedelta(days=offset),
                    time(hour=self._rng.randint(6, 18)),
                    tzinfo=UTC,
                )
                code, severity, description = self._rng.choice(_FAULT_CATALOGUE)

                resolved, downtime, cost = self._resolution(occurred, severity)
                result.append(
                    MachineFault(
                        machine_code=machine.machine_code,
                        occurred_at=occurred,
                        fault_code=code,
                        severity=severity,
                        description=description,
                        resolved_at=resolved,
                        downtime_hours=downtime,
                        repair_cost_usd=cost,
                    )
                )

        return sorted(result, key=lambda f: (f.machine_code, f.occurred_at))

    def _resolution(
        self, occurred: datetime, severity: FaultSeverity
    ) -> tuple[datetime | None, float | None, float | None]:
        # A fifth of faults are still open when the window ends.
        if self._rng.random() < _UNRESOLVED_FAULT_SHARE:
            return None, None, None

        downtime = {
            FaultSeverity.INFO: self._rng.uniform(0.2, 2.0),
            FaultSeverity.WARNING: self._rng.uniform(1.0, 12.0),
            FaultSeverity.CRITICAL: self._rng.uniform(8.0, 72.0),
        }[severity]
        cost = {
            FaultSeverity.INFO: self._rng.uniform(0, 120),
            FaultSeverity.WARNING: self._rng.uniform(80, 900),
            FaultSeverity.CRITICAL: self._rng.uniform(600, 7_500),
        }[severity]

        resolved = occurred + timedelta(hours=downtime)
        end = datetime.combine(self._config.end_date, time(hour=23, minute=59), tzinfo=UTC)
        if resolved > end:
            return None, None, None

        return resolved, round(downtime, 2), round(cost, 2)
