"""Generator configuration.

Volume and date range are parameters rather than constants so the same code
produces a laptop-sized demo and a dataset large enough to exercise partition
pruning. `PROFILES` names the presets; anything can be overridden per run.

The window spans a full year even in the smallest profile, because a shorter
one would contain no complete crop cycle — and therefore no harvest, no yield
and nothing for the economics dashboard to show.
"""

from __future__ import annotations

from datetime import date, timedelta

from pydantic import BaseModel, ConfigDict, Field, model_validator


class GeneratorConfig(BaseModel):
    """Everything that determines what the generator produces.

    Two runs with the same config and seed produce byte-identical output, which
    is what makes the generator's own tests meaningful.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    seed: int = Field(default=20260731, description="Deterministic RNG seed")

    # --- estate size ---
    n_farms: int = Field(default=5, ge=1, le=500)
    fields_per_farm: int = Field(default=4, ge=1, le=100)
    sensors_per_field: int = Field(default=2, ge=1, le=50)
    machines_per_farm: int = Field(default=3, ge=0, le=50)

    # --- window ---
    start_date: date = Field(default=date(2025, 8, 1))
    end_date: date = Field(default=date(2026, 8, 1))
    reading_interval_minutes: int = Field(default=120, ge=1, le=1440)

    # --- machinery ---
    telemetry_interval_minutes: int = Field(
        default=15,
        ge=1,
        le=1440,
        description="Sampling rate while a machine is working; parked machines emit a daily beat",
    )
    faults_per_machine_year: float = Field(default=4.0, ge=0.0, le=100.0)

    # --- imagery ---
    imagery_interval_days: int = Field(
        default=5, ge=1, le=90, description="Satellite revisit period"
    )
    drone_flights_per_season: int = Field(default=3, ge=0, le=50)
    cloudy_observation_ratio: float = Field(
        default=0.18,
        ge=0.0,
        le=1.0,
        description="Share of satellite passes obscured enough to yield no index",
    )

    # --- fleet health ---
    # Real fleets are never fully healthy, and the Silver rules that filter
    # these out are only worth testing if the data actually contains them.
    faulty_sensor_ratio: float = Field(default=0.05, ge=0.0, le=0.5)
    decommissioned_sensor_ratio: float = Field(default=0.025, ge=0.0, le=0.5)
    missing_channel_ratio: float = Field(
        default=0.02, ge=0.0, le=0.5, description="Probability a single channel is null"
    )

    # --- agronomy ---
    crop_failure_ratio: float = Field(
        default=0.04,
        ge=0.0,
        le=0.5,
        description="Share of plantings that fail outright, so yield analysis has a tail",
    )

    # How much of the irrigation deficit a farm actually manages to deliver.
    # Without a spread here, irrigation would exactly cover what rain does not,
    # every planting would receive precisely its water demand, and water would
    # never limit yield — erasing the very relationship the water-use dashboard
    # exists to show. Real farms fall short: allocations are capped, boreholes
    # run slow, pumps fail, and diesel costs money.
    irrigation_adequacy_min: float = Field(default=0.55, ge=0.0, le=2.0)
    irrigation_adequacy_max: float = Field(default=1.05, ge=0.0, le=2.0)

    @model_validator(mode="after")
    def _check_adequacy_band(self) -> GeneratorConfig:
        if self.irrigation_adequacy_min > self.irrigation_adequacy_max:
            msg = "irrigation_adequacy_min must not exceed irrigation_adequacy_max"
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _check_dates(self) -> GeneratorConfig:
        if self.end_date < self.start_date:
            msg = "end_date must not precede start_date"
            raise ValueError(msg)
        return self

    @property
    def n_sensors(self) -> int:
        return self.n_farms * self.fields_per_farm * self.sensors_per_field

    @property
    def n_fields(self) -> int:
        return self.n_farms * self.fields_per_farm

    @property
    def n_machines(self) -> int:
        return self.n_farms * self.machines_per_farm

    @property
    def window_days(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def readings_per_sensor(self) -> int:
        span = self.end_date - self.start_date + timedelta(days=1)
        return int(span.total_seconds() // 60 // self.reading_interval_minutes)

    @property
    def estimated_readings(self) -> int:
        """Row count the sensor table will receive. Used to warn before a huge run."""
        return self.n_sensors * self.readings_per_sensor


PROFILES: dict[str, GeneratorConfig] = {
    # A full year, complete in well under a minute. ~175k sensor readings.
    "small": GeneratorConfig(),
    # A year at finer resolution across a larger estate. ~3M sensor readings;
    # enough to make ClickHouse partitioning matter.
    "medium": GeneratorConfig(
        n_farms=20,
        fields_per_farm=6,
        sensors_per_field=3,
        machines_per_farm=4,
        start_date=date(2025, 8, 1),
        end_date=date(2026, 8, 1),
        reading_interval_minutes=60,
    ),
    # Three seasons, for year-over-year comparison. ~16M sensor readings and
    # several minutes of generation — use deliberately.
    "large": GeneratorConfig(
        n_farms=50,
        fields_per_farm=6,
        sensors_per_field=2,
        machines_per_farm=5,
        start_date=date(2023, 8, 1),
        end_date=date(2026, 8, 1),
        reading_interval_minutes=60,
    ),
}


def get_profile(name: str) -> GeneratorConfig:
    """Look up a named profile, failing with the valid options listed."""
    try:
        return PROFILES[name]
    except KeyError:
        valid = ", ".join(sorted(PROFILES))
        msg = f"unknown generator profile {name!r}; valid profiles: {valid}"
        raise KeyError(msg) from None
