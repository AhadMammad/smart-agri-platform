"""Generator configuration.

Volume and date range are parameters rather than constants so the same code
produces a laptop-sized demo and a dataset large enough to exercise partition
pruning. `PROFILES` names the presets; anything can be overridden per run.
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

    n_farms: int = Field(default=5, ge=1, le=500)
    fields_per_farm: int = Field(default=4, ge=1, le=100)
    sensors_per_field: int = Field(default=2, ge=1, le=50)

    start_date: date = Field(default=date(2026, 7, 2))
    end_date: date = Field(default=date(2026, 8, 1))
    reading_interval_minutes: int = Field(default=60, ge=1, le=1440)

    # Real fleets are never fully healthy, and the Silver rules that filter
    # these out are only worth testing if the data actually contains them.
    faulty_sensor_ratio: float = Field(default=0.05, ge=0.0, le=0.5)
    decommissioned_sensor_ratio: float = Field(default=0.025, ge=0.0, le=0.5)
    missing_channel_ratio: float = Field(
        default=0.02, ge=0.0, le=0.5, description="Probability a single channel is null"
    )

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
    def readings_per_sensor(self) -> int:
        span = self.end_date - self.start_date + timedelta(days=1)
        return int(span.total_seconds() // 60 // self.reading_interval_minutes)

    @property
    def estimated_readings(self) -> int:
        """Row count this config will produce. Used to warn before a huge run."""
        return self.n_sensors * self.readings_per_sensor


PROFILES: dict[str, GeneratorConfig] = {
    # Phase 2 default: complete in seconds, big enough for a real chart.
    "small": GeneratorConfig(),
    # A full season across more farms — for exercising partitioning.
    "medium": GeneratorConfig(
        n_farms=20,
        fields_per_farm=6,
        sensors_per_field=3,
        start_date=date(2025, 8, 1),
        end_date=date(2026, 8, 1),
        reading_interval_minutes=60,
    ),
    # Multi-season at 15-minute density. Slow; use deliberately.
    "large": GeneratorConfig(
        n_farms=50,
        fields_per_farm=10,
        sensors_per_field=4,
        start_date=date(2023, 8, 1),
        end_date=date(2026, 8, 1),
        reading_interval_minutes=15,
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
