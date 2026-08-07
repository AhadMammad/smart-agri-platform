"""Tests for the Phase 6 Gold marts.

Silver frames are written straight into the fake lake rather than produced by
running Bronze and Silver first. The marts are what is under test here, and a
fixture that spelled out six domains of source tables would obscure which column
each assertion actually depends on.

Every classification threshold in `gold_marts` is pinned here. A mart that
silently reclassifies a field from `optimal` to `dry` is worse than one that
crashes, because it looks like a change in the weather.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl
import pytest
from tests.fakes import FakeClickHouseSink, FakeControlStore, FakeHdfsStore

from smart_agri.config import LakeZone
from smart_agri.pipelines import get_pipeline
from smart_agri.pipelines.base import PipelineContext
from smart_agri.pipelines.bronze import DATA_FILE, INGEST_PARTITION_KEY, SNAPSHOT_PARTITION_KEY
from smart_agri.pipelines.gold import RUN_PARTITION_KEY
from smart_agri.pipelines.gold_marts import (
    DEFICIT_MM,
    HIGH_VIGOUR_PCT,
    IDLE_HEAVY_PCT,
    LOW_VIGOUR_PCT,
)

if TYPE_CHECKING:
    from collections.abc import Callable

LOGICAL_DATE = date(2026, 8, 1)
PARTITION = LOGICAL_DATE.isoformat()

#: Maize: a 100-day cycle makes days-after-sowing and cycle-progress-percent the
#: same number, so every canopy-stage assertion below reads directly.
MAIZE_CYCLE_DAYS = 100
MAIZE_PEAK_NDVI = 0.80


# --- lake fixtures -----------------------------------------------------------


@pytest.fixture
def hdfs() -> FakeHdfsStore:
    return FakeHdfsStore()


@pytest.fixture
def context(hdfs: FakeHdfsStore) -> PipelineContext:
    return PipelineContext(
        hdfs=hdfs,  # type: ignore[arg-type]
        control=FakeControlStore(),  # type: ignore[arg-type]
        clickhouse=FakeClickHouseSink(),  # type: ignore[arg-type]
    )


@pytest.fixture
def put_dim(hdfs: FakeHdfsStore) -> Callable[[str, pl.DataFrame], None]:
    """Write a Silver dimension into this run's snapshot partition."""

    def write(dataset: str, frame: pl.DataFrame) -> None:
        hdfs.files[
            hdfs.zone_path(
                LakeZone.SILVER, dataset, f"{SNAPSHOT_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ] = frame

    return write


@pytest.fixture
def put_fact(hdfs: FakeHdfsStore) -> Callable[[str, pl.DataFrame], None]:
    """Write a Silver fact into an ingest partition, as incremental Silver does."""

    def write(dataset: str, frame: pl.DataFrame) -> None:
        hdfs.files[
            hdfs.zone_path(
                LakeZone.SILVER, dataset, f"{INGEST_PARTITION_KEY}={PARTITION}", DATA_FILE
            )
        ] = frame

    return write


@pytest.fixture
def put_gold(hdfs: FakeHdfsStore) -> Callable[[str, pl.DataFrame], None]:
    def write(dataset: str, frame: pl.DataFrame) -> None:
        hdfs.files[
            hdfs.zone_path(LakeZone.GOLD, dataset, f"{RUN_PARTITION_KEY}={PARTITION}", DATA_FILE)
        ] = frame

    return write


@pytest.fixture
def read_gold(hdfs: FakeHdfsStore) -> Callable[[str], pl.DataFrame]:
    def read(dataset: str) -> pl.DataFrame:
        return hdfs.files[
            hdfs.zone_path(LakeZone.GOLD, dataset, f"{RUN_PARTITION_KEY}={PARTITION}", DATA_FILE)
        ]

    return read


# --- shared dimensions -------------------------------------------------------


def _farms() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "farm_id": [1],
            "farm_code": ["EG-001"],
            "farm_name": ["Nile Delta Farm"],
            "region": ["Nile Delta"],
            "country_code": ["EG"],
        }
    )


def _fields() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "field_id": [10, 11],
            "field_code": ["EG-001-F01", "EG-001-F02"],
            "field_name": ["North", "South"],
            "farm_id": [1, 1],
            "area_ha": [12.0, 20.0],
            "soil_type": ["clay", "loam"],
        }
    )


def _plantings() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "planting_id": [100, 101],
            "field_id": [10, 11],
            "farm_id": [1, 1],
            "variety_id": [1000, 1000],
            "season": ["2026-summer", "2026-summer"],
            "planted_on": [date(2026, 3, 1), date(2026, 3, 1)],
            "expected_harvest_on": [date(2026, 6, 9), date(2026, 6, 9)],
            "area_ha": [12.0, 20.0],
            "status": ["harvested", "growing"],
        }
    )


def _varieties() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "variety_id": [1000],
            "variety_code": ["MZ-01"],
            "variety_name": ["Giza 168"],
            "crop_code": ["MAIZE"],
        }
    )


def _crops() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "crop_code": ["MAIZE"],
            "crop_name": ["Maize"],
            "category": ["cereal"],
            "cycle_days": pl.Series([MAIZE_CYCLE_DAYS], dtype=pl.Int32),
            "peak_ndvi": [MAIZE_PEAK_NDVI],
        }
    )


# --- crop health -------------------------------------------------------------


class TestGoldFieldCropHealthDaily:
    @pytest.fixture
    def observations(self) -> pl.DataFrame:
        """One field, four passes across the cycle, plus a cloudy one."""
        return pl.DataFrame(
            {
                "observation_id": [1, 2, 3, 4, 5],
                "field_id": [10, 10, 10, 10, 10],
                "farm_id": [1, 1, 1, 1, 1],
                "planting_id": [100, 100, 100, 100, 100],
                # 10, 50, 60 and 90 days after sowing — bare, peak, peak,
                # senescing on a 100-day cycle.
                "observed_on": [
                    date(2026, 3, 11),
                    date(2026, 4, 20),
                    date(2026, 4, 30),
                    date(2026, 5, 30),
                    date(2026, 4, 20),
                ],
                "ndvi": [0.15, 0.78, 0.40, 0.45, 0.02],
                "ndwi": [0.10, 0.30, 0.20, 0.15, 0.01],
                "evi": [0.12, 0.60, 0.35, 0.30, 0.01],
                "cloud_cover_pct": [5.0, 5.0, 5.0, 5.0, 95.0],
                # The last pass was lost to cloud and must not reach an average.
                "usable": [True, True, True, True, False],
            }
        )

    @pytest.fixture
    def health(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
        observations: pl.DataFrame,
    ) -> pl.DataFrame:
        put_dim("dim_field", _fields())
        put_dim("dim_farm", _farms())
        put_dim("dim_planting", _plantings())
        put_dim("dim_crop_variety", _varieties())
        put_dim("dim_crop", _crops())
        put_fact("fact_field_index", observations)

        get_pipeline("gold.field_crop_health_daily", context).run(LOGICAL_DATE)
        return read_gold("field_crop_health_daily")

    def test_a_cloudy_pass_is_excluded_from_the_average(self, health: pl.DataFrame) -> None:
        """The failure this prevents: an overcast day dragging every field's
        NDVI toward zero and reading as sudden crop stress."""
        day = health.filter(pl.col("observed_on") == date(2026, 4, 20)).to_dicts()[0]
        assert day["observation_count"] == 1
        assert day["avg_ndvi"] == pytest.approx(0.78)

    def test_days_after_sowing_is_measured_from_the_planting(self, health: pl.DataFrame) -> None:
        row = health.filter(pl.col("observed_on") == date(2026, 3, 11)).to_dicts()[0]
        assert row["days_after_sowing"] == 10
        assert row["cycle_progress_pct"] == pytest.approx(10.0)

    @pytest.mark.parametrize(
        ("observed_on", "expected"),
        [
            (date(2026, 3, 11), "bare"),  # 10% through
            (date(2026, 4, 20), "peak"),  # 50%
            (date(2026, 4, 30), "peak"),  # 60%
            (date(2026, 5, 30), "senescing"),  # 90%
        ],
    )
    def test_canopy_stage_follows_cycle_progress(
        self, health: pl.DataFrame, observed_on: date, expected: str
    ) -> None:
        row = health.filter(pl.col("observed_on") == observed_on).to_dicts()[0]
        assert row["canopy_stage"] == expected

    def test_vigour_is_judged_against_the_crops_own_peak(self, health: pl.DataFrame) -> None:
        """0.78 against a peak of 0.80 is 97.5% — high. 0.40 is 50% — low."""
        strong = health.filter(pl.col("observed_on") == date(2026, 4, 20)).to_dicts()[0]
        weak = health.filter(pl.col("observed_on") == date(2026, 4, 30)).to_dicts()[0]

        assert strong["ndvi_vs_expected_pct"] == pytest.approx(97.5)
        assert strong["ndvi_vs_expected_pct"] >= HIGH_VIGOUR_PCT
        assert strong["vigour_flag"] == "high"

        assert weak["ndvi_vs_expected_pct"] == pytest.approx(50.0)
        assert weak["ndvi_vs_expected_pct"] < LOW_VIGOUR_PCT
        assert weak["vigour_flag"] == "low"

    def test_a_bare_field_has_no_vigour_verdict(self, health: pl.DataFrame) -> None:
        """Before there is a canopy, a low NDVI is the expected state. Flagging
        it as poor vigour would fill the dashboard with alarms every sowing."""
        row = health.filter(pl.col("observed_on") == date(2026, 3, 11)).to_dicts()[0]
        assert row["canopy_stage"] == "bare"
        assert row["vigour_flag"] == "unknown"

    def test_an_observation_with_no_planting_is_bare_ground_not_a_failure(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
        observations: pl.DataFrame,
    ) -> None:
        put_dim("dim_field", _fields())
        put_dim("dim_farm", _farms())
        put_dim("dim_planting", _plantings())
        put_dim("dim_crop_variety", _varieties())
        put_dim("dim_crop", _crops())
        put_fact(
            "fact_field_index",
            observations.with_columns(pl.lit(None, dtype=pl.Int64).alias("planting_id")),
        )

        get_pipeline("gold.field_crop_health_daily", context).run(LOGICAL_DATE)
        gold = read_gold("field_crop_health_daily")

        assert gold.height == 4
        assert gold["crop_code"].null_count() == 4
        assert set(gold["canopy_stage"].to_list()) == {"unknown"}
        assert set(gold["vigour_flag"].to_list()) == {"unknown"}


# --- irrigation --------------------------------------------------------------


def _weather(dates: list[date], rain: list[float], et0: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "weather_date": dates,
            "field_id": [10] * len(dates),
            "field_code": ["EG-001-F01"] * len(dates),
            "farm_id": [1] * len(dates),
            "farm_code": ["EG-001"] * len(dates),
            "region": ["Nile Delta"] * len(dates),
            "country_code": ["EG"] * len(dates),
            "field_area_ha": [12.0] * len(dates),
            "is_actual": [True] * len(dates),
            "precipitation_mm": rain,
            "et0_mm": et0,
            "gdd_base10": [10.0] * len(dates),
        }
    )


class TestGoldFieldIrrigationDaily:
    @pytest.fixture
    def irrigation(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        put_gold: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
    ) -> pl.DataFrame:
        days = [date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3)]
        put_dim("dim_field", _fields())
        put_gold(
            "field_weather_daily",
            _weather(days, rain=[0.0, 20.0, 0.0], et0=[6.0, 5.0, 6.0]),
        )
        # Only the first day was irrigated. The other two must still produce
        # rows — the dry unirrigated day is the one that matters.
        put_fact(
            "fact_irrigation",
            pl.DataFrame(
                {
                    "irrigation_id": [1, 2],
                    "field_id": [10, 10],
                    "planting_id": [100, 100],
                    "event_date": [date(2026, 5, 1), date(2026, 5, 1)],
                    "depth_mm": [8.0, 4.0],
                    "water_volume_m3": [960.0, 480.0],
                    "duration_minutes": pl.Series([60, 30], dtype=pl.Int32),
                    "energy_kwh": [30.0, 15.0],
                    "method": ["drip", "sprinkler"],
                }
            ),
        )

        get_pipeline("gold.field_irrigation_daily", context).run(LOGICAL_DATE)
        return read_gold("field_irrigation_daily")

    def test_every_weather_day_produces_a_row(self, irrigation: pl.DataFrame) -> None:
        """The spine is the weather, not the events. Aggregating events alone
        would drop exactly the dry days an irrigation dashboard exists for."""
        assert irrigation.height == 3
        assert irrigation["water_date"].to_list() == [
            date(2026, 5, 1),
            date(2026, 5, 2),
            date(2026, 5, 3),
        ]

    def test_a_day_with_no_irrigation_records_zero_not_null(self, irrigation: pl.DataFrame) -> None:
        row = irrigation.filter(pl.col("water_date") == date(2026, 5, 3)).to_dicts()[0]
        assert row["irrigation_events"] == 0
        assert row["irrigation_mm"] == 0.0
        assert row["irrigation_method"] == "none"

    def test_two_methods_on_one_day_are_reported_as_mixed(self, irrigation: pl.DataFrame) -> None:
        """Naming one of them would misattribute the other's water."""
        row = irrigation.filter(pl.col("water_date") == date(2026, 5, 1)).to_dicts()[0]
        assert row["irrigation_events"] == 2
        assert row["irrigation_mm"] == pytest.approx(12.0)
        assert row["irrigation_method"] == "mixed"

    def test_supply_is_irrigation_plus_rain_against_evaporation(
        self, irrigation: pl.DataFrame
    ) -> None:
        irrigated = irrigation.filter(pl.col("water_date") == date(2026, 5, 1)).to_dicts()[0]
        assert irrigated["water_supplied_mm"] == pytest.approx(12.0)
        assert irrigated["water_deficit_mm"] == pytest.approx(6.0 - 12.0)
        assert irrigated["supply_status"] == "surplus"
        assert irrigated["irrigation_share_pct"] == pytest.approx(100.0)

    def test_a_dry_unirrigated_day_is_a_deficit(self, irrigation: pl.DataFrame) -> None:
        row = irrigation.filter(pl.col("water_date") == date(2026, 5, 3)).to_dicts()[0]
        assert row["water_supplied_mm"] == pytest.approx(0.0)
        assert row["water_deficit_mm"] == pytest.approx(6.0)
        assert row["water_deficit_mm"] > DEFICIT_MM
        assert row["supply_status"] == "deficit"

    def test_rain_alone_can_cover_demand(self, irrigation: pl.DataFrame) -> None:
        row = irrigation.filter(pl.col("water_date") == date(2026, 5, 2)).to_dicts()[0]
        assert row["irrigation_mm"] == 0.0
        assert row["water_supplied_mm"] == pytest.approx(20.0)
        assert row["supply_status"] == "surplus"
        assert row["irrigation_share_pct"] == pytest.approx(0.0)

    def test_it_fails_with_the_missing_upstream_named(
        self, context: PipelineContext, put_dim: Callable[[str, pl.DataFrame], None]
    ) -> None:
        """A water balance without weather is not a degraded mart, it is a wrong
        one — so this fails, and the error says which pipeline never ran."""
        put_dim("dim_field", _fields())

        with pytest.raises(FileNotFoundError, match="gold.field_weather_daily"):
            get_pipeline("gold.field_irrigation_daily", context).run(LOGICAL_DATE)


# --- machinery ---------------------------------------------------------------


def _machines() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "machine_id": [500, 501],
            "machine_code": ["MC-01", "MC-02"],
            "farm_id": [1, 1],
            "machine_type": ["tractor", "combine"],
            "manufacturer": ["John Deere", "Claas"],
            "model": ["6120M", "Lexion"],
            "rated_power_hp": pl.Series([120, 400], dtype=pl.Int32),
        }
    )


def _telemetry(machine_id: int, running: list[bool], idle: list[bool]) -> pl.DataFrame:
    count = len(running)
    return pl.DataFrame(
        {
            "telemetry_id": list(range(machine_id * 100, machine_id * 100 + count)),
            "machine_id": [machine_id] * count,
            "reading_date": [date(2026, 5, 1)] * count,
            "engine_running": running,
            "is_idle": idle,
            "engine_hours": [1000.0 + i for i in range(count)],
            "fuel_rate_l_per_h": [12.0] * count,
            "fuel_level_pct": [60.0] * count,
            "engine_temp_c": [85.0] * count,
            "speed_kmh": [8.0] * count,
        }
    )


class TestGoldMachineDaily:
    def _run(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
        telemetry: pl.DataFrame,
        operations: pl.DataFrame | None = None,
        faults: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        put_dim("dim_machine", _machines())
        put_dim("dim_farm", _farms())
        put_fact("fact_machine_telemetry", telemetry)
        if operations is not None:
            put_fact("fact_machine_operation", operations)
        if faults is not None:
            put_fact("fact_machine_fault", faults)

        get_pipeline("gold.machine_daily", context).run(LOGICAL_DATE)
        return read_gold("machine_daily")

    def test_a_working_machine_reports_fuel_per_hectare(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
    ) -> None:
        gold = self._run(
            context,
            put_dim,
            put_fact,
            read_gold,
            telemetry=_telemetry(500, running=[True] * 10, idle=[False] * 10),
            operations=pl.DataFrame(
                {
                    "operation_id": [1],
                    "machine_id": [500],
                    "operation_date": [date(2026, 5, 1)],
                    "duration_hours": [6.0],
                    "area_covered_ha": [12.0],
                    "fuel_used_litres": [72.0],
                    "distance_km": [40.0],
                }
            ),
        )

        row = gold.to_dicts()[0]
        assert row["utilisation_status"] == "working"
        assert row["area_covered_ha"] == pytest.approx(12.0)
        assert row["fuel_per_ha"] == pytest.approx(6.0)
        assert row["idle_ratio_pct"] == pytest.approx(0.0)

    def test_a_parked_machine_has_no_idle_ratio(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
    ) -> None:
        """Reporting 0% would put a machine that never started at the top of a
        "least idle" ranking."""
        gold = self._run(
            context,
            put_dim,
            put_fact,
            read_gold,
            telemetry=_telemetry(500, running=[False] * 8, idle=[False] * 8),
        )

        row = gold.to_dicts()[0]
        assert row["utilisation_status"] == "parked"
        assert row["idle_ratio_pct"] is None
        assert row["operations"] == 0
        assert row["fuel_used_litres"] == 0.0

    def test_a_mostly_idle_machine_is_idle_not_working(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
    ) -> None:
        gold = self._run(
            context,
            put_dim,
            put_fact,
            read_gold,
            telemetry=_telemetry(500, running=[True] * 10, idle=[True] * 8 + [False] * 2),
            operations=pl.DataFrame(
                {
                    "operation_id": [1],
                    "machine_id": [500],
                    "operation_date": [date(2026, 5, 1)],
                    "duration_hours": [1.0],
                    "area_covered_ha": [2.0],
                    "fuel_used_litres": [10.0],
                    "distance_km": [5.0],
                }
            ),
        )

        row = gold.to_dicts()[0]
        assert row["idle_ratio_pct"] == pytest.approx(80.0)
        assert row["idle_ratio_pct"] > IDLE_HEAVY_PCT
        assert row["utilisation_status"] == "idle"

    def test_an_open_critical_fault_outranks_everything_else(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
    ) -> None:
        """A combine that covered ground with a critical fault open is down, not
        working — and a maintenance dashboard saying otherwise is worse than
        none."""
        gold = self._run(
            context,
            put_dim,
            put_fact,
            read_gold,
            telemetry=_telemetry(500, running=[True] * 10, idle=[False] * 10),
            operations=pl.DataFrame(
                {
                    "operation_id": [1],
                    "machine_id": [500],
                    "operation_date": [date(2026, 5, 1)],
                    "duration_hours": [6.0],
                    "area_covered_ha": [12.0],
                    "fuel_used_litres": [72.0],
                    "distance_km": [40.0],
                }
            ),
            faults=pl.DataFrame(
                {
                    "fault_id": [1],
                    "machine_id": [500],
                    "occurred_date": [date(2026, 5, 1)],
                    "severity": ["critical"],
                    "is_open": [True],
                    "downtime_hours": [4.0],
                    "repair_cost_usd": [1200.0],
                }
            ),
        )

        row = gold.to_dicts()[0]
        assert row["critical_faults"] == 1
        assert row["open_faults"] == 1
        assert row["downtime_hours"] == pytest.approx(4.0)
        assert row["utilisation_status"] == "down"


# --- planting economics ------------------------------------------------------


class TestGoldPlantingEconomics:
    @pytest.fixture
    def economics(
        self,
        context: PipelineContext,
        put_dim: Callable[[str, pl.DataFrame], None],
        put_fact: Callable[[str, pl.DataFrame], None],
        put_gold: Callable[[str, pl.DataFrame], None],
        read_gold: Callable[[str], pl.DataFrame],
    ) -> pl.DataFrame:
        put_dim("dim_planting", _plantings())
        put_dim("dim_field", _fields())
        put_dim("dim_farm", _farms())
        put_dim("dim_crop_variety", _varieties())
        put_dim("dim_crop", _crops())

        # Only planting 100 is harvested; 101 is still growing.
        put_fact(
            "fact_harvest",
            pl.DataFrame(
                {
                    "harvest_id": [1],
                    "planting_id": [100],
                    "harvested_on": [date(2026, 6, 9)],
                    "yield_tonnes": [60.0],
                    "yield_t_ha": [5.0],
                    "quality_grade": ["A"],
                    "revenue_usd": [18000.0],
                }
            ),
        )
        put_fact(
            "fact_field_cost",
            pl.DataFrame(
                {
                    "cost_id": [1, 2, 3],
                    "planting_id": [100, 100, 100],
                    "cost_category": ["seed", "fertilizer", "irrigation"],
                    "amount_usd": [1200.0, 2400.0, 900.0],
                }
            ),
        )
        put_fact(
            "fact_input_application",
            pl.DataFrame({"application_id": [1, 2], "planting_id": [100, 100]}),
        )
        put_fact(
            "fact_irrigation",
            pl.DataFrame(
                {
                    "irrigation_id": [1, 2],
                    "planting_id": [100, 100],
                    "depth_mm": [30.0, 20.0],
                }
            ),
        )
        # Two days inside the window and one after it: the crop must not be
        # credited with rain that fell once it was already off the field.
        put_gold(
            "field_weather_daily",
            _weather(
                [date(2026, 5, 1), date(2026, 5, 2), date(2026, 7, 1)],
                rain=[25.0, 25.0, 500.0],
                et0=[6.0, 6.0, 6.0],
            ),
        )

        get_pipeline("gold.planting_economics", context).run(LOGICAL_DATE)
        return read_gold("planting_economics")

    def test_one_row_per_planting(self, economics: pl.DataFrame) -> None:
        assert economics.height == 2
        assert economics["planting_id"].to_list() == [100, 101]

    def test_cost_is_pivoted_by_category_and_totalled(self, economics: pl.DataFrame) -> None:
        row = economics.filter(pl.col("planting_id") == 100).to_dicts()[0]
        assert row["cost_seed_usd"] == pytest.approx(1200.0)
        assert row["cost_fertilizer_usd"] == pytest.approx(2400.0)
        assert row["cost_irrigation_usd"] == pytest.approx(900.0)
        # A category with no spend is zero, not a missing column.
        assert row["cost_fuel_usd"] == 0.0
        assert row["cost_total_usd"] == pytest.approx(4500.0)
        assert row["cost_per_ha_usd"] == pytest.approx(4500.0 / 12.0)

    def test_margin_is_revenue_less_cost(self, economics: pl.DataFrame) -> None:
        row = economics.filter(pl.col("planting_id") == 100).to_dicts()[0]
        assert row["gross_margin_usd"] == pytest.approx(18000.0 - 4500.0)
        assert row["margin_per_ha_usd"] == pytest.approx(13500.0 / 12.0)

    def test_weather_is_counted_only_inside_the_planting_window(
        self, economics: pl.DataFrame
    ) -> None:
        """Joining on field alone would credit a summer crop with the rain that
        fell on the same ground months later."""
        row = economics.filter(pl.col("planting_id") == 100).to_dicts()[0]
        assert row["rainfall_mm"] == pytest.approx(50.0)
        assert row["gdd_accumulated"] == pytest.approx(20.0)

    def test_water_use_efficiency_combines_yield_and_water_received(
        self, economics: pl.DataFrame
    ) -> None:
        row = economics.filter(pl.col("planting_id") == 100).to_dicts()[0]
        assert row["irrigation_mm"] == pytest.approx(50.0)
        assert row["water_received_mm"] == pytest.approx(100.0)
        # 5 t/ha on 100 mm is 5 tonnes per hectare per 100 mm.
        assert row["water_use_efficiency_t_per_100mm"] == pytest.approx(5.0)

    def test_a_growing_planting_has_no_yield_rather_than_zero(
        self, economics: pl.DataFrame
    ) -> None:
        """Zero would read as a total crop failure on every yield chart."""
        row = economics.filter(pl.col("planting_id") == 101).to_dicts()[0]
        assert row["outcome"] == "growing"
        assert row["harvested_on"] is None
        assert row["yield_tonnes"] is None
        assert row["yield_t_ha"] is None
        assert row["revenue_usd"] is None
        assert row["water_use_efficiency_t_per_100mm"] is None
        # Cost still accrues on a crop in the ground; it simply had none here.
        assert row["cost_total_usd"] == 0.0
