"""Gold marts: one table per dashboard, at the grain its questions are asked.

Phase 2 proved the shape with a single soil aggregate. These four complete it,
one per analytics domain:

| Mart | Grain | Answers |
|---|---|---|
| `field_crop_health_daily` | field x observation | how the canopy is developing |
| `field_irrigation_daily`  | field x day        | whether water supply met demand |
| `machine_daily`           | machine x day      | what the fleet did and cost |
| `planting_economics`      | planting           | what the crop returned |

Each carries its dimension attributes on the row, so a chart is a single scan
with no joins — the exit criterion this phase is measured against.

The aggregation lives here in Polars rather than in ClickHouse materialized
views for the reason `docs/architecture.md` gives: a classification rule in a
SQL string is a rule no unit test can reach. Every threshold below is a module
constant, and every one of them is pinned by a test.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from smart_agri.contracts import (
    GOLD_FIELD_CROP_HEALTH_DAILY,
    GOLD_FIELD_IRRIGATION_DAILY,
    GOLD_MACHINE_DAILY,
    GOLD_PLANTING_ECONOMICS,
    empty_frame_for,
)
from smart_agri.pipelines.gold import GoldPipeline
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from smart_agri.pipelines.base import RunContext

logger = get_logger(__name__)


# --- Crop health -------------------------------------------------------------

#: Canopy stage boundaries as a share of the crop's cycle. NDVI peaks around
#: 60% of the way through, which is where `PEAK` sits; before `DEVELOPING` the
#: ground is still mostly bare whatever the index says.
BARE_PROGRESS_PCT = 15.0
PEAK_START_PROGRESS_PCT = 45.0
PEAK_END_PROGRESS_PCT = 75.0

#: Vigour bands, as a percentage of the crop's own peak NDVI. A crop at half of
#: what its variety reaches is struggling; one at 95% is doing better than the
#: model expects for its stage.
LOW_VIGOUR_PCT = 65.0
HIGH_VIGOUR_PCT = 95.0


# --- Irrigation --------------------------------------------------------------

#: Daily water balance thresholds in millimetres. A field within a couple of
#: millimetres of its demand is balanced rather than marginally either way.
SURPLUS_MM = -2.0
DEFICIT_MM = 2.0


# --- Machinery ---------------------------------------------------------------

#: A machine is "working" if it ran and covered ground, "idle" if it ran without
#: doing so, "parked" if it never ran, and "down" if a critical fault is open —
#: which outranks everything else, because a broken machine that happens to have
#: idled is still broken.
IDLE_HEAVY_PCT = 60.0


class GoldFieldCropHealthDailyPipeline(GoldPipeline):
    """Vegetation indices per field, in the context of what is growing on it.

    An NDVI of 0.3 means nothing on its own: it is healthy for a crop two weeks
    after sowing and alarming for one at mid-cycle. Joining the active planting
    on gives the reading a stage, and the crop's own peak gives it a scale.
    """

    name = "gold.field_crop_health_daily"
    dataset = "field_crop_health_daily"
    schema = GOLD_FIELD_CROP_HEALTH_DAILY

    def extract(self, run: RunContext) -> pl.DataFrame:
        observations = self._read_silver_facts("fact_field_index")
        if observations.is_empty():
            logger.warning("no_silver_observations", pipeline=self.name)
            return observations

        # `farm_id` deliberately not selected: the observation already carries
        # it, and taking it from both sides would leave a suffixed duplicate.
        fields = self._read_silver_dim("dim_field", run).select(
            "field_id", "field_code", "field_name", "area_ha", "soil_type"
        )
        farms = self._read_silver_dim("dim_farm", run).select(
            "farm_id", "farm_code", "region", "country_code"
        )
        plantings = self._read_silver_dim("dim_planting", run).select(
            "planting_id", "season", "variety_id", "planted_on"
        )
        varieties = self._read_silver_dim("dim_crop_variety", run).select(
            "variety_id", "variety_code", "crop_code"
        )
        crops = self._read_silver_dim("dim_crop", run).select(
            "crop_code", "crop_name", "category", "cycle_days", "peak_ndvi"
        )

        # A cloudy pass carries no usable signal, and averaging it in would drag
        # every field's NDVI toward zero on overcast days.
        usable = observations.filter(pl.col("usable"))

        return (
            usable.join(fields, on="field_id", how="inner")
            .join(farms, on="farm_id", how="inner")
            # Left from here: an observation between crop cycles has no planting,
            # and that is bare ground rather than a broken join.
            .join(plantings, on="planting_id", how="left")
            .join(varieties, on="variety_id", how="left")
            .join(crops, on="crop_code", how="left")
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return empty_frame_for(GOLD_FIELD_CROP_HEALTH_DAILY)

        aggregated = frame.group_by(
            "observed_on",
            "field_id",
            "field_code",
            "field_name",
            "farm_id",
            "farm_code",
            "region",
            "country_code",
            "soil_type",
            "area_ha",
            "planting_id",
            "season",
            "crop_code",
            "crop_name",
            "category",
            "variety_code",
            "planted_on",
            "cycle_days",
            "peak_ndvi",
        ).agg(
            pl.len().cast(pl.Int64).alias("observation_count"),
            pl.col("ndvi").mean().alias("avg_ndvi"),
            pl.col("ndvi").max().alias("max_ndvi"),
            pl.col("ndwi").mean().alias("avg_ndwi"),
            pl.col("evi").mean().alias("avg_evi"),
            pl.col("cloud_cover_pct").mean().alias("avg_cloud_cover_pct"),
        )

        with_stage = aggregated.with_columns(
            (pl.col("observed_on") - pl.col("planted_on"))
            .dt.total_days()
            .cast(pl.Int32)
            .alias("days_after_sowing"),
            pl.col("area_ha").alias("field_area_ha"),
            pl.col("category").alias("crop_category"),
            pl.col("peak_ndvi").alias("peak_ndvi_expected"),
        ).with_columns(
            (pl.col("days_after_sowing") / pl.col("cycle_days") * 100).alias("cycle_progress_pct"),
            (pl.col("avg_ndvi") / pl.col("peak_ndvi_expected") * 100).alias("ndvi_vs_expected_pct"),
        )

        return (
            # Two passes, not one: the vigour flag reads `canopy_stage`, and
            # expressions inside a single `with_columns` cannot see each other.
            with_stage.with_columns(self._canopy_stage().alias("canopy_stage"))
            .with_columns(self._vigour_flag().alias("vigour_flag"))
            .with_columns(
                pl.col(
                    "avg_ndvi",
                    "max_ndvi",
                    "avg_ndwi",
                    "avg_evi",
                    "avg_cloud_cover_pct",
                    "cycle_progress_pct",
                    "ndvi_vs_expected_pct",
                ).round(4)
            )
            .select(GOLD_FIELD_CROP_HEALTH_DAILY.columns.keys())
            .sort("observed_on", "field_id")
        )

    @staticmethod
    def _canopy_stage() -> pl.Expr:
        """Where in the cycle the canopy is, from cycle progress alone.

        Deliberately not from NDVI: the stage is what NDVI is judged *against*,
        so deriving it from NDVI would make the vigour flag circular.
        """
        progress = pl.col("cycle_progress_pct")
        return (
            pl.when(progress.is_null())
            .then(pl.lit("unknown"))
            .when(progress < BARE_PROGRESS_PCT)
            .then(pl.lit("bare"))
            .when(progress < PEAK_START_PROGRESS_PCT)
            .then(pl.lit("developing"))
            .when(progress <= PEAK_END_PROGRESS_PCT)
            .then(pl.lit("peak"))
            .otherwise(pl.lit("senescing"))
        )

    @staticmethod
    def _vigour_flag() -> pl.Expr:
        """How the canopy compares with what this crop can reach.

        Only meaningful once there is a canopy to judge: before the crop has
        developed, a low NDVI is the expected state, not a warning.
        """
        ratio = pl.col("ndvi_vs_expected_pct")
        return (
            pl.when(ratio.is_null() | pl.col("canopy_stage").is_in(["unknown", "bare"]))
            .then(pl.lit("unknown"))
            .when(ratio < LOW_VIGOUR_PCT)
            .then(pl.lit("low"))
            .when(ratio >= HIGH_VIGOUR_PCT)
            .then(pl.lit("high"))
            .otherwise(pl.lit("normal"))
        )


class GoldFieldIrrigationDailyPipeline(GoldPipeline):
    """Water supplied against water demanded, per field per day.

    Built on the weather spine rather than on the irrigation events, and that
    choice is the whole point. Aggregating events alone would produce rows only
    for days something was applied — but the days that matter most to an
    irrigation dashboard are the dry ones where *nothing* was, and those rows
    would simply not exist.
    """

    name = "gold.field_irrigation_daily"
    dataset = "field_irrigation_daily"
    schema = GOLD_FIELD_IRRIGATION_DAILY

    def extract(self, run: RunContext) -> pl.DataFrame:
        # The spine: every field, every day weather covers.
        weather = self._read_gold("field_weather_daily", run)
        if weather.is_empty():
            logger.warning("no_gold_weather", pipeline=self.name)
            return weather

        fields = self._read_silver_dim("dim_field", run).select("field_id", "soil_type")
        events = self._read_silver_facts("fact_irrigation")

        daily = (
            self._aggregate_events(events) if not events.is_empty() else self._empty_daily(weather)
        )

        return (
            weather.select(
                pl.col("weather_date").alias("water_date"),
                "field_id",
                "field_code",
                "farm_id",
                "farm_code",
                "region",
                "country_code",
                "field_area_ha",
                "is_actual",
                pl.col("precipitation_mm").alias("rainfall_mm"),
                "et0_mm",
            )
            .join(fields, on="field_id", how="inner")
            .join(daily, on=["field_id", "water_date"], how="left")
        )

    @staticmethod
    def _aggregate_events(events: pl.DataFrame) -> pl.DataFrame:
        """Irrigation rolled up to one row per field per day."""
        return (
            events.group_by(pl.col("field_id"), pl.col("event_date").alias("water_date"))
            .agg(
                pl.len().cast(pl.Int64).alias("irrigation_events"),
                pl.col("depth_mm").sum().alias("irrigation_mm"),
                pl.col("water_volume_m3").sum().alias("water_volume_m3"),
                pl.col("duration_minutes").sum().cast(pl.Int64).alias("irrigation_minutes"),
                pl.col("energy_kwh").sum().alias("energy_kwh"),
                # A day may combine drip and furrow; naming one of them would
                # misattribute the other's water.
                pl.col("method").n_unique().alias("_methods"),
                pl.col("method").first().alias("_method"),
            )
            .with_columns(
                pl.when(pl.col("_methods") > 1)
                .then(pl.lit("mixed"))
                .otherwise(pl.col("_method"))
                .alias("irrigation_method")
            )
            .drop("_methods", "_method")
        )

    @staticmethod
    def _empty_daily(weather: pl.DataFrame) -> pl.DataFrame:
        """A correctly-typed empty aggregate, for a lake with no irrigation yet."""
        return pl.DataFrame(
            schema={
                "field_id": weather.schema["field_id"],
                "water_date": pl.Date,
                "irrigation_events": pl.Int64,
                "irrigation_mm": pl.Float64,
                "water_volume_m3": pl.Float64,
                "irrigation_minutes": pl.Int64,
                "energy_kwh": pl.Float64,
                "irrigation_method": pl.String,
            }
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return empty_frame_for(GOLD_FIELD_IRRIGATION_DAILY)

        filled = frame.with_columns(
            # A day with no irrigation applied nothing — that is a real zero,
            # unlike rainfall, where null means the weather is simply unknown.
            pl.col("irrigation_events").fill_null(0),
            pl.col("irrigation_mm").fill_null(0.0),
            pl.col("water_volume_m3").fill_null(0.0),
            pl.col("irrigation_minutes").fill_null(0),
            pl.col("irrigation_method").fill_null(pl.lit("none")),
        )

        with_balance = filled.with_columns(
            (pl.col("irrigation_mm") + pl.col("rainfall_mm")).alias("water_supplied_mm")
        ).with_columns(
            (pl.col("et0_mm") - pl.col("water_supplied_mm")).alias("water_deficit_mm"),
            pl.when(pl.col("water_supplied_mm") > 0)
            .then(pl.col("irrigation_mm") / pl.col("water_supplied_mm") * 100)
            .otherwise(None)
            .alias("irrigation_share_pct"),
        )

        return (
            with_balance.with_columns(self._supply_status().alias("supply_status"))
            .with_columns(
                pl.col(
                    "irrigation_mm",
                    "water_volume_m3",
                    "energy_kwh",
                    "rainfall_mm",
                    "et0_mm",
                    "water_supplied_mm",
                    "water_deficit_mm",
                    "irrigation_share_pct",
                ).round(3)
            )
            .select(GOLD_FIELD_IRRIGATION_DAILY.columns.keys())
            .sort("water_date", "field_id")
        )

    @staticmethod
    def _supply_status() -> pl.Expr:
        deficit = pl.col("water_deficit_mm")
        return (
            pl.when(deficit.is_null())
            .then(pl.lit("unknown"))
            .when(deficit > DEFICIT_MM)
            .then(pl.lit("deficit"))
            .when(deficit < SURPLUS_MM)
            .then(pl.lit("surplus"))
            .otherwise(pl.lit("balanced"))
        )


class GoldMachineDailyPipeline(GoldPipeline):
    """What each machine did, burned and cost, per day.

    Telemetry is the spine because it is the only dense source: a machine emits
    readings whether or not it worked, so a parked day still produces a row.
    Operations and faults join onto it, and their absence means zero rather than
    a missing day.
    """

    name = "gold.machine_daily"
    dataset = "machine_daily"
    schema = GOLD_MACHINE_DAILY

    def extract(self, run: RunContext) -> pl.DataFrame:
        telemetry = self._read_silver_facts("fact_machine_telemetry")
        if telemetry.is_empty():
            logger.warning("no_silver_telemetry", pipeline=self.name)
            return telemetry

        machines = self._read_silver_dim("dim_machine", run).select(
            "machine_id",
            "machine_code",
            "farm_id",
            "machine_type",
            "manufacturer",
            "model",
            "rated_power_hp",
        )
        farms = self._read_silver_dim("dim_farm", run).select("farm_id", "farm_code", "region")

        daily_telemetry = telemetry.group_by(
            pl.col("machine_id"), pl.col("reading_date").alias("activity_date")
        ).agg(
            pl.len().cast(pl.Int64).alias("telemetry_readings"),
            pl.col("engine_running").sum().cast(pl.Int64).alias("running_readings"),
            pl.col("is_idle").sum().cast(pl.Int64).alias("idle_readings"),
            pl.col("engine_hours").max().alias("engine_hours_end"),
            pl.col("fuel_rate_l_per_h").mean().alias("avg_fuel_rate_l_per_h"),
            pl.col("fuel_level_pct").min().alias("min_fuel_level_pct"),
            pl.col("engine_temp_c").max().alias("max_engine_temp_c"),
            pl.col("speed_kmh").mean().alias("avg_speed_kmh"),
        )

        return (
            daily_telemetry.join(machines, on="machine_id", how="inner")
            .join(farms, on="farm_id", how="inner")
            .join(
                self._aggregate_operations(self._read_silver_facts("fact_machine_operation")),
                on=["machine_id", "activity_date"],
                how="left",
            )
            .join(
                self._aggregate_faults(self._read_silver_facts("fact_machine_fault")),
                on=["machine_id", "activity_date"],
                how="left",
            )
        )

    @staticmethod
    def _aggregate_operations(operations: pl.DataFrame) -> pl.DataFrame:
        if operations.is_empty():
            return pl.DataFrame(
                schema={
                    "machine_id": pl.Int64,
                    "activity_date": pl.Date,
                    "operations": pl.Int64,
                    "operation_hours": pl.Float64,
                    "area_covered_ha": pl.Float64,
                    "fuel_used_litres": pl.Float64,
                    "distance_km": pl.Float64,
                }
            )
        return operations.group_by(
            pl.col("machine_id"), pl.col("operation_date").alias("activity_date")
        ).agg(
            pl.len().cast(pl.Int64).alias("operations"),
            pl.col("duration_hours").sum().alias("operation_hours"),
            pl.col("area_covered_ha").sum().alias("area_covered_ha"),
            pl.col("fuel_used_litres").sum().alias("fuel_used_litres"),
            pl.col("distance_km").sum().alias("distance_km"),
        )

    @staticmethod
    def _aggregate_faults(faults: pl.DataFrame) -> pl.DataFrame:
        if faults.is_empty():
            return pl.DataFrame(
                schema={
                    "machine_id": pl.Int64,
                    "activity_date": pl.Date,
                    "faults": pl.Int64,
                    "critical_faults": pl.Int64,
                    "open_faults": pl.Int64,
                    "downtime_hours": pl.Float64,
                    "repair_cost_usd": pl.Float64,
                }
            )
        return faults.group_by(
            pl.col("machine_id"), pl.col("occurred_date").alias("activity_date")
        ).agg(
            pl.len().cast(pl.Int64).alias("faults"),
            (pl.col("severity") == "critical").sum().cast(pl.Int64).alias("critical_faults"),
            pl.col("is_open").sum().cast(pl.Int64).alias("open_faults"),
            pl.col("downtime_hours").sum().alias("downtime_hours"),
            pl.col("repair_cost_usd").sum().alias("repair_cost_usd"),
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return empty_frame_for(GOLD_MACHINE_DAILY)

        filled = frame.with_columns(
            pl.col("operations").fill_null(0),
            pl.col("operation_hours").fill_null(0.0),
            pl.col("area_covered_ha").fill_null(0.0),
            pl.col("fuel_used_litres").fill_null(0.0),
            pl.col("faults").fill_null(0),
            pl.col("critical_faults").fill_null(0),
            pl.col("open_faults").fill_null(0),
            pl.col("downtime_hours").fill_null(0.0),
            pl.col("repair_cost_usd").fill_null(0.0),
        )

        derived = filled.with_columns(
            # Only meaningful against time the engine actually ran. A machine
            # that never started has no idle ratio, and calling it 0% would put
            # it at the top of a "least idle" chart.
            pl.when(pl.col("running_readings") > 0)
            .then(pl.col("idle_readings") / pl.col("running_readings") * 100)
            .otherwise(None)
            .alias("idle_ratio_pct"),
            pl.when(pl.col("area_covered_ha") > 0)
            .then(pl.col("fuel_used_litres") / pl.col("area_covered_ha"))
            .otherwise(None)
            .alias("fuel_per_ha"),
        )

        return (
            derived.with_columns(self._utilisation_status().alias("utilisation_status"))
            .with_columns(
                pl.col(
                    "idle_ratio_pct",
                    "engine_hours_end",
                    "avg_fuel_rate_l_per_h",
                    "min_fuel_level_pct",
                    "max_engine_temp_c",
                    "avg_speed_kmh",
                    "operation_hours",
                    "area_covered_ha",
                    "fuel_used_litres",
                    "distance_km",
                    "fuel_per_ha",
                    "downtime_hours",
                    "repair_cost_usd",
                ).round(3)
            )
            .select(GOLD_MACHINE_DAILY.columns.keys())
            .sort("activity_date", "machine_id")
        )

    @staticmethod
    def _utilisation_status() -> pl.Expr:
        """An open critical fault outranks whatever the machine was doing.

        A combine that idled for an hour with a critical fault open is down, not
        idle — and a maintenance dashboard that says otherwise is worse than no
        dashboard.
        """
        return (
            pl.when(pl.col("critical_faults") > 0)
            .then(pl.lit("down"))
            .when(pl.col("running_readings") == 0)
            .then(pl.lit("parked"))
            .when(pl.col("area_covered_ha") > 0)
            .then(
                pl.when(pl.col("idle_ratio_pct") > IDLE_HEAVY_PCT)
                .then(pl.lit("idle"))
                .otherwise(pl.lit("working"))
            )
            .otherwise(pl.lit("idle"))
        )


class GoldPlantingEconomicsPipeline(GoldPipeline):
    """One row per planting: what it cost, what it returned, and on what water.

    The mart the yield-and-economics dashboard exists for. Everything it needs
    is on the row — crop, area, yield, revenue, cost by category, water received
    and degree-days accumulated — because the questions worth asking here are
    cross-domain, and answering them by joining four facts at query time is how
    a dashboard becomes slow and inconsistent at the same time.
    """

    name = "gold.planting_economics"
    dataset = "planting_economics"
    schema = GOLD_PLANTING_ECONOMICS

    #: Cost categories become one column each. Fixed rather than derived from
    #: the data so a season with no pesticide spend still reports 0, not a
    #: missing column that breaks the chart.
    COST_CATEGORIES = (
        "seed",
        "fertilizer",
        "crop_protection",
        "irrigation",
        "fuel",
        "labour",
        "machinery",
        "other",
    )

    def extract(self, run: RunContext) -> pl.DataFrame:
        plantings = self._read_silver_dim("dim_planting", run)
        if plantings.is_empty():
            logger.warning("no_silver_plantings", pipeline=self.name)
            return plantings

        fields = self._read_silver_dim("dim_field", run).select(
            "field_id", "field_code", "soil_type"
        )
        farms = self._read_silver_dim("dim_farm", run).select(
            "farm_id", "farm_code", "region", "country_code"
        )
        varieties = self._read_silver_dim("dim_crop_variety", run).select(
            "variety_id", "variety_code", "variety_name", "crop_code"
        )
        crops = self._read_silver_dim("dim_crop", run).select("crop_code", "crop_name", "category")

        base = (
            plantings.join(fields, on="field_id", how="inner")
            .join(farms, on="farm_id", how="inner")
            .join(varieties, on="variety_id", how="inner")
            .join(crops, on="crop_code", how="inner")
        )

        return (
            base.join(self._harvests(), on="planting_id", how="left")
            .join(self._costs(), on="planting_id", how="left")
            .join(self._inputs(), on="planting_id", how="left")
            .join(self._irrigation(), on="planting_id", how="left")
            .join(self._weather(base, run), on="planting_id", how="left")
        )

    def _harvests(self) -> pl.DataFrame:
        harvests = self._read_silver_facts("fact_harvest")
        if harvests.is_empty():
            return pl.DataFrame(
                schema={
                    "planting_id": pl.Int64,
                    "harvested_on": pl.Date,
                    "yield_tonnes": pl.Float64,
                    "yield_t_ha": pl.Float64,
                    "quality_grade": pl.String,
                    "revenue_usd": pl.Float64,
                }
            )
        # One harvest per planting is a source uniqueness constraint, so this is
        # a projection rather than an aggregation.
        return harvests.select(
            "planting_id",
            "harvested_on",
            "yield_tonnes",
            "yield_t_ha",
            "quality_grade",
            "revenue_usd",
        ).unique(subset=["planting_id"], keep="last")

    def _costs(self) -> pl.DataFrame:
        """Cost pivoted to one column per category, plus the total."""
        costs = self._read_silver_facts("fact_field_cost")
        columns = {f"cost_{category}_usd": pl.Float64 for category in self.COST_CATEGORIES}

        if costs.is_empty():
            return pl.DataFrame(
                schema={"planting_id": pl.Int64, "cost_total_usd": pl.Float64, **columns}
            )

        attributed = costs.filter(pl.col("planting_id").is_not_null())
        return attributed.group_by("planting_id").agg(
            pl.col("amount_usd").sum().alias("cost_total_usd"),
            *[
                pl.col("amount_usd")
                .filter(pl.col("cost_category") == category)
                .sum()
                .alias(f"cost_{category}_usd")
                for category in self.COST_CATEGORIES
            ],
        )

    def _inputs(self) -> pl.DataFrame:
        inputs = self._read_silver_facts("fact_input_application")
        if inputs.is_empty():
            return pl.DataFrame(schema={"planting_id": pl.Int64, "input_applications": pl.Int64})
        return (
            inputs.filter(pl.col("planting_id").is_not_null())
            .group_by("planting_id")
            .agg(pl.len().cast(pl.Int64).alias("input_applications"))
        )

    def _irrigation(self) -> pl.DataFrame:
        events = self._read_silver_facts("fact_irrigation")
        if events.is_empty():
            return pl.DataFrame(
                schema={
                    "planting_id": pl.Int64,
                    "irrigation_events": pl.Int64,
                    "irrigation_mm": pl.Float64,
                }
            )
        return (
            events.filter(pl.col("planting_id").is_not_null())
            .group_by("planting_id")
            .agg(
                pl.len().cast(pl.Int64).alias("irrigation_events"),
                pl.col("depth_mm").sum().alias("irrigation_mm"),
            )
        )

    def _weather(self, base: pl.DataFrame, run: RunContext) -> pl.DataFrame:
        """Rainfall and degree-days accumulated over each planting's own window.

        A range join: weather belongs to a planting only for the days between
        sowing and harvest. Joining on field alone would credit a summer crop
        with the winter rain that fell on the same ground.
        """
        empty = pl.DataFrame(
            schema={
                "planting_id": pl.Int64,
                "rainfall_mm": pl.Float64,
                "gdd_accumulated": pl.Float64,
            }
        )
        try:
            weather = self._read_gold("field_weather_daily", run)
        except FileNotFoundError:
            logger.warning("no_gold_weather_for_economics", pipeline=self.name)
            return empty
        if weather.is_empty():
            return empty

        windows = base.select(
            "planting_id",
            "field_id",
            "planted_on",
            # Still growing: accumulate to today rather than to a harvest that
            # has not happened.
            pl.coalesce(pl.col("expected_harvest_on"), pl.col("planted_on")).alias("window_end"),
        )

        return (
            weather.select("field_id", "weather_date", "precipitation_mm", "gdd_base10")
            .join(windows, on="field_id", how="inner")
            .filter(pl.col("weather_date").is_between(pl.col("planted_on"), pl.col("window_end")))
            .group_by("planting_id")
            .agg(
                pl.col("precipitation_mm").sum().alias("rainfall_mm"),
                pl.col("gdd_base10").sum().alias("gdd_accumulated"),
            )
        )

    def transform(self, frame: pl.DataFrame, run: RunContext) -> pl.DataFrame:  # noqa: ARG002
        if frame.is_empty():
            return empty_frame_for(GOLD_PLANTING_ECONOMICS)

        cost_columns = [f"cost_{category}_usd" for category in self.COST_CATEGORIES]

        filled = frame.with_columns(
            pl.col("cost_total_usd").fill_null(0.0),
            *[pl.col(column).fill_null(0.0) for column in cost_columns],
            pl.col("input_applications").fill_null(0),
            pl.col("irrigation_events").fill_null(0),
            pl.col("irrigation_mm").fill_null(0.0),
            pl.col("category").alias("crop_category"),
        )

        derived = filled.with_columns(
            (pl.col("cost_total_usd") / pl.col("area_ha")).alias("cost_per_ha_usd"),
            (pl.col("revenue_usd") - pl.col("cost_total_usd")).alias("gross_margin_usd"),
            (pl.col("irrigation_mm") + pl.col("rainfall_mm")).alias("water_received_mm"),
        ).with_columns(
            (pl.col("gross_margin_usd") / pl.col("area_ha")).alias("margin_per_ha_usd"),
            # Tonnes per hectare per 100 mm. Undefined without water or without
            # a harvest, and a zero there would read as a total failure.
            pl.when(pl.col("water_received_mm") > 0)
            .then(pl.col("yield_t_ha") / pl.col("water_received_mm") * 100)
            .otherwise(None)
            .alias("water_use_efficiency_t_per_100mm"),
        )

        return (
            derived.with_columns(pl.col("status").alias("outcome"))
            .with_columns(
                pl.col(
                    "yield_tonnes",
                    "yield_t_ha",
                    "revenue_usd",
                    "cost_total_usd",
                    *cost_columns,
                    "cost_per_ha_usd",
                    "gross_margin_usd",
                    "margin_per_ha_usd",
                    "irrigation_mm",
                    "rainfall_mm",
                    "water_received_mm",
                    "gdd_accumulated",
                    "water_use_efficiency_t_per_100mm",
                ).round(3)
            )
            .select(GOLD_PLANTING_ECONOMICS.columns.keys())
            .sort("planted_on", "planting_id")
        )
