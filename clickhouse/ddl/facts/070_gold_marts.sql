-- The Gold marts — one table per analytics domain.
--
-- Each is built by a Polars pipeline and inserted whole, so the classification
-- rules (canopy stage, supply status, utilisation) are unit-tested against
-- fixture frames rather than living in a SQL string no test can reach.
--
-- Every one carries its dimension attributes on the row. That denormalisation
-- is the point: a dashboard question here is a single scan with no joins, which
-- is the criterion this phase is measured against.

CREATE TABLE IF NOT EXISTS agg_field_crop_health_daily
(
    observed_on          Date,
    field_id             Int64,
    field_code           String,
    field_name           String,
    farm_id              Int64,
    farm_code            String,
    region               LowCardinality(String),
    country_code         LowCardinality(String),
    soil_type            LowCardinality(String),
    field_area_ha        Float64,
    -- Null between crop cycles: bare ground is a real state, not a join failure.
    planting_id          Nullable(Int64),
    season               Nullable(String),
    crop_code            Nullable(String),
    crop_name            Nullable(String),
    crop_category        Nullable(String),
    variety_code         Nullable(String),
    days_after_sowing    Nullable(Int32),
    cycle_progress_pct   Nullable(Float64),
    observation_count    Int64,
    avg_ndvi             Nullable(Float64),
    max_ndvi             Nullable(Float64),
    avg_ndwi             Nullable(Float64),
    avg_evi              Nullable(Float64),
    avg_cloud_cover_pct  Nullable(Float64),
    peak_ndvi_expected   Nullable(Float64),
    ndvi_vs_expected_pct Nullable(Float64),
    canopy_stage         LowCardinality(String),
    vigour_flag          LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_on)
ORDER BY (field_id, observed_on)
COMMENT 'Canopy development per field, in the context of what is growing on it';


-- Built on the weather spine, not on the irrigation events. A day with no
-- irrigation still has a row, because the dry days nothing was applied on are
-- precisely what an irrigation dashboard exists to surface.
CREATE TABLE IF NOT EXISTS agg_field_irrigation_daily
(
    water_date           Date,
    field_id             Int64,
    field_code           String,
    farm_id              Int64,
    farm_code            String,
    region               LowCardinality(String),
    country_code         LowCardinality(String),
    soil_type            LowCardinality(String),
    field_area_ha        Float64,
    irrigation_events    Int64,
    irrigation_mm        Float64,
    water_volume_m3      Float64,
    irrigation_minutes   Int64,
    energy_kwh           Nullable(Float64),
    irrigation_method    LowCardinality(String),
    rainfall_mm          Nullable(Float64),
    et0_mm               Nullable(Float64),
    water_supplied_mm    Nullable(Float64),
    -- Positive means demand exceeded supply.
    water_deficit_mm     Nullable(Float64),
    irrigation_share_pct Nullable(Float64),
    is_actual            Bool,
    supply_status        LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(water_date)
ORDER BY (field_id, water_date)
COMMENT 'Water supplied against water demanded, per field per day';


CREATE TABLE IF NOT EXISTS agg_machine_daily
(
    activity_date         Date,
    machine_id            Int64,
    machine_code          String,
    machine_type          LowCardinality(String),
    manufacturer          LowCardinality(String),
    model                 LowCardinality(String),
    rated_power_hp        Int32,
    farm_id               Int64,
    farm_code             String,
    region                LowCardinality(String),
    telemetry_readings    Int64,
    running_readings      Int64,
    idle_readings         Int64,
    -- Null when the engine never ran. A parked machine has no idle ratio, and
    -- a zero there would top a "least idle" chart.
    idle_ratio_pct        Nullable(Float64),
    engine_hours_end      Nullable(Float64),
    avg_fuel_rate_l_per_h Nullable(Float64),
    min_fuel_level_pct    Nullable(Float64),
    max_engine_temp_c     Nullable(Float64),
    avg_speed_kmh         Nullable(Float64),
    operations            Int64,
    operation_hours       Float64,
    area_covered_ha       Float64,
    fuel_used_litres      Float64,
    distance_km           Nullable(Float64),
    fuel_per_ha           Nullable(Float64),
    faults                Int64,
    critical_faults       Int64,
    open_faults           Int64,
    downtime_hours        Float64,
    repair_cost_usd       Float64,
    utilisation_status    LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (machine_id, activity_date)
COMMENT 'What each machine did, burned and cost, per day';


-- One row per planting, not per day: the economics of a crop cycle only close
-- when the cycle does. Yield and revenue are Nullable because a planting still
-- in the ground has neither, and zero would read as a total crop failure.
CREATE TABLE IF NOT EXISTS agg_planting_economics
(
    planting_id                      Int64,
    field_id                         Int64,
    field_code                       String,
    farm_id                          Int64,
    farm_code                        String,
    region                           LowCardinality(String),
    country_code                     LowCardinality(String),
    soil_type                        LowCardinality(String),
    season                           LowCardinality(String),
    planted_on                       Date,
    expected_harvest_on              Date,
    status                           LowCardinality(String),
    crop_code                        LowCardinality(String),
    crop_name                        String,
    crop_category                    LowCardinality(String),
    variety_code                     String,
    variety_name                     String,
    area_ha                          Float64,
    harvested_on                     Nullable(Date),
    yield_tonnes                     Nullable(Float64),
    yield_t_ha                       Nullable(Float64),
    quality_grade                    Nullable(String),
    revenue_usd                      Nullable(Float64),
    cost_total_usd                   Float64,
    cost_seed_usd                    Float64,
    cost_fertilizer_usd              Float64,
    cost_crop_protection_usd         Float64,
    cost_irrigation_usd              Float64,
    cost_fuel_usd                    Float64,
    cost_labour_usd                  Float64,
    cost_machinery_usd               Float64,
    cost_other_usd                   Float64,
    cost_per_ha_usd                  Float64,
    gross_margin_usd                 Nullable(Float64),
    margin_per_ha_usd                Nullable(Float64),
    input_applications               Int64,
    irrigation_events                Int64,
    irrigation_mm                    Float64,
    rainfall_mm                      Nullable(Float64),
    water_received_mm                Nullable(Float64),
    gdd_accumulated                  Nullable(Float64),
    -- Tonnes per hectare per 100 mm received. The headline efficiency number,
    -- and the reason water and yield share one mart.
    water_use_efficiency_t_per_100mm Nullable(Float64),
    outcome                          LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYear(planted_on)
ORDER BY (planting_id)
COMMENT 'One row per crop cycle: what it cost, returned, and on what water';
