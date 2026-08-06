-- Weather tables.
--
-- `dim_date` is derived from the weather series rather than generated for a
-- fixed range, so it can never fall short of the facts that join to it.

CREATE TABLE IF NOT EXISTS dim_date
(
    date_key     Date,
    year         Int32,
    quarter      Int8,
    month        Int8,
    month_name   LowCardinality(String),
    day_of_month Int8,
    day_of_week  Int8,
    day_name     LowCardinality(String),
    iso_week     Int8,
    is_weekend   Bool
)
ENGINE = ReplacingMergeTree
ORDER BY date_key
COMMENT 'Date dimension spanning the weather series';


-- One row per farm per day.
--
-- `is_actual` separates measurement from prediction. A forecast row is real
-- data and belongs in the table, but a chart correlating rainfall with observed
-- soil moisture must filter to actuals — otherwise it compares what happened
-- against what was merely expected.

CREATE TABLE IF NOT EXISTS fact_weather_daily
(
    farm_id               Int64,
    farm_code             String,
    weather_date          Date,
    source                LowCardinality(String),
    is_actual             Bool,
    latitude              Float64,
    longitude             Float64,
    temp_max_c            Nullable(Float64),
    temp_min_c            Nullable(Float64),
    temp_mean_c           Nullable(Float64),
    precipitation_mm      Nullable(Float64),
    rain_mm               Nullable(Float64),
    precipitation_hours   Nullable(Float64),
    et0_mm                Nullable(Float64),
    solar_radiation_mj_m2 Nullable(Float64),
    wind_speed_max_kmh    Nullable(Float64),
    weather_code          Nullable(Int32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(weather_date)
ORDER BY (farm_id, weather_date)
COMMENT 'Daily weather per farm, from Open-Meteo';


-- Weather pushed down to field level, with the derived agronomy the dashboards
-- query: degree-days, rolling rainfall, and the balance between what fell and
-- what evaporated.

CREATE TABLE IF NOT EXISTS agg_field_weather_daily
(
    weather_date          Date,
    field_id              Int64,
    field_code            String,
    farm_id               Int64,
    farm_code             String,
    region                LowCardinality(String),
    country_code          LowCardinality(String),
    field_area_ha         Float64,
    is_actual             Bool,
    temp_max_c            Nullable(Float64),
    temp_min_c            Nullable(Float64),
    temp_mean_c           Nullable(Float64),
    precipitation_mm      Nullable(Float64),
    et0_mm                Nullable(Float64),
    solar_radiation_mj_m2 Nullable(Float64),
    gdd_base10            Nullable(Float64),
    gdd_cumulative        Nullable(Float64),
    rainfall_7d_mm        Nullable(Float64),
    rainfall_30d_mm       Nullable(Float64),
    et0_7d_mm             Nullable(Float64),
    water_balance_mm      Nullable(Float64),
    water_balance_7d_mm   Nullable(Float64),
    aridity_flag          LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(weather_date)
ORDER BY (field_id, weather_date)
COMMENT 'Daily weather and derived agronomy per field';
