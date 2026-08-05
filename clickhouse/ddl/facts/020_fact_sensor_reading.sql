-- The sensor reading fact table.
--
-- ReplacingMergeTree keyed on the source primary key: a re-ingest of an
-- overlapping window can present the same reading twice, and the engine
-- collapses the duplicate rather than double-counting it. Reads that must not
-- see a duplicate before a merge has run should use FINAL, or query the daily
-- aggregate below, which is what the dashboard does.

CREATE TABLE IF NOT EXISTS fact_sensor_reading
(
    reading_id         Int64,
    sensor_id          Int64,
    field_id           Int64,
    farm_id            Int64,
    reading_ts         DateTime64(3, 'UTC'),
    reading_date       Date,
    soil_moisture_pct  Nullable(Float64),
    soil_temperature_c Nullable(Float64),
    soil_ph            Nullable(Float64),
    soil_ec_ds_m       Nullable(Float64),
    battery_pct        Nullable(Float64)
)
ENGINE = ReplacingMergeTree
-- Monthly partitions: a day is too granular for the volumes here and would
-- produce thousands of tiny parts.
PARTITION BY toYYYYMM(reading_date)
ORDER BY (field_id, sensor_id, reading_ts)
PRIMARY KEY (field_id, sensor_id, reading_ts)
COMMENT 'Raw soil telemetry, one row per sensor per timestamp';


-- The daily field aggregate the Superset dashboard queries.
--
-- Built by the Gold pipeline in Polars rather than by ClickHouse, so the
-- aggregation logic is unit-tested against fixture frames instead of living in
-- a SQL string. Dimension attributes are carried on the row so a chart needs no
-- joins at query time.

CREATE TABLE IF NOT EXISTS agg_field_soil_daily
(
    reading_date           Date,
    farm_id                Int64,
    farm_code              String,
    farm_name              String,
    country_code           LowCardinality(String),
    region                 LowCardinality(String),
    field_id               Int64,
    field_code             String,
    field_name             String,
    soil_type              LowCardinality(String),
    field_area_ha          Float64,
    active_sensors         Int64,
    reading_count          Int64,
    avg_soil_moisture_pct  Nullable(Float64),
    min_soil_moisture_pct  Nullable(Float64),
    max_soil_moisture_pct  Nullable(Float64),
    avg_soil_temperature_c Nullable(Float64),
    avg_soil_ph            Nullable(Float64),
    avg_soil_ec_ds_m       Nullable(Float64),
    min_battery_pct        Nullable(Float64),
    moisture_stress        LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(reading_date)
ORDER BY (field_id, reading_date)
COMMENT 'Daily soil metrics per field — the dashboard''s primary source';
