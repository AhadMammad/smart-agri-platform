-- Dimensions for the soil-sensor slice.
--
-- All three are small and snapshot-loaded, so a plain MergeTree with a
-- TRUNCATE-then-insert refresh is both simplest and trivially idempotent.
-- Every statement is IF NOT EXISTS so `smart-agri init-clickhouse` can run on
-- every deploy.

CREATE TABLE IF NOT EXISTS dim_farm
(
    farm_id      Int64,
    farm_code    String,
    farm_name    String,
    country_code LowCardinality(String),
    region       LowCardinality(String),
    latitude     Float64,
    longitude    Float64,
    area_ha      Float64
)
ENGINE = MergeTree
ORDER BY farm_id
COMMENT 'Farm dimension, one row per holding';

CREATE TABLE IF NOT EXISTS dim_field
(
    field_id   Int64,
    field_code String,
    farm_id    Int64,
    farm_code  String,
    field_name String,
    area_ha    Float64,
    -- LowCardinality because there are six soil classes across millions of
    -- fact rows; it makes the dictionary-encoded join key much cheaper.
    soil_type  LowCardinality(String),
    latitude   Float64,
    longitude  Float64
)
ENGINE = MergeTree
ORDER BY field_id
COMMENT 'Field dimension, one row per cultivated parcel';

CREATE TABLE IF NOT EXISTS dim_sensor
(
    sensor_id    Int64,
    sensor_code  String,
    field_id     Int64,
    field_code   String,
    farm_id      Int64,
    sensor_type  LowCardinality(String),
    model        LowCardinality(String),
    depth_cm     Int32,
    installed_on Date,
    status       LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY sensor_id
COMMENT 'Sensor dimension, one row per in-field probe';
