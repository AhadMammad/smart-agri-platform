-- The remaining dimensions of the star schema.
--
-- All snapshot-loaded and small, so a plain MergeTree with a
-- TRUNCATE-then-insert refresh is both simplest and trivially idempotent.
-- Every column list matches the Silver contract it is loaded from; the load
-- fails loudly on a mismatch rather than silently dropping a column.
--
-- LowCardinality wherever the domain is a closed set — six soil classes, five
-- irrigation methods, three drought-tolerance bands. It is a dictionary
-- encoding, so it pays for itself on every join and every GROUP BY.

CREATE TABLE IF NOT EXISTS dim_region
(
    region_code      String,
    region_name      String,
    country_code     LowCardinality(String),
    country_name     LowCardinality(String),
    macro_region     LowCardinality(String),
    rainfall_pattern LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY region_code
COMMENT 'Agricultural region reference';

CREATE TABLE IF NOT EXISTS dim_soil_type
(
    soil_code        String,
    soil_name        String,
    -- The band this soil class holds water in. What makes a moisture reading
    -- interpretable: 18% is dry for clay and comfortable for sand.
    moisture_min_pct Float64,
    moisture_max_pct Float64,
    drainage         LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY soil_code
COMMENT 'Soil class reference, with the moisture band each class holds';

CREATE TABLE IF NOT EXISTS dim_crop
(
    crop_code       String,
    crop_name       String,
    category        LowCardinality(String),
    is_perennial    Bool,
    base_temp_c     Float64,
    gdd_to_maturity Int32,
    cycle_days      Int32,
    water_demand_mm Int32,
    -- The NDVI this crop reaches at full canopy. The scale every vigour
    -- comparison is made against.
    peak_ndvi       Float64
)
ENGINE = MergeTree
ORDER BY crop_code
COMMENT 'Crop reference, with the agronomic constants each cycle is modelled on';

CREATE TABLE IF NOT EXISTS dim_crop_variety
(
    variety_id           Int64,
    variety_code         String,
    crop_code            LowCardinality(String),
    variety_name         String,
    maturity_days        Int32,
    yield_potential_t_ha Float64,
    drought_tolerance    LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY variety_id
COMMENT 'Crop variety dimension';

CREATE TABLE IF NOT EXISTS dim_planting
(
    planting_id         Int64,
    field_id            Int64,
    farm_id             Int64,
    variety_id          Int64,
    season              LowCardinality(String),
    planted_on          Date,
    expected_harvest_on Date,
    area_ha             Float64,
    seed_rate_kg_ha     Nullable(Float64),
    -- growing | harvested | failed | terminated. Mutable, which is why the
    -- source is snapshot-extracted rather than watermarked.
    status              LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY planting_id
COMMENT 'Planting dimension, one row per crop cycle on a field';

CREATE TABLE IF NOT EXISTS dim_machine
(
    machine_id               Int64,
    machine_code             String,
    farm_id                  Int64,
    machine_type             LowCardinality(String),
    manufacturer             LowCardinality(String),
    model                    LowCardinality(String),
    year_built               Int32,
    rated_power_hp           Int32,
    purchase_date            Date,
    engine_hours_at_purchase Float64,
    status                   LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY machine_id
COMMENT 'Machine dimension, one row per fleet asset';
