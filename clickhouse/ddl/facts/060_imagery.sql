-- Imagery facts: satellite and drone vegetation indices.
--
-- Indices are Nullable because a pass lost to cloud is a real observation with
-- no signal. `usable` carries that judgement so a chart filters on a flag
-- rather than re-deriving a cloud threshold each time.

CREATE TABLE IF NOT EXISTS fact_field_index
(
    observation_id  Int64,
    field_id        Int64,
    farm_id         Int64,
    planting_id     Nullable(Int64),
    observed_on     Date,
    source          LowCardinality(String),
    platform        Nullable(String),
    ndvi            Nullable(Float64),
    ndwi            Nullable(Float64),
    evi             Nullable(Float64),
    cloud_cover_pct Nullable(Float64),
    resolution_m    Float64,
    usable          Bool
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_on)
ORDER BY (field_id, observed_on, observation_id)
COMMENT 'Vegetation index observations per field';
