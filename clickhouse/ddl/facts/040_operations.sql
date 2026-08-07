-- Operations facts: irrigation, inputs, harvest and cost.
--
-- ReplacingMergeTree keyed on the source primary key throughout. A re-ingest of
-- an overlapping window presents the same row twice, and the engine collapses
-- the duplicate rather than double-counting it into a total someone is about to
-- make a spending decision on.
--
-- Monthly partitions: a day is too granular at these volumes and would produce
-- thousands of tiny parts.

CREATE TABLE IF NOT EXISTS fact_irrigation
(
    irrigation_id    Int64,
    field_id         Int64,
    farm_id          Int64,
    planting_id      Nullable(Int64),
    event_ts         DateTime64(3, 'UTC'),
    event_date       Date,
    method           LowCardinality(String),
    water_source     LowCardinality(String),
    water_volume_m3  Float64,
    -- Depth is what compares to rainfall; volume is what compares to a pump
    -- meter. Both are kept because the dashboards ask both questions.
    depth_mm         Float64,
    duration_minutes Int32,
    energy_kwh       Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (field_id, event_ts, irrigation_id)
COMMENT 'Irrigation events, one row per application';

CREATE TABLE IF NOT EXISTS fact_input_application
(
    application_id       Int64,
    field_id             Int64,
    farm_id              Int64,
    planting_id          Nullable(Int64),
    applied_on           Date,
    input_type           LowCardinality(String),
    product_name         String,
    application_method   LowCardinality(String),
    rate_kg_ha           Float64,
    quantity_kg          Float64,
    unit_cost_usd_per_kg Nullable(Float64),
    total_cost_usd       Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(applied_on)
ORDER BY (field_id, applied_on, application_id)
COMMENT 'Fertiliser, pesticide and other input applications';

CREATE TABLE IF NOT EXISTS fact_harvest
(
    harvest_id           Int64,
    planting_id          Int64,
    field_id             Int64,
    farm_id              Int64,
    harvested_on         Date,
    area_harvested_ha    Float64,
    yield_tonnes         Float64,
    yield_t_ha           Float64,
    moisture_pct         Nullable(Float64),
    quality_grade        LowCardinality(String),
    price_usd_per_tonne  Nullable(Float64),
    revenue_usd          Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(harvested_on)
ORDER BY (field_id, harvested_on, harvest_id)
COMMENT 'Harvest outcomes, one row per completed planting';

CREATE TABLE IF NOT EXISTS fact_field_cost
(
    cost_id       Int64,
    field_id      Int64,
    farm_id       Int64,
    planting_id   Nullable(Int64),
    cost_date     Date,
    cost_category LowCardinality(String),
    amount_usd    Float64,
    description   Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(cost_date)
ORDER BY (field_id, cost_date, cost_id)
COMMENT 'Field-level spend by category';
