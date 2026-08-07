-- Machinery facts: operations, telemetry and faults.

CREATE TABLE IF NOT EXISTS fact_machine_operation
(
    operation_id     Int64,
    machine_id       Int64,
    field_id         Int64,
    farm_id          Int64,
    planting_id      Nullable(Int64),
    operation_type   LowCardinality(String),
    started_at       DateTime64(3, 'UTC'),
    finished_at      DateTime64(3, 'UTC'),
    operation_date   Date,
    duration_hours   Float64,
    area_covered_ha  Float64,
    fuel_used_litres Float64,
    distance_km      Nullable(Float64),
    operator_name    Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(operation_date)
ORDER BY (machine_id, started_at, operation_id)
COMMENT 'Field operations, one row per machine per job';


-- The densest table in the platform: machines emit while working and, more
-- sparsely, while parked. Ordered by machine then time because every query
-- against it is one machine's series over a window.
CREATE TABLE IF NOT EXISTS fact_machine_telemetry
(
    telemetry_id    Int64,
    machine_id      Int64,
    farm_id         Int64,
    operation_id    Nullable(Int64),
    reading_ts      DateTime64(3, 'UTC'),
    reading_date    Date,
    engine_hours    Float64,
    engine_running  Bool,
    is_idle         Bool,
    fuel_level_pct  Nullable(Float64),
    fuel_rate_l_per_h Nullable(Float64),
    engine_temp_c   Nullable(Float64),
    engine_rpm      Nullable(Int32),
    speed_kmh       Nullable(Float64),
    latitude        Nullable(Float64),
    longitude       Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(reading_date)
ORDER BY (machine_id, reading_ts)
PRIMARY KEY (machine_id, reading_ts)
COMMENT 'Machine telemetry, one row per machine per timestamp';


-- `resolved_at` is null while the fault is open, which is exactly what a
-- maintenance-due signal keys off — hence Nullable rather than a sentinel date.
CREATE TABLE IF NOT EXISTS fact_machine_fault
(
    fault_id        Int64,
    machine_id      Int64,
    farm_id         Int64,
    occurred_at     DateTime64(3, 'UTC'),
    occurred_date   Date,
    fault_code      LowCardinality(String),
    severity        LowCardinality(String),
    description     String,
    resolved_at     Nullable(DateTime64(3, 'UTC')),
    is_open         Bool,
    downtime_hours  Nullable(Float64),
    repair_cost_usd Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(occurred_date)
ORDER BY (machine_id, occurred_at, fault_id)
COMMENT 'Machine faults, open and resolved';
