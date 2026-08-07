-- Materialized views.
--
-- A materialized view in ClickHouse is an insert trigger: it sees rows as they
-- arrive in the source table and writes the aggregate state to its own target.
-- The target table is therefore declared explicitly (TO ...) rather than
-- implicitly, so it can be truncated and rebuilt independently of the view.

-- Weekly rollup per field, for charts that span months where daily points would
-- be unreadable. Aggregate states rather than finished numbers, so partial
-- weeks combine correctly as more days arrive.
CREATE TABLE IF NOT EXISTS agg_field_soil_weekly
(
    week_start             Date,
    field_id               Int64,
    farm_id                Int64,
    region                 LowCardinality(String),
    soil_type              LowCardinality(String),
    days_observed          AggregateFunction(uniq, Date),
    avg_soil_moisture_pct  AggregateFunction(avg, Nullable(Float64)),
    avg_soil_temperature_c AggregateFunction(avg, Nullable(Float64)),
    min_soil_moisture_pct  AggregateFunction(min, Nullable(Float64)),
    max_soil_moisture_pct  AggregateFunction(max, Nullable(Float64)),
    dry_days               AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(week_start)
ORDER BY (field_id, week_start)
COMMENT 'Weekly soil rollup per field, as aggregate states';

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_field_soil_weekly
TO agg_field_soil_weekly
AS
SELECT
    toMonday(reading_date)                                     AS week_start,
    field_id,
    farm_id,
    region,
    soil_type,
    uniqState(reading_date)                                    AS days_observed,
    avgState(avg_soil_moisture_pct)                            AS avg_soil_moisture_pct,
    avgState(avg_soil_temperature_c)                           AS avg_soil_temperature_c,
    minState(min_soil_moisture_pct)                            AS min_soil_moisture_pct,
    maxState(max_soil_moisture_pct)                            AS max_soil_moisture_pct,
    sumState(toUInt64(moisture_stress = 'dry'))                AS dry_days
FROM agg_field_soil_daily
GROUP BY week_start, field_id, farm_id, region, soil_type;


-- Readable wrapper over the aggregate states above. Superset points at this,
-- not at the AggregatingMergeTree table, because -Merge functions in a chart
-- definition are an easy thing to get subtly wrong.
CREATE OR REPLACE VIEW v_field_soil_weekly AS
SELECT
    week_start,
    field_id,
    farm_id,
    region,
    soil_type,
    uniqMerge(days_observed)          AS days_observed,
    avgMerge(avg_soil_moisture_pct)   AS avg_soil_moisture_pct,
    avgMerge(avg_soil_temperature_c)  AS avg_soil_temperature_c,
    minMerge(min_soil_moisture_pct)   AS min_soil_moisture_pct,
    maxMerge(max_soil_moisture_pct)   AS max_soil_moisture_pct,
    sumMerge(dry_days)                AS dry_days
FROM agg_field_soil_weekly
GROUP BY week_start, field_id, farm_id, region, soil_type;


-- Current condition per field: the most recent day for which each field has
-- data. Backs the dashboard's status tiles.
CREATE OR REPLACE VIEW v_field_latest_condition AS
SELECT
    d.field_id,
    d.field_code,
    d.field_name,
    d.farm_code,
    d.farm_name,
    d.region,
    d.soil_type,
    d.field_area_ha,
    d.reading_date                AS last_reading_date,
    d.avg_soil_moisture_pct,
    d.avg_soil_temperature_c,
    d.avg_soil_ph,
    d.avg_soil_ec_ds_m,
    d.min_battery_pct,
    d.active_sensors,
    d.moisture_stress
FROM agg_field_soil_daily AS d
INNER JOIN
(
    SELECT field_id, max(reading_date) AS reading_date
    FROM agg_field_soil_daily
    GROUP BY field_id
) AS latest
ON d.field_id = latest.field_id AND d.reading_date = latest.reading_date;
