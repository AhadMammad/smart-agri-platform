-- Views completing the four dashboards.
--
-- The marts answer the daily questions directly. These cover the rollups and
-- "current state" questions that would otherwise be a GROUP BY or a
-- self-join rebuilt in every chart — and rebuilt slightly differently each
-- time, which is how two tiles on one dashboard end up disagreeing.

-- --- Field & crop health -----------------------------------------------------

-- Latest canopy state per field, for the status tiles.
CREATE OR REPLACE VIEW v_field_latest_crop_health AS
SELECT
    h.field_id,
    h.field_code,
    h.field_name,
    h.farm_code,
    h.region,
    h.soil_type,
    h.field_area_ha,
    h.observed_on AS last_observed_on,
    h.crop_code,
    h.crop_name,
    h.season,
    h.days_after_sowing,
    h.cycle_progress_pct,
    h.avg_ndvi,
    h.ndvi_vs_expected_pct,
    h.canopy_stage,
    h.vigour_flag
FROM agg_field_crop_health_daily AS h
INNER JOIN
(
    SELECT field_id, max(observed_on) AS observed_on
    FROM agg_field_crop_health_daily
    GROUP BY field_id
) AS latest
ON h.field_id = latest.field_id AND h.observed_on = latest.observed_on;


-- The canopy curve per planting: NDVI against cycle progress, which is the
-- shape the agronomy model is asserted to produce.
CREATE OR REPLACE VIEW v_planting_ndvi_curve AS
SELECT
    planting_id,
    crop_code,
    crop_name,
    season,
    region,
    field_code,
    observed_on,
    days_after_sowing,
    cycle_progress_pct,
    canopy_stage,
    avg_ndvi,
    peak_ndvi_expected,
    ndvi_vs_expected_pct
FROM agg_field_crop_health_daily
WHERE planting_id IS NOT NULL;


-- --- Irrigation & water ------------------------------------------------------

-- Irrigation, rainfall and the soil's response, per field per day.
--
-- This is the join the platform exists to make possible: what was applied, what
-- fell, what evaporated, and what the probes measured — in one row.
-- Restricted to actuals, for the same reason `v_field_water_daily` is: a
-- forecast rainfall figure compared against a measured moisture reading shows a
-- relationship partly produced by the forecast model.
CREATE OR REPLACE VIEW v_field_water_supply_daily AS
SELECT
    i.water_date,
    i.field_id,
    i.field_code,
    i.farm_code,
    i.region,
    i.country_code,
    i.soil_type,
    i.field_area_ha,
    i.irrigation_events,
    i.irrigation_mm,
    i.water_volume_m3,
    i.energy_kwh,
    i.irrigation_method,
    i.rainfall_mm,
    i.et0_mm,
    i.water_supplied_mm,
    i.water_deficit_mm,
    i.irrigation_share_pct,
    i.supply_status,
    s.avg_soil_moisture_pct,
    s.min_soil_moisture_pct,
    s.moisture_stress,
    s.active_sensors
FROM agg_field_irrigation_daily AS i
LEFT JOIN agg_field_soil_daily AS s
    ON i.field_id = s.field_id AND i.water_date = s.reading_date
WHERE i.is_actual;


-- Monthly water summary per field: applied, fallen, evaporated, and how many
-- days ran a deficit.
--
-- The ratio is computed in an outer SELECT over the totals rather than beside
-- them. ClickHouse resolves a bare column name to a SELECT alias where one
-- exists, so `sum(irrigation_mm)` next to `... AS irrigation_mm` reads as an
-- aggregate over an aggregate and is rejected outright.
CREATE OR REPLACE VIEW v_field_irrigation_monthly AS
SELECT
    *,
    if(water_supplied_mm > 0, irrigation_mm / water_supplied_mm * 100, NULL)
        AS irrigation_share_pct
FROM
(
    SELECT
        toStartOfMonth(water_date)         AS month_start,
        field_id,
        field_code,
        farm_code,
        region,
        any(field_area_ha)                 AS field_area_ha,
        sum(irrigation_events)             AS irrigation_events,
        sum(irrigation_mm)                 AS irrigation_mm,
        sum(water_volume_m3)               AS water_volume_m3,
        sum(energy_kwh)                    AS energy_kwh,
        sum(rainfall_mm)                   AS rainfall_mm,
        sum(et0_mm)                        AS et0_mm,
        sum(water_supplied_mm)             AS water_supplied_mm,
        sum(water_deficit_mm)              AS water_deficit_mm,
        countIf(supply_status = 'deficit') AS deficit_days
    FROM agg_field_irrigation_daily
    WHERE is_actual
    GROUP BY month_start, field_id, field_code, farm_code, region
);


-- --- Machinery & fleet -------------------------------------------------------

-- Fleet utilisation per machine over its whole series, for the ranking tables.
CREATE OR REPLACE VIEW v_machine_utilisation AS
SELECT
    *,
    if(area_covered_ha > 0, fuel_used_litres / area_covered_ha, NULL) AS fuel_per_ha
FROM
(
    SELECT
        machine_id,
        machine_code,
        machine_type,
        manufacturer,
        model,
        farm_code,
        region,
        min(activity_date)                      AS first_active_on,
        max(activity_date)                      AS last_active_on,
        count()                                 AS days_observed,
        countIf(utilisation_status = 'working') AS working_days,
        countIf(utilisation_status = 'idle')    AS idle_days,
        countIf(utilisation_status = 'parked')  AS parked_days,
        countIf(utilisation_status = 'down')    AS down_days,
        sum(operation_hours)                    AS operation_hours,
        sum(area_covered_ha)                    AS area_covered_ha,
        sum(fuel_used_litres)                   AS fuel_used_litres,
        avg(idle_ratio_pct)                     AS avg_idle_ratio_pct,
        sum(faults)                             AS faults,
        sum(critical_faults)                    AS critical_faults,
        sum(downtime_hours)                     AS downtime_hours,
        sum(repair_cost_usd)                    AS repair_cost_usd
    FROM agg_machine_daily
    GROUP BY machine_id, machine_code, machine_type, manufacturer, model, farm_code, region
);


-- Machines carrying an unresolved fault.
--
-- Keyed on the fault still being open, **not** on it having occurred on the
-- machine's most recent day. Those are different questions, and the second one
-- is nearly always answered "no": a fault raised in April is still open in
-- August, but the machine has reported six hundred healthy days since. Filtering
-- to the latest day emptied this view while twenty faults stood open.
CREATE OR REPLACE VIEW v_machine_attention AS
SELECT
    machine_id,
    machine_code,
    machine_type,
    farm_code,
    region,
    sum(m.open_faults)                       AS open_faults,
    sum(m.critical_faults)                   AS critical_faults,
    sum(m.downtime_hours)                    AS downtime_hours,
    sum(m.repair_cost_usd)                   AS repair_cost_usd,
    maxIf(m.activity_date, m.open_faults > 0) AS last_fault_on,
    max(m.activity_date)                     AS last_activity_date,
    argMax(m.utilisation_status, m.activity_date) AS current_status
FROM agg_machine_daily AS m
GROUP BY machine_id, machine_code, machine_type, farm_code, region
HAVING open_faults > 0;


-- --- Yield & economics -------------------------------------------------------

-- Season and crop rollup: the margin and efficiency league table.
CREATE OR REPLACE VIEW v_crop_season_economics AS
SELECT
    *,
    -- Area-weighted, not a mean of means: averaging per-hectare yields across
    -- fields of different sizes over-weights the small ones. Only the area that
    -- actually produced a yield counts, or a half-harvested season reads as a
    -- collapse in yield rather than a season still in progress.
    if(harvested_area_ha > 0, yield_tonnes / harvested_area_ha, NULL) AS yield_t_ha,
    if(area_ha > 0, gross_margin_usd / area_ha, NULL)                 AS margin_per_ha_usd
FROM
(
    SELECT
        season,
        crop_code,
        crop_name,
        crop_category,
        region,
        country_code,
        count()                                     AS plantings,
        countIf(outcome = 'harvested')              AS harvested,
        sum(p.area_ha)                              AS area_ha,
        -- `p.` is load-bearing: unqualified, `area_ha` here would resolve to
        -- the `sum(area_ha) AS area_ha` alias above and nest one aggregate
        -- inside another, which ClickHouse rejects.
        sum(if(p.yield_t_ha IS NULL, 0, p.area_ha)) AS harvested_area_ha,
        sum(p.yield_tonnes)                         AS yield_tonnes,
        sum(p.revenue_usd)                          AS revenue_usd,
        sum(p.cost_total_usd)                       AS cost_total_usd,
        sum(p.gross_margin_usd)                     AS gross_margin_usd,
        sum(p.irrigation_mm)                        AS irrigation_mm,
        sum(p.rainfall_mm)                          AS rainfall_mm,
        avg(p.water_use_efficiency_t_per_100mm)     AS water_use_efficiency_t_per_100mm,
        avg(p.gdd_accumulated)                      AS gdd_accumulated
    FROM agg_planting_economics AS p
    GROUP BY season, crop_code, crop_name, crop_category, region, country_code
);


-- Cost structure per planting, long rather than wide, so a stacked chart reads
-- it without naming eight columns.
CREATE OR REPLACE VIEW v_planting_cost_breakdown AS
SELECT planting_id, field_code, farm_code, region, season, crop_code, area_ha,
       category, amount_usd,
       if(cost_total_usd > 0, amount_usd / cost_total_usd * 100, NULL) AS share_pct
FROM agg_planting_economics
ARRAY JOIN
    ['seed', 'fertilizer', 'crop_protection', 'irrigation',
     'fuel', 'labour', 'machinery', 'other'] AS category,
    [cost_seed_usd, cost_fertilizer_usd, cost_crop_protection_usd, cost_irrigation_usd,
     cost_fuel_usd, cost_labour_usd, cost_machinery_usd, cost_other_usd] AS amount_usd;


-- Farm-level scorecard, for the top-line tiles across all four dashboards.
CREATE OR REPLACE VIEW v_farm_scorecard AS
SELECT
    *,
    if(area_ha > 0, gross_margin_usd / area_ha, NULL) AS margin_per_ha_usd
FROM
(
    SELECT
        farm_id,
        farm_code,
        region,
        country_code,
        count()               AS plantings,
        sum(area_ha)          AS area_ha,
        sum(yield_tonnes)     AS yield_tonnes,
        sum(revenue_usd)      AS revenue_usd,
        sum(cost_total_usd)   AS cost_total_usd,
        sum(gross_margin_usd) AS gross_margin_usd,
        sum(irrigation_mm)    AS irrigation_mm,
        sum(rainfall_mm)      AS rainfall_mm
    FROM agg_planting_economics
    GROUP BY farm_id, farm_code, region, country_code
);


-- --- Farm & field map ---------------------------------------------------------

-- Every farm and every field as one point each, long rather than two separate
-- tables, so the map is a single chart over a single dataset. `entity_type`
-- is the discriminator a chart colours/groups by; `soil_type` only applies to
-- fields and is NULL on farm rows.
CREATE OR REPLACE VIEW v_farm_field_locations AS
SELECT
    'farm'      AS entity_type,
    farm_id     AS entity_id,
    farm_code   AS entity_code,
    farm_name   AS entity_name,
    farm_id,
    farm_code,
    farm_name,
    NULL        AS soil_type,
    country_code,
    region,
    latitude,
    longitude,
    area_ha
FROM dim_farm
UNION ALL
SELECT
    'field'         AS entity_type,
    f.field_id      AS entity_id,
    f.field_code    AS entity_code,
    f.field_name    AS entity_name,
    f.farm_id,
    f.farm_code,
    m.farm_name,
    f.soil_type,
    m.country_code,
    m.region,
    f.latitude,
    f.longitude,
    f.area_ha
FROM dim_field AS f
INNER JOIN dim_farm AS m ON f.farm_id = m.farm_id;
