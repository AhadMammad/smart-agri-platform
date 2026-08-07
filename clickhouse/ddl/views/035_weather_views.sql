-- Views joining weather to the soil series.
--
-- This is the join Phase 4 exists to make possible: rainfall on a field against
-- the soil moisture measured in it. Doing it once here means every chart asks
-- the same question the same way, rather than each rebuilding the join.

-- Rainfall against measured soil moisture, per field per day.
--
-- Restricted to `is_actual`: correlating observed moisture against a *forecast*
-- rainfall figure would be comparing a measurement with a prediction, and the
-- relationship it appears to show would be partly an artefact of the model that
-- produced the forecast.
CREATE OR REPLACE VIEW v_field_water_daily AS
SELECT
    w.weather_date                AS observation_date,
    w.field_id,
    w.field_code,
    w.farm_code,
    w.region,
    w.country_code,
    w.field_area_ha,
    s.soil_type,
    w.precipitation_mm,
    w.rainfall_7d_mm,
    w.et0_mm,
    w.et0_7d_mm,
    w.water_balance_mm,
    w.water_balance_7d_mm,
    w.gdd_base10,
    w.gdd_cumulative,
    w.temp_mean_c,
    w.aridity_flag,
    s.avg_soil_moisture_pct,
    s.min_soil_moisture_pct,
    s.max_soil_moisture_pct,
    s.avg_soil_temperature_c,
    s.moisture_stress,
    s.active_sensors,
    s.reading_count
FROM agg_field_weather_daily AS w
INNER JOIN agg_field_soil_daily AS s
    ON w.field_id = s.field_id AND w.weather_date = s.reading_date
WHERE w.is_actual;


-- Monthly water summary per field: what fell, what evaporated, and how the soil
-- responded. Backs the irrigation & water dashboard's headline tiles.
CREATE OR REPLACE VIEW v_field_water_monthly AS
SELECT
    toStartOfMonth(observation_date) AS month_start,
    field_id,
    field_code,
    farm_code,
    region,
    any(field_area_ha)               AS field_area_ha,
    sum(precipitation_mm)            AS rainfall_mm,
    sum(et0_mm)                      AS et0_mm,
    sum(water_balance_mm)            AS water_balance_mm,
    avg(avg_soil_moisture_pct)       AS avg_soil_moisture_pct,
    avg(temp_mean_c)                 AS avg_temp_c,
    max(gdd_cumulative)              AS gdd_cumulative,
    countIf(aridity_flag = 'dry')    AS dry_days,
    countIf(moisture_stress = 'dry') AS moisture_stressed_days
FROM v_field_water_daily
GROUP BY month_start, field_id, field_code, farm_code, region;


-- Latest weather per farm, for status tiles alongside the soil view.
CREATE OR REPLACE VIEW v_farm_latest_weather AS
SELECT
    f.farm_id,
    f.farm_code,
    f.weather_date AS last_weather_date,
    f.is_actual,
    f.temp_max_c,
    f.temp_min_c,
    f.temp_mean_c,
    f.precipitation_mm,
    f.et0_mm,
    f.wind_speed_max_kmh,
    f.weather_code
FROM fact_weather_daily AS f
INNER JOIN
(
    SELECT farm_id, max(weather_date) AS weather_date
    FROM fact_weather_daily
    WHERE is_actual
    GROUP BY farm_id
) AS latest
ON f.farm_id = latest.farm_id AND f.weather_date = latest.weather_date;
