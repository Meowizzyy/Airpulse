INSERT INTO gold.air_quality_alerts (
    event_id,
    city,
    observed_at,
    alert_type,
    severity,
    european_aqi,
    pm2_5,
    pm10,
    message
)
SELECT
    event_id,
    city,
    observed_at,
    'high_aqi' AS alert_type,
    CASE
        WHEN european_aqi < 80 THEN 'warning'
        ELSE 'critical'
    END AS severity,
    european_aqi,
    pm2_5,
    pm10,
    'European AQI threshold exceeded' AS message
FROM silver.air_quality_observations
WHERE european_aqi >= 60

UNION ALL

SELECT
    event_id,
    city,
    observed_at,
    'high_pm2_5' AS alert_type,
    CASE
        WHEN pm2_5 < 50 THEN 'warning'
        ELSE 'critical'
    END AS severity,
    european_aqi,
    pm2_5,
    pm10,
    'PM2.5 threshold exceeded' AS message
FROM silver.air_quality_observations
WHERE pm2_5 >= 25

UNION ALL

SELECT
    event_id,
    city,
    observed_at,
    'high_pm10' AS alert_type,
    CASE
        WHEN pm10 < 75 THEN 'warning'
        ELSE 'critical'
    END AS severity,
    european_aqi,
    pm2_5,
    pm10,
    'PM10 threshold exceeded' AS message
FROM silver.air_quality_observations
WHERE pm10 >= 45

ON CONFLICT (event_id, alert_type)
DO UPDATE SET
    severity = EXCLUDED.severity,
    european_aqi = EXCLUDED.european_aqi,
    pm2_5 = EXCLUDED.pm2_5,
    pm10 = EXCLUDED.pm10,
    message = EXCLUDED.message;
