INSERT INTO gold.daily_air_quality_stats (
    date, 
    city, 
    avg_pm2_5, 
    max_pm2_5, 
    avg_pm10, 
    max_pm10, 
    avg_aqi,
    max_aqi, 
    poor_air_hours
)
SELECT 
    observed_at :: DATE,
    city,
    AVG(pm2_5),
    MAX(pm2_5),
    AVG(pm10),
    MAX(pm10),
    AVG(european_aqi),
    MAX(european_aqi),
    SUM(
        CASE 
            WHEN european_aqi >= 60 THEN 1
                ELSE 0 
        END 
    )
FROM silver.air_quality_observations
GROUP BY observed_at :: DATE, city
ON CONFLICT (date, city)
DO UPDATE SET
    avg_pm2_5 = EXCLUDED.avg_pm2_5,
    max_pm2_5 = EXCLUDED.max_pm2_5,
    avg_pm10 = EXCLUDED.avg_pm10,
    max_pm10 = EXCLUDED.max_pm10,
    avg_aqi = EXCLUDED.avg_aqi,
    max_aqi = EXCLUDED.max_aqi,
    poor_air_hours = EXCLUDED.poor_air_hours,
    loaded_at = NOW();