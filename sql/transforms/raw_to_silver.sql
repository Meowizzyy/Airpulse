INSERT INTO silver.air_quality_observations (
    event_id,
    city,
    latitude,
    longitude,
    observed_at,
    pm2_5,
    pm10,
    carbon_monoxide,
    nitrogen_dioxide,
    sulphur_dioxide,
    ozone,
    european_aqi,
    source,
    ingested_at
)
SELECT
    event_id,
    payload ->> 'city' AS city,
    (payload ->> 'latitude')::DOUBLE PRECISION AS latitude,
    (payload ->> 'longitude')::DOUBLE PRECISION AS longitude,
    (payload ->> 'observed_at')::TIMESTAMPTZ AS observed_at,
    (payload ->> 'pm2_5')::DOUBLE PRECISION AS pm2_5,
    (payload ->> 'pm10')::DOUBLE PRECISION AS pm10,
    (payload ->> 'carbon_monoxide')::DOUBLE PRECISION AS carbon_monoxide,
    (payload ->> 'nitrogen_dioxide')::DOUBLE PRECISION AS nitrogen_dioxide,
    (payload ->> 'sulphur_dioxide')::DOUBLE PRECISION AS sulphur_dioxide,
    (payload ->> 'ozone')::DOUBLE PRECISION AS ozone,
    (payload ->> 'european_aqi')::INTEGER AS european_aqi,
    source,
    ingested_at
FROM raw.kafka_events
ON CONFLICT (event_id) 
DO UPDATE SET
    city = EXCLUDED.city,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    observed_at = EXCLUDED.observed_at,
    pm2_5 = EXCLUDED.pm2_5,
    pm10 = EXCLUDED.pm10,
    carbon_monoxide = EXCLUDED.carbon_monoxide,
    nitrogen_dioxide = EXCLUDED.nitrogen_dioxide,
    sulphur_dioxide = EXCLUDED.sulphur_dioxide,
    ozone = EXCLUDED.ozone,
    european_aqi = EXCLUDED.european_aqi,
    source = EXCLUDED.source,
    ingested_at = EXCLUDED.ingested_at;