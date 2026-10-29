CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.air_quality_observations (
    event_id          UUID PRIMARY KEY,
    city              TEXT NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude         DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    observed_at       TIMESTAMPTZ NOT NULL,
    pm2_5             DOUBLE PRECISION CHECK (pm2_5 >= 0),
    pm10              DOUBLE PRECISION CHECK (pm10 >= 0),
    carbon_monoxide   DOUBLE PRECISION CHECK (carbon_monoxide >= 0),
    nitrogen_dioxide  DOUBLE PRECISION CHECK (nitrogen_dioxide >= 0),
    sulphur_dioxide   DOUBLE PRECISION CHECK (sulphur_dioxide >= 0),
    ozone             DOUBLE PRECISION CHECK (ozone >= 0),
    european_aqi      INTEGER CHECK (european_aqi >= 0),
    source            TEXT NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
