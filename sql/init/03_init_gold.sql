CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_air_quality_stats (
    date            DATE NOT NULL,
    city            TEXT NOT NULL,
    avg_pm2_5       DOUBLE PRECISION CHECK (avg_pm2_5 >= 0),
    max_pm2_5       DOUBLE PRECISION CHECK (max_pm2_5 >= 0),
    avg_pm10        DOUBLE PRECISION CHECK (avg_pm10 >= 0),
    max_pm10        DOUBLE PRECISION CHECK (max_pm10 >= 0),
    avg_aqi         DOUBLE PRECISION CHECK (avg_aqi >= 0),
    max_aqi         DOUBLE PRECISION CHECK (max_aqi >= 0),
    poor_air_hours  INTEGER NOT NULL CHECK (poor_air_hours >= 0),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (date, city),
    CHECK (avg_pm2_5 IS NULL OR max_pm2_5 IS NULL OR avg_pm2_5 <= max_pm2_5),
    CHECK (avg_pm10 IS NULL OR max_pm10 IS NULL OR avg_pm10 <= max_pm10),
    CHECK (avg_aqi IS NULL OR max_aqi IS NULL OR avg_aqi <= max_aqi)
);

CREATE TABLE IF NOT EXISTS gold.air_quality_alerts (
    alert_id        BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    city            TEXT NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL,
    alert_type      TEXT NOT NULL CHECK (alert_type IN ('high_aqi', 'high_pm2_5', 'high_pm10')),
    severity        TEXT NOT NULL CHECK (severity IN ('warning', 'critical')),
    european_aqi    INTEGER NOT NULL CHECK (european_aqi >= 0),
    pm2_5           DOUBLE PRECISION NOT NULL CHECK (pm2_5 >= 0),
    pm10            DOUBLE PRECISION NOT NULL CHECK (pm10 >= 0),
    message         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(event_id, alert_type)
);
