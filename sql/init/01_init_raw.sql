CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.kafka_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version >= 1),
    source TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(topic, partition_id, offset_id)
);
