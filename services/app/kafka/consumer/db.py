import json
import logging
import os

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


def create_connection():
    database_url = os.getenv("NEON_DATABASE_URL")
    if not database_url:
        raise RuntimeError("NEON_DATABASE_URL environment variable is required")

    conn = psycopg.connect(database_url)
    logger.info("Connected to Neon PostgreSQL database=%s", conn.info.dbname)
    return conn


def insert_raw_event(conn, msg):
    raw_value = msg.value().decode("utf-8")
    event = json.loads(raw_value)
    event_key = msg.key().decode("utf-8") if msg.key() is not None else ""

    insert_query = """
        INSERT INTO raw.kafka_events (
            event_id,
            topic,
            partition_id,
            offset_id,
            event_key,
            event_type,
            event_version,
            source,
            ingested_at,
            payload
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (topic, partition_id, offset_id)
        DO NOTHING
        RETURNING id;
    """

    data_to_insert = (
        event["event_id"],
        msg.topic(),
        msg.partition(),
        msg.offset(),
        event_key,
        event["event_type"],
        event["event_version"],
        event["source"],
        event["ingested_at"],
        Jsonb(event["payload"]),
    )

    with conn.cursor() as cursor:
        cursor.execute(insert_query, data_to_insert)
        inserted = cursor.fetchone()

    if inserted:
        logger.info(
            "Inserted RAW event row_id=%s topic=%s partition=%s offset=%s",
            inserted[0],
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
        return inserted[0]

    logger.info(
        "RAW event already exists topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )
    return None
