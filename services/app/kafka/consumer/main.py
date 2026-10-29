import logging
import os

from confluent_kafka import Consumer

from app.kafka.consumer.db import create_connection, insert_raw_event
from app.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092",
)
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "airquality.raw.v1")
KAFKA_CONSUMER_GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP_ID",
    "airquality-raw-consumer-v1",
)

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_CONSUMER_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}


def run_consumer() -> None:
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([KAFKA_RAW_TOPIC])
    conn = create_connection()

    logger.info(
        "Consumer started bootstrap_servers=%s group_id=%s topic=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_CONSUMER_GROUP_ID,
        KAFKA_RAW_TOPIC,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Kafka consumer error=%s", msg.error())
                continue

            logger.info(
                "Received Kafka message topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

            try:
                insert_raw_event(conn, msg)
                conn.commit()

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

                logger.info(
                    "Committed message topic=%s partition=%s offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

            except Exception:
                conn.rollback()
                logger.exception(
                    "Message processing failed topic=%s partition=%s offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

    except KeyboardInterrupt:
        logger.info("Consumer shutdown requested")
    finally:
        conn.close()
        consumer.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    run_consumer()
