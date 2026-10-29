import logging
import os

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092",
)
TOPIC = os.getenv("KAFKA_RAW_TOPIC", "airquality.raw.v1")

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "enable.idempotence": True,
}


def create_producer() -> Producer:
    logger.info("Creating Kafka producer bootstrap_servers=%s", KAFKA_BOOTSTRAP_SERVERS)
    return Producer(PRODUCER_CONFIG)


def delivery_report(err, msg):
    if err is not None:
        logger.error("Kafka delivery failed error=%s", err)
        return

    logger.info(
        "Kafka delivery succeeded topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def send_event(producer: Producer, event) -> None:
    key = event.payload.city.encode("utf-8")
    value = event.model_dump_json().encode("utf-8")

    producer.produce(
        topic=TOPIC,
        key=key,
        value=value,
        on_delivery=delivery_report,
    )

    remaining = producer.flush(timeout=10)
    if remaining:
        raise RuntimeError(f"Kafka producer still has {remaining} undelivered message(s)")
