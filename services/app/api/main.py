import logging
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, status

from app.kafka.producer.producer import create_producer, send_event
from app.logging_config import configure_logging
from app.schemas import AirQualityEvent, AirQualityObservation

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="AirPulse Ingestion API")
producer = create_producer()


@app.get("/health")
def get_health():
    return {"status": "ok"}


@app.post(
    "/api/v1/air-quality-events",
    status_code=status.HTTP_202_ACCEPTED,
)
def create_air_quality_event(event: AirQualityObservation):
    kafka_event = AirQualityEvent(
        event_id=uuid4(),
        event_type="air_quality.observed",
        event_version=1,
        source="open_meteo",
        ingested_at=datetime.now(timezone.utc),
        payload=event,
    )

    send_event(producer, kafka_event)
    logger.info(
        "Accepted air-quality event event_id=%s city=%s observed_at=%s",
        kafka_event.event_id,
        event.city,
        event.observed_at.isoformat(),
    )

    return kafka_event
