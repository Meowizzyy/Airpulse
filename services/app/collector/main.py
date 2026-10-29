import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from app.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

INGESTION_API_URL = os.getenv(
    "INGESTION_API_URL",
    "http://localhost:8000/api/v1/air-quality-events",
)
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
OPEN_METEO_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


def fetch_with_retry(
    method: str,
    url: str,
    max_retries: int = 3,
    initial_delay: int = 2,
    **kwargs,
):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(method, url, **kwargs)
            status = response.status_code

            if 200 <= status < 300:
                return response

            if 400 <= status < 500 and status not in (408, 429):
                logger.error(
                    "Permanent HTTP error status=%s url=%s; retries stopped",
                    status,
                    url,
                )
                return None

            logger.warning(
                "Transient HTTP error status=%s url=%s attempt=%s/%s",
                status,
                url,
                attempt,
                max_retries,
            )

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            logger.warning(
                "Network error url=%s attempt=%s/%s error=%s",
                url,
                attempt,
                max_retries,
                exc,
            )

        if attempt < max_retries:
            sleep_time = initial_delay * (2 ** (attempt - 1))
            logger.info("Retrying in %s seconds", sleep_time)
            time.sleep(sleep_time)

    logger.error("Request failed after %s attempts url=%s", max_retries, url)
    return None


def get_air_quality_data():
    params = {
        "latitude": 43.2389,
        "longitude": 76.8897,
        "current": (
            "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,"
            "sulphur_dioxide,ozone,european_aqi"
        ),
        "timezone": "Asia/Almaty",
    }

    response = fetch_with_retry(
        "GET",
        OPEN_METEO_API_URL,
        params=params,
        timeout=10,
    )
    return response.json() if response is not None else None


def map_air_quality(data: dict) -> dict:
    current = data["current"]
    observed_at = datetime.fromisoformat(current["time"]).replace(
        tzinfo=ZoneInfo("Asia/Almaty")
    ).isoformat()

    return {
        "city": "Almaty",
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "observed_at": observed_at,
        "pm2_5": current["pm2_5"],
        "pm10": current["pm10"],
        "carbon_monoxide": current["carbon_monoxide"],
        "nitrogen_dioxide": current["nitrogen_dioxide"],
        "sulphur_dioxide": current["sulphur_dioxide"],
        "ozone": current["ozone"],
        "european_aqi": current["european_aqi"],
    }


def post_air_quality_data(air_quality: dict) -> bool:
    response = fetch_with_retry(
        "POST",
        INGESTION_API_URL,
        json=air_quality,
        timeout=10,
    )

    if response is None:
        return False

    logger.info("Air-quality observation published to ingestion API")
    return True


def run_collector():
    logger.info(
        "Collector started poll_interval_seconds=%s ingestion_api=%s",
        POLL_INTERVAL_SECONDS,
        INGESTION_API_URL,
    )

    while True:
        logger.info("Starting collection cycle")

        raw_data = get_air_quality_data()
        if raw_data is None:
            logger.error("Collection cycle skipped because Open-Meteo request failed")
        else:
            air_quality = map_air_quality(raw_data)
            logger.info(
                "Fetched Open-Meteo observation city=%s observed_at=%s aqi=%s",
                air_quality["city"],
                air_quality["observed_at"],
                air_quality["european_aqi"],
            )
            post_air_quality_data(air_quality)

        logger.info("Sleeping for %s seconds until next poll", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_collector()
