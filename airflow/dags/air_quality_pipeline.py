import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def check_raw_data():
    hook = PostgresHook(postgres_conn_id="airpulse_db")
    count = hook.get_first("SELECT COUNT(*) FROM raw.kafka_events;")[0]

    if count == 0:
        raise ValueError("RAW layer is empty")

    logger.info("RAW validation passed total_rows=%s", count)


def validate_silver():
    hook = PostgresHook(postgres_conn_id="airpulse_db")

    total_rows = hook.get_first(
        "SELECT COUNT(*) FROM silver.air_quality_observations;"
    )[0]

    if total_rows == 0:
        raise ValueError("Silver layer is empty")

    bad_rows_query = """
        SELECT COUNT(*)
        FROM silver.air_quality_observations
        WHERE
            event_id IS NULL
            OR city IS NULL
            OR latitude NOT BETWEEN -90 AND 90
            OR longitude NOT BETWEEN -180 AND 180
            OR pm2_5 < 0
            OR pm10 < 0
            OR european_aqi < 0;
    """

    bad_rows = hook.get_first(bad_rows_query)[0]

    if bad_rows > 0:
        raise ValueError(f"Silver validation failed: {bad_rows} invalid row(s) found")

    logger.info("Silver validation passed total_rows=%s", total_rows)


def validate_gold():
    hook = PostgresHook(postgres_conn_id="airpulse_db")

    daily_quality_query = """
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(
                CASE
                    WHEN avg_pm2_5 < 0
                      OR max_pm2_5 < 0
                      OR avg_pm10 < 0
                      OR max_pm10 < 0
                      OR avg_aqi < 0
                      OR max_aqi < 0
                      OR poor_air_hours < 0
                      OR avg_pm2_5 > max_pm2_5
                      OR avg_pm10 > max_pm10
                      OR avg_aqi > max_aqi
                    THEN 1 ELSE 0
                END
            ), 0) AS invalid_rows
        FROM gold.daily_air_quality_stats;
    """

    total_rows, invalid_daily_rows = hook.get_first(daily_quality_query)

    if total_rows == 0:
        raise ValueError("Gold daily statistics table is empty")

    if invalid_daily_rows > 0:
        raise ValueError(
            f"Gold daily validation failed: {invalid_daily_rows} invalid row(s) found"
        )

    alerts_quality_query = """
        SELECT COUNT(*)
        FROM gold.air_quality_alerts
        WHERE
            alert_type NOT IN ('high_aqi', 'high_pm2_5', 'high_pm10')
            OR severity NOT IN ('warning', 'critical')
            OR european_aqi < 0
            OR pm2_5 < 0
            OR pm10 < 0
            OR (alert_type = 'high_aqi' AND european_aqi < 60)
            OR (alert_type = 'high_pm2_5' AND pm2_5 < 25)
            OR (alert_type = 'high_pm10' AND pm10 < 45);
    """

    invalid_alert_rows = hook.get_first(alerts_quality_query)[0]

    if invalid_alert_rows > 0:
        raise ValueError(
            f"Gold alerts validation failed: {invalid_alert_rows} invalid row(s) found"
        )

    logger.info(
        "Gold validation passed daily_rows=%s invalid_alert_rows=%s",
        total_rows,
        invalid_alert_rows,
    )


with DAG(
    dag_id="air_quality_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    template_searchpath=["/opt/airflow/sql/transforms"],
    tags=["airpulse", "air-quality"],
) as air_quality_pipeline:

    start = EmptyOperator(task_id="start")

    check_raw_task = PythonOperator(
        task_id="check_raw_data",
        python_callable=check_raw_data,
    )

    raw_to_silver_task = SQLExecuteQueryOperator(
        task_id="raw_to_silver",
        conn_id="airpulse_db",
        sql="raw_to_silver.sql",
    )

    validate_silver_task = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver,
    )

    build_daily_gold_task = SQLExecuteQueryOperator(
        task_id="build_daily_gold",
        conn_id="airpulse_db",
        sql="silver_to_daily_gold.sql",
    )

    build_gold_alerts_task = SQLExecuteQueryOperator(
        task_id="build_gold_air_quality_alerts",
        conn_id="airpulse_db",
        sql="silver_to_gold_alerts.sql",
    )

    validate_gold_task = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
    )

    start >> check_raw_task >> raw_to_silver_task >> validate_silver_task
    validate_silver_task >> [build_daily_gold_task, build_gold_alerts_task]
    [build_daily_gold_task, build_gold_alerts_task] >> validate_gold_task
