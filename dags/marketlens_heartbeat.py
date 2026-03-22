"""Minimal DAG so the Airflow UI has a MarketLens-tagged pipeline after install."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _ping():
    return "ok"


with DAG(
    dag_id="marketlens_heartbeat",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["marketlens"],
) as dag:
    PythonOperator(task_id="ping", python_callable=_ping)
