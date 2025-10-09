import sys
from datetime import datetime, timedelta
from typing import List, Tuple

import requests
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

from airflow import DAG

sys.path.append("/opt/airflow/scripts")
from clickhouse_metrics import get_ecommerce_metrics  # noqa: E402

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def save_metrics_to_clickhouse():
    client = Client(
        host="clickhouse",
        port=9000,
        user="admin",
        password="password",
        database="ecommerce",
    )

    metrics: List[Tuple] = get_ecommerce_metrics()  # type: ignore

    insert_query = """
    INSERT INTO ecommerce.historical_metrics
    (timestamp, metric_name, metric_value, labels)
    VALUES
    """

    data = []
    for metric_name, metric_value, timestamp, labels in metrics:
        data.append(
            {
                "timestamp": timestamp,
                "metric_name": metric_name,
                "metric_value": float(metric_value),
                "labels": labels,
            }
        )

    if data:
        client.execute(insert_query, data)
        print(f"✅ Saved {len(data)} metrics to ClickHouse historical table")


def push_metrics_to_victoriametrics():
    print("=== Starting metrics collection ===")
    try:
        metrics: List[Tuple] = get_ecommerce_metrics()  # type: ignore
        print(f"✅ Retrieved {len(metrics)} metrics from ClickHouse")

        victoriametrics_url = "http://victoriametrics:8428/api/v1/import"

        for i, (metric_name, metric_value, timestamp, labels) in enumerate(metrics):
            print(
                f"  Metric {i + 1}: {metric_name} = {metric_value}, timestamp = {timestamp}"
            )

            metric_data = {
                "metric": {"__name__": metric_name, **labels},
                "values": [float(metric_value)],
                "timestamps": [int(timestamp.timestamp() * 1000)],
            }

            try:
                response = requests.post(
                    victoriametrics_url,
                    json=metric_data,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                response.raise_for_status()
                print("✅ Successfully sent to VictoriaMetrics")
            except Exception as e:
                print(f"❌ Error sending {metric_name}: {e}")

    except Exception as e:
        print(f"❌ Error in metrics collection: {e}")
        raise


with DAG(
    "ecommerce_metrics_to_vm",
    default_args=default_args,
    description="Push ecommerce metrics to VictoriaMetrics every minute",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:

    save_metrics = PythonOperator(
        task_id="save_metrics_to_clickhouse",
        python_callable=save_metrics_to_clickhouse,
    )

    push_metrics = PythonOperator(
        task_id="push_metrics_to_victoriametrics",
        python_callable=push_metrics_to_victoriametrics,
    )

    push_metrics >> save_metrics
