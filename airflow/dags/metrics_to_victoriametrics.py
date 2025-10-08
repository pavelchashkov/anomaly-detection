import sys
from datetime import datetime, timedelta

import requests
from airflow.operators.python import PythonOperator

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


def push_metrics_to_victoriametrics():
    metrics = get_ecommerce_metrics()

    victoriametrics_url = "http://victoriametrics:8428/api/v1/import"

    for metric_name, metric_value, timestamp, labels in metrics:  # type: ignore # TODO add check type Iterable[Tuple]
        metric_data = {
            "metric": metric_name,
            "value": float(metric_value),
            "timestamp": int(timestamp.timestamp()),
            "labels": labels,
        }

        try:
            response = requests.post(
                victoriametrics_url,
                json=metric_data,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            print(f"Successfully sent metric: {metric_name} = {metric_value}")
        except Exception as e:
            print(f"Error sending metric {metric_name}: {e}")


with DAG(
    "ecommerce_metrics_to_vm",
    default_args=default_args,
    description="Push ecommerce metrics to VictoriaMetrics every minute",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:

    push_metrics = PythonOperator(
        task_id="push_metrics_to_victoriametrics",
        python_callable=push_metrics_to_victoriametrics,
    )
