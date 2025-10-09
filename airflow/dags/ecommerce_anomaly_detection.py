import statistics
from datetime import datetime, timedelta
from typing import List, Tuple

from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def detect_anomalies():
    """The main function for detecting anomalies in all metrics"""
    client = Client(
        host="clickhouse",
        port=9000,
        user="admin",
        password="password",
        database="ecommerce",
    )

    print("=== Starting anomaly detection ===")

    metrics_to_check = [
        "ecommerce_conversion_rate",
        "ecommerce_gmv",
        "ecommerce_cart_abandonment_rate",
    ]

    total_anomalies = 0

    for metric_name in metrics_to_check:
        print(f"Checking {metric_name}...")

        data: List = client.execute(  # type: ignore
            f"""
            SELECT metric_value, timestamp
            FROM ecommerce.historical_metrics
            WHERE metric_name = '{metric_name}'
            AND timestamp >= now() - interval 30 minute
            ORDER BY timestamp
        """
        )

        if len(data) < 5:
            print(f"⚠️  Not enough data: {len(data)} points")
            continue

        values = [float(row[0]) for row in data]
        timestamps = [row[1] for row in data]

        # Z-score detection
        historical_values = values[:-1]
        current_value = values[-1]
        current_timestamp = timestamps[-1]

        mean = statistics.mean(historical_values)
        stdev = statistics.stdev(historical_values) if len(historical_values) > 1 else 0

        if stdev > 0:
            z_score = abs((current_value - mean) / stdev)

            if z_score > 2.0:
                # Check if this anomaly has already been saved.
                query_result: List[Tuple] = client.execute(  # type: ignore
                    f"""
                    SELECT count()
                    FROM ecommerce.anomalies
                    WHERE metric_name = '{metric_name}'
                    AND timestamp = '{current_timestamp}'
                """
                )
                existing = query_result[0][0]

                if existing == 0:
                    print("🚨 ANOMALY DETECTED!")
                    print(f"     Value: {current_value:.4f}")
                    print(f"     Mean: {mean:.4f}")
                    print(f"     StdDev: {stdev:.4f}")
                    print(f"     Z-score: {z_score:.2f}")

                    save_anomaly(
                        client,
                        metric_name,
                        current_timestamp,
                        current_value,
                        z_score,
                        mean,
                        stdev,
                    )
                    total_anomalies += 1
                else:
                    print("📝 Anomaly already recorded")
            else:
                print(f"✅ Normal (z-score: {z_score:.2f})")
        else:
            print("📊 No variance in data")

    print(f"=== Anomaly detection complete: {total_anomalies} new anomalies found ===")
    return total_anomalies


def save_anomaly(client, metric_name, timestamp, value, z_score, mean, stdev):
    """Saves the detected anomaly in ClickHouse"""
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS ecommerce.anomalies (
            timestamp DateTime64(3),
            metric_name String,
            metric_value Float64,
            z_score Float64,
            mean_value Float64,
            stdev_value Float64,
            detected_at DateTime64(3) DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (metric_name, timestamp)
    """
    )

    client.execute(
        "INSERT INTO ecommerce.anomalies (timestamp, metric_name, metric_value, z_score, mean_value, stdev_value) VALUES",
        [
            {
                "timestamp": timestamp,
                "metric_name": metric_name,
                "metric_value": value,
                "z_score": z_score,
                "mean_value": mean,
                "stdev_value": stdev,
            }
        ],
    )


with DAG(
    "ecommerce_anomaly_detection",
    default_args=default_args,
    description="Detect anomalies in ecommerce metrics using Z-score",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
) as dag:

    detect_anomalies_task = PythonOperator(
        task_id="detect_anomalies", python_callable=detect_anomalies
    )
