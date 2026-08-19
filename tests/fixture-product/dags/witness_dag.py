"""A minimal product DAG: three branches that must overlap, then a join.

PARALLEL ON PURPOSE. A one-task DAG cannot tell CeleryExecutor from
SequentialExecutor, which is how the emulator ran Sequential behind a green
witness for as long as it did. This platform declares Celery in its own compose
because Fabric forbids overriding the executor, and a fixture that could not
notice the difference would leave that claim untested here too.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def branch(name):
    def run():
        import json
        import time

        started = time.time()
        time.sleep(3)
        with open(f"/opt/airflow/dags/{name}.window", "w", encoding="utf-8") as fh:
            json.dump({"started": started, "ended": time.time()}, fh)

    return run


with DAG(
    "witness_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    fan = [
        PythonOperator(task_id=f"branch_{i}", python_callable=branch(f"branch_{i}"))
        for i in range(3)
    ]
    PythonOperator(task_id="join", python_callable=lambda: None) << fan
