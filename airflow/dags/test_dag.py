import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

def print_hello():
    logger.info("Hello from Airflow DAG!")


with DAG(
    dag_id="test_hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test"],
) as dag:

    task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )
