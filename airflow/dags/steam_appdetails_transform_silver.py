from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from gamelens.transformers.steam_appdetails import AppDetailsTransformerPolars


@dag(
    dag_id="transform_appdetails_dag",
    start_date=datetime(2025, 10, 19),
    catchup=False,
    schedule="@daily",
    params={
        "date": None  # YYYY-MM-DD or None for DAG run date
    },
    max_active_runs=2,
    max_active_tasks=2,
    tags=["steam", "silver", "transform"],
)
def steam_appdetails_transform_silver():
    """
    Loads Steam appdetails data from bronze layer, transforms it, validates and saves to silver layer.
    """

    @task()
    def transform():
        date_str = get_current_context().get("params", {}).get("date") or get_current_context().get("ds")
        process_date = datetime.strptime(date_str, "%Y-%m-%d")

        transformer = AppDetailsTransformerPolars(process_date)
        transformer.run()

    transform()

steam_appdetails_transform_silver_dag = steam_appdetails_transform_silver()
