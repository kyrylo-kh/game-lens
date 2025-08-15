import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.stats import Stats
from airflow.utils.dates import days_ago
from gamelens.clients.steam_client import SteamAPI

logger = logging.getLogger(__name__)

S3_PATH_TEMPLATE = "s3://gamelens/bronze/steam/applist/year={year}/month={month}/day={day}/applist.jsonl.gz"

with DAG(
    dag_id="steam__applist_load__bronze_snapshot",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["steam", "bronze", "applist"],
    description="Daily snapshot of Steam applist to S3 bronze in JSONL",
) as dag:
    """
    Daily snapshot of Steam applist to S3 bronze in JSONL.
    """

    @task()
    def fetch_applist() -> List[Dict]:
        """
        Fetch the Steam App List.
        """
        return SteamAPI().get_app_list()

    @task()
    def save_to_lake(apps: List[Dict]):
        context = get_current_context()
        date = datetime.strptime(context["ds"], "%Y-%m-%d")

        s3_path = S3_PATH_TEMPLATE.format(
            year=date.year,
            month=f"{date.month:02}",
            day=f"{date.day:02}"
        )

        pd.DataFrame(apps).to_json(s3_path, orient="records", lines=True, compression="gzip")
        logger.info(f"Saved applist: {s3_path} rows={len(apps)}")

        Stats.incr("steam.applist.rows", count=len(apps))

    apps = fetch_applist()
    save_to_lake(apps)
