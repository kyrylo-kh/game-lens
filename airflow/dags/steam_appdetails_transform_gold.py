
from datetime import date, datetime, timedelta
from typing import Any, List

import s3fs

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from gamelens.storage.constants import S3_STEAM_APP_DETAILS_SILVER_TEMPLATE
from gamelens.transformers.gold.steam_appdetails_gold_loader import SteamAppDetailsGoldLoader
from gamelens.utils.common import S3_STORAGE_OPTIONS

with DAG(
    dag_id="steam_appdetails_transform_gold",
    start_date=datetime(2025, 10, 19),
    catchup=False,
    schedule=None,
    params={
        'mode': Param(
            default='custom',
            enum=['daily', 'custom'],
            description='Execution mode'
        ),
        'start_date': Param(
            default='2025-10-20',
            format='date',
            description='Start date for the date range (only for custom)'
        ),
        'end_date': Param(
            format='date',
            description='End date for the date range (only for custom)'
        ),
    },
    max_active_runs=3,
    max_active_tasks=3,
    tags=["steam", "gold"],
) as dag:

    @task(task_display_name='Generate dates')
    def get_date_range(
        start_date: str,
        end_date: str,
        params: dict[str, Any],
        ts: str
    ) -> List[date]:
        if params['mode'] == 'daily':
            return [datetime.fromisoformat(ts).date()]

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        return [
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
        ]

    @task(task_display_name='Load to Gold')
    def gold_load(process_date: date):
        path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=process_date.year,
            month=f"{process_date.month:02d}",
            day=f"{process_date.day:02d}",
            file_name="game_daily.parquet"
        )
        fs = s3fs.S3FileSystem(**S3_STORAGE_OPTIONS)
        if not fs.exists(path):
            raise AirflowSkipException(f"No Silver for {process_date}")

        loader = SteamAppDetailsGoldLoader(process_date)
        loader.run()

    dates = get_date_range(
        start_date='{{ params.start_date }}',
        end_date='{{ params.end_date }}',
    )

    gold_load.expand(process_date=dates)
