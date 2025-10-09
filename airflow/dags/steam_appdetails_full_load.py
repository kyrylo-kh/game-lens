import logging
import re
from datetime import datetime
from typing import Dict, List

import boto3
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.stats import Stats
from gamelens.clients.steam_client import SteamAPI
from gamelens.storage.constants import S3_STEAM_APP_DETAILS_TEMPLATE, S3_STEAM_APP_LIST_TEMPLATE
from gamelens.utils.app_details import get_unique_app_ids_from_all_app_details

logger = logging.getLogger(__name__)

PARTITION_SIZE = 1000
PROGRESS_LOG_INTERVAL = 100

@dag(
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    params={
        "app_list_date": None  # YYYY-MM-DD or None for DAG run date
    },
    tags=["manual", "one-off", "steam"],
)
def steam_appdetails_full_load_dag():
    """
    Full Steam app details load.
    """

    @task()
    def get_app_list() -> List[Dict]:
        """Load Steam app list from S3 partition based on date parameter."""
        context = get_current_context()
        app_list_date_param = context["params"]["app_list_date"]

        if app_list_date_param:
            app_list_date = datetime.strptime(app_list_date_param, "%Y-%m-%d")
        else:
            app_list_date = datetime.strptime(context["ds"], "%Y-%m-%d")

        s3_path = S3_STEAM_APP_LIST_TEMPLATE.format(
            year=app_list_date.year,
            month=f"{app_list_date.month:02}",
            day=f"{app_list_date.day:02}",
            file_name="applist.jsonl.gz"
        )

        logger.info(f"Loading app list from: {s3_path}")

        df = pd.read_json(s3_path, compression="gzip", lines=True)

        if "applist" in df.columns and "apps" in df.iloc[0]["applist"]:
            apps = df.iloc[0]["applist"]["apps"]
        else:
            apps = df.to_dict("records")

        logger.info(f"Loaded {len(apps)} apps from S3 partition {app_list_date.strftime('%Y-%m-%d')}")
        return apps

    @task()
    def determine_apps_to_process(apps: List[Dict]) -> List[int]:
        """
        Determine which apps to fetch.

        Exclude already processed apps based on existing app details in S3.
        """

        already_processed_appids = get_unique_app_ids_from_all_app_details()
        logger.info(f"Found {len(already_processed_appids)} already processed app IDs in all app details")

        appids_to_process = [app.get("appid", 0) for app in apps if app.get("appid", 0) not in already_processed_appids]
        logger.info(f"Determined {len(appids_to_process)} apps to process after excluding already processed")

        return appids_to_process

    def _save_batch_to_s3(results: List[Dict], date: datetime, batch_idx: int) -> None:
        """Save a batch of results to S3."""
        if not results:
            return

        filename = f"appdetails_first_part_{batch_idx:03d}.jsonl.gz"
        s3_path = S3_STEAM_APP_DETAILS_TEMPLATE.format(
            year=date.year,
            month=f"{date.month:02}",
            day=f"{date.day:02}",
            file_name=filename
        )

        df = pd.DataFrame(results)
        df.to_json(s3_path, orient="records", lines=True, compression="gzip")

        logger.info(f"Saved batch {batch_idx}: {s3_path} ({len(results)} apps)")

    def get_start_batch_index(date: datetime) -> int:
        s3 = boto3.client("s3")
        objects = s3.list_objects_v2(
            Bucket="gamelens",
            Prefix=f"bronze/steam/appdetails/year={date.year}/month={date.month:02}/day={date.day:02}/"
        )
        pattern = re.compile(r"appdetails_first_part_(\d{3})\.jsonl\.gz")
        indices = []
        for obj in objects.get("Contents", []):
            match = pattern.search(obj["Key"])
            if match:
                indices.append(int(match.group(1)))

        index = max(indices) + 1 if indices else 0

        logger.info(f"Existing batch indices for {date.strftime('%Y-%m-%d')}: {indices}")
        logger.info(f"Starting new batch index at: {index}")

        return index

    @task()
    def process_apps(appids: List[int]):
        """Process app IDs with memory-efficient batching."""
        context = get_current_context()
        date = datetime.strptime(context["ds"], "%Y-%m-%d")
        # NOTE: 200 requests allowed per 5 minutes = 1 request per ~1.5 seconds
        steam_client = SteamAPI(timeout=60, max_retries=22, backoff_factor=15.0, requests_delay=1.6)
        total_error = 0
        total_successful = 0
        current_batch = []
        batch_idx = get_start_batch_index(date)

        context = get_current_context()
        date = datetime.strptime(context["ds"], "%Y-%m-%d")

        logger.info(f"Starting to process {len(appids)} apps")

        for i, appid in enumerate(appids):
            try:
                app_details = steam_client.get_app_details(appid)

                if app_details.get("success"):
                    result = {
                        "appid": appid,
                        "data": app_details.get("data"),
                        "success": True,
                        "fetched_at": datetime.utcnow().isoformat()
                    }
                    total_successful += 1
                    current_batch.append(result)
                else:
                    logger.warning(f"Failed to fetch details for app {appid}: {app_details}")
                    total_error += 1

            except Exception as e:
                logger.error(f"Error processing app {appid}: {e}")
                total_error += 1

            if total_error > 1000:
                logger.error("Too many errors encountered, aborting processing.")
                break

            if len(current_batch) >= PARTITION_SIZE:
                _save_batch_to_s3(current_batch, date, batch_idx)
                current_batch.clear()
                batch_idx += 1

            if (i + 1) % PROGRESS_LOG_INTERVAL == 0:
                logger.info(f"Processed {i + 1}/{len(appids)} apps ({total_successful} successful)")

        if current_batch:
            _save_batch_to_s3(current_batch, date, batch_idx)

        logger.info(f"Completed processing: {total_successful} apps successful")

        Stats.incr("steam.appdetails.successful", count=total_successful)


    apps = get_app_list()
    appids_to_process = determine_apps_to_process(apps)
    process_apps(appids_to_process)


steam_appdetails_full_load = steam_appdetails_full_load_dag()
