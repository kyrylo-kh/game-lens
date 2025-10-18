import logging
import re
import time
from datetime import date as datetime_date
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import boto3
import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.stats import Stats
from gamelens.clients.steam_client import SteamAPI
from gamelens.storage.constants import S3_STEAM_APP_DETAILS_TEMPLATE
from gamelens.utils.app_details import get_active_app_ids, get_app_details_for_date
from gamelens.utils.common import remove_all_tmp_files_from_s3

logger = logging.getLogger(__name__)


PARTITION_SIZE = 1_000
PROGRESS_LOG_INTERVAL = 500
FILE_NAME = "appdetails"
MAX_CONSECUTIVE_ERRORS = 50


def _save_batch_to_s3(results: List[Dict], date: datetime, batch_idx: int) -> None:
    if not results:
        return
    filename = f"tmp_{FILE_NAME}_{batch_idx:03d}.jsonl.gz"
    s3_path = S3_STEAM_APP_DETAILS_TEMPLATE.format(
        year=date.year,
        month=f"{date.month:02}",
        day=f"{date.day:02}",
        file_name=filename
    )
    df = pd.DataFrame(results)
    df.to_json(s3_path, orient="records", lines=True, compression="gzip")
    logger.info(f"Saved batch {batch_idx}: {s3_path} ({len(results)} apps)")

def _get_start_batch_index(date: datetime) -> int:
    s3 = boto3.client("s3")

    prefix = f"bronze/steam/appdetails/year={date.year}/month={date.month:02}/day={date.day:02}/"
    resp = s3.list_objects_v2(Bucket="gamelens", Prefix=prefix)
    pattern = re.compile(fr"tmp_{FILE_NAME}_(\d{3})\.jsonl\.gz")
    indices = []
    for obj in resp.get("Contents", []) or []:
        m = pattern.search(obj["Key"])
        if m:
            indices.append(int(m.group(1)))
    return (max(indices) + 1) if indices else 0

def _fetch_app_details(steam_client: SteamAPI, appid: int) -> Optional[Dict]:
    """
    Fetch app details from Steam API.
    """
    try:
        response = steam_client.get_app_details(appid, retry_on_unsuccessful=True)
        if response and response.get("success"):
            return {
                "appid": appid,
                "data": response.get("data"),
                "success": True,
                "fetched_at": datetime.utcnow().isoformat()
            }

        logger.warning(f"App {appid} returned success=False: {response}")
    except Exception as e:
        logger.warning(f"Error fetching app {appid}: {e}")

    return None

@dag(
    schedule="0 0 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_exponential_backoff": True,
        "retry_delay": timedelta(seconds=10)
    },
    tags=["steam", "incremental", "nightly"],
    start_date=pd.Timestamp("2025-10-18"),
)
def steam_appdetails_daily_update_dag():
    """
    Nightly incremental update.
    """

    @task()
    def load_target_appids() -> Set[int]:
        date = datetime_date.today()

        appids = get_active_app_ids()
        existing_appids = get_app_details_for_date(date)

        if not existing_appids.empty:
            logger.warning(
                f"Excluding {len(existing_appids)} already fetched app IDs for date {date.strftime('%Y-%m-%d')}"
            )
            appids = set(appids) - set(existing_appids["appid"].astype("int64").tolist())

        if not appids:
            logger.info("No app IDs to process, skipping DAG run.")
            raise AirflowSkipException("App IDs list is empty")

        logger.info(f"Selected {len(appids)} app_ids for fetch")
        return appids

    @task(retry_delay=timedelta(minutes=10))
    def process_apps(appids: Set[int]) -> Dict[str, int]:
        date = datetime_date.today()

        steam_client = SteamAPI(
            timeout=10,
            max_retries=1,
            backoff_factor=5.0,
            requests_delay=1.5,
        )

        total_ok = 0
        total_err = 0
        consecutive_errors = 0

        batch = []
        batch_idx = _get_start_batch_index(date)

        logger.info(f"Starting fetching for {len(appids)} app_ids")

        for i, appid in enumerate(appids, start=1):
            item = _fetch_app_details(steam_client, appid)

            if item:
                batch.append(item)
                total_ok += 1
                consecutive_errors = 0
            else:
                total_err += 1
                consecutive_errors += 1

            if len(batch) >= PARTITION_SIZE:
                _save_batch_to_s3(batch, date, batch_idx)
                batch.clear()
                batch_idx += 1

            if (i % PROGRESS_LOG_INTERVAL) == 0:
                logger.info(f"Progress {i}/{len(appids)} (ok={total_ok}, err={total_err})")

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.warning(f"{consecutive_errors} consecutive errors, cooling down...")
                time.sleep(60)
                consecutive_errors = 0

        if batch:
            _save_batch_to_s3(batch, date, batch_idx)

        Stats.incr("steam.appdetails.daily.ok", count=total_ok)
        Stats.incr("steam.appdetails.daily.err", count=total_err)
        logger.info(f"Daily update completed: ok={total_ok}, err={total_err}")

        return {"ok": total_ok, "err": total_err}

    @task()
    def combine_batches(_stats) -> None:
        date = datetime_date.today()
        df = get_app_details_for_date(date)

        if df.empty:
            logger.info(f"No app details fetched for {date.strftime('%Y-%m-%d')}, skipping combine.")
            return

        try:
            df.to_json(
                S3_STEAM_APP_DETAILS_TEMPLATE.format(
                    year=date.year,
                    month=f"{date.month:02}",
                    day=f"{date.day:02}",
                    file_name=f"{FILE_NAME}_combined.jsonl.gz"
                ),
                orient="records",
                lines=True,
                compression="gzip"
            )
        except Exception as e:
            logger.error(f"Error saving combined file for {date.strftime('%Y-%m-%d')}: {e}")
            raise

        logger.info(f"Saved combined app details for {date.strftime('%Y-%m-%d')} ({len(df)} apps)")
        remove_all_tmp_files_from_s3(S3_STEAM_APP_DETAILS_TEMPLATE.format(
            year=date.year,
            month=f"{date.month:02}",
            day=f"{date.day:02}",
            file_name=""
        ))

    appids = load_target_appids()
    stats = process_apps(appids)
    combine_batches(stats)

steam_appdetails_daily_update = steam_appdetails_daily_update_dag()
