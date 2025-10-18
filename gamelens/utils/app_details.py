from typing import List, Set

import pandas as pd

from gamelens.storage.constants import S3_STEAM_ACTIVE_APP_IDS_LATEST, S3_STEAM_APP_DETAILS_TEMPLATE
from gamelens.utils.duckdb import connect_duck


def get_unique_app_ids_from_all_app_details() -> Set[int]:
    """
    Fetch unique app IDs from all app details stored in S3.
    """
    with connect_duck() as connection:
        result = connection.execute(r"""
            SELECT array_agg(DISTINCT appid::BIGINT)
            FROM read_json(
            's3://gamelens/bronze/steam/appdetails/year=*/month=*/day=*/*.jsonl.gz',
            columns = {appid: 'BIGINT'},
            hive_partitioning = 1
            )
            WHERE appid IS NOT NULL
        """).fetchone()[0]

    return set(result) if result else set()


def get_active_app_ids() -> List[int]:
    """
    Fetch active app IDs from the latest active_app_ids parquet file in S3.
    """

    df = pd.read_parquet(S3_STEAM_ACTIVE_APP_IDS_LATEST, columns=["appid"])
    return df["appid"].dropna().astype("int64").tolist()


def get_app_details_for_date(date, columns=[]) -> pd.DataFrame:
    """
    Fetch app details for a specific date from S3.
    """
    s3_path = S3_STEAM_APP_DETAILS_TEMPLATE.format(
        year=date.year,
        month=f"{date.month:02}",
        day=f"{date.day:02}",
        file_name="*.jsonl.gz",
    )

    if not columns:
        select_clause = "*"
        empty_df = pd.DataFrame()
    else:
        cols = [f'"{col}"' for col in columns]
        select_clause = ", ".join(cols)
        empty_df = pd.DataFrame(columns=columns)

    with connect_duck() as con:
        n_files = con.execute(f"SELECT COUNT(*) FROM glob('{s3_path}')").fetchone()[0]
        if n_files == 0:
            return empty_df

        query = f"""
            SELECT {select_clause}
            FROM read_json('{s3_path}', hive_partitioning=1, union_by_name=true)
        """
        return con.execute(query).df()
