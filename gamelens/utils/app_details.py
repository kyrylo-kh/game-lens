from typing import Set

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
