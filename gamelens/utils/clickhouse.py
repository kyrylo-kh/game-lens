import logging
import math
import os
from datetime import date
from typing import Any, Dict, List, Optional

from clickhouse_driver import Client

logger = logging.getLogger(__name__)

# TODO: make singleton client
def get_client() -> Client:
    """Return configured ClickHouse client."""
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", 9000))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DB", "gamelens")

    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        settings={
            "max_block_size": 200_000,
            "strings_encoding": "utf8",
        },
    )
    return client


def exec_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
    client = get_client()
    result = None
    try:
        statements = [s.strip() for s in sql.strip().split(";") if s.strip()]
        for idx, statement in enumerate(statements, 1):
            logger.debug(f"[ClickHouse] Executing statement {idx}/{len(statements)}")
            result = client.execute(statement, params or {})
        return result
    except Exception as e:
        logger.error(f"[ClickHouse] exec_sql error: {e}\nSQL: {sql}\nParams: {params}")
        raise


def insert_columnar(
    table: str,
    data: Dict[str, List[Any]],
    batch_size: int = 3000,
    snapshot_date: Optional[date] = None,
    delete_snapshot_date: Optional[bool] = False,
) -> int:
    logger.info(f"[ClickHouse] insert_columnar: {table}, snapshot_date: {snapshot_date}")
    client = get_client()
    if not data:
        logger.warning(f"[ClickHouse] insert_columnar: no data for {table}")
        return 0

    columns = list(data.keys())
    n = len(next(iter(data.values())))
    if n == 0:
        logger.warning(f"[ClickHouse] insert_columnar: empty batch for {table}")
        return 0

    total_inserted = 0
    num_batches = math.ceil(n / batch_size)

    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, n)
        chunk = [data[c][start:end] for c in columns]

        try:
            if delete_snapshot_date:
                client.execute(
                    f"ALTER TABLE {table} DELETE WHERE snapshot_date = toDate('{snapshot_date.strftime('%Y-%m-%d')}')",
                )

            client.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
                chunk,
                types_check=True,
                columnar=True,
            )
            total_inserted += len(chunk[0])
        except Exception as e:
            logger.error(f"[ClickHouse] insert_columnar error batch {i+1}/{num_batches}: {e}")
            raise RuntimeError(f"Failed to insert batch {i+1} into {table}") from e

    logger.info(f"[ClickHouse] inserted {total_inserted} rows into {table}")
    return total_inserted
