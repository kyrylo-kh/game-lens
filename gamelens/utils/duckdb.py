import os
from contextlib import contextmanager
from typing import Iterator

import duckdb

from gamelens.settings import settings


@contextmanager
def connect_duck(
    db_path: str = "/opt/airflow/storage/etl.duckdb",
    secret_name: str = "s3_default",
    threads: int = 8,
) -> Iterator[duckdb.DuckDBPyConnection]:
    # ensure folder exists (DuckDB creates the file if missing)
    d = os.path.dirname(db_path)
    if d:
        os.makedirs(d, exist_ok=True)

    connection = duckdb.connect(db_path)
    try:
        try:
            connection.execute("LOAD httpfs;")
        except duckdb.Error:
            connection.execute("INSTALL httpfs; LOAD httpfs;")

        secret_exists = connection.execute(
            "SELECT 1 FROM duckdb_secrets() WHERE name = ?",
            [secret_name]
        ).fetchone()

        if not secret_exists:
            connection.execute(f"""
                CREATE SECRET {secret_name} (
                    TYPE s3,
                    KEY_ID '{settings.aws_access_key_id}',
                    SECRET '{settings.aws_secret_access_key}',
                    REGION '{settings.aws_region}'
                );
            """)

        connection.execute(f"PRAGMA threads={threads};")

        yield connection
    finally:
        connection.close()
