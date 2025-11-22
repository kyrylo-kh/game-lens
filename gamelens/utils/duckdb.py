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
            options = [
                "TYPE s3",
                f"KEY_ID '{settings.aws_access_key_id}'",
                f"SECRET '{settings.aws_secret_access_key}'",
                f"REGION '{settings.aws_region}'",
            ]
            if settings.aws_endpoint_url:
                endpoint = settings.aws_endpoint_url.replace('http://', '').replace('https://', '')
                options.append(f"ENDPOINT '{endpoint}'")
                options.append("USE_SSL false")
                options.append("URL_STYLE 'path'")

            options_str = ",\n                    ".join(options)

            connection.execute(f"""
                CREATE SECRET {secret_name} (
                    {options_str}
                );
            """)

        connection.execute(f"PRAGMA threads={threads};")

        yield connection
    finally:
        connection.close()
