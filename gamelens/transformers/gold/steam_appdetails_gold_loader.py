import logging
from datetime import datetime
from pathlib import Path

import polars as pl

from gamelens.storage.constants import S3_STEAM_APP_DETAILS_SILVER_TEMPLATE
from gamelens.utils.clickhouse import exec_sql, insert_columnar
from gamelens.utils.common import POLARS_STORAGE_OPTIONS

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent.parent.parent / "models" / "sql"


class SteamAppDetailsGoldLoader:
    """Loads Silver Steam data into Gold ClickHouse schema."""

    def __init__(self, snapshot_date):
        self.snapshot_date = snapshot_date

    def ensure_schema(self) -> None:
        """Create ClickHouse tables/views if not exist."""
        logger.info("Ensuring ClickHouse schema...")
        schema_sql = (SQL_DIR / "ch_schema_gold.sql").read_text()
        exec_sql(schema_sql)

    def run(self) -> None:
        """Execute full Gold load for snapshot_date."""
        logger.info(f"Starting Gold load for {self.snapshot_date}")

        self.ensure_schema()

        # Read all silver tables
        df_main = self._read_silver_main()
        df_genres = self._read_silver_genres()
        df_categories = self._read_silver_categories()
        df_companies = self._read_silver_companies()

        if df_main.is_empty():
            logger.warning("No main data - skipping")
            return

        # Load fact & dim
        self._load_fact(df_main)
        self._load_dim(df_main, df_genres, df_categories, df_companies)

        logger.info(f"Gold load complete ({len(df_main)} games)")

    def _read_silver_main(self) -> pl.DataFrame:
        """Read main game_daily.parquet."""
        path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.snapshot_date.year,
            month=f"{self.snapshot_date.month:02d}",
            day=f"{self.snapshot_date.day:02d}",
            file_name="game_daily.parquet",
        )
        logger.info(f"Reading {path}")
        return pl.read_parquet(path, storage_options=POLARS_STORAGE_OPTIONS)

    def _read_silver_genres(self) -> pl.DataFrame:
        path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.snapshot_date.year,
            month=f"{self.snapshot_date.month:02d}",
            day=f"{self.snapshot_date.day:02d}",
            file_name="game_genre.parquet",
        )
        logger.info(f"Reading {path}")
        return pl.read_parquet(path, storage_options=POLARS_STORAGE_OPTIONS)

    def _read_silver_categories(self) -> pl.DataFrame:
        path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.snapshot_date.year,
            month=f"{self.snapshot_date.month:02d}",
            day=f"{self.snapshot_date.day:02d}",
            file_name="game_category.parquet",
        )
        logger.info(f"Reading {path}")
        return pl.read_parquet(path, storage_options=POLARS_STORAGE_OPTIONS)

    def _read_silver_companies(self) -> pl.DataFrame:
        path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.snapshot_date.year,
            month=f"{self.snapshot_date.month:02d}",
            day=f"{self.snapshot_date.day:02d}",
            file_name="game_company.parquet",
        )
        logger.info(f"Reading {path}")
        return pl.read_parquet(path, storage_options=POLARS_STORAGE_OPTIONS)

    def _load_fact(self, df: pl.DataFrame) -> None:
        """Load fact_game_daily via input() SQL."""
        logger.info(f"Loading fact ({len(df)} rows)")

        # Prepare columns
        fact = df.select(
            [
                pl.col("snapshot_date").cast(pl.Date),
                pl.col("appid").cast(pl.UInt32),
                pl.col("price").cast(pl.Float64),
                (pl.col("price") * 1.0).alias("price_usd"),  # TODO: real conversion
                pl.col("currency").cast(pl.Utf8).fill_null("USD"),
                pl.col("discount_percent").cast(pl.UInt8).fill_null(0),
                pl.col("recommendations_total").cast(pl.Int64).fill_null(0),
                pl.col("metacritic_score").cast(pl.Int16).fill_null(-1),
                pl.col("achievements_total").cast(pl.Int16).fill_null(0),
                pl.col("is_free").cast(pl.UInt8).fill_null(0),
                pl.col("fetched_at").cast(pl.Datetime),
            ]
        )

        data = {c: fact[c].to_list() for c in fact.columns}

        insert_columnar("fact_game_daily", data, snapshot_date=self.snapshot_date)
        logger.info("Fact loaded")

    def _load_dim(
        self, df: pl.DataFrame, df_genres: pl.DataFrame, df_categories: pl.DataFrame, df_companies: pl.DataFrame
    ) -> None:
        """Load dim_game with arrays."""
        logger.info(f"Loading dim ({len(df)} rows)")

        # Aggregate arrays
        genres_agg = (
            df_genres.group_by("appid").agg(pl.col("genre").alias("genres"))
            if not df_genres.is_empty()
            else pl.DataFrame({"appid": [], "genres": []}, schema={"appid": pl.UInt32, "genres": pl.List(pl.Utf8)})
        )

        cats_agg = (
            df_categories.group_by("appid").agg(pl.col("category").alias("categories"))
            if not df_categories.is_empty()
            else pl.DataFrame(
                {"appid": [], "categories": []}, schema={"appid": pl.UInt32, "categories": pl.List(pl.Utf8)}
            )
        )

        devs_agg = (
            df_companies.filter(pl.col("role") == "developer")
            .group_by("appid")
            .agg(pl.col("company").alias("developers"))
            if not df_companies.is_empty()
            else pl.DataFrame(
                {"appid": [], "developers": []}, schema={"appid": pl.UInt32, "developers": pl.List(pl.Utf8)}
            )
        )

        pubs_agg = (
            df_companies.filter(pl.col("role") == "publisher")
            .group_by("appid")
            .agg(pl.col("company").alias("publishers"))
            if not df_companies.is_empty()
            else pl.DataFrame(
                {"appid": [], "publishers": []}, schema={"appid": pl.UInt32, "publishers": pl.List(pl.Utf8)}
            )
        )

        dim = (
            df.select(
                [
                    pl.col("appid").cast(pl.UInt32),
                    pl.col("name").cast(pl.Utf8),
                    pl.col("type").cast(pl.Utf8).fill_null("unknown"),
                    pl.col("release_date").cast(pl.Date),
                    pl.col("required_age").cast(pl.UInt8).fill_null(0),
                    pl.col("coming_soon").cast(pl.UInt8).fill_null(0),
                ]
            )
            .unique(subset=["appid"], keep="last")
            .join(genres_agg, on="appid", how="left")
            .join(cats_agg, on="appid", how="left")
            .join(devs_agg, on="appid", how="left")
            .join(pubs_agg, on="appid", how="left")
            .with_columns(
                [
                    pl.col("genres").fill_null([]),
                    pl.col("categories").fill_null([]),
                    pl.col("developers").fill_null([]),
                    pl.col("publishers").fill_null([]),
                    pl.lit(datetime.utcnow()).alias("updated_at"),
                ]
            )
        )

        data = {c: dim[c].to_list() for c in dim.columns}

        insert_columnar("dim_game", data)
        logger.info("Dim loaded")
