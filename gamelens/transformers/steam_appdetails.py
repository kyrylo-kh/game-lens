import logging
from datetime import date

import polars as pl

from gamelens.storage.constants import S3_STEAM_APP_DETAILS_SILVER_TEMPLATE, S3_STEAM_APP_DETAILS_TEMPLATE
from gamelens.utils.common import POLARS_STORAGE_OPTIONS

logger = logging.getLogger(__name__)


class AppDetailsTransformerPolars:
    """Handles bronze - silver transformation for Steam appdetails."""

    def __init__(self, date: date) -> None:
        self.date = date

    def run(self) -> None:
        """Main entry point"""
        logger.info(f"Starting bronze to silver transformation of AppDetails for {self.date}")
        df_raw = self._load_bronze()
        if df_raw.is_empty():
            logger.warning("No data found - skipping")
            return

        df = self._transform(df_raw)

        self._save_silver(df)

        self._save_silver_bridges(df)

        logger.info(f"Transform complete ({len(df)} records)")

    def _transform(self, df_raw: pl.DataFrame) -> pl.DataFrame:
        logger.info("Transforming data...")
        df = df_raw.filter(pl.col("success"))
        df = df.with_columns(
            [
                pl.lit(self.date).alias("snapshot_date"),
                pl.col("appid").cast(pl.UInt32),
                pl.col("fetched_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False),
                pl.col("data").struct.field("type").alias("type"),
                pl.col("data").struct.field("name").alias("name"),
                pl.col("data")
                    .struct.field("release_date")
                    .struct.field("date")
                    .str.strptime(pl.Date, "%d %b, %Y", strict=False)
                    .alias("release_date"),
                pl.col("data").struct.field("release_date").struct.field("coming_soon").alias("coming_soon"),
                pl.col("data")
                    .struct.field("required_age")
                    .cast(pl.Utf8)
                    .str.replace_all(r"\\\\", "")
                    .str.replace_all(r'"', "")
                    .str.replace_all(r"[^0-9]", "")
                    .cast(pl.Int64, strict=False)
                    .alias("required_age"),
                pl.col("data").struct.field("is_free").cast(pl.Boolean).alias("is_free"),
                # Price: keep both minor (cents) and float
                pl.col("data").struct.field("price_overview").struct.field("final").alias("price_minor"),
                (pl.col("data").struct.field("price_overview").struct.field("final") / 100.0).alias("price"),
                pl.col("data").struct.field("price_overview").struct.field("currency").alias("currency"),
                pl.col("data")
                    .struct.field("price_overview")
                    .struct.field("discount_percent")
                    .cast(pl.Int64)
                    .alias("discount_percent"),
                pl.col("data")
                    .struct.field("recommendations")
                    .struct.field("total")
                    .cast(pl.Int64)
                    .fill_null(0)
                    .alias("recommendations_total"),
                pl.col("data")
                    .struct.field("metacritic")
                    .struct.field("score")
                    .cast(pl.Int64)
                    .alias("metacritic_score"),
                pl.col("data")
                    .struct.field("achievements")
                    .struct.field("total")
                    .cast(pl.Int64)
                    .alias("achievements_total"),
                pl.col("data")
                    .struct.field("genres")
                    .list.eval(pl.element().struct.field("description"))
                    .alias("genres"),
                pl.col("data")
                    .struct.field("categories")
                    .list.eval(pl.element().struct.field("description"))
                    .alias("categories"),
                pl.col("data").struct.field("developers").alias("developers"),
                pl.col("data").struct.field("publishers").alias("publishers"),
            ]
        )

        df = df.drop(["data", "success"])

        # Dedup by (appid, snapshot_date) keeping latest fetched_at
        df = df.sort("fetched_at", descending=True)
        df = df.unique(subset=["appid", "snapshot_date"], keep="first")

        return df

    def _load_bronze(self) -> pl.DataFrame:
        s3_path = S3_STEAM_APP_DETAILS_TEMPLATE.format(
            year=self.date.year,
            month=f"{self.date.month:02}",
            day=f"{self.date.day:02}",
            file_name="appdetails_combined.jsonl.gz",
        )
        logger.info(f"Reading bronze from {s3_path}")
        df = pl.scan_ndjson(
            s3_path, infer_schema_length=10000, storage_options=POLARS_STORAGE_OPTIONS
        ).collect(streaming=True)
        logger.info(f"Loaded {len(df)} records from bronze")
        return df

    def _save_silver(self, df: pl.DataFrame) -> None:
        df_main = df.drop(["genres", "categories", "developers", "publishers"])

        s3_path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.date.year,
            month=f"{self.date.month:02}",
            day=f"{self.date.day:02}",
            file_name="game_daily.parquet",
        )
        df_main.write_parquet(s3_path, compression="snappy", storage_options=POLARS_STORAGE_OPTIONS)
        logger.info(f"Saved main: {s3_path}")

    def _save_silver_bridges(self, df: pl.DataFrame) -> None:
        """Save exploded bridge tables for genres, categories, companies."""
        base_path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.date.year, month=f"{self.date.month:02}", day=f"{self.date.day:02}", file_name=""
        )

        df_genre = (
            df.select(["snapshot_date", "appid", "genres"])
            .filter(pl.col("genres").is_not_null())
            .explode("genres")
            .rename({"genres": "genre"})
            .filter(pl.col("genre").is_not_null())
        )
        df_genre.write_parquet(
            f"{base_path}game_genre.parquet", compression="snappy", storage_options=POLARS_STORAGE_OPTIONS
        )
        logger.info(f"Saved genres: {len(df_genre)} rows")

        df_categories = (
            df.select(["snapshot_date", "appid", "categories"])
            .filter(pl.col("categories").is_not_null())
            .explode("categories")
            .rename({"categories": "category"})
            .filter(pl.col("category").is_not_null())
        )
        df_categories.write_parquet(
            f"{base_path}game_category.parquet", compression="snappy", storage_options=POLARS_STORAGE_OPTIONS
        )
        logger.info(f"Saved categories: {len(df_categories)} rows")

        developers = (
            df.select(["snapshot_date", "appid", "developers"])
            .filter(pl.col("developers").is_not_null())
            .explode("developers")
            .rename({"developers": "company"})
            .filter(pl.col("company").is_not_null())
            .with_columns(pl.lit("developer").alias("role"))
        )

        publishers = (
            df.select(["snapshot_date", "appid", "publishers"])
            .filter(pl.col("publishers").is_not_null())
            .explode("publishers")
            .rename({"publishers": "company"})
            .filter(pl.col("company").is_not_null())
            .with_columns(pl.lit("publisher").alias("role"))
        )

        df_company = pl.concat([developers, publishers]).unique()
        df_company.write_parquet(
            f"{base_path}game_company.parquet", compression="snappy", storage_options=POLARS_STORAGE_OPTIONS
        )
        logger.info(f"Saved companies: {len(df_company)} rows")
