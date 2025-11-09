import logging
from datetime import date

import polars as pl

from gamelens.storage.constants import S3_STEAM_APP_DETAILS_SILVER_TEMPLATE, S3_STEAM_APP_DETAILS_TEMPLATE

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

        logger.info(f"Transform complete ({len(df)} records)")

    def _transform(self, df_raw: pl.DataFrame) -> pl.DataFrame:
        logger.info("Transforming data...")
        df = df_raw.filter(pl.col("success"))
        df = df.with_columns([
            pl.col("data").struct.field("type").alias("type"),
            pl.col("data").struct.field("name").alias("name"),
            pl.col("data").struct.field("release_date").struct.field("date")
                .str.strptime(pl.Date, "%d %b, %Y", strict=False)
                .alias("release_date"),
            pl.col("data").struct.field("release_date").struct.field("coming_soon").alias("coming_soon"),
            pl.col("data").struct.field("required_age")
                .cast(pl.Utf8)
                .str.replace_all(r'\\\\', '')
                .str.replace_all(r'"', '')
                .str.replace_all(r'[^0-9]', '')
                .cast(pl.Int64, strict=False)
                .alias("required_age"),
            pl.col("data").struct.field("is_free").cast(pl.Boolean).alias("is_free"),
            # FIXME: convert to USD
            (pl.col("data").struct.field("price_overview").struct.field("final") / 100.0)
                .alias("price"),
            pl.col("data").struct.field("price_overview").struct.field("currency").alias("currency"),
            pl.col("data").struct.field("price_overview").struct.field("discount_percent")
                .cast(pl.Int64)
                .alias("discount_percent"),
            pl.col("data").struct.field("recommendations").struct.field("total")
                .cast(pl.Int64)
                .alias("recommendations_total"),
            pl.col("data").struct.field("metacritic").struct.field("score")
                .cast(pl.Int64)
                .alias("metacritic_score"),
            pl.col("data").struct.field("achievements").struct.field("total")
                .cast(pl.Int64)
                .alias("achievements_total"),
            pl.col("data").struct.field("genres")
                .list.eval(pl.element().struct.field("description"))
                .alias("genres"),
            pl.col("data").struct.field("categories")
                .list.eval(pl.element().struct.field("description"))
                .alias("categories"),
            pl.col("data").struct.field("developers").alias("developers"),
            pl.col("data").struct.field("publishers").alias("publishers"),
            # TODO: parse pc_requirements
            # pl.col("data").struct.field("pc_requirements").struct.field("minimum")
            #     .map_elements(parse_ram, return_dtype=pl.Int64)
            #     .alias("pc_min_ram_gb"),
        ])


        df = df.drop(["data", "success"])

        return df

    def _load_bronze(self) -> pl.DataFrame:
        s3_path = S3_STEAM_APP_DETAILS_TEMPLATE.format(
            year=self.date.year,
            month=f"{self.date.month:02}",
            day=f"{self.date.day:02}",
            file_name="appdetails_combined.jsonl.gz",
        )
        logger.info(f"Reading bronze from {s3_path}")
        df = pl.scan_ndjson(s3_path, infer_schema_length=10000).collect(streaming=True)
        logger.info(f"Loaded {len(df)} records from bronze")
        return df

    def _save_silver(self, df: pl.DataFrame) -> None:
        s3_path = S3_STEAM_APP_DETAILS_SILVER_TEMPLATE.format(
            year=self.date.year,
            month=f"{self.date.month:02}",
            day=f"{self.date.day:02}",
            file_name="appdetails.parquet",
        )
        df.write_parquet(
            s3_path,
            compression="snappy",
            use_pyarrow=True
        )

        logger.info(f"Saved {s3_path}")
