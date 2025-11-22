import logging
from typing import Any, Dict

import boto3

from gamelens.settings import settings

logger = logging.getLogger(__name__)


def build_s3_options() -> Dict[str, Any]:
    opts: Dict[str, Any] = {}

    if settings.aws_endpoint_url:
        opts.setdefault("client_kwargs", {})["endpoint_url"] = settings.aws_endpoint_url

    return opts


def remove_all_tmp_files_from_s3(path_prefix: str) -> None:
    """
    Remove all temporary files from S3 with the given prefix.
    """

    s3 = boto3.client("s3", endpoint_url=settings.aws_endpoint_url)
    bucket_name = path_prefix.split("/")[2]
    prefix = "/".join(path_prefix.split("/")[3:])

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    objects_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            if "tmp_" in obj["Key"]:
                objects_to_delete.append({"Key": obj["Key"]})

    assert len(objects_to_delete) <= 11, "Too many objects to delete at once."

    if objects_to_delete:
        logger.warning(
            f"Deleting {len(objects_to_delete)} temporary files from S3: {path_prefix}."
            f"\nFiles: {objects_to_delete}"
        )
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})


def build_polars_s3_options() -> Dict[str, str]:
    """
    Build storage options for Polars native readers (scan_parquet, scan_ndjson).
    These use the Rust object_store crate which expects flat string options.
    """
    opts = {}
    if settings.aws_endpoint_url:
        opts["endpoint"] = settings.aws_endpoint_url
        opts["allow_http"] = "true"

    if settings.aws_access_key_id:
        opts["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key:
        opts["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_region:
        opts["aws_region"] = settings.aws_region

    return opts


S3_STORAGE_OPTIONS = build_s3_options()
POLARS_STORAGE_OPTIONS = build_polars_s3_options()
