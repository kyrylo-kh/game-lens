import logging

import boto3

logger = logging.getLogger(__name__)


def remove_all_tmp_files_from_s3(path_prefix: str) -> None:
    """
    Remove all temporary files from S3 with the given prefix.
    """
    s3 = boto3.client("s3")
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
