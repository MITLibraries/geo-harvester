"""harvester.aws.s3"""

import datetime
import logging
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as S3ClientType

from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)


class S3Client:
    @classmethod
    def get_client(cls) -> "S3ClientType":
        return boto3.client("s3")

    @classmethod
    def list_objects(cls, bucket: str, prefix: str) -> list:
        """List objects for bucket + prefix

        Args:
            bucket: S3 bucket
            prefix: path prefix, where any files beginning with that path will be returned
        """
        client = cls.get_client()
        try:
            response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        except (
            client.exceptions.NoSuchBucket,
            client.exceptions.ClientError,
        ) as exc:
            logger.error(exc)  # noqa: TRY400
            message = (
                f"Could not list objects for: 's3://{bucket}/{prefix}', reason: {exc}"
            )
            raise ValueError(message) from exc
        if "Contents" in response:
            return list(response["Contents"])
        return []

    @classmethod
    def list_objects_uri_and_date(
        cls, bucket: str, prefix: str
    ) -> list[tuple[str, datetime.datetime]]:
        """Return tuple of full s3 path + last modified date for objects"""
        s3_objects = cls.list_objects(bucket, prefix)
        return [
            (
                f"s3://{bucket}/{s3_object['Key']}",
                convert_to_utc(s3_object["LastModified"]),
            )
            for s3_object in s3_objects
        ]
