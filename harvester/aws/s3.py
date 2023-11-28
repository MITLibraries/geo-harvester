"""harvester.aws.s3"""

import datetime
import logging

import boto3
from mypy_boto3_s3.client import S3Client as S3ClientType

from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)


class S3Client:
    @classmethod
    def get_client(cls) -> S3ClientType:
        return boto3.client("s3")

    @classmethod
    def list_objects(cls, bucket: str, prefix: str) -> list:
        """List objects for bucket + prefix"""
        client = cls.get_client()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
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
