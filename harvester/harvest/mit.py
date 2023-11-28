"""harvester.harvest.mit"""

import datetime
import glob
import logging
import os

from attrs import define, field

from harvester.aws.s3 import S3Client
from harvester.aws.sqs import SQSClient
from harvester.harvest import Harvester

logger = logging.getLogger(__name__)


@define
class MITHarvester(Harvester):
    """Harvester of MIT GIS layer zip files."""

    input_files: str = field(default=None)
    sqs_topic_name: str = field(default=None)

    def full_harvest(self) -> None:
        """Perform full harvest of MIT GIS zip files.

        Description of harvest: docs.mit_harvests.md
        """
        # abort immediately if SQS queue is not empty
        if not self.sqs_queue_is_empty():
            message = (
                "Cannot perform full harvest when SQS queue has unprocessed messages"
            )
            raise RuntimeError(message)

        # retrieve ALL zip files from input location
        zip_files = self.list_zip_files()
        if len(zip_files) == 0:
            message = f"Zero zip files found at input path: {self.input_files}, aborting"
            raise ValueError(message)

        message = f"{len(zip_files)} zip file(s) identified for full harvest"
        logger.info(message)

    def incremental_harvest(self) -> None:
        """Perform incremental harvest of MIT GIS zip files."""
        raise NotImplementedError  # pragma: nocover

    def sqs_queue_is_empty(self) -> bool:
        """Check if SQS with file modifications is empty."""
        return SQSClient(self.sqs_topic_name).get_message_count() == 0

    def list_zip_files(self) -> list[tuple[str, datetime.datetime]]:
        """Get list of zip files from local or S3, filtering by modified date if set.

        Returns:
            list of tuples of (filename:str, modified date:DateTime)
        """
        if self.input_files.startswith("s3://"):
            zip_files = self._list_s3_zip_files()
        else:
            zip_files = self._list_local_zip_files()

        # filter by dates if set
        if self.from_datetime_obj:
            zip_files = [item for item in zip_files if item[1] >= self.from_datetime_obj]
        if self.until_datetime_obj:
            zip_files = [item for item in zip_files if item[1] < self.until_datetime_obj]

        return zip_files

    def _list_s3_zip_files(self) -> list[tuple[str, datetime.datetime]]:
        """Get list of zip files from S3."""
        bucket, prefix = self.input_files.replace("s3://", "").split("/", 1)
        s3_objects = S3Client.list_objects_uri_and_date(bucket, prefix)
        return [
            s3_object for s3_object in s3_objects if s3_object[0].lower().endswith(".zip")
        ]

    def _list_local_zip_files(self) -> list[tuple[str, datetime.datetime]]:
        """Get list of zip files from local filesystem."""
        # Manually throw an exception if the base path does not exist as
        #   glob will still return an empty list, somewhat hiding that fact
        if not os.path.exists(self.input_files):
            message = f"Invalid input files path: {self.input_files}"
            raise ValueError(message)

        zip_filepaths = glob.glob(f"{self.input_files}/*.zip")
        return [
            (
                zip_filepath,
                datetime.datetime.fromtimestamp(
                    os.path.getmtime(zip_filepath), tz=datetime.UTC
                ),
            )
            for zip_filepath in zip_filepaths
        ]
