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
    skip_sqs_check: bool = field(default=False)

    def full_harvest(self) -> None:
        """Perform full harvest of MIT GIS zip files."""
        # abort immediately if SQS queue is not empty
        if not self.skip_sqs_check and not self.sqs_queue_is_empty():
            message = (
                "Cannot perform full harvest when SQS queue has unprocessed messages"
            )
            raise RuntimeError(message)

        # retrieve ALL zip files from input location
        zip_files = self.list_zip_files()
        message = f"{len(zip_files)} zip file(s) identified for full harvest"
        logger.info(message)

        # process zip files
        for zip_file in zip_files:
            identifier = zip_file.split(".zip")[0]
            self.process_zip_file(identifier, "created", zip_file)

    def incremental_harvest(self) -> None:
        """Perform incremental harvest of MIT GIS zip files.

        The method SQSClient.get_valid_messages_iter() will iteratively fetch all valid
        messages from the queue.  Once all messages are fetched, it freezes them as a list
        in memory to avoid an infinite loop of fetching messages and failing to process
        a particular message.

        If a message is NOT valid during fetching, it will log this to Sentry and skip,
        but the message will remain in the queue for manual handling and analysis.
        """
        # get frozen list of SQS messages
        client = SQSClient(self.sqs_topic_name)
        zip_file_events = list(client.get_valid_messages_iter())
        message = f"{len(zip_file_events)} message(s) identified for incremental harvest"
        logger.info(message)

        # process zip files
        for zip_file_event in zip_file_events:
            self.process_zip_file(
                zip_file_event.zip_file_identifier,
                zip_file_event.event,
                zip_file_event.zip_file,
            )

    def process_zip_file(self, identifier: str, event: str, zip_file: str) -> None:
        """Process single GIS resource zip file.

        Flow:
            1. identify and retrieve XML metadata in zip file
            2. normalize to Aardvark
            3. write source + Aardvark metadata files to CDN:Public
            4. send EventBridge event noting zip file visibility
        """
        message = (
            f"Processing identifier: '{identifier}', "
            f"event: '{event}', "
            f"filepath: '{zip_file}'"
        )
        logger.debug(message)

    def sqs_queue_is_empty(self) -> bool:
        """Check if SQS with file modifications is empty."""
        return SQSClient(self.sqs_topic_name).get_message_count() == 0

    def list_zip_files(self) -> list[str]:
        """Get list of zip files from local or S3, filtering by modified date if set."""
        if self.input_files.startswith("s3://"):
            zip_file_tuples = self._list_s3_zip_files()
        else:
            zip_file_tuples = self._list_local_zip_files()

        # filter by modified dates if set
        return [
            zip_file
            for zip_file, modified_date in zip_file_tuples
            if (self.from_datetime_obj is None or modified_date >= self.from_datetime_obj)
            and (
                self.until_datetime_obj is None or modified_date < self.until_datetime_obj
            )
        ]

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
