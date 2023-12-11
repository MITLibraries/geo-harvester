"""harvester.harvest.mit"""

import datetime
import glob
import logging
import os
import zipfile
from collections.abc import Iterator
from typing import Literal

import smart_open  # type: ignore[import-untyped]
from attrs import define, field

from harvester.aws.s3 import S3Client
from harvester.aws.sqs import SQSClient, ZipFileEventMessage
from harvester.config import Config
from harvester.harvest import Harvester
from harvester.records import (
    FGDC,
    ISO19139,
    Record,
    SourceRecord,
)

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class MITHarvester(Harvester):
    """Harvester of MIT GIS layer zip files."""

    input_files: str = field(default=None)
    sqs_topic_name: str = field(default=None)
    skip_sqs_check: bool = field(default=False)
    preserve_sqs_messages: bool = field(default=False)
    _sqs_client: SQSClient = field(default=None)

    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by reading zip files from S3:CDN:Restricted.

        For full harvests, prevent running by raising RuntimeError if SQS queue is not
        empty.
        """
        CONFIG.check_required_env_vars()
        if not self.skip_sqs_check and not self._sqs_queue_is_empty():
            message = (
                "Cannot perform full harvest when SQS queue has unprocessed messages"
            )
            raise RuntimeError(message)

        for zip_file in self._list_zip_files():
            identifier = os.path.splitext(zip_file)[0].split("/")[-1]
            yield Record(
                identifier=identifier,
                source_record=self._create_source_record(
                    identifier=identifier,
                    zip_file=zip_file,
                    event="created",
                ),
            )

    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by fetching messages from SQS queue.

        The method SQSClient.get_valid_messages_iter() will iteratively fetch all valid
        messages from an SQS queue of messages that represent files modified in
        S3:CDN:Restricted.

        If a message is NOT valid during fetching, it will log this to Sentry and skip,
        but the message will remain in the queue for manual handling and analysis.
        """
        CONFIG.check_required_env_vars()
        for zip_file_event_message in self.sqs_client.get_valid_messages_iter():
            identifier = zip_file_event_message.zip_file_identifier
            yield Record(
                identifier=identifier,
                source_record=self._create_source_record(
                    identifier=identifier,
                    zip_file=zip_file_event_message.zip_file,
                    event=zip_file_event_message.event,
                    sqs_message=zip_file_event_message,
                ),
            )

    def harvester_specific_steps(self, records: Iterator[Record]) -> Iterator[Record]:
        """Harvest steps specific to MITHarvester

        Additional steps included:
            - sending EventBridge events
            - managing SQS messages after processing
        """
        records = self.filter_failed_records(self.send_eventbridge_event(records))
        if self.harvest_type == "incremental":
            records = self.filter_failed_records(self.delete_sqs_messages(records))
        yield from records

    @property
    def sqs_client(self) -> SQSClient:
        """Return an SQSClient, reusing if already cached on self."""
        if not self._sqs_client:
            self._sqs_client = SQSClient(self.sqs_topic_name)
        return self._sqs_client

    def send_eventbridge_event(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to send EventBridge events indicating access restrictions."""
        for record in records:
            message = f"Record {record.identifier}: sending EventBridge event"
            logger.debug(message)
            yield record

    def delete_sqs_messages(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to delete SQS message after record has been successfully processed."""
        if self.preserve_sqs_messages:
            message = "Flag preserve_sqs_messages set, skipping delete of SQS message"
            logger.warning(message)
        for record in records:
            if not self.preserve_sqs_messages:
                message = f"Record {record.identifier}: deleting SQS message"
                logger.debug(message)
                self.sqs_client.delete_message(
                    record.source_record.sqs_message.receipt_handle
                )
            yield record

    def _sqs_queue_is_empty(self) -> bool:
        """Check if SQS with file modifications is empty."""
        return self.sqs_client.get_message_count() == 0

    def _list_zip_files(self) -> list[str]:
        """Get list of zip files from local or S3, filtering by modified date if set."""
        if self.input_files.startswith("s3://"):
            zip_file_tuples = self._list_s3_zip_files()
        else:
            zip_file_tuples = self._list_local_zip_files()

        # filter by modified dates if set
        return [
            zip_file
            for zip_file, modified_date in zip_file_tuples
            if (
                self.from_datetime_object is None
                or modified_date >= self.from_datetime_object
            )
            and (
                self.until_datetime_object is None
                or modified_date < self.until_datetime_object
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
        # glob will still return an empty list, somewhat hiding that fact
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

    def _identify_and_read_metadata_file(
        self, identifier: str, zip_file: str
    ) -> tuple[str, bytes]:
        """Identify the metadata file in a zip file and read XML bytes.

        This method opens the zip file ONCE, and that object is passed to both
        _find_metadata_file() and _read_metadata_file() to reduce network
        round-trips.  In both cases, the zip file is never read in its entirety; only the
        zipped files are listed and the metadata read.  This is important as some MIT GIS
        zip files can be hundreds of megabytes if not gigabytes.
        """
        with smart_open.open(zip_file, "rb") as file_object, zipfile.ZipFile(
            file_object
        ) as zip_file_object:
            metadata_format, metadata_filename = self._find_metadata_file(
                zip_file_object, identifier
            )
            metadata_bytes = self._read_metadata_file(zip_file_object, metadata_filename)
            return metadata_format, metadata_bytes

    @staticmethod
    def _find_metadata_file(
        zip_file_object: zipfile.ZipFile, identifier: str
    ) -> tuple[str, str]:
        """Identify ISO19139 or FGDC metadata file in the zip file.

        The ordered dictionary is opinionated to return the ISO19139 metadata first if
        found.
        """
        ordered_expected_metadata_filenames = {
            "iso19139": [
                f"{identifier}/{identifier}.iso19139.xml",
                f"{identifier}.iso19139.xml",
            ],
            "fgdc": [
                f"{identifier}/{identifier}.xml",
                f"{identifier}.xml",
            ],
        }

        files = zip_file_object.namelist()
        for (
            metadata_format,
            metadata_filenames,
        ) in ordered_expected_metadata_filenames.items():
            for metadata_filename in metadata_filenames:
                if metadata_filename in files:
                    return metadata_format, metadata_filename

        message = "Could not find ISO19139 or FGDC metadata file in zip file"
        raise FileNotFoundError(message)

    @staticmethod
    def _read_metadata_file(
        zip_file_object: zipfile.ZipFile, metadata_filename: str
    ) -> bytes:
        """Read the metadata file from the zip file object."""
        with zip_file_object.open(metadata_filename, "r") as metadata_file_object:
            return metadata_file_object.read()

    def _create_source_record(
        self,
        identifier: str,
        zip_file: str,
        event: Literal["created", "deleted"],
        sqs_message: ZipFileEventMessage | None = None,
    ) -> SourceRecord:
        """Init a SourceRecord based on event and zip file."""
        metadata_format, data = self._identify_and_read_metadata_file(
            identifier, zip_file
        )
        source_record_classes = {
            "iso19139": ISO19139,
            "fgdc": FGDC,
        }
        source_record_class = source_record_classes[metadata_format]
        return source_record_class(
            data=data,
            event=event,
            zip_file_location=zip_file,
            sqs_message=sqs_message,
        )
