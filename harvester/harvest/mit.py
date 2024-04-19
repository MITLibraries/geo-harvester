"""harvester.harvest.mit"""

import datetime
import fnmatch
import glob
import json
import logging
import os
import zipfile
from collections.abc import Iterator
from typing import Literal

import smart_open  # type: ignore[import-untyped]
from attrs import define, field

from harvester.aws.eventbridge import EventBridgeClient
from harvester.aws.s3 import S3Client
from harvester.aws.sqs import SQSClient, ZipFileEventMessage
from harvester.config import Config
from harvester.harvest import Harvester
from harvester.records import Record
from harvester.records.sources.mit import MITFGDC, MITISO19139

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class MITHarvester(Harvester):
    """Harvester of MIT GIS layer zip files."""

    input_files: str = field(default=None)
    sqs_topic_name: str = field(default=None)
    preserve_sqs_messages: bool = field(default=False)
    skip_eventbridge_events: bool = field(default=False)
    _sqs_client: SQSClient = field(default=None)

    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by reading zip files from S3:CDN:Restricted.

        For full harvests, prevent running by raising RuntimeError if SQS queue is not
        empty.
        """
        CONFIG.check_required_env_vars()

        for zip_file in self._list_zip_files():
            identifier = os.path.splitext(zip_file)[0].split("/")[-1]
            yield Record(
                identifier=identifier,
                source_record=self.create_source_record_from_zip_file(
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
                source_record=self.create_source_record_from_zip_file(
                    identifier=identifier,
                    zip_file=zip_file_event_message.zip_file,
                    event=zip_file_event_message.event,
                    sqs_message=zip_file_event_message,
                ),
            )

    def harvester_specific_steps(self, records: Iterator[Record]) -> Iterator[Record]:
        """Harvest steps specific to MITHarvester

        Additional steps included:
            - write source and normalized metadata records to CDN bucket
            - sending EventBridge events
            - managing SQS messages after processing
        """
        records = self.filter_failed_records(self.write_source_and_normalized(records))
        records = self.filter_failed_records(self.send_eventbridge_event(records))
        if self.harvest_type == "incremental":
            records = self.filter_failed_records(self.delete_sqs_messages(records))
        yield from records

    def write_source_and_normalized(self, records: Iterator[Record]) -> Iterator[Record]:
        """Write source and normalized metadata as standalone files.

        This step is driven by presence of one or both CLI options:
            "--output-source-directory": write source records
            "--output-normalized-directory": write normalized records

        Source and normalized metadata files are most commonly written to the public CDN
        bucket to facilitate download.
        """
        for record in records:
            # write source
            if self.output_source_directory:
                message = f"Record {record.identifier}: writing source metadata"
                logger.debug(message)
                try:
                    self._write_source_metadata(record)
                except Exception as exc:  # noqa: BLE001
                    record.exception_stage = "write_metadata.source"
                    record.exception = exc
                    yield record
                    continue  # pragma: nocover

            # write normalized
            if self.output_normalized_directory:
                message = f"Record {record.identifier}: writing normalized metadata"
                logger.debug(message)
                try:
                    self._write_normalized_metadata(record)
                except Exception as exc:  # noqa: BLE001
                    record.exception_stage = "write_metadata.normalized"
                    record.exception = exc
                    yield record
                    continue  # pragma: nocover

            yield record

    def _write_source_metadata(self, record: Record) -> None:
        """Write source metadata file."""
        source_metadata_filepath = (
            f"{self.output_source_directory.rstrip('/')}/"
            f"{record.source_record.source_metadata_filename.lstrip('/')}"
        )
        with smart_open.open(source_metadata_filepath, "wb") as source_file:
            source_file.write(record.source_record.data)

    def _write_normalized_metadata(self, record: Record) -> None:
        """Write normalized metadata file."""
        normalized_metadata_filepath = (
            f"{self.output_normalized_directory.rstrip('/')}/"
            f"{record.source_record.normalized_metadata_filename.lstrip('/')}"
        )
        with smart_open.open(normalized_metadata_filepath, "w") as normalized_file:
            normalized_file.write(record.normalized_record.to_json(pretty=False))

    def send_eventbridge_event(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to queue EventBridge events indicating access restrictions for a Record.

        These EventBridge events are ultimately handled by the StepFunction
        "geo-upload-<ENV>-shapefile-handler".  That StepFunction will take one of three
        paths based on the event payload:

            1. Copy zip file data from Restricted to Public CDN bucket
                - detail.restricted=false
            2. Delete zip file data from Public CDN bucket
                - detail.restricted=true
            3. Delete zip file data AND metadata from Public AND Restricted CDN bucket
                - detail.deleted=true

        The goal is to decouple knowing whether a record is deleted or restricted and
        actually managing files in S3.  By sending EventBridge events about the record's
        deleted and/or restricted status, the file management work is performed by another
        component that is not this GeoHarvester.

        This method pools all EventBridge events that would be sent for a given Record
        identifier, filtering to only include the last, then publishes all events after
        the Records iterator is fully processed for the harvest.  This is more efficient
        than publishing events as Records are processed, and allows for only publishing an
        event that reflects the current, most recent state of a single Record in S3.
        """
        bucket, path = CONFIG.S3_PUBLIC_CDN_ROOT.removeprefix("s3://").split("/", 1)
        path = path.removesuffix("/")

        # queue EventBridge events
        event_records: dict[str, Record] = {}
        for record in records:
            if not self.skip_eventbridge_events:
                event_records[record.identifier] = record
            yield record

        # after Records yielded, publish EventBridge events
        for record in event_records.values():
            message = f"Record {record.identifier}: sending EventBridge event"
            logger.debug(message)
            try:
                self._prepare_payload_and_send_event(bucket, path, record)
            except Exception:
                logger.exception("Error sending EventBridge event")

    def _prepare_payload_and_send_event(
        self, bucket: str, path: str, record: Record
    ) -> str:
        """Prepare event detail and send event.

        Example detail dictionary, which is serialized to JSON string:
            'Detail': {
                'bucket': 'cdn-origin-dev-XXX',
                'identifier': 'ABC123',
                'restricted': 'false',
                'deleted': 'true',
                'objects': [
                    {'Key': 'cdn/geo/public/ABC123.source.fgdc.xml'},
                    {'Key': 'cdn/geo/public/ABC123.normalized.aardvark.json'},
                    {'Key': 'cdn/geo/public/ABC123.zip'}
                ]
            }
        NOTE: consuming components are expecting bool STRINGS vs actual bools for fields
        'restricted' and 'deleted'
        """
        detail = {
            "bucket": bucket,
            "identifier": record.identifier,
            "restricted": json.dumps(record.source_record.is_restricted),
            "deleted": json.dumps(record.source_record.is_deleted),
            "objects": [
                {"Key": f"{path}/{record.source_record.source_metadata_filename}"},
                {"Key": f"{path}/{record.source_record.normalized_metadata_filename}"},
                {"Key": f"{path}/{record.identifier}.zip"},
            ],
        }
        return EventBridgeClient.send_event(detail=detail)

    @property
    def sqs_client(self) -> SQSClient:
        """Return an SQSClient, reusing if already cached on self."""
        if not self._sqs_client:
            self._sqs_client = SQSClient(self.sqs_topic_name)
        return self._sqs_client

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
                    record.source_record.sqs_message.receipt_handle  # type: ignore[attr-defined]
                )
            yield record

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

    @classmethod
    def _identify_and_read_metadata_file(
        cls, identifier: str, zip_file: str
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
            metadata_format, metadata_filename = cls._find_metadata_file(
                zip_file_object, identifier
            )
            metadata_bytes = cls._read_metadata_file(zip_file_object, metadata_filename)
            return metadata_format, metadata_bytes

    @staticmethod
    def _find_metadata_file(
        zip_file_object: zipfile.ZipFile, identifier: str
    ) -> tuple[str, str]:
        """Identify ISO19139 or FGDC metadata file in the zip file.

        The ordered dictionary is opinionated to return the ISO19139 metadata first if
        found.  Additionally, the casing of zip filename vs zip content filenames cannot
        be guaranteed, so we use fnmatch to look for 'iso19139' anywhere in the filename.
        """
        ordered_expected_metadata_filenames = {
            "iso19139": [
                f"{identifier}/*iso19139.xml",
                f"{identifier}*iso19139.xml",
            ],
            "fgdc": [
                f"{identifier}/*.xml",
                f"{identifier}*.xml",
            ],
        }

        # list of file patterns that will be skipped via fnmatch() below
        skip_conditions = [
            ".aux.xml",  # *.aux.xml maybe present but no FGDC metadata
        ]

        # dictionary of the original filename linked with a lower case form for matching
        files_original = {
            filename: filename.lower() for filename in zip_file_object.namelist()
        }

        # This block loops through the ordered, preferred filenames dictionary and sees
        # if a lowercase form matches any files in the zip file, while also skipping any
        # that match a skipped pattern in skip_conditions.  If a match is found, the
        # original filename is used.
        for (
            metadata_format,
            metadata_filenames,
        ) in ordered_expected_metadata_filenames.items():
            for metadata_filename in metadata_filenames:
                for file_original, file_lower in files_original.items():
                    if any(
                        fnmatch.fnmatch(file_lower, f"*{skip}")
                        for skip in skip_conditions
                    ):
                        continue
                    if fnmatch.fnmatch(file_lower, metadata_filename.lower()):
                        return metadata_format, file_original
        message = "Could not find ISO19139 or FGDC metadata file in zip file"
        raise FileNotFoundError(message)

    @staticmethod
    def _read_metadata_file(
        zip_file_object: zipfile.ZipFile, metadata_filename: str
    ) -> bytes:
        """Read the metadata file from the zip file object."""
        with zip_file_object.open(metadata_filename, "r") as metadata_file_object:
            return metadata_file_object.read()

    @classmethod
    def create_source_record_from_zip_file(
        cls,
        identifier: str,
        zip_file: str,
        event: Literal["created", "deleted"],
        sqs_message: ZipFileEventMessage | None = None,
    ) -> MITFGDC | MITISO19139:
        """Init a SourceRecord based on event and zip file."""
        metadata_format, data = cls._identify_and_read_metadata_file(identifier, zip_file)
        source_record_classes = {
            "iso19139": MITISO19139,
            "fgdc": MITFGDC,
        }
        source_record_class = source_record_classes[metadata_format]
        message = f"Metadata file located and identified: {source_record_class.__name__}"
        logger.debug(message)
        return source_record_class(
            identifier=identifier,
            data=data,
            event=event,
            zip_file_location=zip_file,
            sqs_message=sqs_message,
        )
