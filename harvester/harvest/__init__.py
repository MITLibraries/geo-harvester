"""harvester.harvest"""

import datetime
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Literal

import jsonlines
import smart_open  # type: ignore[import-untyped]
from attrs import define, field
from dateutil.parser import parse as date_parser

from harvester.config import Config
from harvester.records import Record
from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class Harvester(ABC):
    """Harvester class extended by MITHarvester and OGMHarvester."""

    harvest_type: Literal["full", "incremental"] = field(default=None)
    from_date: str = field(default=None)
    until_date: str = field(default=None)
    output_source_directory: str = field(default=None)
    output_normalized_directory: str = field(default=None)
    output_file: str = field(default=None)

    processed_records_count: int = field(default=0)
    failed_records: list[dict] = field(factory=list)
    successful_records: list[str] = field(factory=list)

    def harvest(self) -> dict:
        """Main entrypoint for harvests.

        This method chains together multiple methods, passing an iterator of Records.  The
        effect is a single record is fully processed as it's pulled through the methods
        via the loop that saves successfully processed Record identifiers.  Any failures,
        for any steps, are caught via the self.filter_failed_records() method, and the
        full failed Record instance is saved to self.failed_records.
        """
        records = self.filter_failed_records(self.get_source_records())
        records = self.filter_failed_records(self.normalize_source_records(records))
        records = self.filter_failed_records(self.write_source_and_normalized(records))
        records = self.filter_failed_records(self.write_combined_normalized(records))
        records = self.filter_failed_records(self.harvester_specific_steps(records))

        self.successful_records = [record.identifier for record in records]

        return {
            "processed_records_count": self.processed_records_count,
            "successful_records": len(self.successful_records),
            "failed_records_count": len(self.failed_records),
        }

    def harvester_specific_steps(self, records: Iterator[Record]) -> Iterator[Record]:
        """Optional method to run steps specific to harvester type (MIT or OGM)."""
        yield from records

    def get_source_records(self) -> Iterator[Record]:
        """Method to identify source record metadata records for the harvest.

        This method relies on harvester type to initialize the iterator of Records with
        a SourceRecord attached that downstream steps will utilize.
        """
        if self.harvest_type == "full":
            records = self.full_harvest_get_source_records()
        elif self.harvest_type == "incremental":
            records = self.incremental_harvest_get_source_records()
        else:
            message = f"harvest type: '{self.harvest_type}' not recognized"
            raise ValueError(message)
        for record in records:
            message = (
                f"Record {record.identifier}: retrieved source record, event '"
                f"{record.source_record.event}'"
            )
            logger.debug(message)
            self.processed_records_count += 1
            yield record

    @abstractmethod
    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Harvester specific method to get source records for full harvest."""

    @abstractmethod
    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Harvester specific method to get source records for incremental harvest."""

    def normalize_source_records(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to normalize source record metadata to MITAardvark records."""
        for record in records:
            message = f"Record {record.identifier}: normalizing source record"
            logger.debug(message)
            try:
                record.normalized_record = record.source_record.normalize()
            except Exception as exc:  # noqa: BLE001
                record.exception_stage = "normalize_source_records"
                record.exception = exc
            yield record

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

    def write_combined_normalized(self, records: Iterator[Record]) -> Iterator[Record]:
        """Write single, combined JSONLines file of all normalized MITAardvark.

        This step is driven by presence of the CLI option:
            "--output-file": write combined normalized metadata

        This is the expected file format expected and used by the TIMDEX pipeline.

        A file is opened for writing before the iteration through all records in the
        iterator pipeline.  Each record is written to the output file.  When all files
        processed, the open file is automatically closed via the context manager.
        """
        if self.output_file:
            with smart_open.open(self.output_file, "w") as normalized_file:
                writer = jsonlines.Writer(normalized_file)
                for record in records:
                    message = (
                        f"Record {record.identifier}: writing to combined normalized "
                        f"metadata"
                    )
                    logger.debug(message)
                    try:
                        writer.write(record.normalized_record.to_dict())
                    except Exception as exc:  # noqa: BLE001
                        record.exception_stage = "write_combined_normalized"
                        record.exception = exc
                    yield record

        # if not writing combined normalized metadata, just yield records
        else:
            yield from records

    def filter_failed_records(self, records: Iterator[Record]) -> Iterator[Record]:
        """Filter out and log Records that encountered an exception.

        For Records that encounter an exception during any stage in the harvest pipeline,
        a dictionary is saved with the Record's identifier, the failed step, and the
        encountered Exception object.  The Record is then removed from the remainder of
        the harvest by not yielding it.  Records without exception are yielded untouched.
        """
        for record in records:
            if record.exception:
                failure_dict = {
                    "record_identifier": record.identifier,
                    "harvest_step": record.exception_stage,
                    "exception": record.exception,
                }
                self.failed_records.append(failure_dict)
                message = f"Record error: {failure_dict}"
                logger.error(message)
            else:
                yield record

    @property
    def from_datetime_object(self) -> datetime.datetime | None:
        """Parses from date with UTC timezone offset set."""
        if self.from_date:
            return convert_to_utc(date_parser(self.from_date))
        return None

    @property
    def until_datetime_object(self) -> datetime.datetime | None:
        """Parses until date with UTC timezone offset set."""
        if self.until_date:
            return convert_to_utc(date_parser(self.until_date))
        return None
