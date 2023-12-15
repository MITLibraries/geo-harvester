"""harvester.harvest"""

import datetime
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Literal

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

    processed_records_count: int = field(default=0)
    failed_records: list[Record] = field(default=[])
    successful_records: list[Record] = field(default=[])

    def harvest(self) -> dict:
        """Main entrypoint for harvests."""
        records = self.filter_failed_records(self.get_source_records())
        records = self.filter_failed_records(self.normalize_source_records(records))
        records = self.filter_failed_records(self.update_public_cdn_bucket(records))
        records = self.filter_failed_records(self.write_to_timdex_bucket(records))
        records = self.filter_failed_records(self.harvester_specific_steps(records))

        # NOTE: this will be revised with better understanding of what to keep
        self.successful_records = list(records)

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
            # WIP: even for deleted records, we likely WILL still normalize such that we
            # have a record to provide Transmog where 'gbl_suppressed_b=True' and and
            # pipeline can remove from TIMDEX.
            if record.source_record.event == "deleted":
                yield record
            else:
                try:
                    record.normalized_record = record.source_record.normalize()
                except Exception as exc:  # noqa: BLE001
                    record.exception_stage = "normalize_source_records"
                    record.exception = exc
                yield record

    def update_public_cdn_bucket(self, records: Iterator[Record]) -> Iterator[Record]:
        """Write OR delete source and normalized metadata from S3:CDN:Public.

        TODO: this will be renamed as it will ONLY write records
        TODO: decouple name/description from destination, focus on format
        """
        for record in records:
            message = (
                f"Record {record.identifier}: writing metadata records to S3:CDN:Public"
            )
            logger.debug(message)
            yield record

    def write_to_timdex_bucket(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to write MITAardvark files to S3:TIMDEX.

        TODO: decouple name/description from destination, focus on format
        """
        for record in records:
            message = (
                f"Record {record.identifier}: writing MITAardvark records to S3:TIMDEX"
            )
            logger.debug(message)
            yield record

    def filter_failed_records(self, records: Iterator[Record]) -> Iterator[Record]:
        """Filter and log Records that encountered an exception."""
        for record in records:
            if record.exception:
                self.failed_records.append(record)
                message = (
                    f"Record error: '{record.identifier}', "
                    f"step: '{record.exception_stage}', exception: '{record.exception}'"
                )
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
