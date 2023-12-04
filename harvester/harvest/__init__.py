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
        records = self.filter_failed_records(self.write_to_public_cdn_bucket(records))
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
            message = f"Record {record.identifier}: retrieved source record"
            logger.warning(message)
            self.processed_records_count += 1
            yield record

    @abstractmethod
    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Harvester specific method to get source records for full harvest."""

    @abstractmethod
    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Harvester specific method to get source records for incremental harvest."""

    def normalize_source_records(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to normalize source record metadata to MITAardvark files."""
        for record in records:
            message = f"Record {record.identifier}: normalizing source record"
            logger.debug(message)
            if record.source_record.event == "deleted":
                logger.debug("Skipping normalization, record deleted")
                yield record
            else:
                yield record

    def write_to_public_cdn_bucket(self, records: Iterator[Record]) -> Iterator[Record]:
        """Write OR delete source and normalized metadata from S3:CDN:Public."""
        for record in records:
            message = (
                f"Record {record.identifier}: writing metadata records to S3:CDN:Public"
            )
            logger.debug(message)
            yield record

    def write_to_timdex_bucket(self, records: Iterator[Record]) -> Iterator[Record]:
        """Method to write MITAardvark files to S3:TIMDEX."""
        for record in records:
            message = (
                f"Record {record.identifier}: writing MITAardvark records to S3:TIMDEX"
            )
            logger.debug(message)
            yield record

    def filter_failed_records(self, records: Iterator[Record]) -> Iterator[Record]:
        """Filter Records that encountered an error during harvest step."""
        for record in records:
            message = f"Record {record.identifier}: checking for errors"
            logger.debug(message)
            if record.error_message is not None:
                self.failed_records.append(record)
                message = (
                    f"Record error: '{record.identifier}', "
                    f"'{record.error_stage}', '{record.error_message}'"
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
