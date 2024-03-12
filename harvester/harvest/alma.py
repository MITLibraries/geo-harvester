"""harvester.harvest.alma"""

import glob
import logging
import re
from collections.abc import Iterator
from typing import Literal, cast

import smart_open  # type: ignore[import-untyped]
from attrs import define, field
from dateutil.parser import parse as date_parser
from lxml import etree
from marcalyx import Record as MARCRecord  # type: ignore[import-untyped]

from harvester.aws.s3 import S3Client
from harvester.config import Config
from harvester.harvest import Harvester
from harvester.records import (
    MARC,
    Record,
)
from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class MITAlmaHarvester(Harvester):
    """Harvester of MIT Alma MARC Records."""

    input_files: str = field(default=None)

    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by parsing MARC records from FULL Alma exports."""
        CONFIG.check_required_env_vars()
        yield from self._get_source_records()

    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by parsing MARC records from DAILY Alma exports."""
        CONFIG.check_required_env_vars()
        yield from self._get_source_records()

    def _get_source_records(self) -> Iterator[Record]:
        """Shared method to get MARC records for full and incremental harvests."""
        all_marc_records = self.parse_marc_records_from_files()
        for marc_record in self.filter_geospatial_marc_records(all_marc_records):
            identifier, source_record = self.create_source_record_from_marc_record(
                marc_record
            )
            yield Record(
                identifier=identifier,
                source_record=source_record,
            )

    def _list_xml_files(self) -> Iterator[str]:
        """Provide list of MARC record set XML files filtered by harvest type and date.

        Example filepath: alma-2024-03-01-daily-extracted-records-to-index_19.xml
            - run_type=extracted
            - run_date=2024-03-01
        """
        if self.input_files.startswith("s3://"):
            filepaths = self._list_s3_xml_files()
        else:
            filepaths = self._list_local_xml_files()  # pragma: nocover

        harvest_type_map = {"full": "full", "incremental": "daily"}
        date_regex = re.compile(r".+?alma-(\d{4}-\d{2}-\d{2})-.*")

        for filepath in filepaths:

            # skip for run type
            if harvest_type_map[self.harvest_type] not in filepath:
                continue

            # parse date from filepath
            match = date_regex.match(filepath)
            if not match:  # pragma: nocover
                message = f"Could not parse date from filepath: {filepath}"
                logger.warning(message)
                continue
            filepath_date = convert_to_utc(date_parser(match.group(1)))

            # skip if date out of bounds for harvester from/until dates
            if self.from_datetime_object and filepath_date < self.from_datetime_object:
                continue
            if self.until_datetime_object and filepath_date >= self.until_datetime_object:
                continue

            yield filepath

    def _list_s3_xml_files(self) -> list[str]:
        """Return a list of S3 URIs for extracted Alma XML files.

        Example self.input_files = "s3://timdex-extract-dev-222053980223/alma/"
        """
        bucket, prefix = self.input_files.replace("s3://", "").split("/", 1)
        s3_objects = S3Client.list_objects_uri_and_date(bucket, prefix)
        return [
            s3_object[0]
            for s3_object in s3_objects
            if s3_object[0].lower().endswith(".xml")
        ]

    def _list_local_xml_files(self) -> list[str]:
        """Return a list of local filepaths of extracted Alma XML files."""
        return glob.glob(f"{self.input_files.removesuffix('/')}/*.xml")

    def parse_marc_records_from_files(self) -> Iterator[MARCRecord]:
        """Yield parsed MARCRecords from list of filepaths."""
        for filepath in self._list_xml_files():
            with smart_open.open(filepath, "rb") as f:
                context = etree.iterparse(f, events=("end",), tag="record")
                for _event, elem in context:
                    yield MARCRecord(elem)
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

    def filter_geospatial_marc_records(
        self, marc_records: Iterator[MARCRecord]
    ) -> Iterator[MARCRecord]:
        """Yield geospatial MARC records by filtering on defined criteria."""
        for i, record in enumerate(marc_records):
            if i % 10_000 == 0 and i > 0:  # pragma: nocover
                message = f"{i} MARC records scanned for geospatial filtering"
                logger.info(message)

            # skip if leader doesn't have a/c/n/p
            if record.leader[5] not in ("a", "c", "n", "p"):
                continue

            # skip if Genre/Form 655 does not contain "Maps."
            if not any(
                "Maps." in subfield.value
                for form in record["655"]
                for subfield in form.subfield("a")
            ):
                continue

            # skip if call number prefix not in list
            if not any(
                subfield.value in ("MAP", "CDROM", "DVDROM")
                for locations in record["949"]
                for subfield in locations.subfield("k")
            ):
                continue

            # skip if shelving location not in list
            if not any(
                subfield.value in ("MAPRM", "GIS")
                for locations in record["985"]
                for subfield in locations.subfield("aa")
            ):
                continue

            yield record

    def create_source_record_from_marc_record(
        self, marc_record: MARCRecord
    ) -> tuple[str, MARC]:
        """Create MARC SourceRecord from parsed MARC record."""
        # derive identifier from ControlField 001
        try:
            identifier = next(
                item for item in marc_record.controlFields() if item.tag == "001"
            ).value
        except IndexError as exc:  # pragma: nocover
            message = "Could not extract identifier from ControlField 001"
            raise ValueError(message) from exc

        # derive event from Leader 5th character
        event: Literal["created", "deleted"] = cast(
            Literal["created", "deleted"],
            {
                "a": "created",
                "c": "created",
                "n": "created",
                "p": "created",
                "d": "deleted",
            }[marc_record.leader[5]],
        )

        return identifier, MARC(
            origin="mit",
            identifier=identifier,
            data=etree.tostring(marc_record.node),
            marc=marc_record,
            event=event,
        )
