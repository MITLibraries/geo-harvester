"""harvester.harvest.alma"""

import glob
import logging
import re
from collections.abc import Iterator

import smart_open  # type: ignore[import-untyped]
from attrs import define, field
from dateutil.parser import parse as date_parser
from lxml import etree
from marcalyx import Record as MARCRecord  # type: ignore[import-untyped]

from harvester.aws.s3 import S3Client
from harvester.config import Config
from harvester.harvest import Harvester
from harvester.records import Record
from harvester.records.sources.alma import AlmaMARC
from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)

CONFIG = Config()

# map GeoHarvest harvest type to record stage in filepath
HARVEST_TYPE_MAP = {"full": "full", "incremental": "daily"}

# regex to extract YYYY-MM-DD from filepath
FILEPATH_DATE_REGEX = re.compile(r".+?alma-(\d{4}-\d{2}-\d{2})-.*")


@define
class MITAlmaHarvester(Harvester):
    """Harvester of MIT Alma MARC Records."""

    input_files: str = field(default=None)

    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by parsing MARC records from FULL Alma exports.

        While both full and incremental harvests share the same code path for fetching and
        filtering records, the base Harvester class requires this explicit method to be
        defined.
        """
        CONFIG.check_required_env_vars()
        yield from self._get_source_records()

    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Identify files for harvest by parsing MARC records from DAILY Alma exports.

        While both full and incremental harvests share the same code path for fetching and
        filtering records, the base Harvester class requires this explicit method to be
        defined.
        """
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

    def parse_marc_records_from_files(self) -> Iterator[MARCRecord]:
        """Yield parsed MARCRecords from list of filepaths."""
        for filepath in self._list_xml_files():
            with smart_open.open(filepath, "rb") as f:
                context = etree.iterparse(f, events=("end",), tag="record")
                for _event, element in context:
                    yield MARCRecord(element)
                    element.clear()
                    while element.getprevious() is not None:
                        del element.getparent()[0]

    def _list_xml_files(self) -> Iterator[str]:
        """Provide list of MARC record set XML files filtered by harvest type and date.

        Retrieve list of XML files from S3 or local filesystem, then yield filepaths that
        match harvest options.
        """
        if self.input_files.startswith("s3://"):
            filepaths = self._list_s3_xml_files()
        else:
            filepaths = self._list_local_xml_files()  # pragma: nocover

        for filepath in filepaths:
            if not self._filter_filepath_by_harvest_type(filepath):
                continue
            if not self._filter_filepath_by_dates(filepath):
                continue
            yield filepath

    def _filter_filepath_by_harvest_type(self, filepath: str) -> bool:
        """Bool if harvest type aligns with record stage in filepath.

        Example filepath: alma-2024-03-01-daily-extracted-records-to-index_19.xml
            - run_type=daily
        """
        return HARVEST_TYPE_MAP[self.harvest_type] in filepath

    def _filter_filepath_by_dates(self, filepath: str) -> bool:
        """Bool if from and/or until dates align with filepath dates.

        Example filepath: alma-2024-03-01-daily-extracted-records-to-index_19.xml
            - run_date=2024-03-01
        """
        match = FILEPATH_DATE_REGEX.match(filepath)
        if not match:  # pragma: nocover
            message = f"Could not parse date from filepath: {filepath}"
            logger.warning(message)
            return False

        filepath_date = convert_to_utc(date_parser(match.group(1)))
        if self.from_datetime_object and filepath_date < self.from_datetime_object:
            return False
        if self.until_datetime_object and filepath_date >= self.until_datetime_object:
            return False

        return True

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

    def filter_geospatial_marc_records(
        self, marc_records: Iterator[MARCRecord]
    ) -> Iterator[MARCRecord]:
        """Yield geospatial MARC records by filtering on defined criteria."""
        for i, record in enumerate(marc_records):
            if i % 10_000 == 0 and i > 0:  # pragma: nocover
                message = f"{i} MARC records scanned for geospatial filtering"
                logger.info(message)

            # skip if leader doesn't have a/c/n/p
            if record.leader[5] not in ("a", "c", "d", "n", "p"):
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
    ) -> tuple[str, AlmaMARC]:
        """Create AlmaMARC source record from parsed MARC record."""
        identifier = AlmaMARC.get_identifier_from_001(marc_record)
        event = AlmaMARC.get_event_from_leader(marc_record)

        return identifier, AlmaMARC(
            identifier=identifier,
            data=etree.tostring(marc_record.node),
            marc=marc_record,
            event=event,
        )
