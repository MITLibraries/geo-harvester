"""harvester.harvest.alma"""

# ruff: noqa: TRY003, EM101
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
from harvester.harvest.exceptions import AlmaCannotIdentifyLatestFullRunDateError
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
        """Method to identify MARC records for a full harvest.

        For a full harvest, the harvester will examine the list of XML files
        to determine the latest full run date by parsing dates from the filepaths,
        which is then assigned to self.from_date. This enables the harvester
        to get source records the from corresponding "full-extracted" XML file(s) and
        "daily-extracted" XML file(s) with a run date >= latest full run date.
        """
        CONFIG.check_required_env_vars()
        filepaths = self._list_xml_files()
        if not self.from_date:
            from_date = self._get_latest_full_run_date(filepaths)
            if not from_date:
                raise AlmaCannotIdentifyLatestFullRunDateError(
                    "'from_date' required for full alma harvests."
                )
            self.from_date = from_date
        filtered_filepaths = self._filter_filepaths_by_dates(filepaths)
        yield from self._get_source_records(filtered_filepaths)

    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Method to identify MARC records for an incremental (daily) harvest.

        For an incremental harvest, the harvester will filter the list of XML files
        to a subset where the string "daily" appears in the filepath and then
        proceed to filter the filepaths by date. This enables the harvester to get
        source records from corresponding "daily-extracted" XML file(s).
        """
        CONFIG.check_required_env_vars()
        filepaths = self._list_xml_files()
        filtered_filepaths = self._filter_filepaths_by_harvest_type(filepaths)
        filtered_filepaths = self._filter_filepaths_by_dates(filtered_filepaths)
        yield from self._get_source_records(filtered_filepaths)

    def _get_source_records(self, filepaths: list[str]) -> Iterator[Record]:
        """Shared method to get MARC records for full and incremental harvests."""
        all_marc_records = self.parse_marc_records_from_files(filepaths)
        for marc_record in self.filter_geospatial_marc_records(all_marc_records):
            identifier, source_record = self.create_source_record_from_marc_record(
                marc_record
            )
            yield Record(
                identifier=identifier,
                source_record=source_record,
            )

    def parse_marc_records_from_files(self, filepaths: list[str]) -> Iterator[MARCRecord]:
        """Identify and yield parsed MARCRecords from filepaths of Alma exports."""
        for filepath in filepaths:
            with smart_open.open(filepath, "rb") as f:
                context = etree.iterparse(f, events=("end",), tag="record")
                for _event, element in context:
                    yield MARCRecord(element)
                    element.clear()
                    while element.getprevious() is not None:
                        del element.getparent()[0]

    def _get_latest_full_run_date(self, filepaths: list[str]) -> str | None:
        """Get the date from the latest Alma full extract."""
        extracted_dates = sorted(
            filter(
                None,
                (
                    self._get_date_from_filepath(filepath)
                    for filepath in self._filter_filepaths_by_harvest_type(filepaths)
                ),
            ),
            reverse=True,
        )

        if not extracted_dates:
            return None
        return extracted_dates[0]

    def _get_date_from_filepath(self, filepath: str) -> str | None:
        """Get date string from filepath."""
        match = FILEPATH_DATE_REGEX.match(filepath)
        if not match:  # pragma: nocover
            message = f"Could not parse date from filepath: {filepath}"
            logger.warning(message)
            return None
        return match.group(1)

    def _filter_filepaths_by_harvest_type(self, filepaths: list[str]) -> list[str]:
        """Filter list of XML files by harvest type.

        Given a list of XML files, the method will search for the presence of the
        harvest type ("daily" or "full") in the filepath.

        Example filepath: alma-2024-03-01-daily-extracted-records-to-index_19.xml
            - run_type=daily
        """
        return [
            filepath
            for filepath in filepaths
            if HARVEST_TYPE_MAP[self.harvest_type] in filepath
        ]

    def _filter_filepaths_by_dates(self, filepaths: list[str]) -> list[str]:
        """Filter list of XML files by date.

        Given a list of XML files, the method will retrieve the date (YYYY-MM-DD)
        from the filepath and check whether the date falls within the specified
        'from_date' and/or 'until_date' arguments passed to the harvester.

        Note: If 'from_date' and 'until_date' are not provided, the original list
        of XML files will be returned. For full harvests, 'from_date' is required
        and an error is thrown (by the full_harvest_get_source_records() method)
        if not provided or cannot be derived.

        Example filepath: alma-2024-03-01-daily-extracted-records-to-index_19.xml
            - run_date=2024-03-01
        """
        filtered_filepaths = []
        for filepath in filepaths:
            if filepath_date_string := self._get_date_from_filepath(filepath):
                filepath_date = convert_to_utc(date_parser(filepath_date_string))

                # include where filepath date meets harvester from/until date criteria
                if (
                    self.from_datetime_object is None
                    or filepath_date >= self.from_datetime_object
                ) and (
                    self.until_datetime_object is None
                    or filepath_date < self.until_datetime_object
                ):
                    filtered_filepaths.append(filepath)

        return filtered_filepaths

    def _list_xml_files(self) -> list[str]:
        """Retrieve list of XML files from S3 or local filesystem."""
        if self.input_files.startswith("s3://"):
            return self._list_s3_xml_files()
        return self._list_local_xml_files()  # pragma: nocover

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
