"""harvester.records.sources.alma"""

import json
import logging
from typing import Literal, cast

from attrs import define, field
from marcalyx import Record as MARCRecord  # type: ignore[import-untyped]

from harvester.config import Config
from harvester.records import SourceRecord
from harvester.records.formats import MARC

logger = logging.getLogger(__name__)

CONFIG = Config()


@define(slots=False)
class AlmaSourceRecord(SourceRecord):
    """Class to extend SourceRecord for harvested Alma MARC XML records.

    Extended Args:
        None at this time
    """

    origin: Literal["alma"] = field(default="alma")

    def _schema_provider_s(self) -> str:
        """Shared field method: schema_provider_s"""
        return "MIT Libraries"  # pragma: nocover


@define(slots=False)
class AlmaMARC(AlmaSourceRecord, MARC):
    metadata_format: Literal["marc"] = field(default="marc")

    @staticmethod
    def get_identifier_from_001(marc_record: MARCRecord) -> str:
        """Static method to extract identifier from 001 tag."""
        try:
            identifier = next(
                item for item in marc_record.controlFields() if item.tag == "001"
            ).value
        except IndexError as exc:  # pragma: nocover
            message = "Could not extract identifier from ControlField 001"
            raise ValueError(message) from exc
        return identifier

    @staticmethod
    def get_event_from_leader(marc_record: MARCRecord) -> Literal["created", "deleted"]:
        """Static method to determine a harvest event from leader."""
        return cast(
            'Literal["created", "deleted"]',
            {
                "a": "created",
                "c": "created",
                "d": "deleted",
                "n": "created",
                "p": "created",
            }[marc_record.leader[5]],
        )

    def _dct_references_s(self) -> str:
        """Shared field method: dct_references_s.

        The primary URL returned is the Primo item page.
        """
        primo_url = (
            "https://mit.primo.exlibrisgroup.com/permalink/01MIT_INST/jp08pj/alma"
            + self.get_identifier_from_001(self.marc)
        )
        return json.dumps({"http://schema.org/url": primo_url})
