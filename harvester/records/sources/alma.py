"""harvester.records.sources.alma"""

import logging
from typing import Literal

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

    def _dct_references_s(self) -> str:
        """Shared field method: dct_references_s"""
        raise NotImplementedError

    def _schema_provider_s(self) -> str:
        """Shared field method: schema_provider_s"""
        return "MIT Libraries"


@define(slots=False)
class AlmaMARC(AlmaSourceRecord, MARC):
    metadata_format: Literal["marc"] = field(default="marc")
    marc: MARCRecord = field(default=None)
