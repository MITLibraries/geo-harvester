"""harvester.harvest.records.gbl1"""

from typing import Literal

from attrs import define, field

from harvester.records.record import JSONSourceRecord


@define
class GBL1(JSONSourceRecord):
    """WIP: until OGM records are harvested and then normalized."""

    metadata_format: Literal["gbl1"] = field(default="gbl1")
