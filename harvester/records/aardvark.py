"""harvester.harvest.records.aardvark"""

from typing import Literal

from attrs import define, field

from harvester.records.record import JSONSourceRecord


@define
class Aardvark(JSONSourceRecord):
    """WIP: until OGM records are harvested and then normalized."""

    metadata_format: Literal["aardvark"] = field(default="aardvark")
