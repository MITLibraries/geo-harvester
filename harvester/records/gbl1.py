"""harvester.harvest.records.gbl1"""

from typing import Literal

from attrs import define, field

from harvester.records.record import MITAardvark, SourceRecord


@define
class GBL1(SourceRecord):
    metadata_format: Literal["gbl1"] = field(default="gbl1")

    def normalize(self) -> "MITAardvark":
        return MITAardvark()
