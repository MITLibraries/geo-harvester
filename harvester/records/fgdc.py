"""harvester.harvest.records.fgdc"""

from typing import Literal

from attrs import define, field

from harvester.records.record import MITAardvark, SourceRecord


@define
class FGDC(SourceRecord):
    metadata_format: Literal["fgdc"] = field(default="fgdc")

    def normalize(self) -> "MITAardvark":
        return MITAardvark()
