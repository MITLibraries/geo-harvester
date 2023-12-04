"""harvester.harvest.records.aardvark"""

from typing import Literal

from attrs import define, field

from harvester.records.record import MITAardvark, SourceRecord


@define
class Aardvark(SourceRecord):
    metadata_format: Literal["aardvark"] = field(default="aardvark")

    def normalize(self) -> "MITAardvark":
        return MITAardvark()
