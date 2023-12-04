"""harvester.harvest.records.iso19139"""

from typing import Literal

from attrs import define, field

from harvester.records.record import MITAardvark, SourceRecord


@define
class ISO19139(SourceRecord):
    metadata_format: Literal["iso19139"] = field(default="iso19139")

    def normalize(self) -> "MITAardvark":
        return MITAardvark()
