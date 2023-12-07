"""harvester.harvest.records.fgdc"""

from typing import Literal

from attrs import define, field

from harvester.records.record import XMLSourceRecord


@define
class FGDC(XMLSourceRecord):
    """FGDC metadata format SourceRecord class."""

    metadata_format: Literal["fgdc"] = field(default="fgdc")
    # ruff: noqa: TD002,TD003,FIX002
    # TODO: define namespace map for FGDC files
