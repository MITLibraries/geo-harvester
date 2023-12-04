"""harvester.harvest.records.record"""

from abc import abstractmethod
from typing import Literal

from attrs import define, field


@define
class Record:
    """Class to represent a record in both its 'source' and 'normalized' form.

    Args:
        identifier: unique identifier determined for the record
            - for MIT records, this comes from the base name of the zip file
            - for OGM records, this likely will come from the metadata itself
        source_record: instance of SourceRecord
        normalized_record: instance of MITAardvark
        error_message: string of error encountered during harvest
        error_stage: what part of the harvest pipeline was the error encountered
    """

    identifier: str = field()
    source_record: "SourceRecord" = field()
    normalized_record: "MITAardvark" = field(default=None)
    error_message: str = field(default=None)
    error_stage: str = field(default=None)


@define
class MITAardvark:
    """Class to represent SourceRecord normalized to an MIT compliant Aardvark file.

    # NOTE: likely more fields and methods when getting into normalization work
    """

    data: dict = field(default=None, repr=False)

    def validate(self) -> None:
        """Validate that Aardvark is compliant for MIT purposes."""
        raise NotImplementedError


@define
class SourceRecord:
    """Class to represent the original, source_record form of a record.

    A source_record record may be FGDC, ISO19139, GeoBlacklight (GBL1), or Aardvark
    metadata formats.
    """

    metadata_format: Literal["fgdc", "iso19139", "gbl1", "aardvark"] = field(default=None)
    data: str | bytes | None = field(default=None, repr=False)
    zip_file_location: str = field(default=None)
    event: Literal["created", "deleted"] = field(default=None)

    @abstractmethod
    def normalize(self) -> MITAardvark | None:
        """Method to transform SourceRecord to MIT Aardvark MITAardvark instance."""


class DeletedSourceRecord(SourceRecord):
    """Class to represent a SourceRecord that has been deleted."""

    def normalize(self) -> None:
        message = "Normalization of a deleted record is not possible"
        raise RuntimeError(message)
