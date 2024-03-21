"""harvester.records.formats.marc"""

# ruff: noqa: N802

from typing import Literal

from attrs import define, field

from harvester.records.record import MarcalyxSourceRecord


@define
class MARC(MarcalyxSourceRecord):
    """MIT MARC metadata format SourceRecord class."""

    metadata_format: Literal["marc"] = field(default="marc")

    ##########################
    # Required Field Methods
    ##########################

    def _dct_accessRights_s(self) -> str:
        raise NotImplementedError

    def _dct_title_s(self) -> str | None:
        raise NotImplementedError

    def _gbl_resourceClass_sm(self) -> list[str]:
        raise NotImplementedError

    def _dcat_bbox(self) -> str | None:
        raise NotImplementedError

    def _locn_geometry(self) -> str | None:
        raise NotImplementedError

    ##########################
    # Optional Field Methods
    ##########################

    def _dct_description_sm(self) -> list[str]:
        raise NotImplementedError

    def _dcat_keyword_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dcat_keyword_sm."""
        raise NotImplementedError

    def _dct_alternative_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dct_alternative_sm."""
        raise NotImplementedError

    def _dct_creator_sm(self) -> list[str] | None:
        raise NotImplementedError

    def _dct_format_s(self) -> str | None:
        raise NotImplementedError

    def _dct_issued_s(self) -> str | None:
        raise NotImplementedError

    def _dct_identifier_sm(self) -> list[str]:
        raise NotImplementedError

    def _dct_language_sm(self) -> list[str]:
        raise NotImplementedError

    def _dct_publisher_sm(self) -> list[str]:
        raise NotImplementedError

    def _dct_rights_sm(self) -> list[str]:
        raise NotImplementedError

    def _dct_spatial_sm(self) -> list[str] | None:
        raise NotImplementedError

    def _dct_subject_sm(self) -> list[str] | None:
        raise NotImplementedError

    def _dct_temporal_sm(self) -> list[str] | None:
        raise NotImplementedError

    def _gbl_dateRange_drsim(self) -> list[str]:
        raise NotImplementedError

    def _gbl_resourceType_sm(self) -> list[str]:
        raise NotImplementedError

    def _gbl_indexYear_im(self) -> list[int]:
        raise NotImplementedError
