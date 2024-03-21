"""harvester.records.formats.aardvark"""

# ruff: noqa: N802

from typing import Literal

from attrs import define, field

from harvester.records.formats.helpers import gbl_resource_class_value_map
from harvester.records.record import JSONSourceRecord


@define
class Aardvark(JSONSourceRecord):
    """GeoBlacklight4.x (Aardvark) metadata format SourceRecord class."""

    metadata_format: Literal["aardvark"] = field(default="aardvark")

    ##########################
    # Required Field Methods
    ##########################

    def _dct_accessRights_s(self) -> str:
        return self.parsed_data["dct_accessRights_s"]

    def _dct_title_s(self) -> str | None:
        return self.parsed_data["dct_title_s"]

    def _gbl_resourceClass_sm(self) -> list[str]:
        mapped_values = []
        for value in self.parsed_data.get("gbl_resourceClass_sm", []):
            if mapped_value := gbl_resource_class_value_map().get(value.strip().lower()):
                mapped_values.append(mapped_value)  # noqa: PERF401
        return mapped_values if mapped_values else ["Other"]

    def _dcat_bbox(self) -> str | None:
        return self.parsed_data.get("dcat_bbox", None)

    def _locn_geometry(self) -> str | None:
        return self.parsed_data.get("locn_geometry", None)

    ##########################
    # Optional Field Methods
    ##########################

    def _dct_description_sm(self) -> list[str]:
        return self.parsed_data.get("dct_description_sm", [])

    def _dcat_keyword_sm(self) -> list[str]:
        return self.parsed_data.get("dcat_keyword_sm", [])

    def _dct_alternative_sm(self) -> list[str]:
        return self.parsed_data.get("dct_alternative_sm", [])

    def _dct_creator_sm(self) -> list[str]:
        return self.parsed_data.get("dct_creator_sm", [])

    def _dct_format_s(self) -> str | None:
        return self.get_controlled_dct_format_s_term(self.parsed_data.get("dct_format_s"))

    def _dct_issued_s(self) -> str | None:
        return self.parsed_data.get("dct_issued_s")

    def _dct_identifier_sm(self) -> list[str]:
        return self.parsed_data.get("dct_identifier_sm", [])

    def _dct_language_sm(self) -> list[str]:
        return self.parsed_data.get("dct_language_sm", [])

    def _dct_publisher_sm(self) -> list[str]:
        return self.parsed_data.get("dct_publisher_sm", [])

    def _dct_rights_sm(self) -> list[str]:
        return self.parsed_data.get("dct_rights_sm", [])

    def _dct_spatial_sm(self) -> list[str]:
        return self.parsed_data.get("dct_spatial_sm", [])

    def _dct_subject_sm(self) -> list[str]:
        return self.parsed_data.get("dct_subject_sm", [])

    def _dct_temporal_sm(self) -> list[str]:
        return self.parsed_data.get("dct_temporal_sm", [])

    def _gbl_dateRange_drsim(self) -> list[str]:
        value = self.parsed_data.get("gbl_dateRange_drsim", [])
        if isinstance(value, str):
            return [value]
        return value

    def _gbl_resourceType_sm(self) -> list[str]:
        return self.get_controlled_gbl_resourceType_sm_terms(
            self.parsed_data.get("gbl_resourceType_sm", [])
        )

    def _gbl_indexYear_im(self) -> list[int]:
        date_values = self.parsed_data.get("gbl_indexYear_im", [])
        return [int(date_value) for date_value in date_values]
