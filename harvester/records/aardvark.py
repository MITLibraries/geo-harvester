"""harvester.harvest.records.aardvark"""

# ruff: noqa: N802

import json
from typing import Literal

from attrs import define, field

from harvester.records.record import JSONSourceRecord


@define
class Aardvark(JSONSourceRecord):
    """GeoBlacklight4.x (Aardvark) metadata format SourceRecord class.

    NOTE: This source record class is only used for OGM harvests.
    """

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
            if mapped_value := self.gbl_resource_class_value_map.get(
                value.strip().lower()
            ):
                mapped_values.append(mapped_value)  # noqa: PERF401
        return mapped_values if mapped_values else ["Other"]

    def _dcat_bbox(self) -> str | None:
        return self.parsed_data.get("dcat_bbox", None)

    def _locn_geometry(self) -> str | None:
        return self.parsed_data.get("locn_geometry", None)

    def _dct_references_s_ogm(self) -> dict:
        """Field method helper: "dct_references_s"

        For OGM repositories that provide Aardvark metadata, the most reliable location
        to find an external URL is the 'http://schema.org/url' key in the dct_references_s
        JSON payload.

        If the URI "http://schema.org/downloadUrl" is present, and only a single value,
        use.  If array, skip, as cannot be sure of a single download link to choose from.
        """
        refs_dict = json.loads(self.parsed_data["dct_references_s"])

        # extract required external URL
        url = refs_dict.get("http://schema.org/url")
        if not url:
            error_message = "Could not determine external URL from source metadata"
            raise ValueError(error_message)
        urls_dict = {"http://schema.org/url": url}

        # extract optional download url
        download_uri = "http://schema.org/downloadUrl"
        if download_value := refs_dict.get(download_uri):  # noqa: SIM102
            if isinstance(download_value, str):
                urls_dict[download_uri] = [
                    {
                        "label": "Data",
                        "url": download_value,
                    }
                ]

        return urls_dict

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
        return self.parsed_data.get("dct_format_s")

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
        return self.parsed_data.get("gbl_resourceType_sm", [])

    def _gbl_indexYear_im(self) -> list[int]:
        date_values = self.parsed_data.get("gbl_indexYear_im", [])
        return [int(date_value) for date_value in date_values]
