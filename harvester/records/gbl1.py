"""harvester.harvest.records.gbl1"""

# ruff: noqa: N802

import json
from typing import Literal

from attrs import define, field

from harvester.records.record import JSONSourceRecord


@define
class GBL1(JSONSourceRecord):
    """GeoBlacklight1.x (GBL1) metadata format SourceRecord class.

    NOTE: This source record class is only used for OGM harvests.
    """

    metadata_format: Literal["gbl1"] = field(default="gbl1")

    def _convert_scalar_to_array(self, field_name: str) -> list[str]:
        """Convert a single, scalar GBL1 value to Aardvark array."""
        if value := self.parsed_data.get(field_name):
            return [value]
        return []

    ##########################
    # Required Field Methods
    ##########################

    def _dct_accessRights_s(self) -> str:
        return self.parsed_data["dc_rights_s"]

    def _dct_title_s(self) -> str | None:
        return self.parsed_data["dc_title_s"]

    def _gbl_resourceClass_sm(self) -> list[str]:
        if value := self.parsed_data.get("dc_type_s"):  # noqa: SIM102s
            if mapped_value := self.gbl_resource_class_value_map.get(
                value.strip().lower()
            ):
                return [mapped_value]
        return ["Other"]

    def _dcat_bbox(self) -> str | None:
        return self.parsed_data.get("solr_geom", None)

    def _locn_geometry(self) -> str | None:
        return self._dcat_bbox()

    def _dct_references_s_ogm(self) -> dict:
        """Field method helper: "dct_references_s"

        For most OGM repositories providing GBL1 metadata, pulling this URL from the
        dct_references_s field directly suffices, and this approach is therefore the
        default.  However, some repositories require alternate strategies which can be
        optionally defined in the OGM config YAML using the "external_url_strategy"
        property.

        If the URI "http://schema.org/downloadUrl" is present, and only a single value,
        use.  If array, skip, as cannot be sure of a single download link to choose from.
        """
        # extract required external url
        url: None | str

        alternate_strategy = self.ogm_repo_config.get("external_url_strategy")
        if alternate_strategy:
            strategy_name = alternate_strategy["name"]
            if strategy_name == "base_url_and_slug":
                url = "/".join(
                    [
                        alternate_strategy["base_url"],
                        self.parsed_data[alternate_strategy["gbl1_field"]],
                    ]
                )
            elif strategy_name == "field_value":
                url = self.parsed_data.get(alternate_strategy["gbl1_field"])
                if url and not url.startswith("http"):
                    url = None
            else:
                error_message = f"Alternate URL strategy not recognized: {strategy_name}"
                raise ValueError(error_message)

        else:
            refs_dict = json.loads(self.parsed_data["dct_references_s"])
            url = refs_dict.get("http://schema.org/url")

        if not url:
            error_message = "Could not determine external URL from source metadata"
            raise ValueError(error_message)
        urls_dict = {"http://schema.org/url": url}

        # extract optional download url
        download_uri = "http://schema.org/downloadUrl"
        refs_dict = json.loads(self.parsed_data["dct_references_s"])
        if download_value := refs_dict.get(download_uri):  # noqa: SIM102
            if isinstance(download_value, str):
                urls_dict[download_uri] = [  # type: ignore[assignment]
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
        return self._convert_scalar_to_array("dc_description_s")

    def _dcat_keyword_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dcat_keyword_sm."""
        return []

    def _dct_alternative_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dct_alternative_sm."""
        return []

    def _dct_creator_sm(self) -> list[str] | None:
        return self.parsed_data.get("dc_creator_sm")

    def _dct_format_s(self) -> str | None:
        return self.parsed_data.get("dc_format_s")

    def _dct_issued_s(self) -> str | None:
        return self.parsed_data.get("dct_issued_s")

    def _dct_identifier_sm(self) -> list[str]:
        return self._convert_scalar_to_array("dc_identifier_s")

    def _dct_language_sm(self) -> list[str]:
        if self.parsed_data.get("dc_language_sm"):
            return self._convert_scalar_to_array("dc_language_sm")
        if self.parsed_data.get("dc_language_s"):
            return self._convert_scalar_to_array("dc_language_s")
        return []

    def _dct_publisher_sm(self) -> list[str]:
        return self._convert_scalar_to_array("dc_publisher_s")

    def _dct_rights_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dct_rights_sm."""
        return []

    def _dct_spatial_sm(self) -> list[str] | None:
        return self.parsed_data.get("dct_spatial_sm")

    def _dct_subject_sm(self) -> list[str] | None:
        return self.parsed_data.get("dc_subject_sm")

    def _dct_temporal_sm(self) -> list[str] | None:
        return self.parsed_data.get("dct_temporal_sm")

    def _gbl_dateRange_drsim(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to gbl_dateRange_drsim."""
        return []

    def _gbl_resourceType_sm(self) -> list[str]:
        return self._convert_scalar_to_array("layer_geom_type_s")

    def _gbl_indexYear_im(self) -> list[int]:
        if value := self.parsed_data.get("solr_year_i"):
            if isinstance(value, list):
                return [int(value[0])]
            return [int(value)]
        return []
