"""harvester.harvest.records.fgdc"""
# ruff: noqa: N802, N815; allows camelCase for aardvark fields

from collections import defaultdict
from typing import Literal

from attrs import define, field
from lxml import etree

from harvester.records.record import XMLSourceRecord


@define
class FGDC(XMLSourceRecord):
    """FGDC metadata format SourceRecord class."""

    metadata_format: Literal["fgdc"] = field(default="fgdc")

    ##########################
    # Required Field Methods
    ##########################
    def _dct_accessRights_s(self) -> str:
        xpath_expr = """
        //idinfo
            /accconst
        """
        matches = self.string_list_from_xpath(xpath_expr)
        if matches:
            value = matches[0]
            if "Restricted" in value:
                return "Restricted"
            if "Unrestricted" in value:
                return "Public"
        return "Restricted"

    def _dct_title_s(self) -> str | None:
        xpath_expr = """
        //idinfo
            /citation
                /citeinfo
                    /title
        """
        values = self.string_list_from_xpath(xpath_expr)
        if values:
            return values[0]
        return None

    def _gbl_resourceClass_sm(self) -> list[str] | None:
        """Field method: gbl_resourceClass_sm

        Controlled vocabulary:
            - 'Datasets'
            - 'Maps'
            - 'Imagery'
            - 'Collections'
            - 'Websites'
            - 'Web services'
            - 'Other'
        """
        xpath_expr = """
        //idinfo
            /citation
                /citeinfo
                    /geoform
        """
        values = self.string_list_from_xpath(xpath_expr)
        if not values:
            return None
        value_map = {
            "vector digital data": "Datasets",
            "raster digital data": "Datasets",
            "remote-sensing image": "Image",
        }
        output = []
        for value in values:
            if mapped_value := value_map.get(value.strip().lower()):
                output.append(mapped_value)  # noqa: PERF401
        if output:
            return output
        return None

    def _dcat_bbox(self) -> str:
        xpath_expr = """
        //idinfo
            /spdom
                /bounding
                    /*[
                        self::westbc
                        or self::eastbc
                        or self::northbc
                        or self::southbc
                    ]
        """
        bbox_elements = self.xpath(xpath_expr)
        bbox_data = defaultdict(list)
        for boundary_elem in bbox_elements:
            element_name = etree.QName(boundary_elem).localname
            bbox_data[element_name].append(boundary_elem.text)
        lat_lon_envelope = ", ".join(
            [
                min(bbox_data["westbc"]).strip(),
                max(bbox_data["southbc"]).strip(),
                max(bbox_data["eastbc"]).strip(),
                min(bbox_data["northbc"]).strip(),
            ]
        )
        return f"ENVELOPE({lat_lon_envelope})"

    def _locn_geometry(self) -> str:
        """Field method: locn_geometry

        NOTE: at this time, duplicating bounding box content from dcat_bbox
        """
        return self._dcat_bbox()

    ##########################
    # Optional Field Methods
    ##########################
    def _dct_identifier_sm(self) -> list[str]:
        # <sdtsterm> identifiers
        identifiers = []
        xpath_expr = """
        //spdoinfo
            /ptvctinf
                /sdtsterm[@Name]
        """
        elements = self.xpath(xpath_expr)
        if elements:
            identifiers.extend([element.get("Name") for element in elements])

        # <onlink> identifiers
        xpath_expr = """
        /metadata
            /idinfo
                /citation
                    /citeinfo
                        /onlink
        """
        values = self.string_list_from_xpath(xpath_expr)
        if values:
            identifiers.extend(values)

        return identifiers
