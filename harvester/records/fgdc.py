"""harvester.harvest.records.fgdc"""
# ruff: noqa: N802, N815; allows camelCase for aardvark fields

import logging
from collections import defaultdict
from typing import Literal

from attrs import define, field
from dateutil.parser import ParserError
from lxml import etree

from harvester.records.record import XMLSourceRecord
from harvester.utils import convert_lang_code, date_parser

logger = logging.getLogger(__name__)


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
        value = self.single_string_from_xpath(xpath_expr)
        if value:
            if "Restricted" in value:
                return "Restricted"
            if "Unrestricted" in value:
                return "Public"
        return "Restricted"

    def _dct_title_s(self) -> str:
        xpath_expr = """
        //idinfo
            /citation
                /citeinfo
                    /title
        """
        value = self.single_string_from_xpath(xpath_expr)
        if not value:
            message = "Could not find <title> element"
            raise ValueError(message)
        return value

    def _gbl_resourceClass_sm(self) -> list[str]:
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
        value_map = {
            "vector digital data": "Datasets",
            "raster digital data": "Datasets",
            "remote-sensing image": "Imagery",
        }
        xpath_expr = """
        //idinfo
            /citation
                /citeinfo
                    /geoform
        """
        values = self.string_list_from_xpath(xpath_expr)
        mapped_values = []
        for value in values:  # type: ignore[union-attr] # temp ignore, not need next PR
            if mapped_value := value_map.get(value.strip().lower()):
                mapped_values.append(mapped_value)  # noqa: PERF401
        return mapped_values

    def _dcat_bbox(self) -> str:
        """Field method: dcat_bbox.

        "bbox" stands for "Bounding Box", and it should be the largest possible rectangle
        that encompasses the geographic region for this geospatial resource.  Because
        some metadata files contain multiple geospatial boxes or shapes, the accepted
        approach in the GIS/geospatial community is to craft the LARGEST box that includes
        ALL defined boxes or shapes.

        To this end, min() and max() are used to select the smallest or largest value from
        a list of latitude or longitude values, based on which ever corner is in question.
        """
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
        bbox_elements = self.xpath_query(xpath_expr)
        bbox_data = defaultdict(list)
        for boundary_element in bbox_elements:
            element_name = etree.QName(boundary_element).localname
            bbox_data[element_name].append(boundary_element.text)
        lat_lon_envelope = ", ".join(
            [
                min(bbox_data["westbc"]).strip(),
                max(bbox_data["eastbc"]).strip(),
                min(bbox_data["northbc"]).strip(),
                max(bbox_data["southbc"]).strip(),
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
        elements = self.xpath_query(xpath_expr)
        if elements:
            identifiers.extend([element.get("Name") for element in elements])

        # <onlink> handle URLs
        xpath_expr = """
        /metadata
            /idinfo
                /citation
                    /citeinfo
                        /onlink[contains(text(), 'handle.net')]
        """
        identifiers.extend(self.string_list_from_xpath(xpath_expr))

        # <ftname> filename identifiers
        xpath_expr = """
        //metadata
            /idinfo
                /citation
                    /citeinfo
                        /ftname
        """
        identifiers.extend(self.string_list_from_xpath(xpath_expr))

        return identifiers

    def _dct_subject_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /idinfo
                /keywords
                    //themekey
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_spatial_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /idinfo
                /keywords
                    //placekey
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_temporal_sm(self) -> list[str]:
        values = []
        # <tempkey>
        xpath_expr = """
        //metadata
            /idinfo
                /keywords
                    //tempkey
        """
        values.extend(self.string_list_from_xpath(xpath_expr))

        # <timeinfo..caldate>
        xpath_expr = """
        //metadata
            /idinfo
                /timeprd
                    /timeinfo
                        /sngdate
                            /caldate
        """
        values.extend(self.string_list_from_xpath(xpath_expr))

        # <mdattim..caldate>
        xpath_expr = """
        //metadata
            /idinfo
                /timeperd
                    /timeinfo
                        /mdattim
                            /sngdate
                                /caldate
        """
        values.extend(self.string_list_from_xpath(xpath_expr))

        # <rngdates.begdate>
        xpath_expr = """
        //metadata
            /idinfo
                /timeperd
                    /timeinfo
                        /rngdates
                            /begdate
        """
        values.extend(self.string_list_from_xpath(xpath_expr))

        # parse values
        parsed_values = []
        if values:
            for value in values:
                try:
                    parsed_value = date_parser(value).strftime("%Y-%m-%d")
                except ParserError as exc:
                    message = f"Could not parse date string: {value}, {exc}"
                    logger.debug(message)
                    continue
                parsed_values.append(parsed_value)

        return parsed_values

    def _gbl_dateRange_drsim(self) -> list[str]:
        date_ranges_xpath = """
        //metadata
            /idinfo
                /timeperd
                    /timeinfo
                        /rngdates
        """
        date_range_elements = self.xpath_query(date_ranges_xpath)

        date_ranges = []
        for date_range_element in date_range_elements:
            try:
                begin_date = date_parser(
                    date_range_element.find("begdate").text
                ).strftime("%Y")
                end_date = date_parser(date_range_element.find("enddate").text).strftime(
                    "%Y"
                )
            except ParserError as exc:
                message = (
                    "Could not extract begin or end date from date range: "
                    f"{etree.tostring(date_range_element).decode()}, {exc}"
                )
                logger.debug(message)
                continue
            date_ranges.append(f"[{begin_date} TO {end_date}]")
        return date_ranges

    def _dct_description_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /idinfo
                /descript
                    /abstract
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_creator_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /idinfo
                /citation
                    /citeinfo
                        /origin
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_format_s(self) -> str | None:
        xpath_expr = """
        //metadata
            /spdoinfo
                /direct
        """
        return self.single_string_from_xpath(xpath_expr)

    def _dct_issued_s(self) -> str | None:
        xpath_expr = """
        //metadata
            /idinfo
                /citation
                    /citeinfo
                        /pubdate
        """
        value = self.single_string_from_xpath(xpath_expr)
        if value:
            try:
                return date_parser(value).strftime("%Y-%m-%d")
            except ParserError as exc:
                message = f"Error parsing date string: {value}, {exc}"
                logger.debug(message)
        return None

    def _dct_language_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /idinfo
                /descript
                    /langdata
        """
        lang_codes = self.string_list_from_xpath(xpath_expr)
        three_letter_codes = []
        for lang_code in lang_codes:
            try:
                three_letter_codes.append(convert_lang_code(lang_code))
            except Exception as exc:  # noqa: BLE001
                message = f"Error parsing language code: {lang_code}, {exc}"
                logger.debug(message)
                continue
        return [code for code in three_letter_codes if code is not None]

    def _dct_publisher_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /idinfo
                /citation
                    /citeinfo
                        /pubinfo
                            /publish
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_rights_sm(self) -> list[str]:
        rights = []
        xpath_expr = """
        //metadata
            /idinfo
                /useconst
        """
        rights.extend(self.string_list_from_xpath(xpath_expr))
        xpath_expr = """
        //metadata
            /idinfo
                /acconst
        """
        rights.extend(self.string_list_from_xpath(xpath_expr))
        return rights

    def _gbl_indexYear_im(self) -> list[int]:
        # get all dates from _dct_temporal_sm
        dates = self._dct_temporal_sm()
        years = []
        for date in dates:
            try:
                years.append(int(date_parser(date).strftime("%Y")))
            except ParserError as exc:
                message = f"Could not extract year from date string: {date}, {exc}"
                logger.debug(message)
                continue
        return years

    def _gbl_resourceType_sm(self) -> list[str]:
        xpath_expr = """
        //metadata
            /spdoinfo
                /ptvctinf
                    /sdtsterm
                        /sdtstype
        """
        return self.string_list_from_xpath(xpath_expr)
