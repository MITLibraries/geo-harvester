"""harvester.harvest.records.iso19139"""
# ruff: noqa: N802, N815; allows camelCase for aardvark fields
# ruff: noqa: PERF401; preferring more explicit, non list comprehensions

import logging
from collections import defaultdict
from typing import Literal

from attrs import define, field
from dateutil.parser import ParserError
from dateutil.parser import parse as date_parser
from lxml import etree

from harvester.records.record import XMLSourceRecord
from harvester.utils import convert_lang_code

logger = logging.getLogger(__name__)


@define
class ISO19139(XMLSourceRecord):
    """ISO19139 metadata format SourceRecord class."""

    metadata_format: Literal["iso19139"] = field(default="iso19139")
    nsmap: dict = field(
        default={
            "gmd": "http://www.isotc211.org/2005/gmd",
            "gco": "http://www.isotc211.org/2005/gco",
            "gts": "http://www.isotc211.org/2005/gts",
            "srv": "http://www.isotc211.org/2005/srv",
            "gml": "http://www.opengis.net/gml/3.2",
        },
        repr=False,
    )

    def __attrs_post_init__(self) -> None:
        """Post-init hook for attrs class.

        Actions performed:
            - dynamically update namespace map (self.nsmap) if differences detected
        """
        nsmap = self.root.nsmap
        for prefix, default_uri in self.nsmap.items():
            file_uri = nsmap.get(prefix)
            if file_uri is not None and file_uri != default_uri:
                self.nsmap[prefix] = file_uri

    ##########################
    # Required Field Methods
    ##########################
    def _dct_accessRights_s(self) -> str:
        """Field method: dct_accessRights_s

        If <MD_RestrictionCode> is not present, assume "Public".

        Else, retrieve all <MD_RestrictionCode> elements and look for indication of
        "Public" access in element text or attributes.  If not explicitly "Public",
        default to "Restricted".
        """
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:resourceConstraints
                        /gmd:MD_LegalConstraints
                            /gmd:accessConstraints
                                /gmd:MD_RestrictionCode
        """
        restriction_elements = self.xpath_query(xpath_expr)
        if not restriction_elements:
            return "Public"

        restriction_codes = []
        for restriction_element in restriction_elements:
            if restriction_text := restriction_element.text:
                restriction_codes.append(restriction_text.strip().lower())
            if attribute_code := restriction_element.attrib.get("codeListValue"):
                restriction_codes.append(attribute_code.strip().lower())

        for code in restriction_codes:
            if "public" in code or "unrestricted" in code:
                return "Public"

        return "Restricted"

    def _dct_title_s(self) -> str | None:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:title
                                /gco:CharacterString
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
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:hierarchyLevel
                /gmd:MD_ScopeCode
        """
        values = self.string_list_from_xpath(xpath_expr)
        if not values:
            return []

        # While the following values are allowed for <MD_ScopeCode> only the value
        # "dataset" has been found in example ISO19139 files, and therefore is the only
        # one mapped at this time.
        value_map = {
            "attribute": None,
            "attributeType": None,
            "collectionHardware": None,
            "collectionSession": None,
            "dataset": "Datasets",
            "series": None,
            "nonGeographicDataset": None,
            "dimensionGroup": None,
            "feature": None,
            "featureType": None,
            "property": None,
            "fieldSession": None,
            "software": None,
            "service": None,
            "model": None,
            "tile": None,
        }
        output = []
        for value in values:
            if mapped_value := value_map.get(value.strip().lower()):
                output.append(mapped_value)
        return output

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
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:extent
                        /gmd:EX_Extent
                            /gmd:geographicElement
                                /gmd:EX_GeographicBoundingBox
                                    /*[
                                        self::gmd:westBoundLongitude
                                        or self::gmd:eastBoundLongitude
                                        or self::gmd:southBoundLatitude
                                        or self::gmd:northBoundLatitude
                                    ]
        """
        bbox_elements = self.xpath_query(xpath_expr)

        # build dictionary of min/max values from all bounding boxes found
        bbox_data = defaultdict(list)
        for boundary_elem in bbox_elements:
            element_name = etree.QName(boundary_elem).localname
            bbox_data[element_name].append(boundary_elem.getchildren()[0].text)
        lat_lon_envelope = ", ".join(
            [
                min(bbox_data["westBoundLongitude"]).strip(),
                max(bbox_data["southBoundLatitude"]).strip(),
                max(bbox_data["eastBoundLongitude"]).strip(),
                min(bbox_data["northBoundLatitude"]).strip(),
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
    def _dct_description_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:abstract
                        /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dcat_keyword_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:descriptiveKeywords
                        /gmd:MD_Keywords
                            /gmd:keyword/gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_alternative_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_DataIdentification
            /gmd:citation
                /gmd:CI_Citation
                    /gmd:alternateTitle
                        /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_creator_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:citedResponsibleParty[
                                gmd:CI_ResponsibleParty
                                    /gmd:role
                                        /gmd:CI_RoleCode
                                            /@codeListValue = 'originator'
                                and not(
                                    gmd:CI_ResponsibleParty
                                        /gmd:organisationName
                                            /gco:CharacterString
                                    | gmd:individualName/gco:CharacterString
                                        = preceding-sibling::gmd:citedResponsibleParty
                                            /gmd:CI_ResponsibleParty
                                                /gmd:organisationName
                                                    /gco:CharacterString
                                    | gmd:individualName
                                        /gco:CharacterString
                                )
                            ]
                                /gmd:CI_ResponsibleParty
                                    /gmd:organisationName
                                        /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_format_s(self) -> str | None:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:distributionInfo
                /gmd:MD_Distribution
                    /gmd:distributionFormat
                        /gmd:MD_Format
                            /gmd:name
                                /gco:CharacterString
        """
        return self.single_string_from_xpath(xpath_expr)

    def _dct_issued_s(self) -> str | None:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:date
                                /gmd:CI_Date[
                                    gmd:dateType
                                        /gmd:CI_DateTypeCode[
                                            normalize-space(text()) = 'publication'
                                        ]
                                ]
                                    /gmd:date
                                        /gco:Date
        """
        value = self.single_string_from_xpath(xpath_expr)
        if value:
            try:
                return date_parser(value).strftime("%Y-%m-%d")
            except ParserError as exc:
                message = f"Error parsing date string: {value}, {exc}"
                logger.debug(message)
        return None

    def _dct_identifier_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:identifier
                                /gmd:MD_Identifier
                                    /gmd:code
                                        /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_language_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:language
                        /gmd:LanguageCode
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
        //gmd:CI_ResponsibleParty[
            gmd:role
                /gmd:CI_RoleCode[@codeListValue = 'publisher']
        ]
            /gmd:organisationName/gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_rights_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:resourceConstraints
                        //gco:CharacterString[text() != 'None']
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_spatial_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:descriptiveKeywords
                    /gmd:MD_Keywords[
                        gmd:type
                            /gmd:MD_KeywordTypeCode[@codeListValue = 'place']
                    ]
                        /gmd:keyword
                            /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_subject_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:topicCategory
                        /gmd:MD_TopicCategoryCode
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_temporal_sm(self) -> list[str]:
        temporal_elements = self._get_temporal_extents()
        output = []

        for instant in temporal_elements["instances"]:
            try:
                output.append(date_parser(instant["timestamp"]).strftime("%Y-%m-%d"))
            except ParserError as exc:
                message = f"Could not parse date string: {instant['timestamp']}, {exc}"
                logger.debug(message)
                continue

        for period in temporal_elements["periods"]:
            try:
                begin_year = date_parser(period["begin_timestamp"]).strftime("%Y")
                end_year = date_parser(period["end_timestamp"]).strftime("%Y")
            except ParserError as exc:
                message = (
                    "Could not extract begin or end date from time period: "
                    f"{period}, {exc}"
                )
                logger.debug(message)
                continue
            output.append(f"{begin_year}-{end_year}")

        return output

    def _gbl_dateRange_drsim(self) -> list[str]:
        temporal_elements = self._get_temporal_extents()
        output = []

        for period in temporal_elements["periods"]:
            try:
                begin_year = date_parser(period["begin_timestamp"]).strftime("%Y")
                end_year = date_parser(period["end_timestamp"]).strftime("%Y")
            except ParserError as exc:
                message = (
                    "Could not extract begin or end date from time period: "
                    f"{period}, {exc}"
                )
                logger.debug(message)
                continue
            output.append(f"{begin_year} TO {end_year}")

        return output

    def _gbl_resourceType_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                /gmd:descriptiveKeywords
                /gmd:MD_Keywords[
                    descendant::gmd:thesaurusName
                        /gmd:CI_Citation/gmd:title
                            /gco:CharacterString[text() = 'FGDC']
                ]
                    /gmd:keyword
                        /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _gbl_indexYear_im(self) -> list[int]:
        """Field method: gbl_indexYear_im

        Retrieves temporal elements, then extracts timestamps from both instances and
        periods.  From this list of timestamps, attempt is made to parse an integer year.
        """
        temporal_elements = self._get_temporal_extents()

        # extract date strings from instances and periods
        dates = [instance["timestamp"] for instance in temporal_elements["instances"]]
        dates.extend(
            [
                timestamp
                for period in temporal_elements["periods"]
                for timestamp in (period["begin_timestamp"], period["end_timestamp"])
            ]
        )

        # convert dates to year integers
        years = []
        for date in dates:
            try:
                years.append(int(date_parser(date).strftime("%Y")))
            except ParserError as exc:
                message = f"Could not extract year from date string: {date}, {exc}"
                logger.debug(message)
                continue
        return years

    ##########################
    # Utility / Helper Methods
    ##########################
    def _get_temporal_extents(self) -> dict:
        """Method to extract TimeInstant and TimePeriod temporal extents.

        https://www.ncei.noaa.gov/sites/default/files/2020-04/ISO%2019115-2
        %20Workbook_Part%20II%20Extentions%20for%20imagery%20and%20Gridded%20Data.pdf
            - pp. 71 explains TimeInstant vs TimePeriod

        Because multiple field methods utilize TemporalExtents, and there is some logic
        required to parse them, this has been centralized into this one method for use
        by multiple field methods.  Those fields methods use one or both of the
        "instances" and "periods" returned from this method.

        Returns:
            dict {
                "instances": list[dict{description, timestamp}],
                "periods": list[dict{description, begin_timestamp, end_timestamp}]
            }
        """
        # get temporal elements
        temporal_elements = self.xpath_query(
            """
            //gmd:MD_Metadata
                /gmd:identificationInfo
                    /gmd:MD_DataIdentification
                        /gmd:extent
                            /gmd:EX_Extent
                                /gmd:temporalElement
                                    /gmd:EX_TemporalExtent
            """
        )

        # parse TimeInstant and TimePeriods
        output = defaultdict(list)
        for temporal_element in temporal_elements:
            if time_instant := self._parse_time_instance(temporal_element):
                output["instances"].append(time_instant)
            if time_period := self._parse_time_period(temporal_element):
                output["periods"].append(time_period)

        return output

    def _parse_time_instance(
        self,
        temporal_element: etree._Element,  # noqa: SLF001,
    ) -> dict | None:
        """Parse TimeInstant

        TimeInstant example:
        --------------------
        <gmd:EX_TemporalExtent id="boundingTemporalExtent">
            <gmd:extent>
                <gml:TimeInstant gml:id="tp_114854">
                    <gml:description>ground condition</gml:description>
                    <gml:timePosition>1990-11-03T00:00:00</gml:timePosition>
                </gml:TimeInstant>
            </gmd:extent>
        </gmd:EX_TemporalExtent>
        """
        instant = temporal_element.find(
            "gmd:extent/gml:TimeInstant", namespaces=self.nsmap
        )

        if instant is None:
            return None

        time_instant_dict: dict[str, str | None] = {
            "description": None,
            "timestamp": None,
        }
        description = instant.find("gml:description", namespaces=self.nsmap)
        if description is not None:
            time_instant_dict["description"] = description.text
        time_instant_dict["timestamp"] = self._parse_time_position(
            instant.find("gml:timePosition", namespaces=self.nsmap)
        )
        return time_instant_dict

    def _parse_time_period(
        self,
        temporal_element: etree._Element,  # noqa: SLF001,
    ) -> dict | None:
        """Parse TimePeriod

        TimePeriod example:
        -------------------
        <gmd:temporalElement>
            <gmd:EX_TemporalExtent id="boundingTemporalExtent">
                <gmd:extent>
                    <gml:TimePeriod gml:id="tp_1234">
                        <gml:description>ground condition</gml:description>
                        <gml:beginPosition>1990-11-03T00:00:00</gml:beginPosition>
                        <gml:endPosition indeterminatePosition="now"/>
                    </gml:TimePeriod>
                </gmd:extent>
            </gmd:EX_TemporalExtent>
        </gmd:temporalElement>
        """
        period = temporal_element.find("gmd:extent/gml:TimePeriod", namespaces=self.nsmap)

        if period is None:
            return None

        time_period_dict: dict[str, str | None] = {
            "description": None,
            "begin_timestamp": None,
            "end_timestamp": None,
        }
        description = period.find("gml:description", namespaces=self.nsmap)
        if description is not None:
            time_period_dict["description"] = description.text
        time_period_dict["begin_timestamp"] = self._parse_time_position(
            period.find("gml:beginPosition", namespaces=self.nsmap)
        )
        time_period_dict["end_timestamp"] = self._parse_time_position(
            period.find("gml:endPosition", namespaces=self.nsmap)
        )
        return time_period_dict

    @staticmethod
    def _parse_time_position(
        position_element: etree._Element | None,  # noqa: SLF001
    ) -> str | None:
        """Parse timestamp from temporal element from attribute or text."""
        if position_element is None:
            return None
        if ip := position_element.attrib.get("indeterminatePosition"):
            return str(ip)
        if position_element.text:
            return position_element.text.strip()
        return None
