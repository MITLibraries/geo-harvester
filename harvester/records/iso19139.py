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
                restriction_codes.append(restriction_text)
            if attribute_code := restriction_element.attrib.get("codeListValue"):
                restriction_codes.append(attribute_code)

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

    ########################
    # Optional Field Methods
    ########################
    def _dct_description_sm(self):
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

    def _dcat_theme_sm(self) -> list[str]:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:topicCategory
                        /gmd:MD_TopicCategoryCode
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_alternative_sm(self):
        xpath_expr = """
        //gmd:MD_DataIdentification
            /gmd:citation
                /gmd:CI_Citation
                    /gmd:alternateTitle
                        /gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_creator_sm(self):
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

    def _dct_format_s(self):
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:distributionInfo
                /gmd:MD_Distribution
                    /gmd:distributionFormat
                        /gmd:MD_Format
                            /gmd:name
                                /gco:CharacterString
        """
        values = self.string_list_from_xpath(xpath_expr)
        if values:
            return values[0]
        return None

    def _dct_issued_s(self):
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:date
                                /gmd:CI_Date[gmd:dateType
                                    /gmd:CI_DateTypeCode[text() = 'publication']]
                                        /gmd:date/gco:Date
        """
        values = self.string_list_from_xpath(xpath_expr)
        if values:
            value = values[0]
            return date_parser(value).strftime("%Y")
        return None

    def _dct_identifier_sm(self):
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

    def _dct_language_sm(self):
        """
        NOTE: controlled via https://opengeometadata.org/ogm-aardvark/#language-values
        """
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:language
                        /gmd:LanguageCode
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_publisher_sm(self):
        xpath_expr = """
        //gmd:CI_ResponsibleParty[
            gmd:role
                /gmd:CI_RoleCode[@codeListValue = 'publisher']
        ]
            /gmd:organisationName/gco:CharacterString
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_rights_sm(self):
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:resourceConstraints
                        //gco:CharacterString[text() != 'None']
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_spatial_sm(self):
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

    def _dct_subject_sm(self):
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:topicCategory
                        /gmd:MD_TopicCategoryCode
        """
        return self.string_list_from_xpath(xpath_expr)

    def _dct_temporal_sm(self):
        temporal_elements = self._get_temporal_extents()
        output = []
        for instant in temporal_elements["instances"]:
            output.append(instant["timestamp"])
        for period in temporal_elements["periods"]:
            output.append(f"{period['begin_timestamp']}-{period['end_timestamp']}")
        return output

    def _gbl_dateRange_drsim(self):
        temporal_elements = self._get_temporal_extents()
        output = []
        for period in temporal_elements["periods"]:
            output.append(f"{period['begin_timestamp']}-{period['end_timestamp']}")
        return output

    def _gbl_resourceType_sm(self):
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

    def _gbl_suppressed_b(self):
        return False

    def _gbl_wxsIdentifier_s(self):
        """
        TODO: look into meaningful identifiers from ISO file
        """
        return "geo:mit:<PLACEHOLDER_FROM_MIT_ISO_ID>"

    def _schema_provider_s(self):
        return "MIT"
