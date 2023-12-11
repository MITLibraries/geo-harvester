"""harvester.harvest.records.iso19139"""

from collections import defaultdict
import datetime
from dateutil.parser import parse as date_parser
import json
from typing import Literal

from attrs import define, field
from lxml import etree

from harvester.records.record import XMLSourceRecord


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
            "gml": "http://www.opengis.net/gml",
        },
        repr=False,
    )

    def _get_temporal_extents(self):
        """
        https://www.ncei.noaa.gov/sites/default/files/2020-04/ISO%2019115-2%20Workbook_Part%20II%20Extentions%20for%20imagery%20and%20Gridded%20Data.pdf
            - pp. 71 explains TimeInstant vs TimePeriod

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

        :return: {
            "instances": list[ datetime ],
            "periods": list[ tuple[datetime,datetime] ]
        }
        """

        def parse_time_position(position_element):
            if position_element is None:
                return None
            if ip := position_element.attrib.get("indeterminatePosition"):
                return ip
            return position_element.text.strip()

        output = defaultdict(list)
        for temporal_element in self.xpath(
            """//gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                /gmd:extent
                    /gmd:EX_Extent
                        /gmd:temporalElement
                            /gmd:EX_TemporalExtent"""
        ):
            # TimeInstant
            instant = temporal_element.find(
                "gmd:extent/gml:TimeInstant", namespaces=self.nsmap
            )
            if instant is not None:
                temporal_dict = {
                    "description": None,
                    "timestamp": None,
                }
                description = instant.find("gml:description", namespaces=self.nsmap)
                if description is not None:
                    temporal_dict["description"] = description.text
                temporal_dict["timestamp"] = parse_time_position(
                    instant.find("gml:timePosition", namespaces=self.nsmap)
                )
                output["instances"].append(temporal_dict)

            # TimePeriod
            period = temporal_element.find(
                "gmd:extent/gml:TimePeriod", namespaces=self.nsmap
            )
            if period is not None:
                temporal_dict = {
                    "description": None,
                    "begin_timestamp": None,
                    "end_timestamp": None,
                }
                description = period.find("gml:description", namespaces=self.nsmap)
                if description is not None:
                    temporal_dict["description"] = description.text
                temporal_dict["begin_timestamp"] = parse_time_position(
                    period.find("gml:beginPosition", namespaces=self.nsmap)
                )
                temporal_dict["end_timestamp"] = parse_time_position(
                    period.find("gml:endPosition", namespaces=self.nsmap)
                )
                output["periods"].append(temporal_dict)

        return output

    def _dcat_bbox(self):
        """
        NOTE: dcat_bbox is not repeatable, but ISO files may have multiple BBOX sections.
            Community XSLT approach is to take min/max for edges to create the largest
            reasonable bounding box.

        Example multiple bounding boxes:
        <extent>
            <EX_Extent>
                <geographicElement>
                    <EX_GeographicBoundingBox>
                        <westBoundLongitude>
                            <gco:Decimal>-92.889337</gco:Decimal>
                        </westBoundLongitude>
                        <eastBoundLongitude>
                            <gco:Decimal>-86.763988</gco:Decimal>
                        </eastBoundLongitude>
                        <southBoundLatitude>
                            <gco:Decimal>42.49193</gco:Decimal>
                        </southBoundLatitude>
                        <northBoundLatitude>
                            <gco:Decimal>47.080713</gco:Decimal>
                        </northBoundLatitude>
                    </EX_GeographicBoundingBox>
                </geographicElement>
                <temporalElement>
                    <EX_TemporalExtent>
                        <extent>
                            <gml:TimeInstant gml:id="d191563e532">
                                <gml:timePosition>2017-01-01T00:00:00
                                </gml:timePosition>
                            </gml:TimeInstant>
                        </extent>
                    </EX_TemporalExtent>
                </temporalElement>
            </EX_Extent>
        </extent>
        <extent>
            <EX_Extent>
                <geographicElement>
                    <EX_GeographicBoundingBox>
                        <extentTypeCode>
                            <gco:Boolean>true</gco:Boolean>
                        </extentTypeCode>
                        <westBoundLongitude>
                            <gco:Decimal>-92.889337</gco:Decimal>
                        </westBoundLongitude>
                        <eastBoundLongitude>
                            <gco:Decimal>-86.763988</gco:Decimal>
                        </eastBoundLongitude>
                        <southBoundLatitude>
                            <gco:Decimal>42.49193</gco:Decimal>
                        </southBoundLatitude>
                        <northBoundLatitude>
                            <gco:Decimal>47.080705</gco:Decimal>
                        </northBoundLatitude>
                    </EX_GeographicBoundingBox>
                </geographicElement>
            </EX_Extent>
        </extent>
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
        bbox_elements = self.xpath(xpath_expr)

        if not bbox_elements:
            return None

        # build dictionary of min/max values from all bounding boxes found
        bbox_data = defaultdict(list)
        for boundary_elem in bbox_elements:
            element_name = etree.QName(boundary_elem).localname
            bbox_data[element_name].append(boundary_elem.getchildren()[0].text)
        lat_lon_envelope = ", ".join(
            [
                min(bbox_data["westBoundLongitude"]),
                max(bbox_data["southBoundLatitude"]),
                max(bbox_data["eastBoundLongitude"]),
                min(bbox_data["northBoundLatitude"]),
            ]
        )

        return f"ENVELOPE({lat_lon_envelope})"

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

    def _dct_accessRights_s(self):
        """
        NOTE: presence of any <MD_RestrictionCode> indicates restricted resource
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
        matches = self.xpath(xpath_expr)
        if not matches:
            return "Public"
        else:
            return "Restricted"

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

    def _dct_description_sm(self):
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:abstract
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

    def _dct_references_s(self):
        """
        https://opengeometadata.org/ogm-aardvark/#references
        https://opengeometadata.org/configure-references-links/

        NOTE: not multivalued, but serialized JSON, so can contain multiple URLs

        Example:
        {
          "dct_references_s": "{\"http://schema.org/downloadUrl\":[
            {
              \"url\":\"https://cugir-data.s3.amazonaws.com/00/79/50/cugir-007950.zip\",
              \"label\":\"Shapefile\"
            },
            {
              \"url\":\"https://cugir-data.s3.amazonaws.com/00/79/50/agBROO.pdf\",
              \"label\":\"PDF\"
            },
            {
              \"url\":\"https://cugir-data.s3.amazonaws.com/00/79/50/agBROO2011.kmz\",
              \"label\":\"KMZ\"
            }]
          }"
        }
        """
        resources = []

        # extract URLs from metadata
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:distributionInfo
                /gmd:MD_Distribution
                    /gmd:transferOptions
                        /gmd:MD_DigitalTransferOptions
                            /gmd:onLine
                                /gmd:CI_OnlineResource
        """
        metadata_online_resources = self.xpath(xpath_expr)
        for item in metadata_online_resources:
            label, protocol = None, None
            if label_match := item.xpath(
                "gmd:name/gco:CharacterString/text()", namespaces=self.nsmap
            ):
                label = label_match[0]
            if protocol_match := item.xpath(
                "gmd:protocol/gco:CharacterString/text()", namespaces=self.nsmap
            ):
                protocol = protocol_match[0]
            url = item.xpath("gmd:linkage/gmd:URL/text()", namespaces=self.nsmap)[0]
            resources.append({"label": label, "protocol": protocol, "url": url})

        # add TIMDEX download URL
        resources.append(
            {
                "label": "TIMDEX S3 Zipfile",
                "protocol": "Download",
                "url": "https://mit.s3.amazonaws.com/path/to/zip/file.zip",
            }
        )
        return json.dumps({"http://schema.org/downloadUrl": resources}, indent=None)

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

    def _dct_title_s(self) -> str:
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:title
                                /gco:CharacterString
        """
        values = self.string_list_from_xpath(xpath_expr)
        if values:
            return values[0]
        return None

    def _gbl_dateRange_drsim(self):
        temporal_elements = self._get_temporal_extents()
        output = []
        for period in temporal_elements["periods"]:
            output.append(f"{period['begin_timestamp']}-{period['end_timestamp']}")
        return output

    def _gbl_resourceClass_sm(self):
        """
        Controlled vocabulary: ['Datasets','Maps','Imagery','Collections','Websites',
        'Web services','Other']
        """
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:hierarchyLevel
                /gmd:MD_ScopeCode
        """
        values = self.string_list_from_xpath(xpath_expr)
        if not values:
            return None
        # TODO: complete and improve this mapping
        value_map = {
            "dataset": "Datasets",
        }
        output = []
        for value in values:
            if mapped_value := value_map.get(value.strip().lower()):
                output.append(mapped_value)
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

    def _id(self):
        # TODO: look into meaningful identifiers from ISO file
        return "geo:mit:<PLACEHOLDER_FROM_MIT_ISO_ID>"

    def _locn_geometry(self):
        """
        NOTE: currently mirroring dcat_bbox
            - consider more advanced geographies if present
        """
        return self._dcat_bbox()

    def _schema_provider_s(self):
        return "MIT"
