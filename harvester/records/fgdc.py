"""harvester.harvest.records.fgdc"""

from collections import defaultdict
import datetime
from typing import Literal

from attrs import define, field
from lxml import etree

from harvester.records.record import XMLSourceRecord


@define
class FGDC(XMLSourceRecord):
    """FGDC metadata format SourceRecord class."""

    metadata_format: Literal["fgdc"] = field(default="fgdc")
    nsmap = {}

    # Required Field Methods
    def _dct_accessRights_s(self):
        xpath_expr = """
        //idinfo
            /accconst
        """
        matches = self.string_list_from_xpath(xpath_expr)
        # TODO: needs additional logic
        if matches:
            value = matches[0]
            if "restricted" in value.lower():
                return "Restricted"
        return "Public"

    def _dct_title_s(self):
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

    def _gbl_resourceClass_sm(self):
        """
        Controlled vocabulary: ['Datasets','Maps','Imagery','Collections','Websites',
        'Web services','Other']
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
        # TODO: complete and improve this mapping
        value_map = {
            "vector digital data": "Datasets",
        }
        output = []
        for value in values:
            if mapped_value := value_map.get(value.strip().lower()):
                output.append(mapped_value)
        return output

    def _id(self):
        # TODO: look into meaningful identifiers from ISO file
        return "geo:mit:<PLACEHOLDER_FROM_MIT_FGDC_ID>"

    def _dcat_bbox(self):
        xpath_expr = """
        //idinfo
            /spdom
                /bounding
                    /*[self::westbc or self::eastbc or self::northbc or self::southbc]            
        """
        bbox_elements = self.xpath(xpath_expr)
        bbox_data = defaultdict(list)
        for boundary_elem in bbox_elements:
            element_name = etree.QName(boundary_elem).localname
            bbox_data[element_name].append(boundary_elem.text)
        lat_lon_envelope = ", ".join(
            [
                min(bbox_data["westbc"]),
                max(bbox_data["southbc"]),
                max(bbox_data["eastbc"]),
                min(bbox_data["northbc"]),
            ]
        )
        return f"ENVELOPE({lat_lon_envelope})"

    def _dct_references_s(self):
        return """{"msg":"URLs here"}"""

    def _locn_geometry(self):
        return self._dcat_bbox()

    # Optional Field Methods
    def _dcat_centroid(self):
        return None

    def _dcat_keyword_sm(self):
        return None

    def _dcat_theme_sm(self):
        return None

    def _dct_alternative_sm(self):
        return None

    def _dct_creator_sm(self):
        return None

    def _dct_description_sm(self):
        return None

    def _dct_format_s(self):
        return None

    def _dct_identifier_sm(self):
        return None

    def _dct_isPartOf_sm(self):
        return None

    def _dct_isReplacedBy_sm(self):
        return None

    def _dct_issued_s(self):
        return None

    def _dct_isVersionOf_sm(self):
        return None

    def _dct_language_sm(self):
        return None

    def _dct_license_sm(self):
        return None

    def _dct_publisher_sm(self):
        return None

    def _dct_relation_sm(self):
        return None

    def _dct_replaces_sm(self):
        return None

    def _dct_rights_sm(self):
        return None

    def _dct_rightsHolder_sm(self):
        return None

    def _dct_source_sm(self):
        return None

    def _dct_spatial_sm(self):
        return None

    def _dct_subject_sm(self):
        return None

    def _dct_temporal_sm(self):
        return None

    def _gbl_dateRange_drsim(self):
        return None

    def _gbl_displayNote_sm(self):
        return None

    def _gbl_fileSize_s(self):
        return None

    def _gbl_georeferenced_b(self):
        return None

    def _gbl_indexYear_im(self):
        return None

    def _gbl_resourceType_sm(self):
        return None

    def _gbl_suppressed_b(self):
        return None

    def _gbl_wxsIdentifier_s(self):
        return None

    def _pcdm_memberOf_sm(self):
        return None

    def _schema_provider_s(self):
        return None
