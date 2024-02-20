"""harvester.harvest.records.record"""

# ruff: noqa: N802, N815; allows camelCase for aardvark fields

import datetime
import json
import logging
from abc import abstractmethod
from typing import Any, Literal

from attrs import asdict, define, field, fields
from attrs.validators import in_, instance_of
from lxml import etree  # type: ignore[import-untyped]

from harvester.aws.sqs import ZipFileEventMessage
from harvester.config import Config
from harvester.records.controlled_terms import (
    DCT_FORMAT_S_OGM_TERMS,
    GBL_RESOURCETYPE_SM_TERMS,
)
from harvester.records.exceptions import FieldMethodError
from harvester.records.validators import MITAardvarkFormatValidator
from harvester.utils import dedupe_list_of_values

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class Record:
    """Class to represent a record in both its 'source' and 'normalized' form.

    Args:
        identifier: unique identifier determined for the record
            - for MIT records, this comes from the base name of the zip file
            - for OGM records, this likely will come from the metadata itself
        source_record: instance of SourceRecord
        normalized_record: instance of MITAardvark
        exception_stage: harvest step in which error/exception occurred
        exception: Exception object
    """

    identifier: str = field()
    source_record: "SourceRecord" = field(default=None)
    normalized_record: "MITAardvark" = field(default=None)
    exception_stage: str = field(default=None)
    exception: Exception = field(default=None)


@define
class MITAardvark:
    """Class to represent an MIT compliant Aardvark file.

    OGM Aardvark spec: https://opengeometadata.org/ogm-aardvark/

    In addition to providing normal instances of this class, this class definition also
    provides the list of fields needed for SourceRecord.normalize() to loop through and
    attempt per-field method calls when normalizing to an MITAardvark instance.
    """

    # aardvark required fields
    dct_accessRights_s: str
    dct_title_s: str
    gbl_mdModified_dt: str
    gbl_mdVersion_s: str
    gbl_resourceClass_sm: list
    id: str

    # additional MIT required fields
    dct_references_s: str

    # optional fields
    dcat_bbox: str = field(default=None)
    dcat_centroid: str = field(default=None)
    dcat_keyword_sm: list = field(default=None)
    dcat_theme_sm: list = field(default=None)
    dct_alternative_sm: list = field(default=None)
    dct_creator_sm: list = field(default=None)
    dct_description_sm: list = field(default=None)
    dct_format_s: str = field(default=None)
    dct_identifier_sm: list = field(default=None)
    dct_isPartOf_sm: list = field(default=None)
    dct_isReplacedBy_sm: list = field(default=None)
    dct_issued_s: str = field(default=None)
    dct_isVersionOf_sm: list = field(default=None)
    dct_language_sm: list = field(default=None)
    dct_license_sm: list = field(default=None)
    dct_publisher_sm: list = field(default=None)
    dct_relation_sm: list = field(default=None)
    dct_replaces_sm: list = field(default=None)
    dct_rights_sm: list = field(default=None)
    dct_rightsHolder_sm: list = field(default=None)
    dct_source_sm: list = field(default=None)
    dct_spatial_sm: list = field(default=None)
    dct_subject_sm: list = field(default=None)
    dct_temporal_sm: list = field(default=None)
    gbl_dateRange_drsim: list = field(default=None)
    gbl_displayNote_sm: list = field(default=None)
    gbl_fileSize_s: str = field(default=None)
    gbl_georeferenced_b: str = field(default=None)
    gbl_indexYear_im: list = field(default=None)
    gbl_resourceType_sm: list = field(default=None)
    gbl_suppressed_b: bool = field(default=None)
    gbl_wxsIdentifier_s: str = field(default=None)
    locn_geometry: str = field(default=None)
    pcdm_memberOf_sm: list = field(default=None)
    schema_provider_s: str = field(default=None)

    def __attrs_post_init__(self) -> None:
        """Run JSON schema validation on the created MITAardvark record

        The JSON schema validation is performed in addition to the validation of
        arguments through attrs attribute validators.
        """
        MITAardvarkFormatValidator(self.to_dict()).validate()

    def to_dict(self) -> dict:
        """Dump MITAardvark record to dictionary."""
        return asdict(self, filter=lambda _, value: value is not None and value != [])

    def to_json(
        self,
        pretty: bool = True,  # noqa: FBT001, FBT002
    ) -> str:
        """Dump MITAardvark record to JSON string."""
        return json.dumps(self.to_dict(), indent=2 if pretty else None)


@define
class SourceRecord:
    """Class to represent the original, source_record form of a record.

    A source_record record may be FGDC, ISO19139, GeoBlacklight (GBL1), or Aardvark
    metadata format, and those classes will extend this base class to define their own
    specific normalization logic.

    To that end, this class does contain some "shared" field methods.  These are methods,
    that provide values for an MITAardvark file, that are shared across all metadata
    types.  These include things like unique identifiers, static values, etc., that are
    metadata format agnostic.

    Args:
        origin: origin of SourceRecord, with optional colon ":" delimited namespace
            - e.g. "mit", "ogm:stanford"
        identifier: unique identifier determined for the record
            - for MIT records, this comes from the base name of the zip file
            - for OGM records, this likely will come from the metadata itself
        metadata_format: literal string of the metadata format
            - "fgdc", "iso19139", "gbl1", "aardvark"
        data: string or bytes of the source file (XML or JSON)
        zip_file_location: path string to the zip file
            - this may be local or S3 URI
        event: literal string of "created" or "deleted"
        sqs_message: ZipFileEventMessage instance
            - present only for MIT harvests
            - by affixing to SourceRecord during record retrieval, it allows for use
            after the record has been processed to manage the message in the queue
        ogm_repo_config: config dictionary of OGM repository from configuration YAML
    """

    origin: Literal["mit", "ogm"] = field(validator=in_(["mit", "ogm"]))
    identifier: str = field(validator=instance_of(str))
    metadata_format: Literal["fgdc", "iso19139", "gbl1", "aardvark"] = field(
        validator=in_(["fgdc", "iso19139", "gbl1", "aardvark"])
    )
    data: str | bytes = field(repr=False)
    zip_file_location: str = field(default=None)
    event: Literal["created", "deleted"] = field(
        default=None, validator=in_(["created", "deleted"])
    )
    sqs_message: ZipFileEventMessage = field(default=None)
    ogm_repo_config: dict = field(default=None)

    @property
    def output_filename_extension(self) -> str:
        """Provide source output filename extension based on metadata format."""
        return {
            "fgdc": "xml",
            "iso19139": "xml",
            "gbl1": "json",
            "aardvark": "json",
        }[self.metadata_format]

    @property
    def source_metadata_filename(self) -> str:
        """Construct output source metadata filename.

        Examples:
            - ABC123.source.fgdc.xml
            - ABC123.source.iso19139.xml
            - ABC123.source.gbl1.json
            - ABC123.source.aardvark.json
        """
        return ".".join(  # noqa: FLY002
            [
                self.identifier,
                "source",
                self.metadata_format,
                self.output_filename_extension,
            ]
        )

    @property
    def normalized_metadata_filename(self) -> str:
        """Construct output normalized metadata filename.

        Example: ABC123.normalized.aardvark.json
        """
        return f"{self.identifier}.normalized.aardvark.json"

    @property
    def is_restricted(self) -> bool:
        """Property to return boolean if the Record is restricted.

        This determination is based on the Aardvark field 'dct_accessRights_s' which is a
        controlled value of "Restricted" or "Public", and is required.
        """
        return {
            "Public": False,
            "Restricted": True,
            None: True,
        }[self._dct_accessRights_s()]

    @property
    def is_deleted(self) -> bool:
        """Property to return boolean if source record is considered deleted.

        This valuation is based on the event that brought the record into harvest.
        """
        if self.event == "deleted":
            return True
        return False

    def get_controlled_dct_format_s_term(self, value: str | None) -> str | None:
        """Get single of controlled term for dct_format_s from original value."""
        if not value:
            return None

        value = value.lower().strip()

        # allow for some variants and similar matches
        # note: order is important; more specific should be first
        if (
            "shapefile" in value
            or value == "shp"
            or value == "avshp"
            or "shp," in value
            or "esri" in value
            or "geodatabase" in value
        ):
            value = "shapefile"
        elif "geotiff" in value:
            value = "geotiff"
        elif "jpeg2000" in value:
            value = "jpeg2000"
        elif "tiff/jpeg" in value or "multiple" in value:
            value = "mixed"
        elif "tiff" in value:
            value = "tiff"
        elif "jpeg" in value or "jpg" in value:
            value = "jpeg"
        elif "tabular" in value:
            value = "tabular"

        return {term.lower(): term for term in DCT_FORMAT_S_OGM_TERMS}.get(value)

    def get_controlled_gbl_resourceType_sm_terms(
        self, values: list[str] | None
    ) -> list[str]:
        """Get list of controlled terms for gbl_resourceType_sm from original values."""
        if not values:
            return []

        controlled_values = []

        # add allowed controlled terms not defined by Aardvark spec
        controlled_terms = GBL_RESOURCETYPE_SM_TERMS
        controlled_terms.update(["Image data", "Vector data", "Mixed"])

        for value in values:
            processed_value = value.strip().lower()

            # allow for some variants and similar matches
            # note: order is important; more specific should be first
            if "polygon" in processed_value:
                processed_value = "polygon data"
            elif "raster" in processed_value:
                processed_value = "raster data"
            elif "point" in processed_value:
                processed_value = "point data"
            elif "line" in processed_value or "string" in processed_value:
                processed_value = "line data"
            elif "image" in processed_value:
                processed_value = "image data"
            elif "vector" in processed_value:
                processed_value = "vector data"
            elif "mixed" in processed_value or "composite" in processed_value:
                processed_value = "mixed"

            if controlled_value := {
                term.lower(): term for term in GBL_RESOURCETYPE_SM_TERMS
            }.get(processed_value):
                controlled_values.append(controlled_value)
        return dedupe_list_of_values(controlled_values)

    def normalize(self) -> MITAardvark:
        """Method to normalize a SourceRecord to an MIT Aardvark MITAardvark instance.

        This is the entrypoint for normalization.  This method will look to MITAardvark
        for a list of Aardvark fields, then attempt to call methods on extending child
        classes based on the field name; e.g. field 'foo' would look for '_foo()` method
        defined on the child class.  The returned value from that method becomes the value
        for the outputted MITAardvark instance.

        Exceptions encountered during normalization will bubble up to the Harvester
        calling context, where it will be handled and recorded as a Record.exception,
        thereby allowing the harvest to continue with other records.
        """
        # get MITAardvark fields
        aardvark_fields = fields(MITAardvark)

        # loop through fields and attempt field-level child class methods if defined
        all_field_values = {}
        for aardvark_field in aardvark_fields:
            if field_method := getattr(self, f"_{aardvark_field.name}", None):
                try:
                    all_field_values[aardvark_field.name] = field_method()
                except Exception as exc:
                    message = (
                        f"Error getting value for field '{aardvark_field.name}': {exc}"
                    )
                    logger.exception(message)
                    raise FieldMethodError(exc, message) from exc

        # dedupe all list fields
        for field_name, field_values in all_field_values.items():
            if isinstance(field_values, list):
                deduped_field_values = [
                    value for value in field_values if value is not None
                ]
                all_field_values[field_name] = dedupe_list_of_values(deduped_field_values)

        # initialize a new MITAardvark instance and return
        return MITAardvark(**all_field_values)

    ####################################
    # Abstract Required Field Methods
    ####################################

    @abstractmethod
    def _dct_accessRights_s(self) -> str:
        pass  # pragma: nocover

    @abstractmethod
    def _dct_title_s(self) -> str | None:
        pass  # pragma: nocover

    @abstractmethod
    def _gbl_resourceClass_sm(self) -> list[str] | None:
        pass  # pragma: nocover

    ####################################
    # Shared Field Methods
    ####################################
    def _id(self) -> str:
        """Shared field method: id

        Construction of origin + identifier.
        """
        return f"{self.origin}:{self.identifier}"

    def _gbl_mdModified_dt(self) -> str:
        """Shared field method: gbl_mdModified_dt"""
        return datetime.datetime.now(tz=datetime.UTC).replace(microsecond=0).isoformat()

    def _gbl_mdVersion_s(self) -> str:
        """Shared field method: gbl_mdVersion_s"""
        return "Aardvark"

    def _dct_references_s(self) -> str:
        """Shared field method: dct_references_s

        Builds a JSON string payload of links for the record.  Work is offloaded to
        methods specific to MIT (self._dct_references_s_mit) and OGM harvests
        (self._dct_references_s_ogm).
        """
        match self.origin:
            case "ogm":
                urls_dict = self._dct_references_s_ogm()
            case "mit":
                urls_dict = self._dct_references_s_mit()
        return json.dumps(urls_dict)

    def _dct_references_s_mit(self) -> dict:
        """Create dct_references_s JSON string for MIT harvests.

        For MIT harvests, this includes the data zip file, source and normalized metadata
        records in CDN, and a link to the TIMDEX item page.
        """
        cdn_folder = {True: "restricted", False: "public"}[self.is_restricted]
        cdn_root = CONFIG.http_cdn_root
        download_urls = [
            {
                "label": "Source Metadata",
                "url": f"{cdn_root}/public/{self.source_metadata_filename}",
            },
            {
                "label": "Aardvark Metadata",
                "url": f"{cdn_root}/public/{self.normalized_metadata_filename}",
            },
            {
                "label": "Data",
                "url": f"{cdn_root}/{cdn_folder}/{self.identifier}.zip",
            },
        ]
        website_url = (
            "https://geodata.libraries.mit.edu/record/"
            f"gismit:{self.identifier.removeprefix('mit:')}"
        )
        return {
            "http://schema.org/downloadUrl": download_urls,
            "http://schema.org/url": website_url,
        }

    def _dct_references_s_ogm(self) -> dict:
        """Create dct_references_s JSON string for OGM harvests.

        For OGM harvests, a single external URL is REQUIRED that points to an item page
        hosted by the external institution, and an OPTIONAL URL that provides a direct
        download.

        While not required for all source classes (e.g. FGDC and ISO), this should be
        overridden by GBL1 and Aardvark which are used for OGM harvests.
        """
        message = (
            "Field method 'dct_references_s' must be overridden by format "
            "specific classes for OGM harvests."
        )
        raise NotImplementedError(message)

    def _schema_provider_s(self) -> str:
        """Shared field method: schema_provider_s

        For MIT harvests, provider is "GIS Lab, MIT Libraries".
        For OGM harvests, provider will come from named defined in OGM configuration YAML.
        """
        match self.origin:
            case "mit":
                return "GIS Lab, MIT Libraries"
            case "ogm":
                return self.ogm_repo_config["name"]

    def _dcat_theme_sm(self) -> list[str]:
        """Shared field method: dcat_theme_sm

        The Aardvark field 'dcat_theme_sm' is designed to be a controlled set of terms
        from this list: https://opengeometadata.org/ogm-aardvark/#theme-values.  The
        shared approach across all metadata formats is to retrieve all values pulled by
        _dct_subject_sm, then extract any subset that matches these terms.
        """
        if not hasattr(self, "_dct_subject_sm"):
            message = (
                "Field method not defined for 'dct_subject_sm', "
                "cannot extract controlled thematic keywords for 'dcat_theme_sm'."
            )
            logger.debug(message)
            return []

        subjects = self._dct_subject_sm()
        if not subjects:
            return []
        theme_list = [
            "agriculture",
            "biology",
            "boundaries",
            "climate",
            "economy",
            "elevation",
            "environment",
            "events",
            "geology",
            "health",
            "imagery",
            "inland waters",
            "land cover",
            "location",
            "military",
            "oceans",
            "property",
            "society",
            "structure",
            "transportation",
            "utilities",
        ]
        return [
            subject.title()
            for subject in subjects
            if subject.lower().strip() in theme_list
        ]

    def _gbl_suppressed_b(self) -> bool:
        """Shared field method: gbl_suppressed_b

        For MITAardvark records, this field is used to indicate if the record should be
        considered deleted, and therefore will be removed from downstream discovery layers
        like TIMDEX.

        The boolean value is determined by the SourceRecord.event:
            - "created" = False (record is not suppressed)
            - "deleted" = True (record is suppressed)
        """
        if self.event == "deleted":
            return True
        return False


@define
class XMLSourceRecord(SourceRecord):
    """Shared parent class for XML based FGDC and ISO19139 source record classes."""

    nsmap: dict = field(default={})
    _root: etree._Element = field(default=None, repr=False)

    @property
    def root(self) -> etree._Element:
        """Property to parse raw xml bytes and return lxml Element.

        This property uses a cached instance at self._root if present to avoid re-parsing
        the XML.
        """
        # lxml note: "Use specific 'len(elem)' or 'elem is not None' test instead."
        if self._root is None:
            self._root = etree.fromstring(self.data)
        return self._root

    def xpath_query(self, xpath_expr: str) -> Any:  # noqa: ANN401
        """Perform XPath query.

        This method automatically includes the namespaces defined for the class.
        """
        return self.root.xpath(xpath_expr, namespaces=self.nsmap)

    @staticmethod
    def remove_whitespace(string: str | None) -> str | None:
        """Removes newlines and excessive whitespace from a string."""
        if string is None:
            return None
        cleaned = " ".join(string.split())
        return cleaned if cleaned else None

    def string_list_from_xpath(self, xpath_expr: str) -> list:
        """Return unique list of strings from XPath matches.

        A list will always be returned, though empty strings and None values will be
        filtered out.  Order will be order discovered via XPath.
        """
        matches = self.xpath_query(xpath_expr)
        strings = [self.remove_whitespace(match.text) for match in matches]
        strings = [string for string in strings if string]
        if all(string is None or string == "" for string in strings):
            return []
        return dedupe_list_of_values(strings)

    def single_string_from_xpath(self, xpath_expr: str) -> str | None:
        """Return single string or None from an Xpath query.

        If the XPath query returns MORE than one textual element, an exception will be
        raised.
        """
        matches = self.xpath_query(xpath_expr)
        if not matches:
            return None
        if len(matches) > 1:
            message = (
                "Expected one or none matches for XPath query, "
                f"but {len(matches)} were found."
            )
            raise ValueError(message)
        return self.remove_whitespace(matches[0].text)


@define
class JSONSourceRecord(SourceRecord):
    """Shared parent class for JSON based GBL1 and Aardvark source record classes."""

    _parsed_data: dict = field(default=None)

    @property
    def parsed_data(self) -> dict:
        """Parse raw JSON string/bytes data and cache to self for future use.

        This property method includes a 'while' loop to handle JSON string data that is
        double encoded, which is the case for some OGM repositories.
        """
        if not self._parsed_data:
            data = self.data
            if isinstance(data, bytes):
                data = data.decode()
            while not isinstance(data, dict):
                data = json.loads(data)
            self._parsed_data = data
        return self._parsed_data

    @property
    def gbl_resource_class_value_map(self) -> dict:
        """Maps values to controlled gbl_resourceClass_sm values for GBL1 and Aardvark.

        https://opengeometadata.org/ogm-aardvark/#resource-class-values
        """
        return {
            "attribute": None,
            "attributeType": None,
            "collectionHardware": None,
            "collectionSession": None,
            "collections": "Collections",
            "dataset": "Datasets",
            "datasets": "Datasets",
            "dimensionGroup": None,
            "feature": None,
            "featureType": None,
            "fieldSession": None,
            "imagery": "Imagery",
            "maps": "Maps",
            "model": None,
            "nonGeographicDataset": None,
            "other": "Other",
            "property": None,
            "series": None,
            "service": None,
            "software": None,
            "tile": None,
            "web services": "Web services",
            "websites": "Websites",
        }
