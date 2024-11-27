"""harvester.harvest.records.record"""

# ruff: noqa: N802, N815

import datetime
import json
import logging
from abc import abstractmethod
from typing import Any, Literal

import marcalyx  # type: ignore[import-untyped]
from attrs import asdict, define, field, fields
from attrs.validators import in_, instance_of
from lxml import etree  # type: ignore[import-untyped]
from marcalyx.marcalyx import DataField, SubField  # type: ignore[import-untyped]

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

type MITAardvarkFieldValue = str | list | bool | None


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
        data: string or bytes of the source file (XML or JSON)
        event: literal string of "created" or "deleted"
    """

    origin: Literal["alma", "mit", "ogm"] = field(validator=in_(["alma", "mit", "ogm"]))
    identifier: str = field(validator=instance_of(str))
    metadata_format: Literal["aardvark", "fgdc", "gbl1", "iso19139", "marc"] = field(
        validator=in_(["aardvark", "fgdc", "gbl1", "iso19139", "marc"])
    )
    data: str | bytes = field(repr=False)
    event: Literal["created", "deleted"] = field(
        default=None, validator=in_(["created", "deleted"])
    )

    @property
    @abstractmethod
    def output_filename_extension(self) -> str:
        """Provide source output filename extension based on metadata format."""

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
        return self.event == "deleted"

    @property
    def is_suppressed(self) -> bool | None:
        """Property to indicate if source record self-identified as suppressed."""
        return False

    def get_controlled_dct_format_s_term(self, value: str | None) -> str | None:
        """Get a single controlled term for dct_format_s from original value.

        If a value is not provided, or does not match a controlled term, this method falls
        back on looking at controlled values from the gbl_resourceType_sm field which may
        indicate the file type (e.g. Vector or Polygon data indicates it is likely a
        Shapefile).
        """
        controlled_value = None

        if value:
            value = value.lower().strip()

            # allow for some variants and similar matches
            # note: order is important; more specific should be first
            if (
                "shapefile" in value
                or value in ("shp", "avshp")
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

            controlled_value = {
                term.lower(): term for term in DCT_FORMAT_S_OGM_TERMS
            }.get(value)

        # if still no controlled format value determined, fallback on looking at
        # controlled resource types that may indicate file format type
        if not controlled_value:
            resource_type_to_format_map = {
                "Polygon data": "Shapefile",
                "Point data": "Shapefile",
                "Line data": "Shapefile",
                "Vector data": "Shapefile",
            }
            for (
                resource_type
            ) in self._gbl_resourceType_sm():  # type: ignore[attr-defined]
                if mapped_value := resource_type_to_format_map.get(resource_type):
                    controlled_value = mapped_value

        return controlled_value

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

        Lastly, values parsed for fields run through a series of post normalization
        quality improvements like removing empty strings, None values from lists, etc.
        """
        # get MITAardvark fields
        aardvark_fields = fields(MITAardvark)

        # loop through fields and attempt field-level child class methods if defined
        all_field_values: dict[str, MITAardvarkFieldValue] = {}
        for aardvark_field in aardvark_fields:
            if field_method := getattr(self, f"_{aardvark_field.name}", None):
                try:
                    all_field_values[aardvark_field.name] = field_method()
                except Exception as exc:
                    message = (
                        f"Error getting value for field '{aardvark_field.name}': {exc}"
                    )
                    logger.debug(message)
                    raise FieldMethodError(exc, message) from exc

        # post normalization quality improvements
        for field_name, original_value in all_field_values.items():
            clean_value = self._remove_none_and_blank_strings(original_value)
            clean_value = self._dedupe_list_fields(clean_value)
            all_field_values[field_name] = clean_value

        # initialize a new MITAardvark instance and return
        return MITAardvark(**all_field_values)  # type: ignore[arg-type]

    @staticmethod
    def _remove_none_and_blank_strings(
        original_value: MITAardvarkFieldValue,
    ) -> MITAardvarkFieldValue:
        """Remove None values and empty strings from MITAardvark field value."""
        if isinstance(original_value, str):
            return None if original_value.strip() == "" else original_value
        if isinstance(original_value, list):
            return [
                value
                for value in original_value
                if value is not None
                and not (isinstance(value, str) and value.strip() == "")
            ]
        return original_value

    @staticmethod
    def _dedupe_list_fields(
        original_value: MITAardvarkFieldValue,
    ) -> MITAardvarkFieldValue:
        """Remove duplicate values from MITAardvark field value list."""
        if isinstance(original_value, list):
            return dedupe_list_of_values(original_value)
        return original_value

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

    @abstractmethod
    def _dct_references_s(self) -> str | None:
        pass  # pragma: nocover

    @abstractmethod
    def _schema_provider_s(self) -> str | None:
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
        return self.event == "deleted"


@define
class XMLSourceRecord(SourceRecord):
    """Parsed XML file type source records."""

    nsmap: dict = field(factory=dict)
    _root: etree._Element = field(default=None, repr=False)

    @property
    def output_filename_extension(self) -> str:
        return "xml"

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
    """Parsed JSON file type source records."""

    _parsed_data: dict = field(default=None)

    @property
    def output_filename_extension(self) -> str:
        return "json"  # pragma: nocover

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


@define
class MarcalyxSourceRecord(XMLSourceRecord):
    """Parsed MARC XML file type source records."""

    marc: marcalyx.Record = field(default=None)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        if self.marc is None:
            marc_record_element = etree.fromstring(self.data)
            self.marc = marcalyx.Record(marc_record_element)

    def get_single_tag(self, tag: str) -> DataField | None:
        """Return a single tag if only one instance of that tag number exists."""
        tags = self.marc.field(tag)
        if len(tags) == 1:
            return tags[0]
        if len(tags) > 1:
            message = f"Multiple tags found in MARC record for tag: {tag}"
            raise ValueError(message)
        return None

    @staticmethod
    def get_single_subfield(tag: DataField, subfield: str) -> SubField | None:
        """Return a single subfield if only one instance of that subfield exists."""
        subfields = tag.subfield(subfield)
        if len(subfields) == 1:
            return subfields[0]
        if len(subfields) > 1:
            message = f"Multiple subfields found in tag for subfield: {subfield}"
            raise ValueError(message)
        return None

    def get_single_tag_subfield_value(self, tag_code: str, subfield_code: str) -> str:
        """Return single subfield value from a single tag."""
        tag = self.get_single_tag(tag_code)
        if not tag:
            message = f"Record has no instances of tag '{tag_code}'"
            raise ValueError(message)
        subfield = self.get_single_subfield(tag, subfield_code)
        if not subfield:
            message = f"Tag does not have single instance of subfield '{subfield_code}'"
            raise ValueError(message)
        return subfield.value.strip()

    def get_multiple_tag_subfield_values(
        self,
        tag_and_subfields: list[tuple[str, str]],
        concat: bool = False,  # noqa: FBT001, FBT002
        separator: str = " ",
    ) -> list[str]:
        """Return list of strings from combinations of tags and subfields.

        This method allows for tags and/or subfields to repeat, returning all values.
        Additionally, this method will not alert if tags or subfield combinations do not
        exist.

        Args:
            tag_and_subfields: list of tag and allowed subfields[str], e.g.
                - [("245", "a"), ("994", "ab")]
            concat: if True, all values for tag will be concatenated with seperator
            separator: character used for concatenation
        """
        values = []
        for tag_code, subfield_codes in tag_and_subfields:
            for tag in self.marc.field(tag_code):
                subfield_values = []
                for subfield_code in subfield_codes:
                    for subfield in tag.subfield(subfield_code):
                        subfield_values.append(subfield.value)  # noqa: PERF401
                if concat:
                    values.append(separator.join(subfield_values))
                else:
                    values.extend(subfield_values)
        return values
