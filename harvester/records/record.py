"""harvester.harvest.records.record"""
# ruff: noqa: N802, N815; allows camelCase for aardvark fields

import datetime
import json
import logging
from abc import abstractmethod
from typing import Any, Literal

from attrs import asdict, define, field, fields
from attrs.validators import instance_of, optional
from lxml import etree  # type: ignore[import-untyped]

from harvester.aws.sqs import ZipFileEventMessage
from harvester.config import Config
from harvester.records.exceptions import FieldMethodError

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

    identifier: str = field(default=None)
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
    dct_accessRights_s: str = field(default=None, validator=instance_of(str))
    dct_title_s: str = field(default=None, validator=instance_of(str))
    gbl_mdModified_dt: str = field(default=None, validator=instance_of(str))
    gbl_mdVersion_s: str = field(default=None, validator=instance_of(str))
    gbl_resourceClass_sm: list = field(default=None, validator=instance_of(list))
    id: str = field(default=None, validator=instance_of(str))  # noqa: A003

    # additional MIT required fields
    dcat_bbox: str = field(default=None, validator=instance_of(str))
    dct_references_s: str = field(default=None, validator=instance_of(str))
    locn_geometry: str = field(default=None, validator=instance_of(str))

    # optional fields
    dcat_centroid: str | None = field(default=None, validator=optional(instance_of(str)))
    dcat_keyword_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dcat_theme_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_alternative_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_creator_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_description_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_format_s: str | None = field(default=None, validator=optional(instance_of(str)))
    dct_identifier_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_isPartOf_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_isReplacedBy_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_issued_s: str | None = field(default=None, validator=optional(instance_of(str)))
    dct_isVersionOf_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_language_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_license_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_publisher_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_relation_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_replaces_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_rights_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_rightsHolder_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_source_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_spatial_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_subject_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    dct_temporal_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    gbl_dateRange_drsim: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    gbl_displayNote_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    gbl_fileSize_s: str | None = field(default=None, validator=optional(instance_of(str)))
    gbl_georeferenced_b: str | None = field(
        default=None, validator=optional(instance_of(str))
    )
    gbl_indexYear_im: str | None = field(
        default=None, validator=optional(instance_of(str))
    )
    gbl_resourceType_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    gbl_suppressed_b: bool | None = field(
        default=None, validator=optional(instance_of(bool))
    )
    gbl_wxsIdentifier_s: str | None = field(
        default=None, validator=optional(instance_of(str))
    )
    pcdm_memberOf_sm: list | None = field(
        default=None, validator=optional(instance_of(list))
    )
    schema_provider_s: str | None = field(
        default=None, validator=optional(instance_of(str))
    )

    def validate(self) -> None:
        """Validate that Aardvark is compliant for MIT purposes.

        WIP: until JSONSchema work from Jira GDT-49
        """
        raise NotImplementedError  # pragma: nocover

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
    metadata formats.

    Args:
        origin: origin of SourceRecord, with optional colon ":" delimited namespace
            - e.g. "mit", "ogm:stanford"
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
    """

    origin: str = field(default=None)
    identifier: str = field(default=None)
    metadata_format: Literal["fgdc", "iso19139", "gbl1", "aardvark"] = field(default=None)
    data: str | bytes | None = field(default=None, repr=False)
    zip_file_location: str = field(default=None)
    event: Literal["created", "deleted"] = field(default=None)
    sqs_message: ZipFileEventMessage = field(default=None)

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
        field_values = {}
        for aardvark_field in aardvark_fields:
            if field_method := getattr(self, f"_{aardvark_field.name}", None):
                try:
                    field_values[aardvark_field.name] = field_method()
                except Exception as exc:
                    message = (
                        f"Error getting value for field '{aardvark_field.name}': {exc}"
                    )
                    logger.exception(message)
                    raise FieldMethodError(message) from exc

        # initialize a new MITAardvark instance and return
        return MITAardvark(**field_values)

    ####################################
    # Abstract Required Field Methods
    ####################################

    @abstractmethod
    def _dct_accessRights_s(self) -> str:
        pass

    @abstractmethod
    def _dct_title_s(self) -> str | None:
        pass

    @abstractmethod
    def _gbl_resourceClass_sm(self) -> list[str] | None:
        pass

    @abstractmethod
    def _dcat_bbox(self) -> str:
        pass

    @abstractmethod
    def _locn_geometry(self) -> str:
        pass

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
        return datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d")

    def _gbl_mdVersion_s(self) -> str:
        """Shared field method: gbl_mdVersion_s"""
        return "Aardvark"

    def _dct_references_s(self) -> str:
        """Shared field method: dct_references_s

        Builds http URLs for the data zip file (if MIT harvest), source and normalized
        metadata files, in the S3 CDN bucket.
        """
        # determine CDN public or restricted folder
        cdn_folder = {True: "restricted", False: "public"}[self.is_restricted]

        # build URLs payload
        cdn_root = CONFIG.http_cdn_root
        download_urls = [
            {
                "label": "Source Metadata",
                "protocol": "Download",
                "url": f"{cdn_root}/public/{self.source_metadata_filename}",
            },
            {
                "label": "Normalized Metadata",
                "protocol": "Download",
                "url": f"{cdn_root}/public/{self.normalized_metadata_filename}",
            },
        ]
        # add data zip file URL if MIT harvest
        if self.origin == "mit":
            download_urls.append(
                {
                    "label": "Data Zipfile",
                    "protocol": "Download",
                    "url": f"{cdn_root}/{cdn_folder}/{self.identifier}.zip",
                }
            )
        urls_payload = {"https://schema.org/downloadUrl": download_urls}

        return json.dumps(urls_payload)


@define
class XMLSourceRecord(SourceRecord):
    nsmap: dict = field(default={})
    _root: etree._Element = field(default=None, repr=False)  # noqa: SLF001

    @property
    def root(self) -> etree._Element:  # noqa: SLF001
        """Property to parse raw xml bytes and return lxml Element.

        This property uses a cached instance at self._root if present to avoid re-parsing
        the XML.
        """
        # lxml note: "Use specific 'len(elem)' or 'elem is not None' test instead."
        if self._root is None:
            self._root = etree.fromstring(self.data)
        return self._root

    def xpath(self, xpath_expr: str) -> Any:  # noqa: ANN401
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

    def string_list_from_xpath(self, xpath_expr: str) -> list | None:
        """Return unique list of strings from XPath matches.

        Order will be order discovered via XPath.
        """
        matches = self.xpath(xpath_expr)
        strings = [self.remove_whitespace(match.text) for match in matches]
        strings = [string for string in strings if string]
        if all(string is None or string == "" for string in strings):
            return None
        return list(set(strings))


@define
class JSONSourceRecord(SourceRecord):
    """WIP: until OGM records are harvested and then normalized."""
