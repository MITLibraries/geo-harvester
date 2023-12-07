"""harvester.harvest.records.record"""
# ruff: noqa: N815; allows camelCase for aardvark fields

import json
import logging
from typing import Any, Literal

from attrs import asdict, define, field, fields
from attrs.validators import instance_of, optional
from lxml import etree  # type: ignore[import-untyped]

from harvester.records.exceptions import FieldMethodError

logger = logging.getLogger(__name__)


@define
class Record:
    """Class to represent a record in both its 'source' and 'normalized' form.

    Args:
        identifier: unique identifier determined for the record
            - for MIT records, this comes from the base name of the zip file
            - for OGM records, this likely will come from the metadata itself
        source_record: instance of SourceRecord
        normalized_record: instance of MITAardvark
        error_message: string of error encountered during harvest
        error_stage: what part of the harvest pipeline was the error encountered
    """

    identifier: str = field()
    source_record: "SourceRecord" = field()
    normalized_record: "MITAardvark" = field(default=None)
    error_message: str = field(default=None)
    error_stage: str = field(default=None)


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
    """

    metadata_format: Literal["fgdc", "iso19139", "gbl1", "aardvark"] = field(default=None)
    data: str | bytes | None = field(default=None, repr=False)
    zip_file_location: str = field(default=None)
    event: Literal["created", "deleted"] = field(default=None)

    def normalize(self) -> MITAardvark | None:
        """Method to normalize a SourceRecord to an MIT Aardvark MITAardvark instance.

        This is the entrypoint for normalization.  This method will look to MITAardvark
        for a list of Aardvark fields, then attempt to call methods on extending child
        classes based on the field name; e.g. field 'foo' would look for '_foo()` method
        defined on the child class.  The returned value from that method becomes the value
        for the outputted MITAardvark instance.

        Exceptions encountered during normalization will bubble up to the Harvester
        calling context, where it will be handled and recorded as a Record.error, thereby
        allowing the harvest to continue with other records.
        """
        # get MITAardvark fields
        aardvark_fields = fields(MITAardvark)

        # loop through fields and attempt field-level child class methods if defined
        field_values = {}
        for aardvark_field in aardvark_fields:
            if field_func := getattr(self, f"_{aardvark_field.name}", None):
                try:
                    field_values[aardvark_field.name] = field_func()
                except Exception as exc:
                    message = (
                        f"Error getting value for field '{aardvark_field.name}': {exc}"
                    )
                    logger.exception(message)
                    raise FieldMethodError(message) from exc

        # initialize a new MITAardvark instance and return
        return MITAardvark(**field_values)


class DeletedSourceRecord(SourceRecord):
    """Class to represent a SourceRecord that has been deleted."""

    event: Literal["deleted"] = field(default="deleted")

    def normalize(self) -> None:
        message = "Cannot normalize a DeletedSourceRecord, no data to normalize."
        raise RuntimeError(message)


@define
class XMLSourceRecord(SourceRecord):
    nsmap: dict = field(default=None)
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
